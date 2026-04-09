"""Provides NCE upload related endpoints."""
from datetime import datetime, UTC
import os
import shutil
import traceback
import zipfile
from io import BytesIO
from typing import Annotated
from uuid import uuid4
from zipfile import BadZipFile

from fastapi import (APIRouter, Depends, File, HTTPException, Security,
                     UploadFile)
from starlette.responses import JSONResponse

from netskope.common.api.routers.auth import get_current_user
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger

from ..models import NCEUpload, EDMStatistics
from ..tasks.share_data import share_data
from ..utils import UPLOAD_PATH, increment_count

router = APIRouter(prefix="/nce_upload", tags=['Netskope EDM Forwarder/Receiver'])
logger = Logger()
db_connector = DBConnector()


def read_file(file, chunk=1024):
    """Read file in chunks."""
    data = file.read(chunk)
    while data:
        yield data
        data = file.read(chunk)


def share_receiver_edm_hashes(hash_dict, source_config_name):
    """Share received edm hashes."""
    execute_celery_task(
        share_data.apply_async,
        "edm.share_data",
        args=[source_config_name],
        kwargs={"hash_dict": hash_dict},
    )


def _get_storage(name: str):
    return db_connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
        {"name": name}, {"storage": True}
    )


def _update_storage(name: str, storage: dict):
    latest_storage = _get_storage(name).get("storage", {}) or {}
    latest_storage.update(storage)
    db_connector.collection(Collections.EDM_CONFIGURATIONS).update_one(
        {"name": name}, {"$set": {"storage": latest_storage}}
    )


@router.post('/')
async def upload_edm(
    file: Annotated[UploadFile, File()],
    parameters: NCEUpload = Depends(NCEUpload),
    _: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Store uploaded EDM at valid denstination.

    Args:
        file (UploadFile): zip file containing EDM Hash
        parameters (NCEUpload): paramters to decide destination and source of EDM hash

    Raises:
        HTTPException: exception indication unusual and invalid request
    """
    try:
        # validating file
        if file:
            try:
                zipfile.ZipFile(file.file)
                file.file.seek(0)
            except BadZipFile:
                raise HTTPException(status_code=400, detail="Only zip file supported")
        # validating source
        ce_identifier = parameters.ce_identifier
        # retrieving destination configuration
        configuration_db_dict = db_connector.collection(
            Collections.EDM_CONFIGURATIONS
        ).find_one({"name": parameters.destination})
        storage = _get_storage(configuration_db_dict.get("name"))
        if storage:
            storage = storage["storage"]
        if not ce_identifier:
            # Generating new CE Identifier
            ce_identifier = f"{os.path.splitext(parameters.file_name)[0]}_{str(uuid4())}"

            # adding ce identifier in storage dict
            storage.setdefault(ce_identifier, {})["file_name"] = parameters.file_name

            # updating storage with new ce identifier
            _update_storage(
                name=configuration_db_dict.get("name"),
                storage=storage
            )

        # Defining destination folder
        destination_folder = f'{UPLOAD_PATH}/{parameters.destination}/{ce_identifier}'
        storage["edm_hash_folder"] = os.path.dirname(destination_folder)
        if os.path.isdir(destination_folder):
            shutil.rmtree(destination_folder)
        os.makedirs(destination_folder)
        if file:
            # Create a BytesIO object from the binary content
            zip_buffer = BytesIO()
            # shutil.copyfileobj(file, zip_buffer)
            for data in read_file(file.file):
                zip_buffer.write(data)

            # Create a ZipFile object using the BytesIO buffer
            with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
                zip_ref.extractall(destination_folder)
            storage[ce_identifier]["edm_hash_folder"] = destination_folder
            storage[ce_identifier]["edm_hashes_cfg"] = f"{destination_folder}/{parameters.edm_hashes_cfg}"
            # storage["edm_hash_available"] = True
            storage[ce_identifier]["last_received_time"] = datetime.now(UTC)
            _update_storage(
                name=configuration_db_dict.get("name"),
                storage=storage
            )
            share_receiver_edm_hashes(
                hash_dict=storage[ce_identifier],
                source_config_name=configuration_db_dict["name"]
            )
            # Update EDM Statistics
            increment_count(EDMStatistics.RECEIVED_HASHES_COUNT.value, 1)

        return JSONResponse(
            content={
                "ce_identifier": ce_identifier
            }
        )
    except HTTPException as error:
        raise error
    except Exception as error:
        logger.error(
            "Error occurred While storing edm hashes.",
            details=traceback.format_exc(),
            error_code="EDM_1019",
        )
        raise HTTPException(500, "Error occurred While storing uploaded csv.") from error
