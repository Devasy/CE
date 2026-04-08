"""Provides manual upload related endpoints."""

import os
import shutil
import traceback
from datetime import datetime, UTC
from typing import Annotated, Optional

from fastapi import APIRouter, File, HTTPException, Security, UploadFile

from netskope.common.api.routers.auth import get_current_user
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger, Scheduler
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.integrations.cfc.models import (
    FileStatus,
    ManualUploadConfigurationDB,
    ManualUploadConfigurationIn,
    ManualUploadConfigurationOut,
    ManualUploadConfigurationUpdateIn,
    StatusType,
)
from netskope.integrations.cfc.tasks.manual_upload_task import manual_upload_task
from netskope.integrations.cfc.utils.cfc_file_utils import CFCFileUtils
from netskope.integrations.cfc.utils import (
    IMAGE_EXTENSION_SUPPORTED,
    MANUAL_UPLOAD_PATH,
    MANUAL_UPLOAD_TASK_DELAY_TIME,
    MANUAL_UPLOAD_PREFIX,
)
from netskope.integrations import trim_space_parameters_fields
from pymongo import ReturnDocument

router = APIRouter(prefix="/manual_upload")
scheduler = Scheduler()
plugin_helper = PluginHelper()
logger = Logger()
connector = DBConnector()


def _create_directory(dir_path):
    """Create a directory at the specified path, including all necessary parent directories.

    Args:
        dir_path (string): The path of the directory to be created.

    Raises:
        OSError: If there's an issue creating the directory.

    """
    try:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    except Exception as error:
        logger.error(
            message=(
                f"{MANUAL_UPLOAD_PREFIX} Hash generation - "
                "Error occurred while creating nested directories to store sanitized data."
            ),
            details=traceback.format_exc(),
        )
        raise error


@router.post("/configuration", tags=["CFC Manual Upload"])
async def create_manual_upload_configuration(
    configuration: ManualUploadConfigurationIn,
    _: User = Security(get_current_user, scopes=["cfc_write"]),
) -> ManualUploadConfigurationOut:
    """Create manual upload configuration.

    Args:
        configuration (ConfigurationIn): The configuration input.
        user (User, optional): The user security object.

    Raises:
        HTTPException: If an error occurs during the creation process.

    Returns:
        ConfigurationOut: The configuration output.
    """
    try:
        if connector.collection(Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS).find_one(
            {"name": configuration.name}
        ):
            raise HTTPException(
                400,
                "Provided configuration name already exist.",
            )
        files = []
        for file in configuration.files:
            files.append(
                FileStatus(
                    file_name=file,
                    status=StatusType.PENDING,
                    updatedAt=datetime.now(UTC),
                )
            )
        config = ManualUploadConfigurationDB(
            files=files, errorState={}, **(configuration.model_dump(exclude=["files"]))
        )
        config_id = (
            connector.collection(Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS)
            .insert_one(config.model_dump())
            .inserted_id
        )

        execute_celery_task(
            manual_upload_task.apply_async,
            "cfc.manual_upload_task",
            args=[str(config_id), configuration.name],
            countdown=MANUAL_UPLOAD_TASK_DELAY_TIME,
        )
        return ManualUploadConfigurationOut(**config.model_dump())
    except HTTPException as error:
        logger.debug(str(error))
        raise error
    except Exception:
        logger.debug(
            "Error occurred  while Creating Manual Upload configuration.",
            details=traceback.format_exc(),
            error_code="CFC_1041",
        )
        raise HTTPException(
            400, "Error occurred  while Creating Manual Upload configuration."
        )


@router.patch("/configuration", tags=["CFC Manual Upload"])
async def update_manual_upload_configuration(
    configuration: ManualUploadConfigurationUpdateIn,
    _: User = Security(get_current_user, scopes=["cfc_write"]),
) -> ManualUploadConfigurationOut:
    """Update manual upload configuration.

    Args:
    configuration (ManualUploadConfigurationUpdateIn): The configuration input.
    user (User, optional): The user security object.

    Raises:
    HTTPException: If an error occurs during the creation process.

    Returns:
    ConfigurationOut: The configuration output.
    """
    try:
        # to trim extra spaces for parameters fields.
        trim_space_parameters_fields(configuration)
        update_payload = configuration.model_dump(exclude_unset=True)
        existing_config_dict = connector.collection(
            Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
        ).find_one({"name": configuration.name})

        if not existing_config_dict:
            raise HTTPException(
                status_code=404,
                detail=f"Manual Upload Configuration '{configuration.name}' not found.",
            )
        update_payload["updatedAt"] = datetime.now(UTC)
        updated_config_from_db = connector.collection(
            Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
        ).find_one_and_update(
            {"name": configuration.name},
            {"$set": update_payload},
            return_document=ReturnDocument.AFTER,
        )

        if not updated_config_from_db:
            raise HTTPException(
                500, "Error occurred while updating Manual Upload configuration."
            )

        updated_configuration = ManualUploadConfigurationDB(**updated_config_from_db)

        return ManualUploadConfigurationOut(**updated_configuration.model_dump())

    except HTTPException as error:
        logger.debug(str(error))
        raise error
    except Exception:
        logger.debug(
            "Error occurred  while Updating Manual Upload configuration.",
            details=traceback.format_exc(),
            error_code="CFC_1042",
        )
        raise HTTPException(
            400, "Error occurred  while Updating Manual Upload configuration."
        )


@router.post(
    "/upload",
    tags=["CFC Manual Upload"],
    description="POST Manual Upload",
)
async def upload_files(
    fileName: str,
    configurationName: str,
    fileType: str,
    lastModified: int,
    size: int,
    file: Annotated[UploadFile, File()],
    path: Optional[str] = "",
    _: User = Security(get_current_user, scopes=["cfc_write"]),
):
    """Store Uploaded file and extract files from zip.

    Args:
        files (Annotated[List[UploadFile], File, optional): The list of uploaded files.
        user (User, optional): The user security object.

    Raises:
        HTTPException: If an error occurs during the creation process.

    Returns:
        dict: The dictionary of extracted files.
    """
    try:
        if os.path.splitext(fileName)[1] not in IMAGE_EXTENSION_SUPPORTED:
            raise HTTPException(
                400,
                "Invalid file type supported.",
            )
        local_path = f"{MANUAL_UPLOAD_PATH}/{configurationName}"
        unique_file_name = fileName
        if path:
            local_path += f"/{os.path.dirname(path).replace('/', '_')}"
            unique_file_name = path
        else:
            path = ""
        file_path = f"{local_path}/{fileName}"

        config = connector.collection(
            Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
        ).find_one({"name": configurationName, "files.file_name": unique_file_name})
        if config is None:
            raise HTTPException(
                400,
                "Provided file doesn't exists in the configuration or configuration with provided name doesn't exists.",
            )
        files: list = config["files"]

        for single_file in files:
            if single_file["file_name"] == unique_file_name:
                if single_file["status"] != StatusType.PENDING:
                    raise HTTPException(
                        400,
                        f"File: {file_path} already exists in the configuration.",
                    )

        destination_config = connector.collection(
            Collections.CFC_CONFIGURATIONS
        ).find_one({"name": config.get("destinationConfiguration")})

        destination = destination_config.get("name")
        classifierID = config.get("classifierID")
        classifierName = config.get("classifierName")
        trainingType = config.get("trainingType")

        CFCFileUtils.add_image_data(
            connector=connector,
            file_name=fileName,
            source=configurationName,
            source_id=str(config["_id"]),
            sourceType="manual",
            size=size,
            last_modified=lastModified,
            extension=fileType,
            path=path,
            sharedWith=[
                {
                    "destinationPlugin": destination,
                    "classifierID": classifierID,
                    "classifierName": classifierName,
                    "trainingType": trainingType,
                    "status": StatusType.PENDING,
                    "lastShared": None,
                }
            ],
        )
        imageId = (
            connector.collection(Collections.CFC_IMAGES_METADATA)
            .find_one(
                {"file": fileName, "sourcePlugin": configurationName, "path": path},
                {"fields": {"_id": 1}},
            )
            .get("_id")
        )

        _create_directory(local_path)
        with open(file_path, "wb") as file_obj:
            shutil.copyfileobj(file.file, file_obj)

        CFCFileUtils.update_file_status(
            configuration_name=configurationName,
            file_name=unique_file_name,
            id=imageId,
            destination_plugin_name=destination,
            classifier_id=classifierID,
            classifier_name=classifierName,
            training_type=trainingType,
            status=StatusType.SCHEDULED,
        )

        connector.collection(Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS).update_one(
            {"name": configurationName},
            {"$set": {"lastUploadTime": datetime.now(UTC)}},
        )

        return {
            "status": "success",
            "message": f"{fileName} is uploaded successfully for configuration {configurationName}",
        }

    except HTTPException as error:
        logger.debug(str(error))
        raise error
    except Exception:
        logger.debug(
            "Error occurred  while storing Manual Upload file.",
            details=traceback.format_exc(),
            error_code="CFC_1043",
        )
        raise HTTPException(500, "Error occurred while storing Manual Upload file.")
