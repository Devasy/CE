"""Provides configuration related endpoints."""
import os
import traceback
from typing import Optional

from fastapi import APIRouter, HTTPException, Security
from fastapi.responses import FileResponse

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import DBConnector, Logger, Scheduler, SecretDict, Collections
from netskope.common.utils.plugin_helper import PluginHelper

from ..models import (
    ConfigurationDB,
    EDMSanitizationConfigurationIn,
    EDMSanitizationConfigurationOut,
    EDMSanitizedDataType,
    EDMSanitizedFileType,
    EDMSanitizedSourceType,
)
from ..utils import FILE_PATH, MANUAL_UPLOAD_PATH

router = APIRouter()
scheduler = Scheduler()
plugin_helper = PluginHelper()
logger = Logger()
db_connector = DBConnector()


@router.post(
    "/edm_sanitization/{plugin_id}",
    response_model=EDMSanitizationConfigurationOut,
    tags=["EDM Sanitization"],
    description="Perform Sanitization on Plugin data and return .good and .bad files.",
)
async def post_sanitization(
    sanitization_config: EDMSanitizationConfigurationIn,
    plugin_id,
    _: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Sanitizes file and store it."""
    PluginClass = plugin_helper.find_by_id(plugin_id)  # NOSONAR S117
    if PluginClass is None:
        raise HTTPException(400, f"Plugin with id='{plugin_id}' does not exist.")

    configuration = db_connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
        {"name": sanitization_config.name}
    )
    if configuration:
        configuration = ConfigurationDB(**configuration)

    plugin = PluginClass(
        sanitization_config.name,
        SecretDict(sanitization_config.parameters),
        configuration.storage if configuration else {},
        None,
        logger,
        plugin_type=sanitization_config.pluginType,
    )
    validation_result = plugin.validate(SecretDict(sanitization_config.parameters))
    if validation_result.success is False:
        raise HTTPException(
            400,
            f"One of the configuration parameter is invalid. "
            f"{validation_result.message}",
        )

    try:
        input_path = f"{FILE_PATH}/{sanitization_config.name}/sample.csv"
        output_path = f"{FILE_PATH}/{sanitization_config.name}"
        plugin.storage.update({
            **(plugin.storage or {}),
            "csv_path": input_path,
            "sanitization_data_path": output_path
        })
        plugin.sanitize(file_name="sample", sample_data=True)
        if configuration:
            db_connector.collection(Collections.EDM_CONFIGURATIONS).update_one(
                {"name": sanitization_config.name},
                {"$set": {"storage": plugin.storage}},
            )
        return {
            "name": sanitization_config.name,
            "sanitizationStatus": True,
            "message": "Sanitization Done Successfully",
        }
    except Exception as err:
        logger.error(
            "Error occurred while Sanitizing Data.",
            details=traceback.format_exc(),
            error_code="EDM_1017",
        )
        raise HTTPException(500, "Error occurred while Sanitizing Data.") from err


@router.get(
    "/edm_sanitization",
    response_class=FileResponse,
    tags=["EDM Sanitization"],
    description="Store Sanitize data and return good and bad files.",
)
async def get_sanitization(
    name: str,
    fileName: Optional[str] = None,
    file_type: EDMSanitizedFileType = EDMSanitizedFileType.GOOD,
    data_type: EDMSanitizedDataType = EDMSanitizedDataType.SAMPLE,
    source_type: EDMSanitizedSourceType = EDMSanitizedSourceType.PLUGIN,
    _: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Send Sanitized Data in response.

    Args:
        name (str): Configuration Name
        file_type (EDMSanitizedFileType, optional): Sanitized
                File type. Defaults to EDMSanitizedFileType.GOOD.
        data_type ()

    Returns:
        requested file content
    """
    try:
        if source_type == EDMSanitizedSourceType.PLUGIN:
            file_location = f"{FILE_PATH}/{name}/{data_type.value}.{file_type.value}"
        elif source_type == EDMSanitizedSourceType.MANUAL_UPLOAD:
            if not fileName:
                raise HTTPException(400, "file name is required for manual upload configurations.")
            fileName = os.path.splitext(fileName)[0]
            if data_type == EDMSanitizedDataType.SAMPLE:
                fileName = f"sample_{fileName}"
            file_location = f"{MANUAL_UPLOAD_PATH}/{name}/{fileName}.{file_type.value}"

        file_name = f"{data_type.value}.{file_type.value}"
        if not os.path.exists(file_location):
            raise HTTPException(400, f"Error occurred While fetching sanitized data. {file_location}")
        return FileResponse(path=file_location, filename=file_name, status_code=200)
    except HTTPException as err:
        logger.debug(str(err))
        raise err
    except Exception as err:
        logger.error(
            "Error occurred While fetching sanitized data.",
            details=traceback.format_exc(),
            error_code="EDM_1018",
        )
        raise HTTPException(
            500, "Error occurred While fetching sanitized data."
        ) from err
