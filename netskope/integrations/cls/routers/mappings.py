"""Apis for cls mapping files."""

from fastapi import APIRouter, Security, HTTPException
import traceback
from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
    PluginHelper,
)

from netskope.integrations.cls.models.mappings import (
    MappingIn,
    MappingOut,
    MappingFileDelete,
    MappingFileUpdate,
)

router = APIRouter()
connector = DBConnector()
logger = Logger()
helper = PluginHelper()

transformation = [
    "Time Stamp",
    "Integer",
    "Floating Point",
    "IPv4 Address",
    "IPv6 address",
    "MAC Address",
    "IP Address",
    "String",
    "String31",
    "String40",
    "String63",
    "String100",
    "String128",
    "String200",
    "String255",
    "String1023",
    "String2048",
    "String4000",
    "String8000",
]


def validate_mapping_file(jsonfile: MappingFileUpdate):
    """Validate the mapping file for all the configured plugins.

    Args:
        jsonfile (MappingFileUpdate): Mapping file and metadata.

    Raises:
        validation_exception: Exception raised for mapping validation error.
    """
    # Validate the mapping file if its used in any of the configured plugin.
    configurations = connector.collection(Collections.CLS_CONFIGURATIONS).find(
        {"attributeMapping": jsonfile.name, "attributeMappingRepo": jsonfile.repo}
    )
    for config in configurations:
        PluginClass = helper.find_by_id(config.get("plugin"))  # NOSONAR S117
        plugin = PluginClass(
            config.get("name"),
            None,
            None,
            None,
            logger,
            mappings=jsonfile.model_dump(),
        )
        validation_result = plugin.validate_mappings()
        if validation_result and not validation_result.success:
            logger.error(
                message=f"Error occurred while validating mapping for configuration {config.get('name')}.",
                details=traceback.format_exc(),
            )
            message = (
                validation_result.message
                or f"Error occurred while validating mappings for configuration {config.get('name')}."
            )
            raise ValueError(message)
        else:
            logger.debug(message=f"Skipped mapping validation for {config['name']}.")


@router.get("/mapping", tags=["CLS Mapping Files"])
async def list_mapping_files(
    user: User = Security(get_current_user, scopes=["cls_read"]),
):
    """List all mapping files."""
    out = []
    for mapping_files in connector.collection(Collections.CLS_MAPPING_FILES).find({}):
        mapping = MappingOut(**mapping_files)
        out.append(mapping)
    return out


@router.post("/mapping", tags=["CLS Mapping Files"])
async def create_mapping_file(
    jsonfile: MappingIn,
    user: User = Security(get_current_user, scopes=["cls_write"]),
):
    """Create Mapping File."""
    # Validate the mapping file if its attached to any of the configured plugin.
    try:
        validate_mapping_file(jsonfile)
    except Exception as e:
        logger.error(
            message="Error occurred while validating mapping for configurations.",
            details=traceback.format_exc(),
        )
        raise HTTPException(400, str(e))

    connector.collection(Collections.CLS_MAPPING_FILES).insert_one(
        jsonfile.model_dump()
    )
    logger.debug(f"CLS mapping file with name '{jsonfile.name}' created.")
    return {"success": True}


@router.delete("/mapping", tags=["CLS Mapping Files"])
async def delete_mapping_file(
    jsonfile: MappingFileDelete,
    user: User = Security(get_current_user, scopes=["cls_write"]),
):
    """Delete mapping file."""
    if (
        connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
            {"attributeMapping": jsonfile.name, "attributeMappingRepo": jsonfile.repo}
        )
        is not None
    ):
        raise HTTPException(
            400, "This mapping file is in use by one of the configurations."
        )
    json_file = connector.collection(Collections.CLS_MAPPING_FILES).find_one(
        {"name": jsonfile.name, "repo": jsonfile.repo}
    )
    if json_file.get("isDefault", False) is False:
        connector.collection(Collections.CLS_MAPPING_FILES).delete_one(
            {"name": jsonfile.name, "repo": jsonfile.repo}
        )
        logger.debug(f"CLS mapping file with name '{jsonfile.name}' deleted.")
        return {"success": True}
    else:
        raise HTTPException(400, "Default mapping file can not be deleted.")


@router.patch("/mapping", tags=["CLS Mapping Files"])
async def update_mapping_file(
    jsonfile: MappingFileUpdate,
    user: User = Security(get_current_user, scopes=["cls_write"]),
):
    """Update mapping files."""
    json_file = connector.collection(Collections.CLS_MAPPING_FILES).find_one(
        {"name": jsonfile.name, "repo": jsonfile.repo}
    )
    if json_file.get("isDefault", False) is False:
        # Validate the mapping file if its attached to any of the configured plugin.
        try:
            validate_mapping_file(jsonfile)
        except Exception as e:
            logger.error(
                message="Error occurred while validating mapping for configurations.",
                details=traceback.format_exc(),
            )
            raise HTTPException(400, str(e))
        connector.collection(Collections.CLS_MAPPING_FILES).update_one(
            {"name": jsonfile.name, "repo": jsonfile.repo},
            {
                "$set": {
                    "jsonData": jsonfile.jsonData,
                    "showWizard": jsonfile.showWizard,
                    "formatOptionsMapping": jsonfile.formatOptionsMapping,
                }
            },
        )
        logger.debug(f"CLS mapping file with name '{jsonfile.name}' updated.")
        return {"success": True}
    else:
        raise HTTPException(400, "Default mapping file can not be updated.")


@router.get("/transformationFields", tags=["CLS Mapping Files"])
def get_transformation_field():
    """Get tranformation field."""
    return transformation
