"""Provides CSV upload related models."""
import os
from datetime import datetime, UTC
from typing import Dict, List, Optional

from pydantic import (
    BaseModel,
    Field,
    FieldValidationInfo,
    StringConstraints,
    field_validator,
)
from typing_extensions import Annotated

from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    SecretDict,
)
from netskope.integrations.edm.utils import MANUAL_UPLOAD_PATH

from .business_rule import Action
from .plugin import ConfigurationDB
from .task_status import StatusType

connector = DBConnector()
helper = PluginHelper()
logger = Logger()


@classmethod
def validate_csv_name(cls, v, info: FieldValidationInfo, **kwargs):
    """Validate csv name."""
    if connector.collection(Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS).find_one(
        {"name": v}
    ):
        raise ValueError("Unfortunately, request is failed. Please try again.")
    if "fileName" not in info.data:
        raise ValueError("fileName is required to validate name.")
    csv_name = info.data["fileName"]
    csv_path = f"{MANUAL_UPLOAD_PATH}/{v}/{csv_name}"
    if not os.path.isfile(csv_path):
        raise ValueError("Provided csv file name does not exists. Upload csv file.")
    return v


@classmethod
def validate_share_with(cls, v):
    """Validate that shareWith is valid."""
    dest_config_name = list(v.keys())[0]
    actions = v.get(dest_config_name, [])
    if not actions:
        raise ValueError("Action is the required field.")
    if (
        actions
        and connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
            {"name": dest_config_name}
        )
        is None
    ):
        raise ValueError(f"EDM configuration {dest_config_name} does not exist.")

    dest_config = ConfigurationDB(
        **connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
            {"name": dest_config_name}
        )
    )
    PluginClass = helper.find_by_id(dest_config.plugin)
    if not PluginClass:
        raise ValueError(f"Plugin with id='{dest_config.plugin}' does not exist.")
    metadata = PluginClass.metadata
    if not metadata.get("push_supported"):
        raise ValueError(
            f"Data push is not supported for configuration {dest_config_name}"
        )
    if (
        PluginHelper.check_plugin_name_with_regex(
            "netskope_edm_forwarder_receiver",
            dest_config.plugin
        )
        and not dest_config.pluginType == "forwarder"
    ):
        raise ValueError(
            f"Data push is not supported for configuration {dest_config_name}."
        )

    plugin = PluginClass(
        dest_config.name,
        SecretDict(dest_config.parameters),
        dest_config.storage or {},
        dest_config.checkpoint,
        logger,
    )
    for action in actions:
        result = plugin.validate_action(action)
        if not result.success:
            raise ValueError(result.message)
    return v


class ManualUploadSanitizationConfigurationIn(BaseModel):
    """Incoming manual upload configuration for sanitization."""

    fileName: Annotated[str, StringConstraints(min_length=1, max_length=215)] = Field(
        ..., description="CSV file for which sanitization is being performed."
    )
    name: str = Field(
        ...,
        description=("Name of the plugin configuration "),
    )
    parameters: dict = Field(
        ..., description="Parameters of the plugin or manual CSV upload."
    )
    _validate_name = field_validator("name")(validate_csv_name)


class ManualUploadSanitizationConfigurationOut(BaseModel):
    """Outgoing manual upload configuration for sanitization."""

    name: str = Field(..., description="Name of configuring Plugin.")
    sanitizationStatus: bool = Field(..., description="Sanitization Status")
    message: str = Field(..., description="Sanitization message")


class ManualUploadConfigurationIn(BaseModel):
    """The incoming configuration model for csv upload."""

    fileName: Annotated[str, StringConstraints(min_length=1, max_length=215)] = Field(
        ..., description="CSV file for which EDM Hashing is being performed."
    )
    name: str = Field(...)
    parameters: dict = Field(
        ..., description="Parameters of the plugin or manual CSV upload."
    )
    sharedWith: Dict[str, List[Action]] = Field(None)

    createdAt: Optional[datetime] = Field(None, validate_default=True)
    updatedAt: Optional[datetime] = Field(None, validate_default=True)

    @field_validator("createdAt", "updatedAt")
    @classmethod
    def _datetime_now_validator(cls, value):
        return datetime.now(UTC)

    status: StatusType = Field(StatusType.SCHEDULED)
    sharedAt: datetime = Field(None)

    _validate_name = field_validator("name")(validate_csv_name)
    _validate_sharedWith = field_validator("sharedWith")(validate_share_with)


class ManualUploadConfigurationDB(BaseModel):
    """CSV Upload configuration database model."""

    fileName: str = Field(...)
    name: str = Field(...)
    sharedWith: Dict[str, List[Action]] = Field(None)
    parameters: dict = Field(...)
    storage: dict = Field({})
    lastRunAt: Optional[datetime] = Field(None)
    lastRunSuccess: Optional[bool] = Field(None)
    status: Optional[StatusType] = Field(None)
    sharedAt: Optional[datetime] = Field(None)
    updatedAt: datetime
    createdAt: datetime
