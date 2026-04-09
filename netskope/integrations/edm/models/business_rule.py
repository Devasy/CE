"""Business rule related schemas."""
from datetime import datetime, UTC
from typing import Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, FieldValidationInfo, field_validator

from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    SecretDict,
)

from .plugin import ConfigurationDB
from .task_status import StatusType

connector = DBConnector()
helper = PluginHelper()
logger = Logger()


@classmethod
def validate_sharedWith(
    cls, v: Dict[str, Dict[str, List[str]]], info: FieldValidationInfo
):
    """Validate action configurations exist."""
    if not v:
        raise ValueError("Atleast one sharing configuration is required")
    if "name" not in info.data:
        raise ValueError("name is required.")
    # This validation is only required for EDM module as of now
    # as we not using business rule for any other purpose than storing sharing.
    if len(list(v.keys())) > 1:
        raise ValueError("Only one source configuration can be added in one sharing.")
    for source, dest_dict in v.items():
        src_config = connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
            {"name": source}
        )
        if not src_config:
            raise ValueError(f"EDM configuration {source} does not exist.")
        src_config = ConfigurationDB(**src_config)
        PluginClass = helper.find_by_id(src_config.plugin)
        if not PluginClass:
            raise ValueError(f"Plugin with id='{src_config.plugin}' does not exist.")
        metadata = PluginClass.metadata
        if (
            PluginHelper.check_plugin_name_with_regex(
                "netskope_edm_forwarder_receiver",
                src_config.plugin
            )
            and src_config.pluginType != "receiver"
        ):
            raise ValueError(f"Data pull is not supported for configuration {source}.")
        if not metadata.get("pull_supported"):
            raise ValueError(f"Data pull is not supported for configuration {source}.")
        if not dest_dict:
            raise ValueError("Atleast one destination configuration is required.")
        if len(list(dest_dict.keys())) > 1:
            raise ValueError(
                "Only one destination configuration can be added with one sharing."
            )
        dest_config_name = list(dest_dict.keys())[0]
        if source == dest_config_name:
            raise ValueError("Destination configuration can not be same as source.")
        rules = connector.collection(Collections.EDM_BUSINESS_RULES).find()
        for rule in rules:
            if source in rule["sharedWith"]:
                if dest_config_name in rule["sharedWith"][source]:
                    raise ValueError(
                        f"Sharing configuration with source as {source} and destination"
                        + f" as {dest_config_name} is already configured."
                    )
        actions = dest_dict[dest_config_name]
        if not actions:
            raise ValueError("Atleast one action is must")
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
                f"Data push is not supported for configuration {dest_config_name}."
            )
        if (
            PluginHelper.check_plugin_name_with_regex(
                "netskope_edm_forwarder_receiver",
                dest_config.plugin
            )
            and dest_config.pluginType != "forwarder"
        ):
            raise ValueError(
                f"Data push is not supported for configuration {dest_config_name}."
            )
        if (
            PluginHelper.check_plugin_name_with_regex(
                "netskope_edm_forwarder_receiver",
                src_config.plugin
            )
            and src_config.pluginType == "receiver"
            and PluginHelper.check_plugin_name_with_regex(
                "netskope_edm_forwarder_receiver",
                dest_config.plugin
            )
        ):
            raise ValueError(
                "Receiver type plugin is not allowed to share data to another Netskope CE machine."
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


class Action(BaseModel):
    """Action model."""

    label: str
    value: str
    parameters: Dict = Field({})


class ActionWithoutParams(BaseModel):
    """ActionWithoutParams model."""

    label: str
    value: str


class BusinessRuleIn(BaseModel):
    """Business rule model."""

    name: Optional[str] = Field(None, validate_default=True)

    @field_validator("name")
    @classmethod
    def validate_is_unique(cls, v: str):
        """Validate that the name is unique."""
        # Currently as no need of particular rule name we are just using business rules for
        # Setting Name as uuid string every time
        value = str(uuid4())
        try_cnt = 0
        while (
            connector.collection(Collections.EDM_BUSINESS_RULES).find_one(
                {"name": value}
            )
            is not None
            and try_cnt < 3
        ):
            try_cnt += 1
            value = str(uuid4())
        return value

    sharedWith: Dict[str, Dict[str, List[Action]]]
    _validate_sharedWith = field_validator("sharedWith")(validate_sharedWith)
    createdAt: Optional[datetime] = Field(default=None, validate_default=True)
    updatedAt: Optional[datetime] = Field(default=None, validate_default=True)

    @field_validator("createdAt", "updatedAt")
    @classmethod
    def _datetime_now_validator(cls, v):
        return datetime.now(UTC)

    status: StatusType = Field(StatusType.SCHEDULED)
    sharedAt: Optional[datetime] = Field(None)


class BusinessRuleOut(BaseModel):
    """Business rule out model."""

    name: str = Field(...)
    sharedWith: Dict[str, Dict[str, List[Action]]] = Field(None)
    status: StatusType = Field(StatusType.SCHEDULED)
    createdAt: datetime
    updatedAt: datetime
    sharedAt: Optional[datetime] = Field(None)


class BusinessRuleDelete(BaseModel):
    """Delete business rule model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_exists(cls, v):
        """Validate that the name exists."""
        if (
            connector.collection(Collections.EDM_BUSINESS_RULES).find_one({"name": v})
            is None
        ):
            raise ValueError("No sharing with this name exists.")
        return v


class BusinessRuleDB(BaseModel):
    """Database business rule model."""

    name: str = Field(...)
    sharedWith: Dict[str, Dict[str, List[Action]]] = Field(...)
    status: StatusType = Field(StatusType.SCHEDULED)
    createdAt: datetime
    updatedAt: datetime
    sharedAt: Optional[datetime] = Field(None)
