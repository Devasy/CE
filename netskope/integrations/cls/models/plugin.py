"""Provides plugins and configurations related models."""
import traceback
import re
from datetime import datetime
from fastapi import HTTPException
from typing import Union, Optional
from pydantic import field_validator, StringConstraints, BaseModel, Field, model_validator
from netskope.common.utils import (
    PluginHelper,
    DBConnector,
    Collections,
    Logger,
    SecretDict,
)
from netskope.common.models import PollIntervalUnit
from netskope.common.utils.validators import validate_poll_interval
from typing_extensions import Annotated

helper = PluginHelper()
connector = DBConnector()
logger = Logger()


class Plugin(BaseModel):
    """Base plugin model."""

    name: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(..., description="Name of the plugin.")
    id: str = Field(..., description="Unique ID of the plugin.")
    version: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        ..., description="Version of the plugin."
    )
    description: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        ..., description="Description of the plugin."
    )
    configuration: list = Field(..., description="Plugin parameter dictionary.")
    icon: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        ..., description="Base64 string representation of the plugin icon."
    )
    netskope: bool


def _validate_parameters(cls, v, values, **kwargs):
    """Make sure that the parameters are valid."""
    values = values.data
    if not values.get("storage"):
        values["storage"] = {}
    if cls is ConfigurationIn:
        if "plugin" not in values:
            raise ValueError("Invalid plugin provided")
        plugin_id = values["plugin"]
    elif cls is ConfigurationUpdate:
        plugin_id = values["oldConfig"].plugin
        values["storage"] = values["oldConfig"].storage or {}

    if cls is ConfigurationUpdate and values["active"] is False:
        return v

    if "name" not in values:
        raise ValueError("Invalid name provided.")

    attributeMapping_dict = None
    if values.get("tenant") is None and values.get("attributeMapping") is not None:
        try:
            attributeMapping_dict = connector.collection(
                Collections.CLS_MAPPING_FILES
            ).find_one(({"name": values["attributeMapping"], "repo": values["attributeMappingRepo"]}))
        except Exception:
            logger.debug(
                "Can not found attribute mapping with name {}".format(
                    values["attributeMapping"]
                ),
                details=traceback.format_exc(),
                error_code="CLS_1000",
            )
            raise ValueError(
                "Could not found attribute mapping with name {}".format(
                    values["attributeMapping"]
                )
            )

        if attributeMapping_dict is None:
            raise ValueError(
                "Could not found attribute mapping with name {}".format(
                    values["attributeMapping"]
                )
            )

    PluginClass = helper.find_by_id(plugin_id)  # NOSONAR
    plugin = PluginClass(
        values["name"],
        None,
        values["storage"],
        None,
        logger,
        mappings=attributeMapping_dict,
    )
    transform_data = v.get("transformData")
    revert_transform_data = False
    if (
        PluginClass
        and PluginClass.metadata
        and not PluginClass.metadata.get("format_options", None)
        and transform_data is not None
    ):
        v.update(
            {
                "transformData": transform_data == "cef"
            }
        )
        revert_transform_data = True
    try:
        if values.get("tenant"):
            result = plugin.validate(SecretDict(v), values.get("tenant", None))
        else:
            result = plugin.validate(SecretDict(v))
        if revert_transform_data:
            v.update(
                {
                    "transformData": transform_data
                }
            )
    except Exception as e:
        logger.error(
            "Error occurred while validating configuration.",
            details=traceback.format_exc(),
            error_code="CLS_1001",
        )
        raise HTTPException(400, str(e))
    if not result.success:
        raise ValueError(result.message)
    return v


def _validate_tenant(cls, v, values, **kwargs):
    """Validate tenant name."""
    values = values.data
    if cls is ConfigurationIn:
        if "plugin" not in values:
            raise ValueError("Invalid plugin provided")
        plugin_id = values["plugin"]
    elif cls is ConfigurationUpdate:
        if "name" not in values:
            raise ValueError("Invalid name provided.")
        plugin_id = values["oldConfig"].plugin
    metadata = helper.find_by_id(plugin_id).metadata
    metadata_types = metadata.get("types", [])
    if metadata.get("netskope") and (
        "alerts" in metadata_types or "events" in metadata_types or "webtx" in metadata_types
    ):
        if (
            connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": v})
            is None
        ):
            raise ValueError(f"No tenant configuration found with name {v}.")
    else:
        return None
    return v


def _validate_attribute_mapping(cls, v, values, **kwargs):
    """Validate mapping."""
    values = values.data
    if cls is ConfigurationIn:
        if "plugin" not in values:
            raise ValueError("Invalid plugin provided")
        plugin_id = values["plugin"]
    elif cls is ConfigurationUpdate:
        if "name" not in values:
            raise ValueError("Invalid name provided.")
        plugin_id = values["oldConfig"].plugin
        if values["active"] is False:
            return v
    metadata = helper.find_by_id(plugin_id).metadata
    if not metadata.get("netskope") and (
        "alerts" in metadata.get("types", []) or "events" in metadata.get("types", [])
    ):
        if (
            connector.collection(Collections.CLS_MAPPING_FILES).find_one(
                {"name": v, "repo": values.get("attributeMappingRepo")})
            is None
        ):
            raise ValueError(f"No attribute mapping file found with name {v}.")
    else:
        return None
    return v


class ConfigurationDB(BaseModel):
    """The database configuration model."""

    name: str = Field(...)
    tenant: Union[str, None] = Field(None)
    active: bool
    pollIntervalUnit: PollIntervalUnit = Field(...)
    pollInterval: int = Field(...)
    attributeMapping: Union[str, None] = Field(None)
    attributeMappingRepo: Union[str, None] = Field(None)
    parameters: Union[dict, None] = Field(None)
    plugin: str = Field(...)

    lockedAt: Union[datetime, None] = Field(None)
    task: Union[dict, None] = Field({})
    logsIngested: int = Field(0)
    bytesIngested: int = Field(0)
    lastRunAt: Union[datetime, None] = Field(None)

    checkpoint: Optional[datetime] = Field(None)
    subCheckpoint: Optional[dict] = Field(None)
    storage: dict = Field({})
    lastRunSuccess: Optional[bool] = None


class ConfigurationOut(BaseModel):
    """The outgoing configuration model."""

    name: str = Field(
        ...,
        description="Name of the configuration.",
    )
    types: list = Field([])
    netskope: bool = Field(False)
    plugin: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        ..., description="Name of the plugin."
    )
    pluginName: Union[Annotated[str, StringConstraints(strip_whitespace=True)], None] = Field(None)
    pluginVersion: Union[str, None] = Field(None)
    pushSupported: bool = Field(True)
    pullSupported: bool = Field(False)
    tenant: Union[Annotated[str, StringConstraints(strip_whitespace=True)], None] = Field(None)
    active: bool = Field(True, description="Indicates if the plugin is active or not.")
    pollIntervalUnit: Union[PollIntervalUnit, None] = Field(None)
    pollInterval: Union[int, None] = Field(None)
    attributeMapping: Union[str, None] = Field(
        None, description="attribute mapping and csv for the plugin."
    )
    attributeMappingRepo: Union[str, None] = Field(
        None, description="attribute mapping repo."
    )
    parameters: dict = Field({}, description="Parameters of the plugin.")
    logsIngested: int = Field(0)
    bytesIngested: int = Field(0)
    lastRunAt: Union[datetime, None] = Field(None)
    lastRunSuccess: Optional[bool] = None


class ConfigurationIn(BaseModel):
    """The incoming configuration model for creation."""

    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)] = Field(  # noqa
        ...,
        description="Name of the configuration.",
    )

    @field_validator("name")
    @classmethod
    def _validate_unique_name(cls, v: str):
        """Validate that the configuration name is unique."""
        if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9 _\-]*[a-zA-Z0-9])*$", v):
            raise ValueError(
                (
                    "Configuration name should start and end with an alpha-numeric character "
                    "and can include alpha-numeric characters, dashes, underscores and spaces."
                )
            )
        if (
            connector.collection(Collections.CLS_CONFIGURATIONS).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"Configuration with name='{v}' already exists.")
        return v

    plugin: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(...)

    @field_validator("plugin")
    @classmethod
    def validate_plugin(cls, v):
        """Make sure that the plugin exists."""
        if helper.find_by_id(v) is None:
            raise ValueError(f"Plugin with id={v} not found.")
        return v

    active: bool = Field(True, description="Indicates if the plugin is active or not.")
    tenant: Union[str, None] = Field(None)
    _validate_tenant = field_validator("tenant")(_validate_tenant)
    pollIntervalUnit: PollIntervalUnit = Field(PollIntervalUnit.MINUTES)
    pollInterval: int = Field(60)
    _validate_poll_interval = field_validator("pollInterval")(
        validate_poll_interval
    )
    attributeMappingRepo: Union[str, None] = Field(
        None, description="attribute mapping repo"
    )
    attributeMapping: Union[str, None] = Field(
        None, description="attribute mapping and csv for the plugin."
    )
    _validate_attribute_mapping = field_validator("attributeMapping")(
        _validate_attribute_mapping
    )

    storage: Union[dict, None] = Field(dict(), description="Storage for plugin.")
    parameters: Union[dict, None] = Field(None, description="Parameters of the plugin.")
    _validate_parameters = field_validator("parameters")(
        _validate_parameters
    )


class ConfigurationUpdate(BaseModel):
    """The incoming configuration model for update."""

    name: str = Field(
        ...,
        description="Name of the configuration.",
    )

    @field_validator("name")
    @classmethod
    def _validate_name_exists(cls, v: str):
        if (
            connector.collection(Collections.CLS_CONFIGURATIONS).find_one({"name": v})
            is None
        ):
            raise ValueError(f"No configuration with name='{v}' exists.")
        return v

    plugin: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(...)

    @field_validator("plugin")
    @classmethod
    def validate_plugin(cls, v):
        """Make sure that the plugin exists."""
        if helper.find_by_id(v) is None:
            raise ValueError(f"Plugin with id={v} not found.")
        return v

    oldConfig: Union[ConfigurationDB, None] = Field(None)

    @model_validator(mode="before")
    def get_old_config(cls, values):
        """Retrive the older configuration."""
        config = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
            {"name": values.get("name")}
        )
        if config is None:
            raise ValueError("Plugin with id={} not found.".format(values.get("name")))

        values["oldConfig"] = ConfigurationDB(**config)
        return values

    active: Union[bool, None] = Field(None, description="Indicates if the plugin is active or not.")
    tenant: Union[str, None] = Field(None)
    _validate_tenant = field_validator("tenant")(_validate_tenant)
    pollIntervalUnit: Union[PollIntervalUnit, None] = Field(None)
    pollInterval: Union[int, None] = Field(None)
    _validate_poll_interval = field_validator("pollInterval")(
        validate_poll_interval
    )
    attributeMappingRepo: Union[str, None] = Field(
        None, description="attribute mapping repo"
    )
    attributeMapping: Union[str, None] = Field(
        None, description="attribute mapping and csv for the plugin."
    )
    _validate_attribute_mapping = field_validator("attributeMapping")(
        _validate_attribute_mapping
    )

    storage: Union[dict, None] = Field(dict(), description="Storage for plugin.")
    parameters: Union[dict, None] = Field(None, description="Parameters of the plugin.")
    _validate_parameters = field_validator("parameters")(
        _validate_parameters
    )


class ConfigurationDelete(BaseModel):
    """The configuration delete model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_configuration_exists(cls, v: str):
        """Validate configuration name."""
        if (
            connector.collection(Collections.CLS_CONFIGURATIONS).find_one({"name": v})
            is None
        ):
            raise ValueError("could not find a configuration with that name")
        return v
