"""Configuration related models."""

import re
from datetime import datetime
from functools import cache
from typing import Annotated, Optional, Union

from pydantic import (
    BaseModel,
    Field,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_validator,
)

from netskope.common.models import PollIntervalUnit
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    SecretDict,
)
from netskope.common.utils.validators import validate_poll_interval

connector = DBConnector()
helper = PluginHelper()
logger = Logger()


def _validate_parameters(self):
    """Validate parameters."""
    if isinstance(self, ConfigurationUpdate):
        self._existing_configuration = ConfigurationDB(
            **connector.collection(Collections.CREV2_CONFIGURATIONS).find_one(
                {"name": self.name}
            )
        )
    if isinstance(self, ConfigurationIn) and self.parameters is None:
        raise ValueError("Invalid parameters provided.")
    PluginClass = helper.find_by_id(  # NOSONAR S117
        self.plugin or self._existing_configuration.plugin
    )
    if not PluginClass:
        raise ValueError("Invalid plugin provided.")
    self._plugin_class = PluginClass  # NOSONAR

    # skip validation if configuration is disabled
    if self.active is False or (
        self.active is None and self._existing_configuration.active is False
    ):
        return self
    if isinstance(self, ConfigurationUpdate):
        self.storage = self.storage | self._existing_configuration.storage

    if self.storage is None:
        self.storage = {}

    plugin = PluginClass(
        self.name,
        None,
        self.storage,
        None,
        logger,
    )
    current_mapped_entities = self.mappedEntities or []
    if isinstance(self, ConfigurationUpdate):
        current_mapped_entities = self.mappedEntities or self._existing_configuration.mappedEntities
    if current_mapped_entities:
        plugin.mappedEntities = [mapped_entity.model_dump() for mapped_entity in current_mapped_entities]
    result = plugin.validate(
        SecretDict(
            (self.parameters or self._existing_configuration.parameters)
            | {"tenant": self.tenant}
        )
    )
    if result.success is False:
        raise ValueError(result.message)
    return self


def _validate_tenant(cls, v: str, info: ValidationInfo) -> str:
    """Validate tenant name."""
    PluginClass = helper.find_by_id(info.data["plugin"])  # NOSONAR
    if PluginClass.metadata.get("netskope", False) and v is None:
        raise ValueError(
            "Tenant name can not be empty for Netskope configurations."
        )
    if v is None:
        return None
    if (
        connector.collection(Collections.NETSKOPE_TENANTS).find_one(
            {"name": v}
        )
        is None
    ):
        raise ValueError(f"No tenant configuration found with name {v}.")
    return v


class EntityMappingField(BaseModel):
    """Entity mapping field model."""

    source: str
    destination: str


class EntityMapping(BaseModel):
    """Entity mapping model."""

    entity: str
    destination: str

    @field_validator("destination")
    @classmethod
    def validate_destination(cls, v):
        """Validate that destination is unique."""
        if (
            connector.collection(Collections.CREV2_ENTITIES).find_one(
                {"name": v}
            )
            is None
        ):
            raise ValueError(f"Entity with name '{v}' does not exist.")
        return v

    fields: list[EntityMappingField] = Field([])

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, v: list[EntityMappingField]):
        """Validate that fields are unique."""
        if len([f.destination for f in v]) != len(
            set([f.destination for f in v])
        ):
            raise ValueError(
                "Can not map multiple source fields to same target field."
            )
        return v


class EntityMappingOut(BaseModel):
    """Entity mapping out model."""

    entity: str
    destination: str
    fields: list[EntityMappingField] = Field([])

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, v: list[EntityMappingField]):
        """Validate that fields are unique."""
        if len([f.destination for f in v]) != len(
            set([f.destination for f in v])
        ):
            raise ValueError(
                "Can not map multiple source fields to same target field."
            )
        return v


class ConfigurationIn(BaseModel):
    """Configuration creation model."""

    name: Annotated[str, StringConstraints(strip_whitespace=True)]

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        """Validate that name is unique."""
        v = v.strip()
        if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9 _\-]*[a-zA-Z0-9])*$", v):
            raise ValueError(
                (
                    "Configuration name should start and end with an alpha-numeric character "
                    "and can include alpha-numeric characters, dashes, underscores and spaces."
                )
            )
        if (
            connector.collection(Collections.CREV2_CONFIGURATIONS).find_one(
                {"name": v}
            )
            is not None
        ):
            raise ValueError(f"Configuration with name '{v}' already exists.")
        return v

    active: bool = Field(True)
    plugin: str
    tenant: Optional[str] = Field(None)
    _validate_tenant = field_validator("tenant")(_validate_tenant)
    pollIntervalUnit: PollIntervalUnit = Field(PollIntervalUnit.MINUTES)
    pollInterval: int = Field(60)
    _validate_poll_interval = field_validator("pollInterval")(
        validate_poll_interval
    )
    parameters: dict = Field({})
    mappedEntities: list[EntityMapping] = Field([])
    storage: Union[dict, None] = Field(dict(), description="Storage for plugin.")
    _plugin_class = None
    _validate_parameters = model_validator(mode="after")(_validate_parameters)


class ConfigurationDB(BaseModel):
    """Database configuration model."""

    name: str
    tenant: Optional[str]
    checkpoints: dict[str, Optional[datetime]] = {}
    active: bool
    plugin: str
    pollIntervalUnit: PollIntervalUnit
    pollInterval: int
    parameters: dict
    storage: dict = {}
    mappedEntities: list[EntityMappingOut]
    lastRunAt: Optional[datetime] = None
    lastRunSuccess: Optional[bool] = None


class ConfigurationUpdate(BaseModel):
    """Configuration update model."""

    name: str = Field(
        ...,
        description="Name of the configuration.",
    )

    @field_validator("name")
    @classmethod
    def validate_name_exists(cls, v):
        """Validate that name exists."""
        if (
            connector.collection(Collections.CREV2_CONFIGURATIONS).find_one(
                {"name": v}
            )
            is None
        ):
            raise ValueError(f"Configuration with name {v} does not exist.")
        return v

    active: Union[bool, None] = Field(
        None, description="Indicates if the plugin is active or not."
    )
    plugin: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(...)
    tenant: Optional[str] = Field(None)
    _validate_tenant = field_validator("tenant")(_validate_tenant)
    pollIntervalUnit: Optional[PollIntervalUnit] = Field(
        None,
        description="Unit of the pollInterval parameter.",
    )
    pollInterval: Optional[int] = Field(None, description="Polling interval.")
    _validate_poll_interval = field_validator("pollInterval")(
        validate_poll_interval
    )
    parameters: Optional[dict] = Field(None)
    mappedEntities: Optional[list[EntityMapping]] = Field(None)
    storage: Union[dict, None] = Field(dict(), description="Storage for plugin.")

    _plugin_class = None
    _existing_configuration: ConfigurationDB = None
    _validate_parameters = model_validator(mode="after")(_validate_parameters)


class ConfigurationOut(BaseModel):
    """Configuration out model."""

    name: str
    tenant: Optional[str]
    active: bool
    plugin: str
    pollIntervalUnit: PollIntervalUnit
    pollInterval: int
    parameters: dict
    mappedEntities: list[EntityMappingOut]
    lastRunAt: Optional[datetime] = None
    lastRunSuccess: Optional[bool] = None
    pluginName: Union[str, None] = None


@cache
def get_plugin_from_configuration_name(name: str):
    """Get plugin from configuration name."""
    config = connector.collection(Collections.CREV2_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if config is None:
        raise ValueError("Configuration not found.")
    config = ConfigurationDB(**config)
    plugin_class = helper.find_by_id(config.plugin)
    if plugin_class is None:
        raise ValueError("Plugin not found.")
    plugin_class.mappedEntities = [mapped_entity.model_dump() for mapped_entity in config.mappedEntities]
    return plugin_class(
        config.name,
        SecretDict(config.parameters),
        config.storage,
        None,
        logger,
    )
