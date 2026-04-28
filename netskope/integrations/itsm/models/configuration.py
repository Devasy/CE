"""Configuration related models."""
import re
from datetime import datetime
from typing import Union
from pydantic import field_validator, StringConstraints, BaseModel, Field

from netskope.common.utils import PluginHelper, DBConnector, Collections
from netskope.common.models import PollIntervalUnit
from typing_extensions import Annotated

helper = PluginHelper()
connector = DBConnector()


class TaskFields(BaseModel):
    """Locking related fields."""

    pull: Union[datetime, None] = Field(None)
    sync: Union[datetime, None] = Field(None)
    update: Union[datetime, None] = Field(None)


class TaskStatusFields(BaseModel):
    """Task status fields."""

    pull: Union[bool, None] = Field(None)
    sync: Union[bool, None] = Field(None)
    update: Union[bool, None] = Field(None)


def validate_tenant(cls, v, values, **kwargs):
    """Make sure that the tenant exists."""
    values = values.data
    PluginClass = helper.find_by_id(values.get("plugin"))  # NOSONAR
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
        raise ValueError(f"Tenant with name {v} does not exist.")
    return v


class ConfigurationIn(BaseModel):
    """Incoming configuration model."""

    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)] = Field(...)

    @field_validator("name")
    @classmethod
    def validate_name_unique(cls, v):
        """Make sure that the name is unique."""
        if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9 _\-]*[a-zA-Z0-9])*$", v):
            raise ValueError(
                (
                    "Configuration name should start and end with an alpha-numeric character "
                    "and can include alpha-numeric characters, dashes, underscores and spaces."
                )
            )
        if (
            connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
                {"name": v}
            )
            is not None
        ):
            raise ValueError(f"Configuration with name={v} already exists.")
        return v

    plugin: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(...)

    @field_validator("plugin")
    @classmethod
    def validate_plugin(cls, v):
        """Make sure that the plugin exists."""
        if helper.find_by_id(v) is None:
            raise ValueError(f"Plugin with id={v} not found.")
        return v

    tenant: Union[str, None] = Field(None)
    _validate_tenant = field_validator("tenant")(validate_tenant)
    active: bool = Field(...)
    parameters: dict = Field(...)
    storage: Union[dict, None] = Field(dict(), description="Storage for plugin.")
    pollIntervalUnit: PollIntervalUnit = Field(
        PollIntervalUnit.SECONDS,
        description="Unit of the pollInterval parameter.",
    )
    pollInterval: int = Field(60, description="Polling interval.")
    updateIncidents: bool = Field(False)


class ConfigurationUpdate(BaseModel):
    """Incoming update configuration model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_name_exists(cls, v):
        """Make sure that the name is unique."""
        if (
            connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
                {"name": v}
            )
            is None
        ):
            raise ValueError(f"Configuration with name={v} does not exist.")
        return v

    plugin: str = Field(...)

    @field_validator("plugin")
    @classmethod
    def validate_plugin(cls, v):
        """Make sure that the plugin exists."""
        if helper.find_by_id(v) is None:
            raise ValueError(f"Plugin with id={v} not found.")
        return v

    tenant: Union[str, None] = Field(None)
    active: Union[bool, None] = Field(None)
    parameters: Union[dict, None] = Field(None)
    storage: Union[dict, None] = Field(dict(), description="Storage for plugin.")
    pollIntervalUnit: Union[PollIntervalUnit, None] = Field(
        None,
        description="Unit of the pollInterval parameter.",
    )
    pollInterval: Union[int, None] = Field(None, description="Polling interval.")
    updateIncidents: Union[bool, None] = Field(None)


class ConfigurationOut(BaseModel):
    """Outgoing configuration model."""

    name: str = Field(...)
    plugin: str = Field(...)
    pluginName: str = Field(...)
    active: bool = Field(...)
    tenant: Union[str, None] = Field(None)
    parameters: dict = Field({})
    pollIntervalUnit: PollIntervalUnit = Field(...)
    pollInterval: int = Field(...)
    lastRunAt: TaskFields = Field(TaskFields())
    lastRunSuccess: TaskStatusFields = Field(TaskStatusFields())
    receivingSupported: bool = True
    sharingSupported: bool = False
    updateIncidents: bool = Field(False)
    netskope: bool = Field(False)


class ConfigurationDB(BaseModel):
    """Database configuration model."""

    name: str = Field(..., min_length=1)
    plugin: str = Field(...)
    active: bool = Field(...)
    tenant: Union[str, None] = Field(None)
    parameters: dict = Field(...)
    storage: dict = {}
    pollIntervalUnit: PollIntervalUnit = Field(...)
    pollInterval: int = Field(...)
    updateIncidents: bool = Field(False)

    checkpoint: Union[datetime, None] = None
    lastRunAt: TaskFields = Field(TaskFields())
    lastRunSuccess: TaskStatusFields = Field(TaskStatusFields())
    lockedAt: TaskFields = Field(TaskFields())
    lockedAtAudit: Union[datetime, None] = None


class ConfigurationDelete(BaseModel):
    """The configuration delete model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_configuration_exists(cls, v: str):
        """Validate configuration name."""
        if (
            connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
                {"name": v}
            )
            is None
        ):
            raise ValueError("could not find a configuration with that name")
        return v
