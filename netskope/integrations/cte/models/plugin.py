"""Provides plugins and configurations related models."""
import re
from enum import Enum
from datetime import datetime
from typing import Union, List
from pydantic import field_validator, StringConstraints, BaseModel, Field

from netskope.common.utils import DBConnector, Collections
from netskope.common.utils.validators import validate_poll_interval
from netskope.common.models import PollIntervalUnit
from typing_extensions import Annotated

connector = DBConnector()


class TaskFields(BaseModel):
    """Locking related fields."""

    pull: Union[datetime, None] = Field(None)
    share: Union[datetime, None] = Field(None)


class TaskStatusFields(BaseModel):
    """Task status fields."""

    pull: Union[bool, None] = Field(None)
    share: Union[bool, None] = Field(None)


class AggregateStrategy(str, Enum):
    """Tags Aggregate Strategies."""

    APPEND = "append"
    OVERWRITE = "overwrite"


class Plugin(BaseModel):
    """Base plugin model."""

    name: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        ..., description="Name of the plugin."
    )
    id: str = Field(..., description="Unique ID of the plugin.")
    version: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        ..., description="Version of the plugin."
    )
    pushSupported: bool = Field(True)
    description: Annotated[
        str, StringConstraints(strip_whitespace=True)
    ] = Field(..., description="Description of the plugin.")
    configuration: list = Field(
        ..., description="Plugin parameter dictionary."
    )
    icon: str = Field(
        ..., description="Base64 string representation of the plugin icon."
    )


def validate_tenant(cls, v):
    """Make sure that the tenant exists."""
    if v is None:
        return None
    if connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": v}) is None:
        raise ValueError(f"Tenant with name {v} does not exist.")
    return v


def validate_aging_criteria(cls, v):
    """Validate the aging criteria."""
    if v > 365:
        raise ValueError("Aging criteria must be less than or equal to 365")
    if v < 0:
        raise ValueError("Aging criteria must be greater than or equal to 0")
    return v


def validate_reputation(cls, v):
    """Validate the Override Reputation."""
    if v is None:
        raise ValueError(
            "Override Reputation must be valid number between 0 to 10."
        )
    if v > 10:
        raise ValueError(
            "Override Reputation must be less than or equal to 10"
        )
    if v < 0:
        raise ValueError(
            "Override Reputation must be greater than or equal to 0"
        )
    return v


class ManualSyncConfig(BaseModel):
    """Storing manual sync configs."""

    source: str = Field(..., description="Name of the source config.")
    rule: str = Field(..., description="Name of the rule.")
    lastseen: int = Field(..., description="Number of lastseen days IoCs to sync.")
    action: dict = Field(..., description="Name of the action.")


class ConfigurationOut(BaseModel):
    """The outgoing configuration model."""

    name: str = Field(..., description="Name of the configuration.")

    plugin: str = Field(..., description="Name of the plugin.")
    pluginName: Union[str, None] = Field(None)
    pluginVersion: Union[str, None] = Field(None)
    pushSupported: bool = Field(True)
    tenant: Union[str, None] = Field(None)
    active: bool = Field(
        ..., description="Indicates if the plugin is active or not."
    )
    sslValidation: bool = Field(
        True,
        description="Indicates if the SSL certificate validation is enabled or not.",
    )
    pollInterval: int = Field(..., description="Polling interval.")
    pollIntervalUnit: PollIntervalUnit = Field(
        ...,
        description="Unit of the pollInterval parameter.",
    )
    checkpoint: Union[datetime, None] = Field(
        None,
        description=(
            "Indicates the last successfully execution of this configuration."
        ),
    )
    reputation: Union[int, None] = Field(None)
    _validate_reputation = field_validator("reputation")(validate_reputation)
    lastRunAt: TaskFields = Field(
        TaskFields(),
        description=(
            "Indicates the last attempt at executing this configuration."
        ),
    )
    ageAfterDays: int = Field(..., ge=0, le=365)
    lastRunSuccess: TaskStatusFields = Field(
        TaskStatusFields(),
        description=(
            "Indicates the status of last execution. `null` indicates that the "
            "plugin is yet to be executed."
        ),
    )
    parameters: dict = Field({}, description="Parameters of the plugin.")
    filters: list = Field(
        ..., description="Filter to apply while uploading indicators."
    )
    lockedAt: TaskFields = Field(TaskFields())
    manualSync: List[ManualSyncConfig] = Field([])
    tagsAggregateStrategy: AggregateStrategy = Field(
        ...,
        description="Storing criteria for tags using selected strategy."
    )


class ConfigurationIn(BaseModel):
    """The incoming configuration model for creation."""

    name: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    ] = Field(
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
            connector.collection(Collections.CONFIGURATIONS).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"Configuration with name='{v}' already exists.")
        return v

    active: bool = Field(
        True, description="Indicates if the plugin is active or not."
    )
    tenant: Union[str, None] = Field(None)
    _validate_tenant = field_validator("tenant")(validate_tenant)
    sslValidation: bool = Field(
        True,
        description="Indicates if the SSL certificate validation is enabled or not.",
    )
    pollIntervalUnit: PollIntervalUnit = Field(
        PollIntervalUnit.SECONDS,
        description="Unit of the pollInterval parameter.",
    )
    reputation: Union[int, None] = Field(0)
    _validate_reputation = field_validator("reputation")(validate_reputation)
    pollInterval: int = Field(60, description="Polling interval.")
    _validate_poll_interval = field_validator("pollInterval")(
        validate_poll_interval
    )
    ageAfterDays: int = Field(90)
    _validate_ageafterDays = field_validator("ageAfterDays")(
        validate_aging_criteria
    )
    parameters: dict = Field(dict(), description="Parameters of the plugin.")
    filters: list = Field(
        [], description="Filter to apply while uploading indicators."
    )
    tagsAggregateStrategy: AggregateStrategy = Field(
        AggregateStrategy.APPEND,
        description="Storing criteria for tags using selected strategy."
    )


class ConfigurationDelete(BaseModel):
    """The configuration delete model."""

    name: str = Field(...)
    keepData: bool = Field(True)

    @field_validator("name")
    @classmethod
    def validate_configuration_exists(cls, v: str):
        """Validate configuration name."""
        if (
            connector.collection(Collections.CONFIGURATIONS).find_one({"name": v})
            is None
        ):
            raise ValueError("could not find a configuration with that name")
        return v


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
            connector.collection(Collections.CONFIGURATIONS).find_one({"name": v})
            is None
        ):
            raise ValueError(f"No configuration with name='{v}' exists.")
        return v

    active: Union[bool, None] = Field(
        None, description="Indicates if the plugin is active or not."
    )
    tenant: Union[str, None] = Field(None)
    _validate_tenant = field_validator("tenant")(validate_tenant)
    sslValidation: Union[bool, None] = Field(
        None,
        description="Indicates if the SSL certificate validation is enabled or not.",
    )
    checkpoint: Union[datetime, None] = Field(
        None,
        description=(
            "Indicates the last successfully execution of this configuration."
        ),
    )
    reputation: Union[int, None] = Field(None)
    _validate_reputation = field_validator("reputation")(validate_reputation)
    ageAfterDays: Union[int, None] = Field(None)
    _validate_ageafterDays = field_validator("ageAfterDays")(
        validate_aging_criteria
    )
    pollIntervalUnit: Union[PollIntervalUnit, None] = Field(
        None,
        description="Unit of the pollInterval parameter.",
    )
    pollInterval: Union[int, None] = Field(
        None, description="Polling interval."
    )
    _validate_poll_interval = field_validator("pollInterval")(
        validate_poll_interval
    )
    parameters: Union[dict, None] = Field(
        None, description="Parameters of the plugin."
    )
    filters: Union[list, None] = Field(
        None, description="Filter to apply while uploading indicators."
    )
    tagsAggregateStrategy: Union[AggregateStrategy, None] = Field(
        None,
        description="Storing criteria for tags using selected strategy."
    )


class ConfigurationDB(BaseModel):
    """The database configuration model."""

    name: str
    active: bool
    sslValidation: bool
    pollIntervalUnit: PollIntervalUnit
    pollInterval: int
    ageAfterDays: int
    parameters: dict
    filters: list
    reputation: int
    providerID: Union[str, None] = Field(None)
    tenant: Union[str, None] = Field(None)

    plugin: str
    storage: dict = Field(dict())
    checkpoint: Union[datetime, None] = Field(None)
    lastRunAt: TaskFields = Field(TaskFields())
    lastRunSuccess: TaskStatusFields = Field(TaskStatusFields())
    lockedAt: TaskFields = Field(TaskFields())
    disabledAt: Union[datetime, None] = Field(None)
    cycleStartedAt: Union[datetime, None] = Field(None)
    subCheckpoint: Union[dict, None] = Field(None)
    manualSync: List[ManualSyncConfig] = Field([])
    tagsAggregateStrategy: AggregateStrategy = Field(
        AggregateStrategy.APPEND,
        description="Storing criteria for tags using selected strategy."
    )
