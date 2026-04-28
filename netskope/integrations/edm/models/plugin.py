"""Provides plugins and configurations related models."""
import re
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import (
    BaseModel,
    Field,
    FieldValidationInfo,
    StringConstraints,
    field_validator,
)
from typing_extensions import Annotated

from netskope.common.models import PollIntervalUnit
from netskope.common.utils import Collections, DBConnector
from netskope.integrations.edm.utils import validate_poll_interval

connector = DBConnector()
ZIP_NAME_SANITIZE_PATTERN = re.compile(r"[\s\-_]")


def get_zip_name_from_configuration(config_name: str) -> str:
    """Return the EDM zip name derived from configuration name."""
    sanitized = ZIP_NAME_SANITIZE_PATTERN.sub("_", config_name.strip())
    return f"{sanitized}_data"


def find_active_zip_name_conflict(config_name: str, exclude_config: str | None = None) -> str | None:
    """Check if another active configuration maps to the same EDM zip name."""
    target_zip_name = get_zip_name_from_configuration(config_name)
    query = {"active": True}
    if exclude_config:
        query["name"] = {"$ne": exclude_config}
    for config in connector.collection(Collections.EDM_CONFIGURATIONS).find(query, {"name": 1}):
        if get_zip_name_from_configuration(config["name"]) == target_zip_name:
            return config["name"]
    return None


def validate_tenant(cls, v):
    """Make sure that the tenant exists."""
    if v is None:
        return None
    if connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": v}) is None:
        raise ValueError(f"Tenant with name {v} does not exist.")
    return v


class ShareDataType(str, Enum):
    """The EDM Data type enumerations."""

    EDM_FILE_HASHES = "edm_file_hashes"


class CleanSampleSourceType(str, Enum):
    """Type of source to fetch data from."""

    PLUGIN = "plugin"
    MANUAL_UPLOAD = "manual_upload"


class CleanSampleFilesIn(BaseModel):
    """The incoming model to clean sample files created during plugin configuration."""

    name: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        None,
        description="Name of the configuration.",
    )
    sourceType: CleanSampleSourceType = Field(
        CleanSampleSourceType.PLUGIN, description="Mode of source"
    )


class CleanSampleFilesOut(BaseModel):
    """The outgoing model to clean sample files created during plugin configuration."""

    name: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        None,
        description="Name of the configuration.",
    )
    sampleFileCleanupStatus: bool = Field(
        ..., description="Sample files clean up operation status"
    )
    message: str = Field(..., description="Sample files clean up message")


class ConfigurationOut(BaseModel):
    """The outgoing configuration model."""

    name: str = Field(..., description="Name of the configuration.")

    plugin: str = Field(..., description="Name of the plugin.")
    pluginName: Optional[str] = Field(None)
    pluginVersion: Optional[str] = Field(None)
    netskope: bool = Field(False)
    pushSupported: bool = Field(False)
    pullSupported: bool = Field(False)
    active: bool = Field(..., description="Indicates if the plugin is active or not.")
    sslValidation: bool = Field(
        True,
        description="Indicates if the SSL certificate validation is enabled or not.",
    )
    pollInterval: int = Field(..., description="Polling interval.")
    pollIntervalUnit: PollIntervalUnit = Field(
        ...,
        description="Unit of the pollInterval parameter.",
    )
    lastRunAt: Optional[datetime] = Field(
        None,
        description=("Indicates the last attempt at executing this configuration."),
    )
    lastRunSuccess: Optional[bool] = Field(
        None,
        description=(
            "Indicates the status of last execution. `null` indicates that the "
            "plugin is yet to be executed."
        ),
    )
    parameters: dict = Field({}, description="Parameters of the plugin.")
    lockedAt: Optional[datetime] = None
    pluginType: Optional[str] = Field(
        None,
        description=(
            "Additional parameter defining type of plugin. Currently being used for"
            + " Netskope EDM Forwarder/Receiver plugin, for forwarder or receiver."
        ),
    )
    tenant: Optional[str] = Field(None)


class ConfigurationIn(BaseModel):
    """The incoming configuration model for creation."""

    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=215)
    ] = Field(
        ...,
        description="Name of the configuration.",
    )
    plugin: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(...)

    @field_validator("name")
    @classmethod
    def _validate_unique_name(cls, v: str):
        """Validate that the configuration name is unique."""
        if not re.match(r"^[a-zA-Z0-9 ]+$", v):
            raise ValueError(
                "Configuration name must contain only alphanumeric characters and spaces (a-z, A-Z, 0-9). "
                "Hyphens and underscores are not allowed."
            )
        if (
            connector.collection(Collections.EDM_CONFIGURATIONS).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"Configuration with name='{v}' already exists.")
        conflict_name = find_active_zip_name_conflict(v)
        if conflict_name:
            raise ValueError(
                (
                    f"Configuration '{conflict_name}' file name is already in use. "
                    "Please change the configuration name."
                )
            )
        return v

    active: bool = Field(True, description="Indicates if the plugin is active or not.")
    pollIntervalUnit: PollIntervalUnit = Field(
        PollIntervalUnit.SECONDS,
        description="Unit of the pollInterval parameter.",
    )
    pollInterval: int = Field(60, description="Polling interval.")
    _validate_poll_interval = field_validator("pollInterval")(validate_poll_interval)
    parameters: dict = Field(dict(), description="Parameters of the plugin.")
    sslValidation: bool = Field(True)
    pluginType: Optional[str] = Field(
        None,
        description=(
            "Additional parameter defining type of plugin. Currently being used for"
            + " Netskope EDM Forwarder/Receiver plugin, for forwarder or receiver."
        ),
    )
    tenant: Optional[str] = Field(None)
    _validate_tenant = field_validator("tenant")(validate_tenant)


class ConfigurationDelete(BaseModel):
    """The configuration delete model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_configuration_exists(cls, v: str):
        """Validate configuration name."""
        if (
            connector.collection(Collections.EDM_CONFIGURATIONS).find_one({"name": v})
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
    plugin: str = Field(...)

    @field_validator("name")
    @classmethod
    def _validate_name_exists(cls, v: str):
        if (
            connector.collection(Collections.EDM_CONFIGURATIONS).find_one({"name": v})
            is None
        ):
            raise ValueError(f"No configuration with name='{v}' exists.")
        return v

    active: Optional[bool] = Field(
        None, description="Indicates if the plugin is active or not."
    )
    pollIntervalUnit: Optional[PollIntervalUnit] = Field(
        None,
        description="Unit of the pollInterval parameter.",
    )
    pollInterval: Optional[int] = Field(None, description="Polling interval.")
    sslValidation: Optional[bool] = Field(
        None,
        description="Indicates if the SSL certificate validation is enabled or not.",
    )
    _validate_poll_interval = field_validator("pollInterval")(validate_poll_interval)
    parameters: Optional[dict] = Field(None, description="Parameters of the plugin.")
    tenant: Optional[str] = Field(None)
    _validate_tenant = field_validator("tenant")(validate_tenant)


class ConfigurationDB(BaseModel):
    """The database configuration model."""

    name: str
    active: bool
    sslValidation: bool
    pollIntervalUnit: PollIntervalUnit
    pollInterval: int
    parameters: dict
    plugin: str
    storage: dict = Field(dict())
    checkpoint: Optional[datetime] = Field(None)
    lastRunAt: Optional[datetime] = Field(None)
    lastRunSuccess: Optional[bool] = Field(None)
    lockedAt: Optional[datetime] = Field(None)
    disabledAt: Optional[datetime] = Field(None)
    createdBy: Optional[str] = Field(None)
    createdAt: Optional[datetime] = Field(None)
    lastUpdatedBy: Optional[str] = Field(None)
    lastUpdatedAt: Optional[datetime] = Field(None)
    pluginType: Optional[str] = Field(None)
    tenant: Optional[str] = Field(None)


class ValidationConfigurationIn(BaseModel):
    """The incoming configuration model for creation."""

    configuration_edit: bool = Field(False)
    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=215)
    ] = Field(
        ...,
        description="Name of the configuration.",
    )

    @field_validator("name")
    @classmethod
    def _validate_config_name(cls, v, info: FieldValidationInfo):
        """Validate that the configuration name is unique."""
        if not info.data.get("configuration_edit", False):
            if not re.match(r"^[a-zA-Z0-9 ]+$", v):
                raise ValueError(
                    "Configuration name must contain only alphanumeric characters and spaces (a-z, A-Z, 0-9). "
                    "Hyphens and underscores are not allowed."
                )
            if (
                connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
                    {"name": v}
                )
                is not None
            ):
                raise ValueError(f"Configuration with name='{v}' already exists.")
            conflict_name = find_active_zip_name_conflict(v)
            if conflict_name:
                raise ValueError(
                    (
                        f"Configuration '{conflict_name}' file name is already in use. "
                        "Please change the configuration name."
                    )
                )
            return v
        return v

    active: bool = Field(True, description="Indicates if the plugin is active or not.")
    pollIntervalUnit: PollIntervalUnit = Field(
        PollIntervalUnit.SECONDS,
        description="Unit of the pollInterval parameter.",
    )
    pollInterval: int = Field(60, description="Polling interval.")
    _validate_poll_interval = field_validator("pollInterval")(validate_poll_interval)
    parameters: dict = Field(dict(), description="Parameters of the plugin.")
    sslValidation: bool = Field(True)
    pluginType: Optional[str] = Field(None)


class ValidationConfigurationOut(BaseModel):
    """The outgoing model for Validation configuration."""

    name: str = Field(..., description="Plugin Name")
    validationStatus: bool = Field(..., description="Validation Status")
    validationMsg: str = Field(..., description="validation msg")
    data: Optional[dict] = Field(None)


class ConfigurationNameValidationIn(BaseModel):
    """The incoming model for configuration name check."""

    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=215)
    ] = Field(
        ...,
        description="Name of the configuration.",
    )

    @field_validator("name")
    @classmethod
    def _validate_unique_name(cls, v: str):
        """Validate that the configuration name is unique."""
        if not re.match(r"^[a-zA-Z0-9 ]+$", v):
            raise ValueError(
                "Configuration name must contain only alphanumeric characters and spaces (a-z, A-Z, 0-9). "
                "Hyphens and underscores are not allowed."
            )
        if (
            connector.collection(Collections.EDM_CONFIGURATIONS).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"Configuration with name='{v}' already exists.")
        conflict_name = find_active_zip_name_conflict(v)
        if conflict_name:
            raise ValueError(
                (
                    f"Configuration '{conflict_name}' file name is already in use. "
                    "Please change the configuration name."
                )
            )
        return v
