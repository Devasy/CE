"""Provides plugins and configurations related models."""
import re
from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import (BaseModel, Field, FieldValidationInfo, StringConstraints,
                      field_validator)
from typing_extensions import Annotated

from netskope.common.models import PollIntervalUnit
from netskope.common.utils import Collections, DBConnector
from netskope.integrations.cfc.utils import validate_poll_interval

connector = DBConnector()


def validate_tenant(cls, value):
    """Make sure that the tenant exists."""
    if value is None:
        return None
    if connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": value}) is None:
        raise ValueError(f"Tenant with name {value} does not exist.")
    return value


class ConfigurationIn(BaseModel):
    """The incoming configuration model for creation."""

    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
    ] = Field(
        ...,
        description="Name of the configuration.",
    )
    plugin: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(...)

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
            connector.collection(Collections.CFC_CONFIGURATIONS).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"Configuration with name='{v}' already exists.")
        return v

    active: bool = Field(True, description="Indicates if the plugin is active or not.")
    tenant: Union[str, None] = Field(None)
    _validate_tenant = field_validator("tenant")(validate_tenant)
    pollIntervalUnit: PollIntervalUnit = Field(
        PollIntervalUnit.SECONDS,
        description="Unit of the pollInterval parameter.",
    )
    pollInterval: int = Field(60, description="Polling interval.")
    _validate_poll_interval = field_validator("pollInterval")(
        validate_poll_interval
    )
    parameters: dict = Field(dict(), description="Parameters of the plugin.")
    sslValidation: bool = Field(False)


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
            connector.collection(Collections.CFC_CONFIGURATIONS).find_one({"name": v})
            is None
        ):
            raise ValueError(f"No configuration with name='{v}' exists.")
        return v

    active: Optional[bool] = Field(None, description="Indicates if the plugin is active or not.")
    tenant: Optional[Union[str, None]] = Field(None)
    _validate_tenant = field_validator("tenant")(validate_tenant)
    pollIntervalUnit: Optional[PollIntervalUnit] = Field(
        None,
        description="Unit of the pollInterval parameter.",
    )
    pollInterval: Optional[int] = Field(None, description="Polling interval.")
    sslValidation: Optional[bool] = Field(
        None,
        description="Indicates if the SSL certificate validation is enabled or not.",
    )
    _validate_poll_interval = field_validator("pollInterval")(
        validate_poll_interval
    )
    parameters: Optional[dict] = Field(None, description="Parameters of the plugin.")


class ConfigurationDelete(BaseModel):
    """The configuration delete model."""

    name: str = Field(...)
    keepData: bool = Field(False)

    @field_validator("name")
    def validate_configuration_exists(cls, v: str):
        """Validate configuration name."""
        if (
            connector.collection(Collections.CFC_CONFIGURATIONS).find_one({"name": v})
            is None
        ):
            raise ValueError("Could not find a configuration with that name")
        return v


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
    tenant: Union[str, None] = Field(None)
    sslValidation: bool = Field(
        False,
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


class ConfigurationDB(BaseModel):
    """The database configuration model."""

    name: str
    active: bool
    tenant: Union[str, None] = Field(None)
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


class ConfigurationNameValidationIn(BaseModel):
    """The incoming configuration name validation model."""

    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
    ] = Field(
        ...,
        description="Name of the configuration.",
    )

    @field_validator("name")
    @classmethod
    def _validate_unique_name(cls, value: str):
        """Validate that the configuration name is unique."""
        if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9 _\-]*[a-zA-Z0-9])*$", value):
            raise ValueError(
                (
                    "Configuration name should start and end with an alpha-numeric character "
                    "and can include alpha-numeric characters, dashes, underscores and spaces."
                )
            )
        if (
            connector.collection(Collections.CFC_CONFIGURATIONS).find_one({"name": value})
            is not None
        ):
            raise ValueError(f"Configuration with name '{value}' already exists.")
        return value


class ConfigurationValidationIn(BaseModel):
    """The incoming configuration validation model."""

    configuration_edit: bool = Field(False)
    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
    ] = Field(
        ...,
        description="Name of the configuration.",
    )

    @field_validator("name")
    @classmethod
    def _validate_config_name(cls, value: str, info: FieldValidationInfo):
        """Validate that the configuration name is unique."""
        if not info.data.get("configuration_edit", False):
            if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9 _\-]*[a-zA-Z0-9])*$", value):
                raise ValueError(
                    (
                        "Configuration name should start and end with an alpha-numeric character "
                        "and can include alpha-numeric characters, dashes, underscores and spaces."
                    )
                )
            if (
                connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
                    {"name": value}
                )
                is not None
            ):
                raise ValueError(f"Configuration with name '{value}' already exists.")
            return value
        return value
    active: bool = Field(True, description="Indicates if the plugin is active or not.")
    pollIntervalUnit: PollIntervalUnit = Field(
        PollIntervalUnit.SECONDS,
        description="Unit of the pollInterval parameter.",
    )
    pollInterval: int = Field(60, description="Polling interval.")
    _validate_poll_interval = field_validator("pollInterval")(validate_poll_interval)
    parameters: dict = Field(dict(), description="Parameters of the plugin.")
    sslValidation: bool = Field(True)


class DirectoryConfigurationOut(BaseModel):
    """The outgoing directory configuration validation model."""

    success: bool = Field(..., description="Indicates if directory configuration is valid.")
    message: str = Field(..., description="Descriptive message for the validation.")
    data: Dict = Field(..., description="Directory configuration data with validation result.")


class DirectoryConfigurationMetadataOut(BaseModel):
    """The outgoing directory configuration metadata model."""

    data: List[Dict] = Field(..., description="Directory configuration metadata.")
    filesCount: Dict = Field(..., description="Files count details.")
    filesSize: Dict = Field(..., description="Files size details.")
