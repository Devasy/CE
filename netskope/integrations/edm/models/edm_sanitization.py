"""Provides sanitization related models."""
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, constr


class EDMSanitizationConfigurationIn(BaseModel):
    """Incoming configurations for Sanitization."""

    name: constr(strip_whitespace=True, min_length=1, max_length=215) = Field(
        ...,
        description=(
            "Name of the plugin configuration "
            "or CSV file for which sanitization is being performed."
        ),
    )
    parameters: dict = Field(
        ..., description="Parameters of the plugin or manual CSV upload."
    )
    pluginType: Optional[str] = Field(
        None,
        description=(
            "Additional parameter defining type of plugin. Currently being used for"
            + " Netskope EDM Forwarder/Receiver plugin, for forwarder or receiver."
        ),
    )


class EDMSanitizationConfigurationOut(BaseModel):
    """Outgoing configurations for Sanitization."""

    name: str = Field(..., description="Name of configuring Plugin.")
    sanitizationStatus: bool = Field(..., description="Sanitization Status")
    message: str = Field(..., description="Sanitization message")


class EDMSanitizedFileType(str, Enum):
    """File to return from get_sanitization endpoint."""

    GOOD = "good"
    BAD = "bad"


class EDMSanitizedDataType(str, Enum):
    """Type of Data used for sanitization."""

    SAMPLE = "sample"


class EDMSanitizedSourceType(str, Enum):
    """Type of the source providing EDM Sanitization Files."""

    PLUGIN = "plugin"
    MANUAL_UPLOAD = "manual_upload"
