"""Models for LLM provider configuration."""

import re
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, StringConstraints, ValidationInfo, field_validator
from typing_extensions import Annotated


class LLMProviderDB(BaseModel):
    """Persisted LLM provider configuration model."""

    name: str
    plugin: str
    parameters: dict = Field(default_factory=dict)
    storage: dict = Field(default_factory=dict)
    active: bool = Field(True)
    sslValidation: bool = Field(True)
    updatedAt: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class LLMProviderIn(BaseModel):
    """Incoming create payload for an LLM provider configuration."""

    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
    ]
    plugin: str = Field(...)
    parameters: dict = Field(default_factory=dict)
    active: bool = Field(True)
    sslValidation: bool = Field(True)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9 _\-]*[a-zA-Z0-9])*$", v):
            raise ValueError(
                "Name should start and end with an alpha-numeric character "
                "and can include alpha-numeric characters, dashes, underscores and spaces."
            )
        return v

    @field_validator("parameters")
    @classmethod
    def _validate_parameters(cls, value):
        if not isinstance(value, dict):
            raise ValueError("Parameters should be a dictionary.")
        return value


class LLMProviderUpdate(BaseModel):
    """Incoming update payload for an LLM provider configuration."""

    plugin: Optional[str] = Field(None)
    parameters: Optional[dict] = Field(None)
    active: Optional[bool] = Field(None)
    sslValidation: Optional[bool] = Field(None)

    @field_validator("parameters")
    @classmethod
    def _validate_parameters(cls, value):
        if value is None:
            return value
        if not isinstance(value, dict):
            raise ValueError("Parameters should be a dictionary.")
        return value


class LLMProviderValidateIn(BaseModel):
    """Incoming validation payload for testing provider credentials."""

    plugin: str = Field(...)
    parameters: dict = Field(default_factory=dict)
    sslValidation: bool = Field(True)

    @field_validator("parameters")
    @classmethod
    def _validate_parameters(cls, value):
        if not isinstance(value, dict):
            raise ValueError("Parameters should be a dictionary.")
        return value


class LLMProviderOut(BaseModel):
    """Outgoing LLM provider configuration model."""

    name: str
    plugin: str
    parameters: dict = Field(default_factory=dict)
    active: bool = Field(True)
    sslValidation: bool = Field(True)
    updatedAt: datetime

    @field_validator("parameters")
    @classmethod
    def _remove_sensitive_parameters(cls, val: dict, info: ValidationInfo) -> dict:
        from netskope.common.utils.plugin_helper import PluginHelper

        plugin_path = (info.data or {}).get("plugin")
        if not plugin_path:
            raise ValueError("Invalid configuration")
        plugin_class = PluginHelper().find_by_id(plugin_path)
        if plugin_class is None:
            return val

        metadata = plugin_class.metadata
        if metadata and metadata.get("configuration"):
            for field in metadata["configuration"]:
                if field.get("type", "") == "password":
                    val.pop(field["key"], None)
        return val
