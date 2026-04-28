"""Repo related models."""
import re
from typing import Dict, List, Optional, Literal

from ..utils import DBConnector, Collections
from pydantic import field_validator, BaseModel, HttpUrl, Field, model_validator


connector = DBConnector()


class PluginRepo(BaseModel):
    """Plugin repo model."""

    name: str
    url: str
    repoType: Literal["public", "private"] = Field("public")
    username: Optional[str] = Field(None)
    password: Optional[str] = Field(None)
    isDefault: bool = Field(False)
    plugins: Dict = Field({})
    plugin_migrates: List = Field([])


class PluginRepoIn(BaseModel):
    """Plugin repo model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def check_unique_name(cls, v):
        """Make sure that repo name is not already in use."""
        if len(v) < 1 or len(v) > 100:
            raise ValueError("Name has to be 1-100 characters long.")
        elif not re.match("^[a-zA-Z][a-zA-Z0-9_]*$", v):
            raise ValueError(
                "Name can only consist of alpha-numeric characters starting with alphabet."
            )
        elif v == "Netskope":
            raise ValueError("Can not create plugin repo with Netskope name.")
        elif (
            connector.collection(Collections.PLUGIN_REPOS).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"Repo name {v} is already in use.")
        return v

    url: str

    @field_validator("url")
    def validate_url(cls, v):
        """Validate plugin repository url."""
        try:
            _ = HttpUrl(v)
        except Exception:
            raise ValueError("Error: invalid or missing URL scheme")
        return v

    repoType: Literal["public", "private"] = Field("public")
    username: Optional[str] = Field(None)
    password: Optional[str] = Field(None)

    @model_validator(mode="after")
    def validate_credentials(self):
        """Validate that private repos have credentials."""
        if self.repoType == "private":
            if not self.username or not self.password:
                raise ValueError("Username and password are required for private repositories.")
        return self

    isDefault: bool = Field(False)
    plugins: Dict = Field({})
    plugin_migrates: List = Field([])


class PluginRepoUpdate(BaseModel):
    """Plugin repo model."""

    name: str

    @field_validator("name")
    @classmethod
    def check_existing_name(cls, v):
        """Make sure that repo name is not already in use."""
        if v == "Default":
            raise ValueError("Can not edit plugin repo with Default name.")
        elif (
            connector.collection(Collections.PLUGIN_REPOS).find_one({"name": v}) is None
        ):
            raise ValueError(f"Repo with name {v} does not exist.")
        return v

    url: str

    @field_validator("url")
    def validate_url(cls, v):
        """Validate plugin repository url."""
        try:
            _ = HttpUrl(v)
        except Exception:
            raise ValueError("Error: invalid or missing URL scheme")
        return v

    repoType: Literal["public", "private"] = Field("public")
    username: Optional[str] = Field(None)
    password: Optional[str] = Field(None)

    @model_validator(mode="after")
    def validate_credentials(self):
        """Validate that private repos have credentials."""
        if self.repoType == "private":
            if not self.username or not self.password:
                raise ValueError("Username and password are required for private repositories.")
        return self

    plugins: Dict = Field({})
    plugin_migrates: List = Field([])


class PluginRepoOut(BaseModel):
    """Outbound plugin repo model."""

    name: str
    url: str = Field("")
    repoType: str = Field("public")
    username: Optional[str] = Field(None)
    hasUpdates: bool = False
    isDefault: bool = Field(False)
    plugins: Dict = Field({})
    plugin_migrates: List = Field([])
