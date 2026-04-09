"""Provides user related models."""

from typing import List, Union
from datetime import datetime
from enum import Enum
from pydantic import field_validator, BaseModel, Field

from netskope.common.utils import DBConnector, Collections, bcrypt_utils
from netskope.common.utils.password_validator import (
    validate_password_against_policy,
)

connector = DBConnector()


class User(BaseModel):
    """Auth token model."""

    username: str
    scopes: List[str]
    firstLogin: bool = Field(True)
    columns: dict = Field({})
    fromSSO: bool = Field(False)


class SecurityScopes(str, Enum):
    """Enumeration for allowed scopes."""

    admin = "admin"
    api = "api"
    me = "me"
    cte_read = "cte_read"
    cte_write = "cte_write"
    cto_read = "cto_read"
    cto_write = "cto_write"
    cre_read = "cre_read"
    cre_write = "cre_write"
    cls_read = "cls_read"
    cls_write = "cls_write"
    edm_read = "edm_read"
    edm_write = "edm_write"
    cfc_read = "cfc_read"
    cfc_write = "cfc_write"
    logs = "logs"
    settings_read = "settings_read"
    settings_write = "settings_write"


class UserOut(BaseModel):
    """The outgoing user model."""

    username: str = Field(...)
    scopes: List[SecurityScopes] = Field(...)


def _validate_scopes(scopes: List[SecurityScopes]):
    """Validate security scopes."""
    # if SecurityScopes.admin in scopes:
    #     raise ValueError("can not create new user with scope 'admin'")
    if SecurityScopes.me not in scopes:
        scopes.append(SecurityScopes.me)


class UserIn(BaseModel):
    """The incoming user model."""

    username: str = Field(...)
    password: str = Field(...)
    scopes: List[SecurityScopes] = Field(...)

    @field_validator("username")
    @classmethod
    def username_must_be_unique(cls, v: str):
        """Validate that the username is unique."""
        if (
            connector.collection(Collections.USERS).find_one({"username": v})
            is not None
        ):
            raise ValueError("Username already in use.")
        return v

    @field_validator("scopes")
    @classmethod
    def scope_validation(cls, v: List[SecurityScopes]):
        """Validate scope."""
        _validate_scopes(v)
        return v

    @field_validator("password")
    @classmethod
    def hash_password(cls, v: str):
        """Validate password."""
        is_valid, errors = validate_password_against_policy(v)
        if not is_valid:
            error_message = ", ".join(errors) if errors else "Password does not meet policy requirements"
            raise ValueError(error_message)
        return bcrypt_utils.hash_password(v)


class UserUpdate(BaseModel):
    """The update user model."""

    username: str = Field(...)
    password: Union[str, None] = Field(None)
    scopes: Union[List[SecurityScopes], None] = Field(None)

    @field_validator("username")
    @classmethod
    def username_must_exist(cls, v: str):
        """Validate that the username exists."""
        user = connector.collection(Collections.USERS).find_one({"username": v})
        if user is None:
            raise ValueError("Username does not exist.")
        elif user.get("username") == "admin":
            raise ValueError("Can not modify admin user.")
        return v

    @field_validator("scopes")
    @classmethod
    def scope_validation(cls, v: List[SecurityScopes]):
        """Validate scope."""
        if v is None:
            return v
        _validate_scopes(v)
        return v

    @field_validator("password")
    @classmethod
    def hash_password(cls, v: str):
        """Validate password."""
        if v is None:
            return v
        is_valid, errors = validate_password_against_policy(v)
        if not is_valid:
            error_message = ", ".join(errors) if errors else "Password does not meet policy requirements"
            raise ValueError(error_message)
        return bcrypt_utils.hash_password(v)


class UserDelete(BaseModel):
    """The delete user model."""

    username: str = Field(...)

    @field_validator("username")
    @classmethod
    def username_must_exist(cls, v: str):
        """Validate that the username exists."""
        user = connector.collection(Collections.USERS).find_one({"username": v})
        if user is None:
            raise ValueError("Username does not exist")
        elif user.get("username") == "admin":
            raise ValueError("Can not delete admin user")
        return v


class UserDB(BaseModel):
    """The database user model."""

    username: str = Field(...)
    password: Union[str, None] = Field(None)
    scopes: List[str] = []
    tokens: List = []
    firstLogin: bool = Field(True)
    columns: dict = Field({})
    sso: bool = Field(False)
    consecutiveFailedAttempts: int = Field(0)
    credentialDownTime: Union[datetime, None] = Field(None)


class TokenDelete(BaseModel):
    """Token delete model."""

    client_id: str = Field(...)
