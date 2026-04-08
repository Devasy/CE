"""Provides other models."""

from typing import List, Union, Dict, Any
from enum import Enum
from datetime import datetime
from pydantic import Field
from pydantic import BaseModel


class Token(BaseModel):
    """Access token model for OAuth 2.0."""

    access_token: str
    token_type: str
    scopes: List[str]
    firstLogin: bool
    passwordPolicyViolation: bool
    passwordPolicy: Union[Dict[str, Any], None] = Field(None)


class ErrorMessage(BaseModel):
    """Error message model."""

    detail: str


class NotificationType(str, Enum):
    """Notification type enumerations."""

    INFO = "info"
    ERROR = "error"
    WARNING = "warning"
    BANNER_INFO = "banner_info"
    BANNER_ERROR = "banner_error"
    BANNER_WARNING = "banner_warn"


class Notification(BaseModel):
    """The outgoing log model."""

    id: Union[str, None] = Field(None)
    message: str
    type: NotificationType = Field(NotificationType.INFO)
    createdAt: datetime
    acknowledged: bool = Field(False)
    is_promotion: bool = Field(False)


class PollIntervalUnit(str, Enum):
    """Enumeration for Poll interval units."""

    SECONDS = "seconds"
    MINUTES = "minutes"
    HOURS = "hours"
    DAYS = "days"


class StatusType(str, Enum):
    """Status type enumerations."""

    COMPLETED = "completed"
    INPROGRESS = "inprogress"
    INQUEUE = "inqueue"
    ERROR = "error"


class TaskStatus(BaseModel):
    """Task status related model."""

    name: str
    status: StatusType = Field(StatusType.INQUEUE)
    startedAt: datetime
    completedAt: Union[datetime, None] = Field(None)
    args: str


class NetskopeFieldType(str, Enum):
    """Netskope field type."""

    ALERT = "alert"
    EVENT = "event"
    WEBTX = "webtx"


class FieldDataType(str, Enum):
    """Netskope field datatype."""

    TEXT = "text"
    NUMBER = "number"
    DATETIME = "datetime"
    BOOLEAN = "boolean"


class NetskopeField(BaseModel):
    """Netskope field model."""

    name: str
    label: str
    type: NetskopeFieldType
    dataType: FieldDataType


class ActionType(str, Enum):
    """Action type on configuration."""

    DISABLE = "disable"
    DELETE = "delete"
