"""Provides logging related models."""

from enum import Enum
from datetime import datetime
from typing import Union
from pydantic import BaseModel, Field


class LogType(str, Enum):
    """Log type enumerations."""

    INFO = "info"
    ERROR = "error"
    WARNING = "warning"
    DEBUG = "debug"


class Log(BaseModel):
    """The outgoing log model."""

    id: Union[str, None] = Field(None)
    message: str
    ce_log_type: LogType = Field(LogType.INFO, alias="type")
    createdAt: datetime = Field(datetime.now())
    errorCode: Union[str, None] = Field(None)
    details: Union[str, None] = Field(None)
    resolution: Union[str, None] = Field(None)

    class Config:
        """Config class for the Log model."""

        validate_by_name = True
