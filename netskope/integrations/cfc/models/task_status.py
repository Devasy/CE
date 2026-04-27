"""Provides task status related models."""
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class StatusType(str, Enum):
    """Status type enumerations."""

    SCHEDULED = "scheduled"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIALLY_COMPLETED = "partially_failed"
    GENERATING_HASH = "generating_hash"
    UPLOADING_HASH = "uploading_hash"
    PENDING = "pending"
    FILE_UPLOAD_FAILED = "file_upload_failed"


class CFCTaskType(str, Enum):
    """EDM Task Type enum which can be plugin or manual."""

    PLUGIN = "plugin"
    MANUAL = "manual"


class CFCPluginTask(BaseModel):
    """CFC Plugin Task for task status."""

    id: str
    source: str
    target: str
    status: StatusType
    updatedAt: Optional[datetime] = Field(
        None,
        description="Updated at time"
    )
    sharedAt: Optional[datetime] = Field(
        None,
        description="Successfully Shared at time"
    )
    createdAt: datetime = Field(
        None,
        description="Configuration creation time"
    )


class CFCManualTask(BaseModel):
    """CFC Manual Task for task status."""

    id: str
    source: str
    target: str
    status: StatusType
    classifierName: str
    trainingType: str
    sharedAt: Optional[datetime] = Field(
        None,
        description="Successfully Shared at time"
    )
    has_failed_uploads: bool = Field(
        False,
        description="Defines that if there are any failed uploads for the task"
    )
    createdAt: datetime = Field(
        None,
        description="Configuration creation time"
    )
