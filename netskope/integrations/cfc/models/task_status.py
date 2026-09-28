"""Provides task status related models."""
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class StatusType(str, Enum):
    """Status type enumerations."""

    SCHEDULED = "scheduled"  # Task is scheduled to run
    PENDING = "pending"  # Waiting for files to be uploaded or task to start
    GENERATING_HASH = "generating_hash"  # Currently generating CFC hashes
    UPLOADING_HASH = "uploading_hash"  # Currently uploading hashes to destination
    SUCCESS = "success"  # All files uploaded and shared successfully
    PARTIAL_SUCCESS = "partial_success"  # All files uploaded, but some sharing failed
    COMPLETED = "completed"  # Pull and share both completed successfully
    PARTIALLY_COMPLETED = "partially_failed"  # Partial pull from source, but shared what was pulled
    FAILED = "failed"  # Operation failed completely
    FILE_UPLOAD_FAILED = "file_upload_failed"  # File upload failed


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
