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
    GENERATING_HASH = "generating_hash"
    UPLOADING_HASH = "uploading_hash"
    UPLOAD_COMPLETED = "upload_completed"
    CHECKING_APPLY_STATUS = "checking_apply_status"
    APPLY_IN_PROGRESS = "apply_in_progress"


class EDMTaskType(str, Enum):
    """EDM Task Type enum which can be plugin or manual."""

    PLUGIN = "plugin"
    MANUAL = "manual"


class EDMTask(BaseModel):
    """EDM Task for task status."""

    id: str
    name: str
    target: str
    status: StatusType
    taskType: EDMTaskType = Field(EDMTaskType.PLUGIN)
    sharedAt: Optional[datetime] = Field(None)
    updatedAt: Optional[datetime] = Field(None)
