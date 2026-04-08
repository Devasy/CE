"""EDM Hashes Status related schemas."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from netskope.common.utils import (
    DBConnector,
    Logger,
)

from .task_status import EDMTaskType

connector = DBConnector()
logger = Logger()


class EDMHashesStatus(BaseModel):
    """EDM Hashes Status model."""

    fileSourceType: EDMTaskType = Field(EDMTaskType.PLUGIN)
    fileSourceID: str = Field(..., validate_default=True)
    fileUploadedAtTenant: str = Field(...)
    file_id: str = Field(..., validate_default=True)
    upload_id: str = Field(..., validate_default=True)
    message: Optional[str] = Field(None)
    createdAt: Optional[datetime] = Field(default=None, validate_default=True)
    updatedAt: Optional[datetime] = Field(default=None, validate_default=True)
