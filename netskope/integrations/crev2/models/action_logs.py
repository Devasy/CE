"""Models related to performed action."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel

from .business_rules import Action


class ActionLogStatus(str, Enum):
    """Action log status enum."""

    SUCCESS = "success"
    FAILED = "failed"
    DECLINED = "declined"
    PENDING_APPROVAL = "pending_approval"
    SCHEDULED = "scheduled"


class RevertActionStatus(str, Enum):
    """Revert action log status enum."""

    SUCCESS = "success"
    FAILED = "failed"


class RevertActionDict(BaseModel):
    """Revert action log model."""

    revertActionStatus: Optional[RevertActionStatus] = None
    revertPerformedAt: Optional[datetime] = None


class ActionLogDB(BaseModel):
    """Action log model."""

    entity: str
    record: dict
    action: Action
    rule: str
    configuration: str
    status: ActionLogStatus
    performedAt: datetime
    revertActionParameters: Optional[RevertActionDict] = None


class ActionBulkAction(str, Enum):
    """Bulk actions."""

    APPROVE = "approve"
    DECLINE = "decline"


class ActionQueryLocator(BaseModel):
    """Action query locator."""

    query: str


class ActionValueLocator(BaseModel):
    """Action value locator."""

    values: list[str]
