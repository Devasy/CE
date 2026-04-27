"""ITSM task related models."""

from enum import Enum
from datetime import datetime
from pydantic import StringConstraints, BaseModel, Field, field_validator
from typing import List, Union, Optional
import json
from jsonschema import validate, ValidationError
from pydantic.functional_validators import BeforeValidator
from netskope.common.utils import parse_dates, DBConnector

from . import Alert, Event, DataType
from ..utils import task_query_schema

from typing_extensions import Annotated


connector = DBConnector()


def validate_id(cls, v, values):
    """Validate id."""
    values = values.data
    if v is None and values["status"] == TaskStatus.FAILED:
        return None
    return v


class Severity(str, Enum):
    """Severity enumeration."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    OTHER = "other"
    INFO = "informational"


class TaskStatus(str, Enum):
    """Tasks status enumeration."""

    NEW = "new"
    IN_PROGRESS = "in_progress"
    ON_HOLD = "on_hold"
    CLOSED = "closed"
    OTHER = "other"
    DELETED = "deleted"
    NOTIFICATION = "notification"
    FAILED = "failed"


class TaskBulkAction(str, Enum):
    """Task bulk actions."""

    APPROVE = "approve"
    DECLINE = "decline"


class TaskRequestStatus(str, Enum):
    """Tasks request status enumeration."""

    DECLINED = "declined"
    PENDING = "pending_approval"
    SCHEDULED = "scheduled"
    APPROVED = "approved"
    NOT_REQUIRED = "not_required"


class SyncStatus(str, Enum):
    """Tasks status enumeration."""

    IN_PROGRESS = "in_progress"
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


def check_object_id(value):
    """Validate object id."""
    if value is None:
        return None
    return str(value)


class UpdatedTaskValues(BaseModel):
    """Updated task values model."""

    status: Union[str, TaskStatus, None] = Field(None)
    oldStatus: Union[str, TaskStatus, None] = Field(None)
    assignee: Union[Annotated[str, StringConstraints(strip_whitespace=True)], None] = (
        Field(None)
    )
    oldAssignee: Union[
        Annotated[str, StringConstraints(strip_whitespace=True)], None
    ] = Field(None)
    severity: Union[str, Severity, None] = Field(None)
    oldSeverity: Union[str, Severity, None] = Field(None)


class Task(BaseModel):
    """Task base model."""

    status: Optional[Union[str, TaskStatus]] = Field(None)
    approvalStatus: TaskRequestStatus = Field(TaskRequestStatus.NOT_REQUIRED)
    severity: Union[str, Severity, None] = Field(None)
    id: Union[str, None] = Field(None)
    _validate_id = field_validator("id")(validate_id)
    dedupeCount: int = Field(0)
    link: Annotated[str, StringConstraints(strip_whitespace=True)] = Field("")
    deletedAt: Union[datetime, None] = Field(None)
    configuration: Union[
        Annotated[str, StringConstraints(strip_whitespace=True)], None
    ] = Field(None)
    createdAt: Union[datetime, None] = Field(None)
    businessRule: Union[str, None] = Field(None)
    dataItem: Union[Alert, Event, None] = Field(None)
    internalId: Union[Annotated[str, BeforeValidator(check_object_id)], None] = Field(
        None, alias="_id"
    )
    evaluationResultTime: Union[datetime, None] = Field(None)
    dataType: Union[DataType, None] = Field(DataType.ALERT)
    dataSubType: Union[str, None] = Field(None)
    lastSyncedAt: Union[datetime, None] = Field(None)
    lastUpdatedAt: Union[datetime, None] = Field(None)
    updatedValues: Union[UpdatedTaskValues, None] = Field(None)
    syncStatus: SyncStatus = Field(SyncStatus.PENDING)


def validate_query(cls, v):
    """Validate the query."""
    try:
        STATIC_DICT, DATAITEM_DICT, TASK_QUERY_SCHEMA = task_query_schema()
        validate(v, TASK_QUERY_SCHEMA)
    except ValidationError as ex:
        raise ValueError(f"Invalid query provided. {ex.message}.")
    except Exception:
        raise ValueError("Could not parse the query.")
    TASK_STRING_FIELDS = list(STATIC_DICT.keys()) + list(DATAITEM_DICT.keys())
    return json.loads(
        json.dumps(v),
        object_hook=lambda pair: parse_dates(pair, TASK_STRING_FIELDS),
    )


class TaskQueryLocator(BaseModel):
    """Task query locator class."""

    query: str


class TaskValueLocator(BaseModel):
    """Task selector based on ids."""

    ids: List[str]
