"""Data Batch Models."""

from pydantic import BaseModel
from datetime import datetime
from typing import List, Dict, Optional
from enum import Enum


class CLSSIEMCountType(str, Enum):
    """CLS SIEM Status Type."""

    FILTERED = "filtered"
    TRANSFORMED = "transformed"
    INGESTED = "ingested"


class CLSSIEMStatusType(str, Enum):
    """CLS SIEM Status Type."""

    PENDING = "pending"
    FILTERED = "filtered"
    FILTERING = "filtering"
    TRANSFORMED = "transformed"
    TRANSFORMING = "transforming"
    INGESTED = "ingested"
    INGESTING = "ingesting"
    ERROR = "error"


class BatchDataType(str, Enum):
    """CLS SIEM Data Type."""

    ALERTS = "alerts"
    EVENTS = "events"
    LOGS = "logs"
    WEBTX = "webtx"
    WEBTX_BLOBS = "webtx_blobs"


class BatchDataSourceType(str, Enum):
    """CLS SIEM Data Source Type."""

    TENANT = "tenant"
    CONFIGURATION = "configuration"


class CLSSIEM(BaseModel):
    """CLS Model."""

    source: str
    destination: str
    status: str
    updated_at: datetime
    count: Dict[CLSSIEMCountType, int]
    size: Dict[CLSSIEMCountType, int]
    error_on: Optional[CLSSIEMStatusType] = None


class DataBatch(BaseModel):
    """Data Batch Model."""

    id: str
    type: BatchDataType
    sub_type: Optional[str] = None
    count: int
    size: Optional[int] = None
    data_source: str
    data_source_type: BatchDataSourceType
    createdAt: datetime
    cls: List[CLSSIEM] = []
