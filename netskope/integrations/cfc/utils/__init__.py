# flake8: noqa
"""Utility modules."""

from datetime import datetime
from typing import List, Optional

import dateutil.parser

from netskope.common.utils import parse_dates as common_parse_dates
from .constants import (
    FILE_PATH,
    FILE_TYPES_SUPPORTED,
    IMAGE_EXTENSION_SUPPORTED,
    MANUAL_UPLOAD_PATH,
    MAX_PENDING_STATUS_TIME,
    MANUAL_UPLOAD_TASK_DELAY_TIME,
    MANUAL_UPLOAD_PREFIX,
)
from .exceptions import CustomException
from .netskope_client import NetskopeClientCFC
from .schema import IMAGE_METADATA_QUERY_SCHEMA
from .task_status import (
    complete_task,
    set_task_status,
)
from .tenant_helper import TenantHelper
from .validators import validate_poll_interval
from .statistics import increment_count
from .hashing import create_hashes


def parse_datetime(value: str) -> Optional[datetime]:
    """Parse objects and converts datetime to datetime objects.

    Args:
        value (str): value from query.

    Returns:
        Optional[datetime]: converted datetime object.
    """
    try:
        if type(value) is str and not value.strip("./:- ").isdigit():
            return dateutil.parser.parse(value).replace(tzinfo=None)
        return None
    except (ValueError, TypeError):
        return None


def parse_dates(*args, **kwargs):
    return common_parse_dates(*args, **kwargs, parse_datetime=parse_datetime)
