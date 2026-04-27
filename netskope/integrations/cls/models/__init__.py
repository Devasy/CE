# flake8: noqa
"""CLS models."""
from typing import List, Union
from datetime import datetime

from pydantic import Field

from .business_rule import (
    BusinessRuleOut,
    BusinessRuleIn,
    BusinessRuleUpdate,
    BusinessRuleDelete,
    BusinessRuleDB,
    Filters,
)
from .plugin import (
    ConfigurationIn,
    ConfigurationOut,
    ConfigurationUpdate,
    Plugin,
    ConfigurationDB,
    ConfigurationDelete,
    PollIntervalUnit,
)

from .mappings import MappingDB


class Batch:
    """Batch related data."""

    destination: str
    messages: List
    fields: List = []
    started_at: datetime
    size: int  # in bytes
    limit_size: int  # in MBs
    limit_time: int  # in seconds
    isSIEM: bool = Field(False)
    rule: Union[str, None] = Field(None)
