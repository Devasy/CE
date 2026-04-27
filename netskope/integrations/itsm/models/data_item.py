"""ITSM alert and event related models."""

import json
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, AliasChoices
from typing import List, Union
from jsonschema import validate, ValidationError

from netskope.common.utils import DBConnector, parse_dates
from netskope.integrations.itsm.utils import alert_event_query_schema

connector = DBConnector()


def validate_query(cls, v):
    """Validate the query."""
    try:
        STATIC_DICT, RAW_DICT, QUERY_SCHEMA = alert_event_query_schema()
        FIELDS = list(STATIC_DICT.keys()) + list(RAW_DICT.keys())
        validate(v, QUERY_SCHEMA)
    except ValidationError as ex:
        raise ValueError(f"Invalid query provided. {ex.message}.")
    except Exception:
        raise ValueError("Could not parse the query.")
    return json.loads(
        json.dumps(v),
        object_hook=lambda pair: parse_dates(pair, FIELDS),
    )


class DataType(str, Enum):
    """Data type enum."""

    ALERT = "alert"
    EVENT = "event"

    @classmethod
    def choices(cls):
        """Choices."""
        return [(data_type.value, data_type.name) for data_type in cls]


class Alert(BaseModel):
    """Alert base model."""

    id: str = Field(...)
    configuration: Union[str, None] = Field(None)
    alertName: str = Field(...)
    alertType: str = Field(...)
    app: Union[str, None] = Field(None)
    appCategory: Union[str, None] = Field(None)
    user: Union[str, None] = Field(None)
    type: str = Field(...)
    timestamp: datetime = Field(...)
    rawData: dict = Field({}, validation_alias=AliasChoices("rawData", "rawAlert"))

    @property
    def rawAlert(self):
        """Raw alert."""
        return self.rawData

    @rawAlert.setter
    def rawAlert(self, value):
        """Set raw alert."""
        self.rawData = value


class Event(BaseModel):
    """Event base model."""

    id: str = Field(...)
    configuration: Union[str, None] = Field(None)
    eventType: str = Field(...)
    user: Union[str, None] = Field(None)
    timestamp: datetime = Field(...)
    rawData: dict = Field({}, validation_alias=AliasChoices("rawData", "rawAlert"))

    @property
    def rawAlert(self):
        """Raw alert."""
        return self.rawData

    @rawAlert.setter
    def rawAlert(self, value):
        """Set raw alert."""
        self.rawData = value


class QueryLocator(BaseModel):
    """Query locator class."""

    query: str


class ValueLocator(BaseModel):
    """Selector based on ids."""

    ids: List[str]
