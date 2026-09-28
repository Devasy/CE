"""ITSM alert and event related models."""

import json
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, AliasChoices, field_serializer
from typing import Any, List, Union
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


# Types pydantic can already put in a JSON response as-is. Anything else in
# rawData is stringified by _json_safe_raw_data.
_JSON_NATIVE_TYPES = (str, int, float, bool, datetime, type(None))


def _json_safe_raw_data(value: Any) -> Any:
    """Recursively coerce a rawData value into something JSON can represent.

    ``rawData`` is an untyped dict, so pydantic serializes its values with the
    "any" serializer: a value of a type it does not know (a BSON ``ObjectId``
    from a joined document, a ``Decimal128``, ``Binary``) raises
    PydanticSerializationError. That happens while FastAPI encodes the response,
    i.e. AFTER the endpoint has returned, so the list endpoints' own
    ``except Exception`` cannot see it and the request surfaces as a bare 500
    from the platform middleware ("Error occurred while processing request")
    instead of a diagnosable error — with the count call (``aggregate=true``)
    still succeeding, since it never touches rawData.

    Producers are expected to store JSON-friendly rawData, but they are spread
    across CTO providers plus the CRE and CTE alert generators, and alerts
    already persisted with a stray BSON value would keep the Alerts page broken
    for good. Degrading such a value to its string form keeps the page readable
    instead.
    """
    if isinstance(value, dict):
        return {str(key): _json_safe_raw_data(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_raw_data(item) for item in value]
    if isinstance(value, _JSON_NATIVE_TYPES):
        return value
    return str(value)


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

    # JSON only: ``model_dump()`` (python mode) is what stores the alert and what
    # feeds ticket field mapping, and must keep the values' real types.
    @field_serializer("rawData", when_used="json")
    def _serialize_raw_data(self, raw_data: dict) -> dict:
        """Make rawData safe to put in an API response."""
        return _json_safe_raw_data(raw_data)

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

    @field_serializer("rawData", when_used="json")
    def _serialize_raw_data(self, raw_data: dict) -> dict:
        """Make rawData safe to put in an API response."""
        return _json_safe_raw_data(raw_data)

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
