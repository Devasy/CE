"""Provides indicator related models."""

from enum import Enum
from urllib.parse import urlparse
import json
from typing import List, Optional, Union
from datetime import datetime
from jsonschema import validate, ValidationError
from pydantic import field_validator, StringConstraints, ConfigDict, BaseModel, Field, BeforeValidator

from netskope.common.utils import DBConnector, Collections, parse_dates

from ..utils.schema import (
    INDICATOR_QUERY_SCHEMA,
    INDICATOR_STRING_FIELDS,
)
from typing_extensions import Annotated

connector = DBConnector()


def validate_tag(v):
    """Validate that the tag exist."""
    if connector.collection(Collections.TAGS).find_one({"name": v}) is None:
        raise ValueError(f"Tag '{v}' does not exist.")
    return v


def validate_query(cls, v):
    """Validate the query."""
    try:
        validate(v, INDICATOR_QUERY_SCHEMA)
    except ValidationError as ex:
        raise ValueError(f"Invalid query provided. {ex.message}.")
    except Exception:
        raise ValueError("Could not parse the query.")
    return json.loads(
        json.dumps(v),
        object_hook=lambda pair: parse_dates(pair, INDICATOR_STRING_FIELDS),
    )


def validate_url(cls, v):
    """Validate URL."""
    v = v.strip()
    if len(v) == 0:
        return v
    parsed = urlparse(v)
    if parsed.scheme not in ["http", "https"]:
        raise ValueError("URL scheme not supported.")
    if not parsed.netloc.strip():
        raise ValueError("Invalid URL provided.")
    return v


class IndicatorType(str, Enum):
    """The indicator type enumerations."""

    URL = "url"
    SHA256 = "sha256"
    MD5 = "md5"

    IPV4 = "ipv4"
    IPV6 = "ipv6"
    HOSTNAME = "hostname"
    DOMAIN = "domain"
    FQDN = "fqdn"


class SeverityType(str, Enum):
    """The severity type enumerations."""

    UNKNOWN = "unknown"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IndicatorQueryLocator(BaseModel):
    """Indicator query locator class.

    The query parameter can be either a JSON string or a dictionary.
    If a string is provided, it will be parsed as JSON.
    """

    query: str


class IndicatorValueLocator(BaseModel):
    """Indicator selector based on values."""

    values: List[str]
    source: Union[Optional[str], None] = Field(None)


class BulkTagEdit(BaseModel):
    """Tag bulk edit class."""

    add: List[Annotated[str, BeforeValidator(validate_tag)]] = Field([])
    remove: List[Annotated[str, BeforeValidator(validate_tag)]] = Field([])


class IndicatorBulkEdit(BaseModel):
    """Indicator bulk edit class."""

    locator: Union[
        IndicatorQueryLocator, IndicatorValueLocator
    ] = IndicatorQueryLocator(query="")
    tags: Union[BulkTagEdit, None] = Field(None)


DESCRIPTIONS = {
    "value": "Value of the indicator.",
    "type": "Type of the indicator.",
    "active": "Indicates whether the indicator has expired or not.",
    "expiresAt": (
        "Timestamp after which the indicator will be marked "
        "inactive. The timestamp is in ISO 8601 UTC format."
    ),
    "firstSeen": (
        "A timestamp indicating when the indicator was first seen "
        "on the source. The timestamp is in ISO 8601 UTC format."
    ),
    "lastSeen": (
        "A timestamp indicating when the indicator was last seen "
        "on the source. The timestamp is in ISO 8601 UTC format."
    ),
    "reputation": "Confidence of information. 1 is low, 10 is high.",
    "severity": "Severity of indicator.",
    "sharedWith": (
        "List of configurations that this indicator was shared with."
    ),
    "tags": "List of tags to be applied to the indicators.",
    "test": (
        "This indicates if the indicator is a test indicator or "
        "not. Test indicators are not shared with any other plugins."
    ),
    "comments": "Additional comments.",
    "extendedInformation": "Link to an external source of information.",
    "internalHits": (
        "The number of times the indicator was reported by Netskope."
    ),
    "externalHits": (
        "The number of times the indicator was reported by non "
        "Netskope plugins."
    ),
}


class Indicator(BaseModel):
    """The indicator model for plugins."""

    value: str = Field(..., description=DESCRIPTIONS["value"])
    type: IndicatorType = Field(..., description=DESCRIPTIONS["type"])
    test: bool = Field(False)
    safe: bool = Field(False)
    expiresAt: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["expiresAt"],
    )
    firstSeen: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["firstSeen"],
    )
    lastSeen: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["lastSeen"],
    )
    reputation: int = Field(
        5,
        description=DESCRIPTIONS["reputation"],
        ge=1,
        le=10,
    )
    severity: SeverityType = Field(
        SeverityType.UNKNOWN,
        description=DESCRIPTIONS["severity"],
    )
    sharedWith: List[str] = Field(
        [],
        description=DESCRIPTIONS["sharedWith"],
    )
    tags: List[Annotated[str, BeforeValidator(validate_tag)]] = Field([], description=DESCRIPTIONS["tags"])

    active: bool = Field(True)
    comments: Annotated[str, StringConstraints(strip_whitespace=True)] = Field("", description=DESCRIPTIONS["comments"])
    extendedInformation: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        "", description=DESCRIPTIONS["extendedInformation"]
    )
    _validate_extended_information = field_validator(
        "extendedInformation"
    )(validate_url)
    retracted: bool = Field(False)
    updated: List[dict] = Field([])


class IndicatorSource(BaseModel):
    """Indicator source class."""

    firstSeen: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["firstSeen"],
    )
    lastSeen: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["lastSeen"],
    )
    reputation: int = Field(
        5,
        description=DESCRIPTIONS["reputation"],
        ge=1,
        le=10,
    )
    severity: SeverityType = Field(
        SeverityType.UNKNOWN,
        description=DESCRIPTIONS["severity"],
    )
    destinations: List[dict] = Field([])
    retractionDestinations: List[dict] = Field([])
    tags: List[str] = Field([], description=DESCRIPTIONS["tags"])
    comments: Annotated[str, StringConstraints(strip_whitespace=True)] = Field("", description=DESCRIPTIONS["comments"])
    extendedInformation: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        "", description=DESCRIPTIONS["extendedInformation"]
    )
    _validate_extended_information = field_validator(
        "extendedInformation"
    )(validate_url)
    retracted: bool = Field(False)


class IndicatorWithSources(Indicator):
    """Indicator with all sources class."""

    sources: List[IndicatorSource] = Field([])


class IndicatorGenerator:
    """Indicator generator class."""

    def __init__(self, indicators, source):
        """Initialize."""
        self._indicators = indicators
        self._source = source

    def all(self, batch_size: int = 0):
        """Get indicators."""
        indicator_list = []
        count = 0
        for indicator in self._indicators:
            source_indicator = None
            if type(indicator) is dict:
                for ioc_with_source in indicator.get("sources", []):
                    if ioc_with_source.get("source", "") == self._source:
                        indicator_source = Indicator(**indicator)
                        indicator_with_source = IndicatorSource(**ioc_with_source)

                        indicator_source.reputation = indicator_with_source.reputation
                        indicator_source.severity = indicator_with_source.severity
                        indicator_source.comments = indicator_with_source.comments
                        indicator_source.extendedInformation = indicator_with_source.extendedInformation
                        indicator_source.tags = indicator_with_source.tags
                        indicator_source.firstSeen = indicator_with_source.firstSeen
                        indicator_source.lastSeen = indicator_with_source.lastSeen

                        source_indicator = indicator_source
                        break
            else:
                source_indicator = indicator
            count += 1
            if batch_size > 0:
                indicator_list.append(source_indicator)
            else:
                yield source_indicator
            if batch_size > 0 and count >= batch_size:
                yield indicator_list
                indicator_list = []
                count = 0
        if len(indicator_list) != 0:
            yield indicator_list


class IndicatorDB(BaseModel):
    """The database indicator model."""

    value: str = Field(...)
    type: IndicatorType = Field(...)
    test: bool = Field(...)
    safe: bool = Field(False)  # if not set, default to False
    active: bool = Field(...)
    sharedWith: List[str] = Field([])
    internalHits: int = Field(
        ...,
        ge=0,
        title="Internal Hits",
    )
    externalHits: int = Field(
        ...,
        ge=0,
        title="External Hits",
    )
    expiresAt: Union[datetime, None] = Field(None)
    source: str = Field(...)
    firstSeen: datetime = Field(...)
    lastSeen: datetime = Field(...)
    reputation: Union[int, None] = Field(
        None,
        ge=1,
        le=10,
    )
    severity: SeverityType = Field(
        SeverityType.UNKNOWN, descriptin=DESCRIPTIONS["severity"]
    )
    comments: str = Field(...)
    tags: List[str] = Field(...)
    extendedInformation: str = Field(...)
    model_config = ConfigDict(from_attributes=True)


class IndicatorSourceDB(BaseModel):
    """Indicator source class."""

    internalHits: int = Field(
        ...,
        ge=0,
        title="Internal Hits",
    )
    externalHits: int = Field(
        ...,
        ge=0,
        title="External Hits",
    )
    source: str = Field(...)
    firstSeen: datetime = Field(...)
    lastSeen: datetime = Field(...)
    reputation: Union[int, None] = Field(
        None,
        ge=1,
        le=10,
    )
    severity: SeverityType = Field(
        SeverityType.UNKNOWN, descriptin=DESCRIPTIONS["severity"]
    )
    comments: str = Field(...)
    tags: List[str] = Field(...)
    extendedInformation: str = Field(...)
    destinations: List[dict] = Field([])
    model_config = ConfigDict(from_attributes=True)
    retracted: bool = Field(False)
    retractionDestinations: List[dict] = Field([])


class IndicatorDBWithSources(IndicatorDB):
    """IndicatorDB all sources class."""

    sources: List[IndicatorSourceDB] = []


class IndicatorOut(BaseModel):
    """The outgoing indicator model."""

    value: str = Field(..., description=DESCRIPTIONS["value"])
    type: IndicatorType = Field(..., description=DESCRIPTIONS["type"])
    test: bool = Field(..., description=DESCRIPTIONS["test"])
    safe: bool = Field(False)
    active: bool = Field(..., description=DESCRIPTIONS["active"])
    internalHits: int = Field(
        ...,
        description=DESCRIPTIONS["internalHits"],
        ge=0,
        title="Internal Hits",
    )
    externalHits: int = Field(
        ...,
        description=DESCRIPTIONS["externalHits"],
        ge=0,
        title="External Hits",
    )
    expiresAt: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["expiresAt"],
        title="Expires At",
    )
    source: str = Field(
        ..., description="The source that first reported this indicator."
    )
    sharedWith: List[str] = Field(
        ...,
        description=DESCRIPTIONS["sharedWith"],
    )
    tags: List[str] = Field([], description="List of applied tags.")
    firstSeen: datetime = Field(
        ...,
        description=DESCRIPTIONS["firstSeen"],
        title="First Seen",
    )
    lastSeen: datetime = Field(
        ...,
        description=DESCRIPTIONS["lastSeen"],
        title="Last Seen",
    )
    reputation: int = Field(
        ...,
        description=DESCRIPTIONS["reputation"],
        ge=1,
        le=10,
    )
    severity: SeverityType = Field(
        SeverityType.UNKNOWN,
        description=DESCRIPTIONS["severity"],
    )
    comments: str = Field(..., description=DESCRIPTIONS["comments"])
    extendedInformation: Annotated[str, StringConstraints(strip_whitespace=True)] = Field("")


class IndicatorSourceOut(BaseModel):
    """The outgoing indicator model."""

    internalHits: int = Field(
        ...,
        description=DESCRIPTIONS["internalHits"],
        ge=0,
        title="Internal Hits",
    )
    externalHits: int = Field(
        ...,
        description=DESCRIPTIONS["externalHits"],
        ge=0,
        title="External Hits",
    )
    source: str = Field(
        ..., description="The source that first reported this indicator."
    )
    tags: List[str] = Field([], description="List of applied tags.")
    firstSeen: datetime = Field(
        ...,
        description=DESCRIPTIONS["firstSeen"],
        title="First Seen",
    )
    lastSeen: datetime = Field(
        ...,
        description=DESCRIPTIONS["lastSeen"],
        title="Last Seen",
    )
    reputation: int = Field(
        ...,
        description=DESCRIPTIONS["reputation"],
        ge=1,
        le=10,
    )
    severity: SeverityType = Field(
        SeverityType.UNKNOWN,
        description=DESCRIPTIONS["severity"],
    )
    destinations: List[dict] = Field([])
    comments: str = Field(..., description=DESCRIPTIONS["comments"])
    extendedInformation: Annotated[str, StringConstraints(strip_whitespace=True)] = Field("")
    retracted: bool = Field(False)
    retractionDestinations: List[dict] = Field([])


class IndicatorOutWithSources(IndicatorOut):
    """IndicatorOut all sources class."""

    sources: List[IndicatorSourceOut] = []


class IndicatorIn(BaseModel):
    """The incoming indicator model."""

    value: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)] = Field(
        ..., description="Value of the indicator."
    )
    type: IndicatorType = Field(..., description="Type of the indicator.")
    test: bool = Field(
        False,
        description=DESCRIPTIONS["test"],
    )
    safe: bool = Field(False)
    active: bool = Field(True, description=DESCRIPTIONS["active"])
    source: str = Field(
        ...,
        description="Source of the indicator. This value should match with "
        "the name of one of the configurations on CTE. If no configuration is "
        "found on the CTE with this value then indicator sharing will not "
        "take place.",
    )

    @field_validator("source")
    @classmethod
    def validate_source_configuration(cls, v):
        """Validate source config."""
        if (
            connector.collection(Collections.CONFIGURATIONS).find_one(
                {"name": v}
            )
            is None
        ):
            raise ValueError(f"Configuration with name='{v}' does not exist.")
        return v

    expiresAt: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["expiresAt"],
        title="Expires At",
    )
    firstSeen: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["firstSeen"],
        title="First Seen",
    )
    lastSeen: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["lastSeen"],
        title="Last Seen",
    )
    tags: List[str] = Field([], description=DESCRIPTIONS["tags"])

    reputation: int = Field(
        5,
        description=DESCRIPTIONS["reputation"],
        ge=1,
        le=10,
    )
    severity: SeverityType = Field(
        SeverityType.UNKNOWN,
        description="Severity of indicator.",
    )
    comments: Annotated[str, StringConstraints(strip_whitespace=True)] = Field("", description="Additional comments.")
    extendedInformation: Annotated[str, StringConstraints(strip_whitespace=True)] = Field("")
    _validate_extended_information = field_validator(
        "extendedInformation"
    )(validate_url)


class IndicatorSourceIn(BaseModel):
    """The incoming indicator model."""

    source: str = Field(
        ...,
        description="Source of the indicator. This value should match with "
        "the name of one of the configurations on CTE. If no configuration is "
        "found on the CTE with this value then indicator sharing will not "
        "take place.",
    )

    @field_validator("source")
    @classmethod
    def validate_source_configuration(cls, v):
        """Validate source config."""
        if (
            connector.collection(Collections.CONFIGURATIONS).find_one(
                {"name": v}
            )
            is None
        ):
            raise ValueError(f"Configuration with name='{v}' does not exist.")
        return v

    firstSeen: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["firstSeen"],
        title="First Seen",
    )
    lastSeen: Union[datetime, None] = Field(
        None,
        description=DESCRIPTIONS["lastSeen"],
        title="Last Seen",
    )
    tags: List[str] = Field([], description=DESCRIPTIONS["tags"])

    reputation: int = Field(
        5,
        description=DESCRIPTIONS["reputation"],
        ge=1,
        le=10,
    )
    severity: SeverityType = Field(
        SeverityType.UNKNOWN,
        description="Severity of indicator.",
    )
    comments: Annotated[str, StringConstraints(strip_whitespace=True)] = Field("", description="Additional comments.")
    extendedInformation: Annotated[str, StringConstraints(strip_whitespace=True)] = Field("")
    _validate_extended_information = field_validator(
        "extendedInformation"
    )(validate_url)


class IndicatorInWithSources(IndicatorIn):
    """IndicatorIn all sources class."""

    sources: List[IndicatorSourceIn] = []


class RetractionUpdate(BaseModel):
    """RetractionUpdate class."""

    retracted: bool = Field(...)
    source: str = Field(...)
    value: str = Field(...)
