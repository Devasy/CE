"""Business rule related schemas."""

import json
from datetime import datetime
from typing import List, Dict, Union
from pydantic import field_validator, StringConstraints, BaseModel, Field, model_validator
from jsonschema import validate, ValidationError

from netskope.common.utils import DBConnector, Collections
from netskope.integrations.itsm.utils import alert_event_query_schema
from typing_extensions import Annotated

connector = DBConnector()


def validate_queues(cls, v, values, **kwargs):
    """Validate that queues are limited to one."""
    if v is None:
        return None
    for key, value in v.items():
        if len(value) > 1:
            raise ValueError("Can not assign multiple queues to same business rule.")
    return v


def validate_mappings(cls, v):
    """Validate that no multiple values are mapped to same target."""
    if v is None:
        return None
    for values in v.values():
        fields = [f.destination_field for f in values]
        if len(fields) != len(set(fields)):
            raise ValueError("Can not map multiple values to same target field.")
        for mapping in values:
            if not mapping.destination_field:
                raise ValueError("Can not save mapping with an empty target field.")
    return v


class FieldMapping(BaseModel):
    """Field mapping model."""

    extracted_field: str = Field(...)
    destination_field: Union[str, None] = Field(None)
    custom_message: Union[str, None] = Field(None)


def _validate_mongo_schema(self):
    """Validate Mongo Query."""
    try:
        [*_, QUERY_SCHEMA] = alert_event_query_schema()
        validate(json.loads(self.mongo), QUERY_SCHEMA)
    except ValidationError:
        self.isValid = False
    except Exception:
        raise ValueError("Could not parse the query.")
    return self


class Filters(BaseModel):
    """Sharing filters model."""

    query: str = Field("")
    mongo: str = Field("{}")
    isValid: bool = Field(True)
    _validate_mongo_schema = model_validator(mode="after")(_validate_mongo_schema)


class DedupeRule(BaseModel):
    """Dedupe rule model."""

    name: str = Field(...)
    filters: Union[Filters, None] = Field(None)
    dedupeFields: Union[List[str], None] = Field(None)

    @field_validator("dedupeFields")
    @classmethod
    def validate_dedupe_fields(cls, v, values, **kwargs):
        """Validate dedupe fields."""
        values = values.data
        if values.get("filters") is None and v is None:
            raise ValueError("filters and dedupeFields can not both be empty.")
        if None not in [v, values.get("filters")]:
            raise ValueError("filters and dedupeFields can not both be set.")
        return v


class Queue(BaseModel):
    """Queue model."""

    label: Annotated[str, StringConstraints(strip_whitespace=True)]
    value: Annotated[str, StringConstraints(strip_whitespace=True)]
    requireApproval: bool = Field(False)

    defaultMappings: Union[Dict[str, List[FieldMapping]], None] = Field(None)
    _validate_mappings = field_validator("defaultMappings")(validate_mappings)


class BusinessRuleIn(BaseModel):
    """Business rule model."""

    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)] = Field(...)

    @field_validator("name")
    @classmethod
    def validate_is_unique(cls, v):
        """Validate that the name is unique."""
        if connector.collection(Collections.ITSM_BUSINESS_RULES).find_one({"name": v}) is not None:
            raise ValueError("A business rule with the same name already exists.")
        return v

    filters: Filters = Field(Filters())
    dedupeRules: List[DedupeRule] = Field([])
    muteRules: List[DedupeRule] = Field([])
    muted: bool = Field(False)

    @field_validator("muted")
    @classmethod
    def validate_not_muted(cls, v):
        """Validate that the rule is not muted."""
        if v is True:
            raise ValueError("Can not create a muted business rule.")
        return v

    unmuteAt: Union[datetime, None] = Field(None)
    queues: Dict[str, List[Queue]] = Field({})
    _validate_queues = field_validator("queues")(validate_queues)


class BusinessRuleUpdate(BaseModel):
    """Business rule model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_exists(cls, v):
        """Validate that the name exists."""
        if connector.collection(Collections.ITSM_BUSINESS_RULES).find_one({"name": v}) is None:
            raise ValueError("No business rule with this name exists.")
        return v

    filters: Union[Filters, None] = Field(None)
    dedupeRules: Union[List[DedupeRule], None] = Field(None)

    @field_validator("dedupeRules")
    @classmethod
    def validate_is_dedupe_rules_unique(cls, v):
        """Validate that the dedupeRules is unique."""
        if v is None:
            return None
        rule_names = list(map(lambda rule: rule.name, v))
        if len(set(rule_names)) != len(rule_names):
            raise ValueError("A deduplication rule with the same name already exists.")
        return v

    muteRules: Union[List[DedupeRule], None] = Field(None)

    @field_validator("muteRules")
    @classmethod
    def validate_is_mute_rules_unique(cls, v):
        """Validate that the muteRules is unique."""
        rule_names = set()
        if v is None:
            return None
        for rule in v:
            rule_names.add(rule.name)
        if len(rule_names) != len(v):
            raise ValueError("A mute rule with the same name already exists.")
        return v

    muted: Union[bool, None] = Field(None)
    unmuteAt: Union[datetime, None] = Field(None)

    @field_validator("unmuteAt")
    @classmethod
    def validate_unmute_time(cls, v, values, **kwargs):
        """Validate unmuteAt time."""
        values = values.data
        if values["muted"] is False:
            return None
        if v is None:
            raise ValueError("Unmute time must be set in order to mute the business rule.")
        if v < datetime.now():
            raise ValueError("Unmute time can not be in past.")
        return v

    queues: Union[Dict[str, List[Queue]], None] = Field(None)
    _validate_queues = field_validator("queues")(validate_queues)


class BusinessRuleOut(BaseModel):
    """Business rule out model."""

    muted: Union[bool, None] = Field(None)
    unmuteAt: Union[datetime, None] = Field(None)
    name: str = Field(...)
    filters: Union[Filters, None] = Field(None)
    dedupeRules: Union[List[DedupeRule], None] = Field(None)
    muteRules: Union[List[DedupeRule], None] = Field(None)
    queues: Union[Dict[str, List[Queue]], None] = Field(None)


class BusinessRuleDelete(BaseModel):
    """Delete business rule model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_exists(cls, v):
        """Validate that the name exists."""
        if connector.collection(Collections.ITSM_BUSINESS_RULES).find_one({"name": v}) is None:
            raise ValueError("No business rule with this name exists.")
        return v


class BusinessRuleDB(BaseModel):
    """Database business rule model."""

    name: str = Field(...)
    filters: Filters = Field(...)
    dedupeRules: List[DedupeRule] = Field(...)
    muteRules: List[DedupeRule] = Field(...)
    muted: bool = Field(...)
    unmuteAt: Union[datetime, None] = Field(None)
    queues: Dict[str, List[Queue]] = Field(...)
