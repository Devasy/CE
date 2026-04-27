"""Business rule related schemas."""
import json
from datetime import datetime
from typing import List, Dict, Union
from pydantic import field_validator, StringConstraints, BaseModel, Field, model_validator
from jsonschema import validate, ValidationError
from netskope.common.utils import DBConnector, Collections, get_database_fields_schema
from typing_extensions import Annotated


connector = DBConnector()
DEFAULT_BUSINESS_RULE_NAME = "All"


def validate_mappings(cls, v):
    """Validate that no multiple values are mapped to same target."""
    if v is None:
        return None
    fields = [f.destination_field for f in v]
    if len(fields) != len(set(fields)):
        raise ValueError("Can not map multiple values to same target field.")
    for mapping in v:
        if not mapping.destination_field:
            raise ValueError("Can not save mapping with an empty target field.")
    return v


def validate_siem_mappings(cls, v: Dict[str, List[str]]):
    """Validate actions."""
    if v is None:
        return None
    for source, destinations in v.items():
        if connector.collection(Collections.CLS_CONFIGURATIONS).find_one({"name": source}) is None:
            raise ValueError(f"Configuration with name {source} does not exist.")
        destinations = list(set(destinations))
        for destination in destinations:
            if connector.collection(Collections.CLS_CONFIGURATIONS).find_one({"name": destination}) is None:
                raise ValueError(f"Configuration with name {destination} does not exist.")
            if destination == source:
                raise ValueError("Source and destination can not be same.")
        v[source] = destinations
    return v


def _validate_mongo_schema(self):
    """Validate Mongo Query."""
    try:
        from netskope.common.utils import FILTER_TYPES

        ALERTS_EVENTS_QUERY_SCHEMA = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "definitions": {
                **FILTER_TYPES,
                "searchRoot": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        **get_database_fields_schema(),
                        "$and": {
                            "type": "array",
                            "items": {"$ref": "#/definitions/searchRoot"},
                        },
                        "$or": {
                            "type": "array",
                            "items": {"$ref": "#/definitions/searchRoot"},
                        },
                        "$nor": {
                            "type": "array",
                            "items": {"$ref": "#/definitions/searchRoot"},
                        },
                    },
                },
            },
            "type": "object",
            "$ref": "#/definitions/searchRoot",
            "additionalProperties": False,
        }

        validate(json.loads(self.mongo), ALERTS_EVENTS_QUERY_SCHEMA)
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


class MuteRule(BaseModel):
    """Mute rule model."""

    name: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(...)
    filters: Filters = Field(Filters())


class BusinessRuleIn(BaseModel):
    """Business rule model."""

    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)] = Field(...)

    @field_validator("name")
    @classmethod
    def validate_is_unique(cls, v):
        """Validate that the name is unique."""
        if connector.collection(Collections.CLS_BUSINESS_RULES).find_one({"name": v}) is not None:
            raise ValueError("A business rule with the same name already exists.")
        return v

    filters: Filters = Field(Filters())
    muteRules: List[MuteRule] = Field([])
    muted: bool = Field(False)

    @field_validator("muted")
    @classmethod
    def validate_not_muted(cls, v):
        """Validate that the rule is not muted."""
        if v is True:
            raise ValueError("Can not create a muted business rule.")
        return v

    unmuteAt: Union[datetime, None] = Field(None)
    siemMappings: Dict[str, List[str]] = Field({})
    _validate_siem_mappings = field_validator("siemMappings")(validate_siem_mappings)
    isDefault: bool = Field(False)


class BusinessRuleUpdate(BaseModel):
    """Business rule model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_exists(cls, v):
        """Validate that the name exists."""
        if connector.collection(Collections.CLS_BUSINESS_RULES).find_one({"name": v}) is None:
            raise ValueError("No business rule with this name exists.")
        # TODO: add validations
        # if v == DEFAULT_BUSINESS_RULE_NAME:
        #     raise ValueError("Default business rule can not be updated.")
        return v

    filters: Union[Filters, None] = Field(None)

    muteRules: Union[List[MuteRule], None] = Field(None)

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

    siemMappings: Union[Dict[str, List[str]], None] = Field(None)
    _validate_siem_mappings = field_validator("siemMappings")(validate_siem_mappings)
    isDefault: Union[bool, None] = Field(None)


class BusinessRuleOut(BaseModel):
    """Business rule out model."""

    name: str = Field(...)
    muted: bool = Field(...)
    unmuteAt: Union[datetime, None] = Field(None)
    filters: Filters = Field(...)
    muteRules: List[MuteRule] = Field(...)
    siemMappings: Dict[str, List[str]] = Field(...)
    isDefault: bool = Field(False)


class BusinessRuleDelete(BaseModel):
    """Delete business rule model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_exists(cls, v):
        """Validate that the name exists."""
        if connector.collection(Collections.CLS_BUSINESS_RULES).find_one({"name": v}) is None:
            raise ValueError("No business rule with this name exists.")
        if v == DEFAULT_BUSINESS_RULE_NAME:
            raise ValueError("Default business rule can not be deleted.")
        return v


class BusinessRuleDB(BaseModel):
    """Database business rule model."""

    name: str = Field(...)
    filters: Filters = Field(...)
    muteRules: List[MuteRule] = Field(...)
    muted: bool = Field(...)
    unmuteAt: Union[datetime, None] = Field(None)
    siemMappings: Dict[str, List[str]] = Field(...)
    isDefault: bool = Field(False)
    siemMappingIDs: Dict[str, Dict] = Field({})
