"""Business rule related schemas."""
import json
from datetime import datetime
from typing import Dict, List, Optional

from jsonschema import ValidationError, validate
from pydantic import BaseModel, Field, StringConstraints, field_validator
from typing_extensions import Annotated

from netskope.common.utils import (Collections, DBConnector, Logger,
                                   PluginHelper)
from netskope.integrations.cfc.utils import IMAGE_METADATA_QUERY_SCHEMA

connector = DBConnector()
helper = PluginHelper()
logger = Logger()


class Action(BaseModel):
    """Model to define plugin action with parameters."""

    label: str
    value: str
    parameters: Dict = Field({})


class ActionWithoutParams(BaseModel):
    """Model to define plugin action without parameters."""

    label: str
    value: str


class Filters(BaseModel):
    """Business rule filters model."""

    query: str = Field("", description="Query formate filter")
    mongo: str = Field("{}", description="Filter to apply on image metadata")

    @field_validator("mongo")
    @classmethod
    def validate_mongo_query(cls, v):
        """Validate mongo query."""
        try:
            validate(json.loads(v), IMAGE_METADATA_QUERY_SCHEMA)
        except ValidationError as ex:
            raise ValueError(f"Invalid query provided. {ex.message}.")
        except Exception:
            raise ValueError("Could not parse the query.")
        return v


class ExceptionRule(BaseModel):
    """Exception rule model."""

    name: str = Field(..., description="Name of exception rule")
    filters: Optional[Filters] = Field(None, description="Filters for exception rules.")


class BusinessRuleIn(BaseModel):
    """Business rule in model."""

    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1)
    ] = Field(..., description="Name of business rule")

    @field_validator("name")
    @classmethod
    def validate_is_unique(cls, v):
        """Validate that the name is unique."""
        if (
            connector.collection(Collections.CFC_BUSINESS_RULES).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"A business rule with the name '{v}' already exists.")
        return v

    filters: Filters = Field(Filters(), description="Filters for business rule")
    exceptions: List[ExceptionRule] = Field(
        [], description="Exception rule for business rule"
    )
    muted: bool = Field(
        False,
        description="Indicates whether the business rule is currently muted or not",
    )

    @field_validator("muted")
    @classmethod
    def validate_not_muted(cls, v):
        """Validate that the rule is not muted."""
        if v is True:
            raise ValueError("Can not create a muted business rule.")
        return v

    unmuteAt: Optional[datetime] = Field(
        None,
        description=(
            "Specifies the datetime at which the business rule"
            " will be automatically unmuted."
        ),
    )


class BusinessRuleUpdate(BaseModel):
    """Business rule update model."""

    name: str = Field(..., description="Name of business rule")

    @field_validator("name")
    @classmethod
    def validate_exists(cls, v):
        """Validate that the name exists."""
        if (
            connector.collection(Collections.CFC_BUSINESS_RULES).find_one({"name": v})
            is None
        ):
            raise ValueError("No business rule with this name exists.")
        return v

    filters: Optional[Filters] = Field(None, description="Filters for business rule")
    exceptions: Optional[List[ExceptionRule]] = Field(
        None, description="Exception rule for business rule"
    )

    @field_validator("exceptions")
    @classmethod
    def validate_is_mute_rules_unique(cls, v):
        """Validate that the exceptions is unique."""
        if v is None:
            return None
        rule_names = {rule.name for rule in v}

        if len(rule_names) != len(v):
            raise ValueError("An exception with the same name already exists.")
        return v

    muted: Optional[bool] = Field(
        None,
        description="Indicates whether the business rule is currently muted or not",
    )
    unmuteAt: Optional[datetime] = Field(
        None,
        description=(
            "Specifies the datetime at which the business rule"
            " will be automatically unmuted."
        ),
    )

    @field_validator("unmuteAt")
    @classmethod
    def validate_unmute_time(cls, v, values, **kwargs):
        """Validate unmuteAt time."""
        values = values.data
        if values["muted"] is False:
            return None
        if v is None:
            raise ValueError(
                "Unmute time must be set in order to mute the business rule."
            )
        now = datetime.now()
        if v.tzinfo is None:
            v = v.replace(tzinfo=now.tzinfo)
        if v < now:
            raise ValueError("Unmute time can not be in past.")
        return v


class BusinessRuleOut(BaseModel):
    """Business rule out model."""

    name: str = Field(..., description="Name of business rule")
    muted: Optional[bool] = Field(
        None,
        description="Indicates whether the business rule is currently muted or not",
    )
    unmuteAt: Optional[datetime] = Field(
        None,
        description=(
            "Specifies the datetime at which the business rule"
            " will be automatically unmute"
        ),
    )
    filters: Optional[Filters] = Field(None, description="Filters for business rule")
    exceptions: Optional[List[ExceptionRule]] = Field(
        None, description="Exception rule for business rule"
    )
    mapped: Optional[bool] = Field(
        False, description="Shows if business rule is used in any sharing or not."
    )
    updatedBy: str = Field(..., description="Updated by username")
    updatedAt: datetime = Field(..., description="Updated at time")


class BusinessRulesDelete(BaseModel):
    """Delete business rules model."""

    names: List[str] = Field(
        ..., description="List of names of business rules to delete."
    )


class BusinessRuleDB(BaseModel):
    """Database business rule model."""

    name: str = Field(..., description="Name of business rule")
    filters: Filters = Field(..., description="Filters for business rule")
    exceptions: List[ExceptionRule] = Field(
        ..., description="Exception rule for business rule"
    )
    muted: bool = Field(
        ...,
        description="Indicates whether the business rule is currently muted or not",
    )
    unmuteAt: Optional[datetime] = Field(
        None,
        description=(
            "Specifies the datetime at which the business rule"
            " will be automatically unmute"
        ),
    )
    updatedBy: str = Field(..., description="Updated by username")
    updatedAt: datetime = Field(..., description="Updated at time")


class BusinessRuleTestOut(BaseModel):
    """Business rule test out model."""

    images_count: int = Field(
        ...,
        description="Count of image metadata fetched using filter for business rule",
    )
    images_size: int = Field(
        ..., description="Size of image metadata fetched using filter for business rule"
    )


class BusinessRuleUsedIn(BaseModel):
    """Business rule used in model."""

    sourceConfiguration: str = Field(
        ..., description="Source configuration of the sharing where business rule is used in"
    )
    destinationConfiguration: str = Field(
        ..., description="Destination configuration of the sharing where business rule is used in"
    )
