"""Provides tagging related models."""

from enum import Enum
from pydantic import field_validator, StringConstraints, BaseModel, Field

from netskope.common.utils import DBConnector, Collections
from typing_extensions import Annotated

connector = DBConnector()


class TagAppliedOn(str, Enum):
    """Tag applied on enumeration."""

    SOME = "some"
    ALL = "all"
    NONE = "none"


class TagIn(BaseModel):
    """Incoming tag model."""

    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)] = Field(...)
    color: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(...)

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        """Validate that the name is unique."""
        if (
            connector.collection(Collections.TAGS).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"Tag with name '{v}' already exists.")
        if len(v) > 500:
            raise ValueError("Tag name exceed maximum allowed length of 500 characters.")
        return v


class TagOut(BaseModel):
    """Outbound tag model."""

    name: str = Field(...)
    color: str = Field(...)
    appliedOn: TagAppliedOn = Field(TagAppliedOn.NONE)


class TagDelete(BaseModel):
    """Tag deletion model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_tag_exists(cls, v):
        """Validate that the tag exists."""
        if (
            connector.collection(Collections.TAGS).find_one({"name": v})
            is None
        ):
            raise ValueError(f"Tag with name '{v}' does not exist.")
        return v
