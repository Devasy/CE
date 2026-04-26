"""Record related models."""

from datetime import datetime
from typing import Union
from pydantic import BaseModel, Field, field_validator

from netskope.common.utils import DBConnector, Collections

connector = DBConnector()


class RecordDB(BaseModel):
    """Record DB model."""

    fields: dict[str, Union[str, int, datetime, list[Union[str, int]]]] = (
        Field({})
    )


class RecordOut(BaseModel):
    """Record model."""

    entity: str = Field(...)

    @field_validator("entity")
    @classmethod
    def _validate_entity_type(cls, v):
        """Validate that vlaue is a valid entity type."""
        if (
            connector.collection(Collections.CREV2_ENTITIES).find_one(
                {"name": v}
            )
            is None
        ):
            raise ValueError(f"Entity with name '{v}' does not exist.")
        return v

    # fields: dict[
    #     str, Union[None, str, int, datetime, list[Union[str, int]]]
    # ] = Field({})
    fields: dict


class RecordQueryLocator(BaseModel):
    """Record query locator."""

    query: str


class RecordValueLocator(BaseModel):
    """Record value locator."""

    ids: list[str]
