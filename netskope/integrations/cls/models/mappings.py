"""Mapping file related models."""

import json
from typing import Union
from pydantic import (
    StringConstraints,
    BaseModel,
    model_validator,
    field_validator,
    Field,
)

from netskope.common.utils import DBConnector, Collections, Logger
from typing_extensions import Annotated


connector = DBConnector()
logger = Logger()


def _validate_name_exists(cls, v: str, values, **kwargs):
    """Validate that the mapping file name is unique."""
    values = values.data
    if (
        connector.collection(Collections.CLS_MAPPING_FILES).find_one(
            {"name": v, "repo": values.get("repo")}
        )
        is None
    ):
        raise ValueError(f"File with name='{v}' does not exists.")
    return v


def _validate_json_data(cls, v: str):
    """Validate that the jsonData should not be empty."""
    try:
        json_object = json.loads(v)
        if not bool(json_object):
            raise ValueError("JSON data should not be empty.")
    except json.decoder.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")
    except Exception as e:
        raise ValueError(f"Error occurred while validating JSON: {e}.")
    return v


class MappingDB(BaseModel):
    """Database of mapping file."""

    name: str = Field(...)
    jsonData: str = Field(...)
    repo: Union[str, None] = Field(None)
    formatOptionsMapping: Union[dict, None] = Field(None)


class MappingIn(BaseModel):
    """Incoming mapping file model."""

    name: Union[Annotated[str, StringConstraints(strip_whitespace=True)], None] = Field(
        None
    )
    jsonData: str = Field("{}")
    formatOptionsMapping: Union[dict, None] = Field(None)
    _validate_json_data = field_validator("jsonData")(_validate_json_data)
    isDefault: bool = Field(False)
    showWizard: bool = Field(True)
    repo: Union[str, None] = Field(None)

    @model_validator(mode="before")
    def _validate_unique_name(cls, v: dict):
        """Validate that the mapping file name is unique."""
        if (
            connector.collection(Collections.CLS_MAPPING_FILES).find_one(
                {"name": v["name"], "repo": v["repo"]}
            )
            is not None
        ):
            raise ValueError(
                f"File with name='{v.get('name', '')}' " f"already exists."
            )
        return v


class MappingOut(BaseModel):
    """Mapping file model for read file."""

    name: Union[str, None] = Field(None)
    jsonData: Union[str, None] = Field(None)
    formatOptionsMapping: Union[dict, None] = Field(None)
    isDefault: bool = Field(False)
    showWizard: bool = Field(True)
    repo: Union[str, None] = Field(None)


class MappingFileDelete(BaseModel):
    """Delete mapping file."""

    repo: Union[str, None] = Field(None)
    name: str = Field(...)
    _validate_name = field_validator("name")(_validate_name_exists)


class MappingFileUpdate(BaseModel):
    """Update mapping file."""

    repo: Union[str, None] = Field(None)
    name: str = Field(...)
    _validate_name = field_validator("name")(_validate_name_exists)

    jsonData: str = Field("{}")
    _validate_json_data = field_validator("jsonData")(_validate_json_data)
    showWizard: bool = Field(True)
    formatOptionsMapping: Union[dict, None] = Field(None)
