"""Entity related models."""

import re
from enum import Enum
from typing import Optional, Union, Annotated

from py_expression_eval import Parser
from pydantic import (
    BaseModel,
    Field,
    ValidationInfo,
    field_validator,
    validator,
    StringConstraints,
)

from netskope.common.utils import Collections, DBConnector

connector = DBConnector()
parser = Parser()


class EntityFieldType(str, Enum):
    """Entity field types."""

    STRING = "string"
    NUMBER = "number"
    LIST = "list"
    DATETIME = "datetime"
    CALCULATED = "calculated"
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    EMAIL = "email"
    REFERENCE = "reference"
    VALUE_MAP_STRING = "value_map_string"
    VALUE_MAP_NUMBER = "value_map_number"
    RANGE_MAP = "range_map"


# START Type params
class ReferenceTypeParams(BaseModel):
    """Reference type parameters."""

    entity: str
    field: str


class CalculatedTypeParams(BaseModel):
    """Calculated type parameters."""

    expression: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1)
    ]
    dependencies: list[str] = Field([], validate_default=True)

    @field_validator("dependencies")
    @classmethod
    def validate_dependencies(cls, v, info: ValidationInfo):
        """Populate dependencies."""
        if not info.data.get("expression", "").strip():
            return []
        try:
            parsed_expr = parser.parse(
                info.data["expression"].replace("$", "")
            )
        except Exception:
            raise ValueError("Invalid expression provided.")
        return parsed_expr.variables()


class BaseValueMapMapping(BaseModel):
    """Base class for value mappings."""

    label: Annotated[str, StringConstraints(strip_whitespace=True)]

    @field_validator("label")
    @classmethod
    def validate_label(cls, v):
        """Validate label."""
        if not v:
            raise ValueError("Label cannot be empty.")
        return v


class ValueMapMappingNumber(BaseValueMapMapping):
    """Value mappings with numerical values."""

    value: Union[int, None]


class ValueMapMappingString(BaseValueMapMapping):
    """Value mappings with string values."""

    value: Union[str, None]


class ValueMapTypeParams(BaseModel):
    """Value map type parameters."""

    mappings: list[Union[ValueMapMappingNumber, ValueMapMappingString]]
    field: str


class RangeMapMapping(BaseModel):
    """Range map."""

    label: Annotated[str, StringConstraints(strip_whitespace=True)]

    @field_validator("label")
    @classmethod
    def validate_label(cls, v):
        """Validate label."""
        if not v:
            raise ValueError("Label cannot be empty.")
        return v

    gte: int
    lte: int


class RangeMapTypeParams(BaseModel):
    """Range map type parameters."""

    mappings: list[RangeMapMapping]
    field: str


# END Type params


class EntityTypeCoalesceStrategy(str, Enum):
    """Entity field coalesce strategies."""

    MERGE = "append"
    OVERWRITE = "overwrite"


class EntityField(BaseModel):
    """Entity field model."""

    label: str
    name: str
    type: EntityFieldType
    params: Union[
        None,
        CalculatedTypeParams,
        ReferenceTypeParams,
        ValueMapTypeParams,
        RangeMapTypeParams,
    ]
    unique: bool
    coalesceStrategy: Optional[EntityTypeCoalesceStrategy]

    @validator("coalesceStrategy")
    def validate_coalesce_strategy(cls, v, values, **kwargs):
        """Validate that strategy is provided if unique is set to False."""
        if values["unique"]:
            return None
        if not values["unique"] and v is None:
            raise ValueError(
                "coalesceStrategy must be provided if unique is set to False."
            )
        return v


class EntityFieldIn(BaseModel):
    """Entity field model."""

    label: Annotated[str, StringConstraints(strip_whitespace=True)]
    name: Optional[str] = Field(None, validate_default=True)

    @field_validator("name")
    @classmethod
    def validate_name(cls, v, info: ValidationInfo):
        """Validate that name is unique."""
        return ("_".join(info.data["label"].strip().lower().split(" "))).replace(".", "_")

    type: EntityFieldType
    params: Union[
        None,
        CalculatedTypeParams,
        ReferenceTypeParams,
        ValueMapTypeParams,
        RangeMapTypeParams,
    ] = None
    unique: bool
    coalesceStrategy: Optional[EntityTypeCoalesceStrategy]

    @validator("coalesceStrategy")
    def validate_coalesce_strategy(cls, v, values, **kwargs):
        """Validate that strategy is provided if unique is set to False."""
        if values["unique"]:
            return None
        if not values["unique"] and v is None:
            raise ValueError(
                "coalesceStrategy must be provided if unique is set to False."
            )
        return v


class Entity(BaseModel):
    """Entity model."""

    name: str
    ongoingCalculationUpdateTaskId: Optional[str] = None
    ongoingMappingUpdateTaskId: Optional[str] = None
    fields: list[EntityField]


class EntityIn(BaseModel):
    """Entity model."""

    name: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=238),
    ]

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        """Validate that name is unique."""
        if not re.match(r"^[a-zA-Z ]+$", v):
            raise ValueError(
                "Only alphabets and spaces are allowed in entity name."
            )
        if (
            connector.collection(Collections.CREV2_ENTITIES).find_one(
                {"name": v}
            )
            is not None
        ):
            raise ValueError(f"Entity with name '{v}' already exists.")
        return v

    fields: list[EntityFieldIn] = Field([])

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, v):
        """Validate that fields are unique."""
        names = []
        for field in v:
            if field.name in names:
                raise ValueError(f"Field name '{field.name}' is not unique.")
            names.append(field.name)

        calculated_field: CalculatedTypeParams
        for calculated_field in filter(
            lambda f: f.type == EntityFieldType.CALCULATED, v
        ):
            if set(calculated_field.params.dependencies) - set(names):
                raise ValueError("Invalid field name provided in expression.")
        return v

    class Config:
        """Configurations."""

        validate_assignment = True


class EntityUpdate(BaseModel):
    """Entity model."""

    name: str = Field(min_length=1, max_length=238)

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        """Validate that name is unique."""
        v = v.strip()
        if (
            connector.collection(Collections.CREV2_ENTITIES).find_one(
                {"name": v}
            )
            is None
        ):
            raise ValueError(f"Entity with name '{v}' does not exist.")
        return v

    fields: list[EntityFieldIn] = Field([])

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, v):
        """Validate that fields are unique."""
        names = []
        for field in v:
            if field.name in names:
                raise ValueError(f"Field name '{field.name}' is not unique.")
            names.append(field.name)

        calculated_field: CalculatedTypeParams
        for calculated_field in filter(
            lambda f: f.type == EntityFieldType.CALCULATED, v
        ):
            if set(calculated_field.params.dependencies) - set(names):
                raise ValueError("Invalid field name provided in expression.")
        return v

    class Config:
        """Configurations."""

        validate_assignment = True


def get_entity_by_name(entity: str) -> Entity:
    """Get entity from name."""
    return Entity(
        **connector.collection(Collections.CREV2_ENTITIES).find_one(
            {"name": entity}
        )
    )
