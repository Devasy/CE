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
    BOOLEAN = "boolean"
    CALCULATED = "calculated"
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    EMAIL = "email"
    REFERENCE = "reference"
    VALUE_MAP_STRING = "value_map_string"
    VALUE_MAP_NUMBER = "value_map_number"
    RANGE_MAP = "range_map"


# The primitive a record actually holds for each field type -- NOT the filter
# widget it renders as. This is the ``valueType`` half of the convention already
# used across the product: ``type`` is the widget, ``valueType`` (when present)
# is the stored primitive, so a Value Map String rendered as a ``select`` still
# advertises that it holds plain text (see ``_field_to_query_def`` in
# crev2/routers/records.py and ``build_threat_indicators_query_schema`` in
# crev2/utils/threat_indicators_entity.py).
#
# Vocabulary is the query-builder's ("text", not "string"), matching the
# existing ``valueType`` values those two modules emit.
#
# A LIST holds STRINGS, so its entry is the element primitive -- the product
# treats a List's contents as text throughout: CSV import builds it by splitting
# on commas and stripping (``map_record`` in crev2/routers/entities.py),
# ``_field_to_query_def`` gives it a text filter widget, and the Threat
# Indicators entity declares ``valueType: "text"`` for its own List fields.
# Pair it with ``multiValued`` to know it is an ARRAY of that primitive.
#
# REFERENCE is deliberately absent: its target field's type is not resolved
# here, and "absent" means "unknown primitive" per that convention -- consumers
# must treat a missing entry as unknown rather than defaulting it to text.
ENTITY_FIELD_VALUE_TYPES = {
    EntityFieldType.STRING: "text",
    EntityFieldType.IPV4: "text",
    EntityFieldType.IPV6: "text",
    EntityFieldType.EMAIL: "text",
    EntityFieldType.VALUE_MAP_STRING: "text",
    # Stores the matched mapping's label, not the number that selected it.
    EntityFieldType.RANGE_MAP: "text",
    EntityFieldType.LIST: "text",
    EntityFieldType.NUMBER: "number",
    # ``int(expr.evaluate(...))`` -- see _update_calculated_fields in
    # crev2/tasks/fetch_records.py.
    EntityFieldType.CALCULATED: "number",
    # ``ValueMapMappingNumber.value`` is an int.
    EntityFieldType.VALUE_MAP_NUMBER: "number",
    EntityFieldType.BOOLEAN: "boolean",
    EntityFieldType.DATETIME: "datetime",
}


# START Type params
class ReferenceTypeParams(BaseModel):
    """Reference type parameters."""

    entity: str
    field: str


class CalculatedTypeParams(BaseModel):
    """Calculated type parameters."""

    expression: Annotated[str, StringConstraints(strip_whitespace=True)]
    dependencies: list[str] = Field([], validate_default=True)

    @field_validator("expression")
    @classmethod
    def validate_expression(cls, value: str) -> str:
        """Validate presence of expression with a friendly message."""
        if not value or not value.strip():
            raise ValueError("Calculated fields require an expression.")
        return value

    @field_validator("dependencies")
    @classmethod
    def validate_dependencies(cls, v, info: ValidationInfo):
        """Populate dependencies."""
        if not info.data.get("expression", "").strip():
            return []
        try:
            parsed_expr = parser.parse(info.data["expression"].replace("$", ""))
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


class NormalizedType(str, Enum):
    """Normalized String types."""

    LOWER = "lowercase"
    UPPER = "uppercase"
    TITLE = "titlecase"


class NormalizedTypeParams(BaseModel):
    """Normalized String parameters."""

    normalization: Union[None, NormalizedType]


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
        NormalizedTypeParams,
    ]
    unique: bool
    coalesceStrategy: Optional[EntityTypeCoalesceStrategy]
    # Free-form provenance/analytics info (e.g. {"ai_suggested": True} for
    # fields created via the CRE Auto-Mapper). Not used in business logic.
    metadata: Optional[dict] = None

    @validator("coalesceStrategy")
    def validate_coalesce_strategy(cls, v, values, **kwargs):
        """Validate that strategy is provided if unique is set to False."""
        if values["unique"] or values.get("type") == EntityFieldType.BOOLEAN:
            return None
        if (
            values.get("type") == EntityFieldType.STRING
            and values.get("params")
            and values.get("params").normalization is not None
        ) or values.get("type") in [
            EntityFieldType.VALUE_MAP_NUMBER,
            EntityFieldType.VALUE_MAP_STRING,
            EntityFieldType.RANGE_MAP,
        ]:
            return v
        if not values["unique"] and v is None:
            raise ValueError(
                "coalesceStrategy must be provided if unique is set to False."
            )
        return v


class EntityFieldIn(BaseModel):
    """Entity field model."""

    label: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    name: Optional[str] = Field(None, validate_default=True)

    @field_validator("label")
    @classmethod
    def validate_label(cls, v):
        """Validate label."""
        if not v:
            raise ValueError("Label cannot be empty.")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v, info: ValidationInfo):
        """Validate that name is unique."""
        label = info.data.get("label")
        if label is None:
            return v
        return ("_".join(label.strip().lower().split(" "))).replace(
            ".", "_"
        )

    type: EntityFieldType
    params: Union[
        None,
        CalculatedTypeParams,
        ReferenceTypeParams,
        ValueMapTypeParams,
        RangeMapTypeParams,
        NormalizedTypeParams,
    ] = None
    unique: bool
    coalesceStrategy: Optional[EntityTypeCoalesceStrategy]
    # Free-form provenance/analytics info (e.g. {"ai_suggested": True}).
    # Must exist here as well as on EntityField: create_field round-trips all
    # existing DB fields through EntityFieldIn (EntityUpdate.fields), so a key
    # missing from this model would be silently stripped on the next write.
    metadata: Optional[dict] = None

    @validator("coalesceStrategy")
    def validate_coalesce_strategy(cls, v, values, **kwargs):
        """Validate that strategy is provided if unique is set to False."""
        if values["unique"] or values.get("type") == EntityFieldType.BOOLEAN:
            return None
        if (
            values.get("type") == EntityFieldType.STRING
            and values.get("params")
            and values.get("params").normalization is not None
        ) or values.get("type") in [
            EntityFieldType.VALUE_MAP_NUMBER,
            EntityFieldType.VALUE_MAP_STRING,
            EntityFieldType.RANGE_MAP,
        ]:
            return v
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


class EntityFieldOut(EntityField):
    """An entity field plus the two computed facts a field mapping gates on.

    Separate from ``EntityField`` because that is also the WRITE shape
    (``EntityUpdate.fields`` round-trips every stored field on each entity
    write), so a key added there would be persisted.

    ``valueType`` is the stored primitive, ``None`` only for a Reference --
    consumers must read that as "unknown", never as text.
    """

    valueType: Optional[str] = None
    multiValued: bool = False
    # Authored value set for the two bounded map types, else None. An empty list
    # means the map has no mappings, so it can never match and stores nothing.
    mapLabels: Optional[list] = None


class EntityOut(Entity):
    """Entity as served by ``GET /entities``, with field mapping metadata."""

    fields: list[EntityFieldOut]


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
            raise ValueError("Only alphabets and spaces are allowed in entity name.")
        if (
            connector.collection(Collections.CREV2_ENTITIES).find_one({"name": v})
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
            connector.collection(Collections.CREV2_ENTITIES).find_one({"name": v})
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
        **connector.collection(Collections.CREV2_ENTITIES).find_one({"name": entity})
    )
