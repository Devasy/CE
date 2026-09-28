"""Models for the CRE Auto-Mapper endpoint (AI-assisted field mapping)."""

from typing import Optional

from pydantic import BaseModel, Field


class AutoMapperRequest(BaseModel):
    """Request body for the auto-mapper endpoint."""

    plugin: str
    parameters: dict
    entity: str
    destination: str  # platform (CE) entity name the user selected in the UI
    # Name of the configuration currently being edited, if any. Its own saved
    # mappings are excluded when computing each platform field's ``is_mapped``
    # flag, so re-mapping an existing configuration does not treat the fields it
    # already maps as "in use by another plugin".
    configuration_name: Optional[str] = None
    # Incremental re-mapping (UI "locked" rows). ``locked_sources`` are plugin
    # field names the user has already committed to and does NOT want
    # re-suggested — they are dropped before the LLM call. ``locked_destinations``
    # are the platform field names those committed rows already occupy; they are
    # reserved so a fresh suggestion for another field cannot collide with a
    # destination the user is keeping. Empty on a first (full) auto-map.
    locked_sources: list[str] = Field(default_factory=list)
    locked_destinations: list[str] = Field(default_factory=list)


class NewFieldSpec(BaseModel):
    """Specification for a new platform entity field to be created (API response shape)."""

    label: str
    type: str
    unique: bool = False


class FieldMappingResult(BaseModel):
    """Mapping of a single plugin field to a platform entity field (API response shape)."""

    source: str
    destination: str
    existing: bool
    new_field: Optional[NewFieldSpec] = None
    reason: str


class FieldMappingResultLLM(BaseModel):
    """Single field mapping as returned by the LLM.

    Every field is REQUIRED and uses primitive types only (no Optional / null), so
    the generated tool schema lists all properties in ``required`` and contains no
    ``anyOf`` / ``$ref``. A field with a default value is excluded from ``required``,
    which let providers (e.g. Anthropic tool-calling) legitimately omit it — that
    was why ``fields`` previously came back empty. Empty strings act as the
    "not applicable" sentinel for the new-field columns. Converted to
    ``FieldMappingResult`` before being returned to the frontend.
    """

    source: str = Field(
        description="Plugin field name being mapped (must match a provided plugin field exactly)."
    )
    destination: str = Field(
        description=(
            "Platform field LABEL — copied exactly from the input when existing "
            "is true, or the label you propose for the new field when existing "
            "is false. Never a machine name: platform fields are identified by "
            "label, and the system derives the name from it."
        )
    )
    existing: bool = Field(
        description="True if destination is an existing platform field; false if a new platform field must be created."
    )
    new_field_label: str = Field(
        description=(
            "Human-readable label for the new field — the same value as "
            "destination. It must not repeat the label of a platform field that "
            "already exists on the entity, not even one of a different type: add "
            "a qualifying prefix or suffix instead (a string 'Hostname' beside an "
            "existing number 'Hostname' becomes 'Device Hostname'). Use an empty "
            "string when existing is true."
        )
    )
    new_field_type: str = Field(
        description="Type of the new field: one of string, number, list, boolean, datetime."
    )
    new_field_unique: bool = Field(
        description=(
            "True ONLY when existing is false AND the plugin field's description "
            "says either that its value can be used to merge/correlate records "
            "with other plugins, or that the value is unique to the person or "
            "device the record is about. False for everything else — including a "
            "required field that is merely the plugin's own internal record "
            "identifier, even one described as 'unique' inside the vendor's own "
            "product or console, since that value is sent back to the plugin on "
            "update and must not collect values written by other plugins. False "
            "whenever existing is true."
        )
    )
    reason: str = Field(
        description=(
            "One or two short sentences in plain, non-technical language telling "
            "the person reviewing this suggestion what will happen and why (e.g. "
            "'This email address identifies the same user in other products, so "
            "creating it as a unique field lets information about that user be "
            "combined into one record.'). Refer to platform fields by their "
            "readable form ('user_email' -> User Email). Do not use technical "
            "vocabulary such as type, unique=true, is_mapped or sample values, "
            "do not narrate your reasoning, and do not restate the input."
        )
    )


class FieldMappingResponse(BaseModel):
    """Field mapping LLM response container.

    Deliberately contains ONLY the mappings list — there is no top-level free-text
    field. When a top-level ``reason`` field existed, the model used it as an escape
    hatch: it explained what *should* be mapped and returned an empty/absent
    ``fields`` list. With ``fields`` as the sole output, producing the mappings is
    the only valid response. ``min_length`` adds a ``minItems: 1`` hint as well.
    """

    fields: list[FieldMappingResultLLM] = Field(
        description=(
            "One entry for EVERY plugin field provided. This is the ONLY output and "
            "MUST contain every plugin field. It MUST NOT be empty. A plugin field "
            "is never omitted: when no existing platform field has a matching type, "
            "or the label a new field would take is already used, propose a new "
            "field under a distinct label instead of leaving the field out."
        ),
        min_length=1,
    )


class AutoMapperResponse(BaseModel):
    """Combined auto-mapper result returned to the frontend."""

    destination: Optional[str] = None
    reason: str
    fields: list[FieldMappingResult] = []
