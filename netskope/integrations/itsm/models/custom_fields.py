"""Provides custom field models for ITSM integration."""

from pydantic import field_validator, StringConstraints, BaseModel, Field
from netskope.common.utils import DBConnector, Collections
from typing_extensions import Annotated
from typing import List, Optional
from enum import Enum

connector = DBConnector()


class MappingDirection(str, Enum):
    """Defines the valid directions for custom field mapping."""

    FORWARD = "forward"
    REVERSE = "reverse"


class FieldInfo(BaseModel):
    """Model for individual field with metadata."""

    name: str = Field(..., description="Field name")
    is_default: bool = Field(
        default=False, description="Whether this field is a default from plugin"
    )


class CustomFieldMapping(BaseModel):
    """Model for field with its mapped value (used in plugin defaults)."""

    name: str = Field(..., description="Field name")
    mapped_value: str = Field(..., description="Value this field maps to in the plugin")
    is_default: bool = Field(
        default=True, description="Whether this field is a default from plugin"
    )


class CustomFieldIn(BaseModel):
    """Incoming custom field model for adding fields to a section."""

    section: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1)
        ] = Field(
            ..., description="Custom section/type, e.g., status, severity, priority, etc."
            )
    fields: List[FieldInfo] = Field(..., min_items=1, description="Fields to add to the section.")

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, v, values):
        """Validate fields."""
        section = values.data.get("section")
        if section:
            existing_fields_lower = {}
            section_doc = connector.collection(Collections.ITSM_CUSTOM_FIELDS).find_one(
                {"section": section}
            )
            if section_doc:
                existing_fields_lower = {
                    (f.get("name") if isinstance(f, dict) else f).lower()
                    for f in section_doc.get("fields", [])
                }

            duplicates = [
                field.name for field in v if field.name.lower() in existing_fields_lower
            ]

            if duplicates:
                raise ValueError(
                    f"Custom Field with the provided name already exist in section '{section}' "
                    "(case is ignored)."
                )
            field_too_long = [field.name for field in v if len(field.name) > 500]
            if field_too_long:
                raise ValueError(
                    f"Cannot create field in section '{section}' with the provided name "
                    "as it exceeds the maximum allowed length of 500 characters."
                )
        return v


class CustomFieldOut(BaseModel):
    """Outbound custom field model."""

    section: str = Field(...)
    fields: List[FieldInfo] = Field(default_factory=list)


class CustomFieldDelete(BaseModel):
    """Custom field deletion model for removing fields from a section."""

    section: str = Field(...)
    fields: List[str] = Field(
        ..., min_items=1, description="Fields to remove from the section."
    )

    @field_validator("fields")
    @classmethod
    def validate_fields_exist(cls, v, values):
        """Validate that field exist."""
        section = values.data.get("section")
        if section:
            section_doc = connector.collection(Collections.ITSM_CUSTOM_FIELDS).find_one(
                {"section": section}
            )
            if not section_doc:
                raise ValueError(f"Section '{section}' does not exist.")

            existing_field_names = [
                f.get("name") if isinstance(f, dict) else f
                for f in section_doc.get("fields", [])
            ]
            missing = set(v) - set(existing_field_names)
            if missing:
                raise ValueError(
                    f"Fields {missing} do not exist in section '{section}'."
                )
        return v


class CustomFieldsSection(BaseModel):
    """Model for a section containing multiple custom fields."""

    section: str = Field(..., description="Section name, e.g., status, severity")
    fields: List[FieldInfo] = Field(
        default_factory=list, description="List of field objects with metadata"
    )
    field_mappings: Optional[List[CustomFieldMapping]] = Field(
        default_factory=list, description="List of field mappings with values"
    )


class CustomFieldsSectionWithMappings(BaseModel):
    """Model for a section with field-to-value mappings (used in plugin defaults)."""

    section: str = Field(..., description="Section name, e.g., status, severity")
    event_field: str = Field(
        ...,
        description="Event field this section maps to, e.g., ticket_status, severity",
    )
    field_mappings: List[CustomFieldMapping] = Field(
        default_factory=list, description="List of field mappings with values"
    )
    destination_label: str = Field(..., description="Label for the destination field")
