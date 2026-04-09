"""API endpoints for managing ITSM custom fields (dynamic sections)."""

from fastapi import APIRouter, HTTPException, status
from netskope.integrations.itsm.models.custom_fields import (
    CustomFieldIn, CustomFieldOut, CustomFieldDelete, CustomFieldsSection, FieldInfo
    )
from netskope.common.utils import DBConnector, Collections, PluginHelper, Logger
from typing import List

router = APIRouter()
connector = DBConnector()
helper = PluginHelper()


@router.get("/custom_fields", response_model=List[CustomFieldsSection])
def list_custom_fields():
    """List all custom fields grouped by section with metadata."""
    cursor = connector.collection(Collections.ITSM_CUSTOM_FIELDS).find()
    return [
        CustomFieldsSection(
            section=doc.get("section"),
            fields=[
                FieldInfo(**field) if isinstance(field, dict) else FieldInfo(name=field, is_default=False)
                for field in doc.get("fields", [])
            ]
        )
        for doc in cursor
    ]


@router.post("/custom_fields", response_model=CustomFieldOut, status_code=status.HTTP_201_CREATED)
def create_custom_field(field: CustomFieldIn):
    """Add fields to a section (create section if needed). Returns the updated section and all fields."""
    section_doc = connector.collection(Collections.ITSM_CUSTOM_FIELDS).find_one({"section": field.section})
    if section_doc:
        field_dicts = [f.model_dump() for f in field.fields]
        connector.collection(Collections.ITSM_CUSTOM_FIELDS).update_one(
            {"section": field.section},
            {"$addToSet": {"fields": {"$each": field_dicts}}}
        )
        updated_doc = connector.collection(Collections.ITSM_CUSTOM_FIELDS).find_one({"section": field.section})
        return CustomFieldOut(**updated_doc)
    else:
        connector.collection(Collections.ITSM_CUSTOM_FIELDS).insert_one(field.model_dump())
        return CustomFieldOut(**field.model_dump())


@router.delete("/custom_fields", response_model=CustomFieldOut)
def delete_custom_field(field: CustomFieldDelete):
    """Remove fields from a section. Validates against default status and all configurations."""
    section_doc = connector.collection(Collections.ITSM_CUSTOM_FIELDS).find_one({"section": field.section})
    if not section_doc:
        raise HTTPException(status_code=404, detail="Section not found.")

    default_fields_in_section = {
        f.get("name") for f in section_doc.get("fields", [])
        if isinstance(f, dict) and f.get("is_default") is True
    }

    field_names_to_delete = set(field.fields)

    attempting_to_delete_defaults = field_names_to_delete.intersection(default_fields_in_section)
    if attempting_to_delete_defaults:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete default fields: {', '.join(sorted(list(attempting_to_delete_defaults)))}"
        )

    field_usage = {field_name: set() for field_name in field_names_to_delete}

    configs_cursor = connector.collection(Collections.ITSM_CONFIGURATIONS).find(
        {"parameters.mapping_config": {"$exists": True}},
        {"name": 1, "parameters.mapping_config": 1}
    )

    for config in configs_cursor:
        config_name = config.get("name")
        mapping_config = config.get("parameters", {}).get("mapping_config", {})

        section_to_check = mapping_config.get(field.section)

        if section_to_check:
            mappings_array = section_to_check.get("mappings", [])
            if isinstance(mappings_array, list):
                for mapping_dict in mappings_array:
                    for source_field in mapping_dict.keys():
                        if source_field in field_usage:
                            field_usage[source_field].add(config_name)

    # Check for any fields that were found in configurations
    conflicts = {field: list(configs) for field, configs in field_usage.items() if configs}

    if conflicts:
        # Build a detailed error message listing all conflicts
        error_messages = []
        for field_name, config_names in conflicts.items():
            config_list_str = ", ".join(sorted(config_names))
            error_messages.append(f"Cannot delete the custom field ,"
                                  f" it is mapped in the plugin configuration - '{config_list_str}'")

        full_error_detail = ". ".join(error_messages)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=full_error_detail
        )

    # If we get here, the field is not in use, so it's safe to delete.
    connector.collection(Collections.ITSM_CUSTOM_FIELDS).update_one(
        {"section": field.section},
        {"$pull": {"fields": {"name": {"$in": field.fields}}}}
    )
    updated_doc = connector.collection(Collections.ITSM_CUSTOM_FIELDS).find_one({"section": field.section})

    if updated_doc and not updated_doc.get("fields"):
        connector.collection(Collections.ITSM_CUSTOM_FIELDS).delete_one({"section": field.section})
        return CustomFieldOut(section=field.section, fields=[])

    return CustomFieldOut(
        section=field.section,
        fields=[
            FieldInfo(**f) if isinstance(f, dict) else FieldInfo(name=f, is_default=False)
            for f in updated_doc.get("fields", [])
        ]
    )


@router.post("/plugins/{plugin}/default_custom_mappings")
def get_plugin_default_mappings(plugin: str, format: str = "structured"):
    """Get default custom mappings from plugin."""
    PluginClass = helper.find_by_id(plugin)  # NOSONAR S117
    if PluginClass is None:
        raise HTTPException(status_code=404, detail=f"Plugin '{plugin}' not found.")

    try:
        plugin_instance = PluginClass(None, None, {}, None, Logger())
        try:
            default_mappings = plugin_instance.get_default_custom_mappings()
        except (NotImplementedError, AttributeError):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="This plugin does not support default custom mappings."
            )

        if format == "configuration":
            mapping_config = {}
            for mapping in default_mappings:
                section_key = mapping.section
                mapping_config[section_key] = {
                    "event_field": mapping.event_field,
                    "destination_label": mapping.destination_label,
                    "mappings": [
                        {fm.name: fm.mapped_value} for fm in mapping.field_mappings
                    ]
                }
            return {"mapping_config": mapping_config}
        else:
            return default_mappings
    except HTTPException as err:
        raise err
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def sync_plugin_defaults_to_custom_fields(plugin_id: str):
    """Fetch plugin's default mapping and performs few validations.

    Fetch a plugin's default mappings and ensures its default fields
    are saved to the ITSM_CUSTOM_FIELDS collection.
    """
    PluginClass = helper.find_by_id(plugin_id)  # NOSONAR S117
    if not PluginClass:
        return

    try:
        plugin_instance = PluginClass(None, None, {}, None, Logger())
        default_mappings = plugin_instance.get_default_custom_mappings()
    except (NotImplementedError, AttributeError):
        return

    for section_data in default_mappings:
        section_name = section_data.section
        default_fields_for_section = [
            FieldInfo(name=fm.name, is_default=True)
            for fm in section_data.field_mappings if fm.is_default
        ]

        if not default_fields_for_section:
            continue

        section_doc = connector.collection(Collections.ITSM_CUSTOM_FIELDS).find_one(
            {"section": section_name}
        )

        if section_doc:
            existing_field_names = {f.get("name") for f in section_doc.get("fields", [])}
            new_fields_to_add = [
                field.model_dump() for field in default_fields_for_section
                if field.name not in existing_field_names
            ]
            if new_fields_to_add:
                connector.collection(Collections.ITSM_CUSTOM_FIELDS).update_one(
                    {"section": section_name},
                    {"$addToSet": {"fields": {"$each": new_fields_to_add}}}
                )
        else:
            connector.collection(Collections.ITSM_CUSTOM_FIELDS).insert_one({
                "section": section_name,
                "fields": [field.model_dump() for field in default_fields_for_section]
            })
