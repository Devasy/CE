"""Threat Indicators CRE entity schema (aligned with CTE Threat IOCs)."""

from typing import Any, Optional

from ..models import EntityFieldType, EntityTypeCoalesceStrategy

THREAT_INDICATORS_ENTITY_NAME = "Threat Indicators"

# Nested group display labels (path tuple -> label), mirrors Threat IOCs fields.js
TI_GROUP_LABELS = {
    ("sources",): "IoCs by Sources",
    ("sources", "retractionDestinations"): "Retraction Result",
    ("sources", "destinations"): "Sharing Result",
}


def _entity_field(
    label: str,
    name: str,
    field_type: EntityFieldType,
    unique: bool = False,
    coalesce_strategy: Optional[EntityTypeCoalesceStrategy] = None,
    params: Optional[dict] = None,
) -> dict:
    """Build a CRE entity field document with an explicit Mongo/indicator field name."""
    doc = {
        "label": label,
        "name": name,
        "type": field_type.value,
        "unique": unique,
        "coalesceStrategy": coalesce_strategy.value if coalesce_strategy else None,
        "params": params,
    }
    return doc


def get_threat_indicators_entity_fields() -> list[dict]:
    """Canonical Threat Indicators entity fields for CREV2_ENTITIES."""
    overwrite = EntityTypeCoalesceStrategy.OVERWRITE
    merge = EntityTypeCoalesceStrategy.MERGE

    top_level = [
        _entity_field("Value", "value", EntityFieldType.STRING, unique=True, coalesce_strategy=overwrite),
        _entity_field(
            "Type",
            "type",
            EntityFieldType.STRING,
            coalesce_strategy=overwrite,
        ),
        _entity_field("Test", "test", EntityFieldType.BOOLEAN),
        _entity_field("Active", "active", EntityFieldType.BOOLEAN),
        _entity_field("Safe", "safe", EntityFieldType.BOOLEAN),
        _entity_field("Shared With", "sharedWith", EntityFieldType.LIST, coalesce_strategy=merge),
        _entity_field("Expires At", "expiresAt", EntityFieldType.DATETIME, coalesce_strategy=overwrite),
        _entity_field(
            "Total Netskope Hits",
            "internalHits",
            EntityFieldType.NUMBER,
            coalesce_strategy=overwrite,
        ),
        _entity_field(
            "Total Other Hits",
            "externalHits",
            EntityFieldType.NUMBER,
            coalesce_strategy=overwrite,
        ),
    ]

    sources_fields = [
        _entity_field("Source", "sources.source", EntityFieldType.STRING, coalesce_strategy=overwrite),
        _entity_field(
            "Netskope Hits",
            "sources.internalHits",
            EntityFieldType.NUMBER,
            coalesce_strategy=overwrite,
        ),
        _entity_field(
            "All Other Hits",
            "sources.externalHits",
            EntityFieldType.NUMBER,
            coalesce_strategy=overwrite,
        ),
        _entity_field(
            "Extended Information",
            "sources.extendedInformation",
            EntityFieldType.STRING,
            coalesce_strategy=overwrite,
        ),
        _entity_field(
            "Comments",
            "sources.comments",
            EntityFieldType.STRING,
            coalesce_strategy=overwrite,
        ),
        _entity_field(
            "First Seen",
            "sources.firstSeen",
            EntityFieldType.DATETIME,
            coalesce_strategy=overwrite,
        ),
        _entity_field(
            "Last Seen",
            "sources.lastSeen",
            EntityFieldType.DATETIME,
            coalesce_strategy=overwrite,
        ),
        _entity_field("Tags", "sources.tags", EntityFieldType.LIST, coalesce_strategy=merge),
        _entity_field(
            "Reputation",
            "sources.reputation",
            EntityFieldType.NUMBER,
            coalesce_strategy=overwrite,
        ),
        _entity_field("Retracted", "sources.retracted", EntityFieldType.BOOLEAN),
        _entity_field(
            "Severity",
            "sources.severity",
            EntityFieldType.STRING,
            coalesce_strategy=overwrite,
        ),
        _entity_field(
            "Retraction Destination Name",
            "sources.retractionDestinations.name",
            EntityFieldType.STRING,
            coalesce_strategy=overwrite,
        ),
        _entity_field(
            "Retraction Destination Status",
            "sources.retractionDestinations.status",
            EntityFieldType.STRING,
            coalesce_strategy=overwrite,
        ),
        _entity_field(
            "Sharing Destination Name",
            "sources.destinations.name",
            EntityFieldType.STRING,
            coalesce_strategy=overwrite,
        ),
        _entity_field(
            "Sharing Destination Status",
            "sources.destinations.status",
            EntityFieldType.STRING,
            coalesce_strategy=overwrite,
        ),
    ]

    return top_level + sources_fields


def build_threat_indicators_query_schema() -> dict:
    """Query-builder schema aligned with CTE Threat IOCs (fields.js).

    That alignment -- not ``_field_to_query_def`` -- is this schema's contract,
    so the two deliberately disagree on List fields: a CRE entity's List is a
    ``text`` widget, the Lists here stay ``select``. Do not "fix" the
    inconsistency. No filter UI renders these nodes: the Records page drops the
    Threat Indicators entity from its selector and both rule builders substitute
    the Threat IOCs field config, which fills Tags and Shared With from live
    lookups. The one consumer left is the action-parameter picker, which matches
    a text parameter on ``valueType`` -- drop that and mapping IoC tags onto a
    text action parameter silently stops working.
    """
    datetime_settings = {
        "timeFormat": "HH:mm:ss",
        "valueFormat": "YYYY-MM-DDTHH:mm:ssZZ",
    }

    return {
        "value": {
            "label": "Value",
            "type": "text",
            "label2": "This filter will be applied on IoC Metadata.",
        },
        "type": {
            "label": "Type",
            "type": "text",
            "label2": "This filter will be applied on IoC Metadata.",
        },
        "test": {
            "label": "Test",
            "type": "boolean",
            "label2": "This filter will be applied on IoC Metadata.",
        },
        "active": {
            "label": "Active",
            "type": "boolean",
            "label2": "This filter will be applied on IoC Metadata.",
        },
        "safe": {
            "label": "Safe",
            "type": "boolean",
            "label2": "This filter will be applied on IoC Metadata.",
        },
        "sharedWith": {
            "label": "Shared With",
            "type": "select",
            # Written by persist_cre_entity_indicators, not by the IoC's source,
            # so the action-parameter picker must not offer it.
            "mappable": False,
            # A List field (see ``get_threat_indicators_entity_fields``):
            # ``type`` is the filter widget, ``valueType`` is what the record
            # holds. Action parameters of type ``text`` match on ``valueType``
            # and would otherwise reject this field. Kept a ``select`` on
            # purpose -- see the docstring on why this diverges from
            # ``_field_to_query_def``, which maps a List to ``text``.
            "valueType": "text",
            "label2": "This filter will be applied on IoC Metadata.",
        },
        "expiresAt": {
            "label": "Expires At",
            "type": "datetime",
            "fieldSettings": datetime_settings,
            "label2": "This filter will be applied on IoC Metadata.",
        },
        "internalHits": {
            "label": "Total Netskope Hits",
            "type": "number",
            "fieldSettings": {"min": 1},
            "preferWidgets": ["number"],
            "label2": "This filter will be applied on IoC Metadata.",
        },
        "externalHits": {
            "label": "Total Other Hits",
            "type": "number",
            "fieldSettings": {"min": 1},
            "preferWidgets": ["number"],
            "label2": "This filter will be applied on IoC Metadata.",
        },
        "sources": {
            "label": "IoCs by Sources",
            "label2": "This filter will be applied on all IoC sources.",
            "type": "!group",
            "subfields": {
                "source": {
                    "label": "Source",
                    "type": "text",
                    "label2": "This filter will be applied on all IoC sources.",
                },
                "internalHits": {
                    "label": "Netskope Hits",
                    "type": "number",
                    "fieldSettings": {"min": 1},
                    "preferWidgets": ["number"],
                    "label2": "This filter will be applied on all IoC sources.",
                },
                "externalHits": {
                    "label": "All Other Hits",
                    "type": "number",
                    "fieldSettings": {"min": 1},
                    "preferWidgets": ["number"],
                    "label2": "This filter will be applied on all IoC sources.",
                },
                "extendedInformation": {
                    "label": "Extended Information",
                    "type": "text",
                    "label2": "This filter will be applied on all IoC sources.",
                },
                "comments": {
                    "label": "Comments",
                    "type": "text",
                    "label2": "This filter will be applied on all IoC sources.",
                },
                "firstSeen": {
                    "label": "First Seen",
                    "type": "datetime",
                    "fieldSettings": datetime_settings,
                    "label2": "This filter will be applied on all IoC sources.",
                },
                "lastSeen": {
                    "label": "Last Seen",
                    "type": "datetime",
                    "fieldSettings": datetime_settings,
                    "label2": "This filter will be applied on all IoC sources.",
                },
                "tags": {
                    "label": "Tags",
                    "type": "select",
                    # A List field -- see the ``sharedWith`` note above.
                    "valueType": "text",
                    # Empty on purpose: mirrors the empty ``listValues`` in
                    # fields.js, which the Threat IOCs page replaces with an
                    # async tag search before rendering.
                    "fieldSettings": {"listValues": []},
                    "label2": "This filter will be applied on all IoC sources.",
                },
                "reputation": {
                    "label": "Reputation",
                    "type": "number",
                    "fieldSettings": {"min": 1, "max": 10},
                    "preferWidgets": ["number"],
                    "label2": "This filter will be applied on all IoC sources.",
                },
                "retracted": {
                    "label": "Retracted",
                    "type": "boolean",
                    "label2": "This filter will be applied on IoC Metadata.",
                },
                "retractionDestinations": {
                    "label": "Retraction Result",
                    "label2": "This filter will be applied on all IoC sources.",
                    "type": "!group",
                    "subfields": {
                        "name": {
                            "label": "Destination Name",
                            "type": "text",
                            "label2": "This filter will be applied on all IoC sources.",
                        },
                        "status": {
                            "label": "Status",
                            "type": "text",
                            "label2": "This filter will be applied on all IoC sources.",
                        },
                    },
                },
                "destinations": {
                    "label": "Sharing Result",
                    "label2": "This filter will be applied on all IoC sources.",
                    "type": "!group",
                    "subfields": {
                        "name": {
                            "label": "Destination Name",
                            "type": "text",
                            "label2": "This filter will be applied on all IoC sources.",
                        },
                        "status": {
                            "label": "Status",
                            "type": "text",
                            "label2": "This filter will be applied on all IoC sources.",
                        },
                    },
                },
                "severity": {
                    "label": "Severity",
                    "type": "text",
                    "label2": "This filter will be applied on all IoC sources.",
                },
            },
        },
    }


def _group_label(path: tuple[str, ...]) -> str:
    if path in TI_GROUP_LABELS:
        return TI_GROUP_LABELS[path]
    return path[-1].replace("_", " ").title()


def merge_nested_group(
    subfields: dict[str, Any],
    path_parts: list[str],
    field_def: dict[str, Any],
    path_prefix: tuple[str, ...] = (),
) -> None:
    """Insert a field definition into nested !group subfields."""
    if len(path_parts) == 1:
        subfields[path_parts[0]] = field_def
        return

    head, *tail = path_parts
    full_path = path_prefix + (head,)
    if head not in subfields or subfields[head].get("type") != "!group":
        subfields[head] = {
            "label": _group_label(full_path),
            "type": "!group",
            "subfields": {},
        }
    merge_nested_group(
        subfields[head]["subfields"],
        tail,
        field_def,
        full_path,
    )
