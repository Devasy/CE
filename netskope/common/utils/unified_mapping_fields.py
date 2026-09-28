"""Utility functions for resolving unified mapping collection fields.

Two field sources are supported:
  - indicators collection: fields extracted programmatically from the Indicator
    and IndicatorSourceOut Pydantic models using type introspection.
  - crev2_entity_<name> collections: fields fetched from the crev2_entities
    MongoDB collection via get_entity_fields_from_db().

``cached_table_fields`` / ``effective_join_field`` at the end of this module read
that metadata to answer "which path do I join on?", which both
``unified_mapping_exec`` (building the $lookup) and ``unified_mapping_indexes``
(indexing what the $lookup reads) need — so it lives with the metadata itself
rather than in either caller.
"""

import re
import types
from datetime import datetime
from enum import Enum
from typing import Optional, get_args, get_origin

from .db_connector import DBConnector, Collections

_connector = DBConnector()

NONE_TYPE = types.NoneType

# Fields from IndicatorSourceOut that are useful in a join / filter context.
# Complex list-of-dict fields (destinations, retractionDestinations) are excluded.
_SOURCE_EXCLUDED_FIELDS = {"destinations", "retractionDestinations"}

# CE bookkeeping fields that may never feed an IOC field mapping or an action
# parameter. Their value is written by CE, not by the source that produced the
# record -- ``sharedWith`` by persist_cre_entity_indicators, ``lastUpdated`` by
# the ingest that stamped the document -- so mapping one ships platform state
# as though it were record data. Filters, joins and unified-mapping recency
# still read them: only mapping is closed.
#
# Scoped to indicator-backed collections by ``is_mappable_field``: a same-named
# field a user adds in the CRE schema editor is their own record data.
UNMAPPABLE_SOURCE_FIELDS = frozenset({"sharedWith", "lastUpdated"})


def is_mappable_field(field_name: str, table: str = "", declared=None) -> bool:
    """Whether a field may be a field-mapping or action-parameter source.

    Scoped to the collection that owns the field. Only indicator-backed data
    carries CE-written fields, so a CRE entity's are open -- its schema is
    user-authored. A unified-mapping key names its own collection
    (``crev2_entity_Users.sharedWith``) and is judged on that; otherwise
    ``table`` says where a bare name lives, and with neither it can only be the
    CE-stamped field, so it stays closed.

    ``declared`` are that entity's declared field names, for a caller that has
    them: ``lastUpdated`` is stamped onto every CRE record too, so on a CRE
    entity only a name the schema actually declares is the source's own data.
    ``None`` means the caller cannot say, and every CRE entity field is opened.
    """
    bare = field_name.rsplit(".", 1)[-1]
    if bare not in UNMAPPABLE_SOURCE_FIELDS:
        return True
    prefix = Collections.CREV2_ENTITY_PREFIX.value
    # The leading segment when it names a collection, else the caller's table --
    # so a key that says which collection it means is never overridden by one.
    head = field_name.split(".", 1)[0]
    names_collection = head == Collections.INDICATORS.value or head.startswith(prefix)
    if not (head if names_collection else table).startswith(prefix):
        return False
    return declared is None or bare in declared


def unmappable_source_fields(specs, table: str = "", declared=None) -> list[str]:
    """The ``$``-reference specs among ``specs`` that point at a closed field.

    Takes the raw spec values so every caller applies one rule: a field mapping
    passes its mapping values, an action its parameter values (which may be a
    list of specs, as ``_map_params`` allows). Static literals are ignored.

    ``table``/``declared`` describe the collection a bare (unprefixed) reference
    resolves against -- a business rule's own entity; see ``is_mappable_field``.
    Unified mapping leaves them empty: its specs are ``$<table>.<field>``, so
    each one already says which collection it means.
    """
    found = set()
    for spec in specs:
        for value in spec if isinstance(spec, list) else [spec]:
            if not (isinstance(value, str) and value.startswith("$")):
                continue
            path = value[1:].lstrip(".")
            if path and not is_mappable_field(path, table, declared):
                found.add(path)
    return sorted(found)


def unmappable_action_sources(
    actions_by_config, table: str = "", declared=None
) -> list[str]:
    """The closed fields any action's parameters reference, across every config.

    Both action validators (CREv2's ``validate_actions`` and unified mapping's
    ``validate_cre_actions``) take the same ``{config: [action]}`` shape; walking
    it here means one report covering every offending action, rather than each
    caller stopping at the first.
    """
    return unmappable_source_fields(
        (
            spec
            for actions in (actions_by_config or {}).values()
            for action in actions or []
            for spec in (action.parameters or {}).values()
        ),
        table,
        declared,
    )


def unmappable_source_message(names) -> str:
    """One wording for the rejection, shared by all four validators."""
    return (
        f"{', '.join(names)} cannot be used as a source: CE writes these fields "
        "itself, rather than the source that produced the record. Only field "
        "mapping and action parameters are closed to them."
    )


# Top-level Indicator fields that are per-source concepts — they exist on both the
# Indicator root and on each IndicatorSourceOut sub-document, but the root copies
# are aggregates / defaults and are only meaningful in the sources context.
# Exposing them at the top level would duplicate what sources.* already provides.
_INDICATOR_TOP_LEVEL_EXCLUDED_FIELDS = {
    "firstSeen",
    "lastSeen",
    "reputation",
    "severity",
    "comments",
    "tags",
    "extendedInformation",
    "retracted",
    "updated",
}


# Display labels for the two indicator enums rendered as multi-selects in a
# unified mapping rule's filter builder. Kept here rather than imported from
# threat_indicators_entity: that module is CRE's own hand-maintained mirror and
# no longer exposes them, and this is their only consumer.
_INDICATOR_TYPE_LABELS = {
    "url": "URL",
    "md5": "MD5",
    "sha256": "SHA256",
    "ipv4": "IPV4",
    "ipv6": "IPV6",
    "ipv4_cidr": "IPv4 CIDR",
    "ipv6_cidr": "IPv6 CIDR",
    "hostname": "Hostname",
    "domain": "Domain",
    "fqdn": "FQDN",
}

_SEVERITY_LABELS = {
    "unknown": "Unknown",
    "low": "Low",
    "medium": "Medium",
    "high": "High",
    "critical": "Critical",
}


# ``_annotation_to_type_string`` result -> the primitive the record holds, in the
# query-builder vocabulary ENTITY_FIELD_VALUE_TYPES uses ("text", not "string").
#
# Deliberately NOT reused from ENTITY_FIELD_VALUE_TYPES, even though a str-Enum
# dict accepts plain-string lookups and five of these six rows now happen to
# match it. The two tables are keyed on different vocabularies -- stored CRE
# field types there, ``_annotation_to_type_string`` results here -- so a shared
# row is a coincidence, not a contract, and "select" has no EntityFieldType at
# all (it is the widget this module assigns to an enum field; all indicator
# enums are ``str`` enums, hence text). Reading a CRE model's table to answer a
# question about CTE annotations would couple the two for no gain.
#
# Knowing an array's element primitive -- "list" is a ``list[str]`` -- is what
# lets it join against a scalar (Mongo matches array elements) instead of
# falling through as unverifiable.
#
# Covers every value ``_annotation_to_type_string`` can return today; looked up
# with ``.get`` so a future addition degrades to "unknown primitive"
# (permissive) rather than raising, or worse, passing a non-primitive through as
# one and false-rejecting every join against it.
_INDICATOR_VALUE_TYPES = {
    "string": "text",
    "select": "text",
    "list": "text",
    "number": "number",
    "boolean": "boolean",
    "datetime": "datetime",
}


def _camel_to_label(name: str) -> str:
    """Convert a camelCase field name to a human-readable label.

    Examples:
        "firstSeen"   -> "First Seen"
        "internalHits" -> "Internal Hits"
        "value"        -> "Value"
    """
    spaced = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)
    return spaced.title()


def _enum_values(annotation) -> Optional[list]:
    """Return the allowed values of an Enum annotation (Optional[...] unwrapped), else None.

    A closed value set is what makes a field a "select" rather than free text, and
    the same values populate the filter's dropdown — see _annotation_to_type_string
    and get_indicator_fields.
    """
    args = get_args(annotation)
    if args:
        non_none = [type_arg for type_arg in args if type_arg is not NONE_TYPE]
        if non_none and get_origin(annotation) is not list:
            annotation = non_none[0]
    try:
        if issubclass(annotation, Enum):
            return [member.value for member in annotation]
    except TypeError:
        pass
    return None


def _annotation_to_type_string(annotation) -> str:
    """Map a Python / Pydantic type annotation to a unified-mapping field type string.

    Supported return values: "string", "number", "boolean", "datetime", "list",
    "select".
    """
    origin = get_origin(annotation)
    args = get_args(annotation)

    # Union[X, None]  (Optional[X])
    if origin is getattr(types, "UnionType", None) or str(origin) in (
        "typing.Union",
        "<class 'types.UnionType'>",
    ):
        non_none = [type_arg for type_arg in args if type_arg is not NONE_TYPE]
        if non_none:
            return _annotation_to_type_string(non_none[0])
        return "string"

    # Explicit Union from typing module (origin == Union via __origin__ trick)
    if hasattr(annotation, "__origin__"):
        # list / List[...]
        if annotation.__origin__ is list:
            return "list"
        # Union
        if str(annotation.__origin__) in ("typing.Union",) or getattr(
            annotation.__origin__, "__name__", ""
        ) == "Union":
            non_none = [type_arg for type_arg in args if type_arg is not NONE_TYPE]
            if non_none:
                return _annotation_to_type_string(non_none[0])
            return "string"

    if origin is list:
        return "list"

    if annotation is bool:
        return "boolean"
    if annotation in (int, float):
        return "number"
    if annotation is datetime:
        return "datetime"
    if annotation is str:
        return "string"

    if _enum_values(annotation) is not None:
        return "select"

    try:
        if issubclass(annotation, str):
            return "string"
    except TypeError:
        pass

    return "string"


def _annotation_to_source_type(annotation) -> str:
    """Return the stored data type behind an annotation, ignoring widget choice.

    ``_annotation_to_type_string`` answers "which filter widget?", and collapses
    every closed value set to "select" — an indicator's enum, a CRE list and a
    CRE value_map all look identical there. A field mapping needs the real type
    (mapping an IOC type from a numeric value map is not the same as from an
    enum), so an enum resolves to whatever kind its members are.
    """
    widget = _annotation_to_type_string(annotation)
    if widget != "select":
        return widget
    values = _enum_values(annotation) or []
    if values and all(isinstance(value, (int, float)) for value in values):
        return "number"
    return "string"


def get_indicator_fields() -> list[dict]:
    """Return field metadata for the indicators collection.

    Fields are extracted directly from the ``Indicator`` Pydantic model via
    ``model_fields``, so they stay in sync with the model automatically.
    Source sub-fields are extracted from ``IndicatorSourceOut`` and returned
    with ``nested_array="sources"`` metadata for ``$unwind`` handling.

    Returns:
        list[dict]: Each entry has keys: name, label, type (filter widget),
        sourceType (stored data type), unique, normalized, and optionally
        values (the allowed values of a "select" field), nested_array and subKey
        (for sources.* sub-fields), fieldSettings.
    """
    from netskope.integrations.cte.models.indicator import Indicator, IndicatorSourceOut

    fields: list[dict] = []
    type_list_values = dict(_INDICATOR_TYPE_LABELS)
    severity_list_values = dict(_SEVERITY_LABELS)

    # The indicators collection has a single unique field — ``value`` — enforced by a
    # unique index created in migration 4.1.0. CTE has no per-field unique concept like
    # CRE entities, so uniqueness is defined here: only ``value`` is unique.
    for field_name, field_info in Indicator.model_fields.items():
        if field_name in _INDICATOR_TOP_LEVEL_EXCLUDED_FIELDS:
            continue
        annotation = field_info.annotation
        annotation_type = _annotation_to_type_string(annotation)
        entry = {
            "name": field_name,
            "label": _camel_to_label(field_name),
            # All three are read off the annotation BEFORE the ``select``
            # overrides below, so the widget change does not erase what the
            # field actually stores.
            "type": annotation_type,
            "sourceType": _annotation_to_source_type(annotation),
            "valueType": _INDICATOR_VALUE_TYPES.get(annotation_type),
            "multiValued": annotation_type == "list",
            "unique": field_name == "value",
            "normalized": False,  # CTE indicators never use CRE-style normalization
            "mappable": is_mappable_field(field_name),
        }
        values = _enum_values(annotation)
        if values is not None:
            entry["values"] = values
        # ``type``/``sharedWith`` are enum / multi-select fields on the CTE side
        # (see ThreatIOCs/fields.js and threat_indicators_entity.py) — reporting
        # them as plain "string"/"list" here would render a free-text filter
        # input instead of the same "Any in"/"Not in" multi-select CTE and CRE
        # business rules use for the identical underlying field.
        if field_name == "type":
            entry["type"] = "select"
            entry["fieldSettings"] = {"listValues": type_list_values}
        elif field_name == "sharedWith":
            entry["type"] = "select"
        fields.append(entry)

    # ``lastUpdated`` is the CE-stamped storage-time field on indicator
    # documents (set in insert_or_update_indicator). It lives on the DB model
    # (IndicatorDB), not the plugin-facing Indicator model, so it is exposed
    # explicitly for unified mapping filtering and recency qualification. Labeled
    # "IoC Last Updated" (not the generic "Last Updated") so it isn't confused
    # with a joined CRE entity's own same-named field in the filter dropdown.
    fields.append({
        "name": "lastUpdated",
        "label": "IoC Last Updated",
        "type": "datetime",
        "sourceType": "datetime",
        "valueType": "datetime",
        "unique": False,
        "normalized": False,
        "multiValued": False,
        # CE-stamped storage time, so filterable but never a mapping source.
        "mappable": False,
    })

    for field_name, field_info in IndicatorSourceOut.model_fields.items():
        if field_name in _SOURCE_EXCLUDED_FIELDS:
            continue
        annotation = field_info.annotation
        annotation_type = _annotation_to_type_string(annotation)
        entry = {
            "name": f"sources.{field_name}",
            "label": f"Source · {_camel_to_label(field_name)}",
            # See the top-level loop: all three computed before the overrides.
            "type": annotation_type,
            "sourceType": _annotation_to_source_type(annotation),
            "valueType": _INDICATOR_VALUE_TYPES.get(annotation_type),
            "multiValued": annotation_type == "list",
            "unique": False,
            "normalized": False,
            "mappable": is_mappable_field(f"sources.{field_name}"),
            "nested_array": "sources",
            "subKey": field_name,
        }
        values = _enum_values(annotation)
        if values is not None:
            entry["values"] = values
        # ``severity``/``tags`` are select fields on the CTE side too (see
        # ThreatIOCs/fields.js's sources.severity / sources.tags) — same
        # reasoning as the top-level ``type``/``sharedWith`` override above.
        if field_name == "severity":
            entry["type"] = "select"
            entry["fieldSettings"] = {"listValues": severity_list_values}
        elif field_name == "tags":
            entry["type"] = "select"
        fields.append(entry)

    return fields


def entity_field_widget_meta(field_doc: dict) -> dict:
    """Translate a stored ``EntityFieldType`` into its query-builder metadata.

    Returns the three facts a stored type carries, each with exactly one job:

    * ``type`` — the filter WIDGET. It follows
      ``crev2/routers/records.py::_field_to_query_def`` where the choice matters
      (numeric types, List, Value Map String) so a joined CREv2-entity field
      filters the way it does on the Records and Business Rules pages, but it
      does NOT reproduce that function's output verbatim: STRING, IPV4, IPV6,
      EMAIL and RANGE_MAP keep their stored type name here where
      ``_field_to_query_def`` flattens all five to ``"text"``.

      That divergence is deliberate and inert. Inert because the consumer
      normalises: ``backendTypeToWidget`` in the UI's unifiedMappingFilterFields
      has a ``default`` branch returning a text widget, so every one of those
      names already renders as text. Deliberate because the stored name carries
      strictly more information than ``"text"`` does -- a caller can still tell
      an IPv4 field from a free-text one -- and nothing downstream needs the
      flattening: the join guard and the IOC field mapping both read
      ``valueType``, not this.
    * ``valueType`` — the stored primitive, from ``ENTITY_FIELD_VALUE_TYPES``.
      For an array field this is the ELEMENT primitive (a List holds text), which
      is what a join needs. ``None`` only for a Reference, whose target field's
      type is not resolved here; callers must read that as unknown, never as text.
    * ``multiValued`` — whether the record holds an array. ``type`` used to
      imply this by accident and lost it the moment two shapes collapsed onto
      one widget, so it is stated outright. Two things make a field an array: a
      List (under either coalesce strategy), and ANY type carrying the append
      (MERGE) strategy, which ingestion writes with ``$concatArrays`` /
      ``$setUnion`` — so an append String holds ``["a", "b"]``, not ``"b"``.
      ``valueType`` stays the ELEMENT primitive either way, which is what a join
      needs: Mongo matches a scalar against an array's elements.

    Plus ``fieldSettings`` when the widget needs options.

    Two types deliberately do NOT become a ``select``:

    * **List** → ``text``, exactly as ``_field_to_query_def`` does. Nothing can
      fill a List's ``listValues`` from the field definition, and a ``select``
      with no options reaches the user as an empty dropdown that also refuses
      custom values. Mongo matches a scalar against an array's elements, so
      ``equal``/``like`` on the bare field work as the user expects.
    * **Numeric types** (number, calculated, value_map_number) → ``number``.
      Their values are ints, and a ``select`` carries its options as JSON object
      keys, i.e. strings, so the filter emitted "5" against a stored 5 and
      matched nothing.
    """
    from netskope.integrations.crev2.models.entities import (
        ENTITY_FIELD_VALUE_TYPES,
        EntityFieldType,
        EntityTypeCoalesceStrategy,
    )

    raw_type = field_doc.get("type", "string")
    try:
        value_type = ENTITY_FIELD_VALUE_TYPES.get(EntityFieldType(raw_type))
    except ValueError:
        # An unrecognised stored type: unknown primitive, permissive downstream.
        value_type = None

    # A List is always an array; anything else becomes one under the append
    # (MERGE) strategy, which ingestion writes with $concatArrays / $setUnion
    # (see _store_records in crev2/tasks/fetch_records.py). A unique field
    # never carries a strategy at all -- EntityField.validate_coalesce_strategy
    # forces it to None -- so a unique field is always single-valued.
    multi_valued = (
        raw_type == "list"
        or field_doc.get("coalesceStrategy") == EntityTypeCoalesceStrategy.MERGE
    )

    meta = {"type": raw_type, "valueType": value_type, "multiValued": multi_valued}
    if raw_type == "list":
        return {**meta, "type": "text"}
    if value_type == "number":
        return {**meta, "type": "number"}
    if raw_type == "value_map_string":
        mappings = (field_doc.get("params") or {}).get("mappings") or []
        list_values = {
            m.get("value", m.get("label")): m.get("label") for m in mappings
        }
        meta = {**meta, "type": "select"}
        if list_values:
            meta["fieldSettings"] = {"listValues": list_values}
        return meta
    return meta


# The stored PRIMITIVE each IOC field's source must hold, matched on
# ``valueType`` and never on the filter-widget ``type``. Read by both rule
# validators, so neither can drift; mirrored by the UI's IocFieldMapping.jsx.
IOC_FIELD_VALUE_TYPES = {
    "value": {"text"},
    "type": {"text"},
    "severity": {"text"},
    "tags": {"text"},
    # A day count, NOT the datetime it is stored as: the share task clamps it to
    # 1-365, and a datetime source would skip that clamp.
    "expiresAt": {"number"},
    "reputation": {"number"},
    "comments": {"text"},
    "test": {"boolean"},
    "safe": {"boolean"},
}

# IOC fields holding a list. A scalar source is wrapped into a one-element list
# by the share task, so only the ELEMENT primitive has to match.
IOC_MULTI_VALUED_FIELDS = {"tags"}


def no_value_reason(
    field_type: Optional[str], unique: bool, map_labels: Optional[list] = None
) -> Optional[str]:
    """Why a field can never hold a value, or None when it can.

    Two store nothing whatever their primitive says: a unique CALCULATED field
    (never computed) and a Value Map / Range Map with no mappings (can never
    match). Mapping either resolves to ``None`` on every record.

    ``map_labels`` is a :func:`map_type_labels` result, so ``[]`` means
    "bounded, and empty" -- scoping the second case to the TEXT map types, the
    only ones that feed ``value``/``type`` where a None drops the record. An
    empty value_map_number is harmless: the share task leaves an optional
    numeric field at its default.

    Facts are passed explicitly, not as a field doc: the callers key the stored
    type differently (``type`` vs ``sourceType``).
    """
    if field_type == "calculated" and unique:
        return "it is a unique Calculated field, which is never computed"
    if map_labels is not None and not map_labels:
        return "it has no mappings configured, so it can never match a value"
    return None


def map_type_labels(field_doc: dict) -> Optional[list]:
    """The authored values a value_map_string / range_map field can ever store.

    The two are asymmetric (see ``_update_mapped_fields``): a Value Map String
    stores its mapping's **value**, a Range Map its **label**. ``None`` -- not
    ``[]`` -- for any other type, meaning "not a bounded set".
    """
    raw_type = field_doc.get("type")
    if raw_type not in ("value_map_string", "range_map"):
        return None
    key = "value" if raw_type == "value_map_string" else "label"
    mappings = (field_doc.get("params") or {}).get("mappings") or []
    return [
        value
        for mapping in mappings
        if isinstance(mapping, dict)
        and (value := mapping.get(key)) is not None
    ]


def _mapping_source_labels(spec, table: str) -> list:
    """Authored values the field a ``$``-spec points at can store, else [].

    ``table`` is the collection a bare (unprefixed) reference resolves against;
    a ``$<collection>.<field>`` spec names its own and overrides it.
    """
    if not (isinstance(spec, str) and spec.startswith("$")):
        return []
    path = spec[1:].lstrip(".")
    head, _, rest = path.partition(".")
    prefix = Collections.CREV2_ENTITY_PREFIX.value
    if rest and (head == Collections.INDICATORS.value or head.startswith(prefix)):
        table, field_name = head, rest
    else:
        field_name = path
    if not table.startswith(prefix):
        return []
    doc = _connector.collection(Collections.CREV2_ENTITIES).find_one(
        {"name": table[len(prefix):]}, {"fields": 1, "_id": 0}
    )
    for field_doc in (doc or {}).get("fields") or []:
        if isinstance(field_doc, dict) and field_doc.get("name") == field_name:
            return map_type_labels(field_doc) or []
    return []


def rule_mapped_tag_labels(rule_doc: dict, table: str = "") -> list:
    """Tag values a rule's field mapping can produce, from its bounded sources.

    Only a Value Map or Range Map feeding ``tags`` yields anything: its value set
    is authored and stored, so the tags it can produce are known. A plain String
    source is unknowable here and yields nothing, matching what
    ``validate_bounded_labels`` can check at save time.

    ``table`` is the rule's own entity collection, for a CTE rule's bare
    references; a unified mapping rule's specs name their collection themselves.
    """
    return _mapping_source_labels(
        (rule_doc.get("fieldMapping") or {}).get("tags"), table
    )


def missing_tags(names) -> list[str]:
    """Which of ``names`` are not tags that exist in CTE.

    One query for the whole set rather than one per name. Shared by the bounded
    map-type check and the static-literal check, so a rule cannot name a
    non-existent tag through either route.
    """
    wanted = {str(name) for name in names}
    if not wanted:
        return []
    existing = {
        doc.get("name")
        for doc in _connector.collection(Collections.TAGS).find(
            {"name": {"$in": sorted(wanted)}}, {"name": 1, "_id": 0}
        )
    }
    return sorted(wanted - existing)


def unknown_tag_message(ioc_field: str, source: str, unknown: list) -> str:
    """One wording for a mapping that names a tag CTE does not have."""
    return (
        f"The '{ioc_field}' field mapping cannot use '{source}': it can produce "
        f"tag(s) that do not exist in CTE ({', '.join(unknown)}). Create them "
        "under Threat Exchange > Tags first, or map a different field."
    )


def validate_bounded_labels(
    ioc_field: str, field_name: str, labels: Optional[list]
) -> Optional[str]:
    """Error message if a bounded value set can never satisfy ``ioc_field``.

    severity, type and tags accept only a closed set; a value outside it fails
    ``Indicator`` validation and drops the WHOLE record as CTE_1101. A map type's
    set is authored, so the mismatch is reported at save instead.

    Takes the label LIST, not the field doc: the CTE rule holds raw field docs
    (via ``map_type_labels``), unified mapping flattened entries (``mapLabels``).

    Args:
        ioc_field (str): IOC field the source is mapped to.
        field_name (str): Source field, as named in the message.
        labels (list | None): Authored value set; ``None`` is always accepted.

    Returns:
        str | None: The rejection message, or ``None`` when acceptable.
    """
    if not labels:
        return None
    from netskope.integrations.cte.models.indicator import (
        IndicatorType,
        SeverityType,
    )

    if ioc_field == "tags":
        unknown = missing_tags(labels)
        return unknown_tag_message(ioc_field, field_name, unknown) if unknown else None
    if ioc_field == "severity":
        allowed = {member.value for member in SeverityType}
    elif ioc_field == "type":
        allowed = {member.value for member in IndicatorType}
    else:
        return None

    unknown = sorted({str(label) for label in labels} - allowed)
    if not unknown:
        return None
    # The comparison is case-sensitive because the enum is: SeverityType("Low")
    # raises, so a mixed-case label would fail Indicator validation at share
    # time too. Saying so keeps the message from reading as a broken check.
    return (
        f"The '{ioc_field}' field mapping cannot use '{field_name}': it can "
        f"produce {', '.join(unknown)}, but '{ioc_field}' only accepts "
        f"{', '.join(sorted(allowed))} (matched exactly, including case)."
    )


def _entity_field_entry(field_doc: dict, table: str = "") -> Optional[dict]:
    """Build one CRE entity field's metadata entry, or None if unusable.

    The single builder for both callers below: ``get_entity_fields_from_db``
    (one entity, feeding ``/rules/fields`` and join validation) and
    ``get_all_unified_mapping_collections`` (every entity in one read, feeding
    ``/collections``). They used to construct this dict separately and had
    already drifted -- an unlabelled field came back humanised from one and raw
    from the other -- which is the same two-producers-disagree bug that left the
    filter builder testing for a ``type`` the payload no longer carried.
    """
    field_name = field_doc.get("name")
    if not field_name:
        return None
    widget = entity_field_widget_meta(field_doc)
    entry = {
        "name": field_name,
        "label": field_doc.get("label") or _camel_to_label(field_name),
        "type": widget["type"],
        # The stored EntityFieldType, before the widget translation folds a
        # value_map_string into "select" and a List into "text". The CRE action
        # form's parameter picker keys on this (UM_SOURCE_TYPE_TO_WIDGET in the
        # UI's Actions/ActionForm.jsx); IOC field mapping keys on valueType.
        "sourceType": field_doc.get("type", "string"),
        "unique": field_doc.get("unique", False),
        # A STRING field with params.normalization stores values as {value, plugins} objects.
        # Joining on such a field with a plain-string field from another collection would
        # silently produce no matches — flag it so validation can reject it early.
        "normalized": bool((field_doc.get("params") or {}).get("normalization")),
        # Kept alongside multiValued, which already folds append in: the payload
        # contract is pinned by test_unified_mapping_rules_field_mapping.
        "coalesceStrategy": field_doc.get("coalesceStrategy"),
        "multiValued": widget["multiValued"],
        # ``table`` scopes the closed-field rule: a user-declared entity field is
        # never CE bookkeeping, whatever it is named.
        "mappable": is_mappable_field(field_name, table),
    }
    # Omitted, not nulled, when unknown — "absent means unknown" is the
    # valueType convention every consumer already follows.
    if widget.get("valueType"):
        entry["valueType"] = widget["valueType"]
    if widget.get("fieldSettings"):
        entry["fieldSettings"] = widget["fieldSettings"]
    # Carried on the entry because a unified mapping rule validates against
    # these and never sees ``params``. Absent means "not a bounded set".
    labels = map_type_labels(field_doc)
    if labels is not None:
        entry["mapLabels"] = labels
    return entry


def get_entity_fields_from_db(entity_name: str) -> list[dict]:
    """Return field metadata for a CREv2 entity collection by querying the DB.

    Args:
        entity_name: The entity name as stored in the ``crev2_entities``
            collection (e.g. ``"Users"``, ``"Devices"``).

    Returns:
        list[dict]: Entries as built by ``_entity_field_entry``.
        Returns an empty list when the entity is not found.
    """
    # Resolves the Threat Indicators bridge name to ``indicators``, so that
    # entity's CE-written fields stay closed while a real entity's do not.
    from netskope.integrations.cte.utils.entity import get_entity_collection

    doc = _connector.collection(Collections.CREV2_ENTITIES).find_one({"name": entity_name})
    if not doc:
        return []

    collection = get_entity_collection(entity_name)
    result = []
    for field_doc in doc.get("fields", []):
        if not isinstance(field_doc, dict):
            continue
        entry = _entity_field_entry(field_doc, collection)
        if entry:
            result.append(entry)
    return result


def get_all_unified_mapping_collections() -> list[dict]:
    """Return metadata for every collection available for unified mapping merging.

    Includes:
    - ``indicators`` — built from the Indicator Pydantic model.
    - ``crev2_entity_<name>`` — one entry per entity document found in the
      ``crev2_entities`` collection, excluding the Threat Indicators CRE entity.

    The Threat Indicators entity is a separate, hand-maintained CRE-side mirror
    of indicator data (for CRE's own business-rule query builder) — it is not
    kept in sync with the Indicator model the way get_indicator_fields() is, and
    it exposes fields (e.g. top-level internalHits/externalHits) that don't
    correspond to real indicators-collection fields. The native ``indicators``
    entry above is the accurate, always-in-sync source for the same data, so
    Threat Indicators is deliberately skipped here to avoid a confusing duplicate.

    "Threat Indicators" is CRE's read-only bridge *name* for the same
    ``indicators`` collection (``get_entity_collection("Threat Indicators")
    == "indicators"`` — see ``cte/utils/entity.py``); it is not a separate,
    populated ``crev2_entity_Threat Indicators`` collection. It's excluded
    here the same way the CRE UI already excludes it from entity pickers
    (``excludeThreatIndicatorsFromEntities`` in the UI's cteEntities.js) —
    joining against it as its own collection would silently return nothing.

    Returns:
        list[dict]: Each entry has keys: collection, label, fields.
    """
    from netskope.integrations.cte.utils.entity import THREAT_INDICATORS_ENTITY

    result: list[dict] = [
        {
            "collection": Collections.INDICATORS.value,
            "label": "Indicators",
            "fields": get_indicator_fields(),
        }
    ]

    prefix = Collections.CREV2_ENTITY_PREFIX.value
    entities = _connector.collection(Collections.CREV2_ENTITIES).find(
        {"name": {"$ne": "Threat Indicators"}}, {"name": 1, "fields": 1, "_id": 0}
    )
    for entity in entities:
        entity_name = entity["name"]
        if entity_name == THREAT_INDICATORS_ENTITY:
            continue
        # One name for both the entries' scope and the payload's key: the two
        # must agree, and Threat Indicators is already excluded above.
        collection = f"{prefix}{entity_name}"
        fields = []
        for field_doc in entity.get("fields", []):
            if field_doc is None or not isinstance(field_doc, dict):
                continue
            field_entry = _entity_field_entry(field_doc, collection)
            if field_entry:
                fields.append(field_entry)
        result.append({
            "collection": collection,
            "label": entity_name,
            "fields": fields,
        })

    return result


def get_unified_mapping_collection_fields(collection_name: str) -> list[dict]:
    """Return field metadata for a single unified-mapping collection.

    Args:
        collection_name: The MongoDB collection name, e.g. ``"indicators"``
            or ``"crev2_entity_Users"``.

    Returns:
        list[dict]: Field metadata list, or empty list if not found.
    """
    if collection_name == Collections.INDICATORS.value:
        return get_indicator_fields()

    prefix = Collections.CREV2_ENTITY_PREFIX.value
    if collection_name.startswith(prefix):
        entity_name = collection_name[len(prefix):]
        return get_entity_fields_from_db(entity_name)

    return []


def get_collection_label(collection_name: str) -> str:
    """Return the human-readable name for a unified-mapping collection.

    ``indicators`` is shown as "Indicators"; a CRE entity collection is shown as
    the entity's own name rather than its ``crev2_entity_`` collection name, so
    the UI never surfaces a raw collection name to the user.
    """
    if collection_name == Collections.INDICATORS.value:
        return "Indicators"
    prefix = Collections.CREV2_ENTITY_PREFIX.value
    if collection_name.startswith(prefix):
        return collection_name[len(prefix):]
    return collection_name


def _mapping_field_entry(table: str, field: dict, is_base: bool) -> dict:
    """Build one flattened unified-mapping field entry from raw field metadata.

    ``key`` is the flattened "table.field" form every unified-mapping consumer
    speaks (rule filters, field mappings, action parameters). ``label`` carries
    the entity prefix for flat lists, while ``fieldLabel`` + ``table`` +
    ``tableLabel`` let a caller group by entity instead — all are provided so a
    consumer never has to parse the label apart or map a raw collection name.
    """
    table_label = get_collection_label(table)
    entry: dict = {
        "key": f"{table}.{field['name']}",
        "label": f"{table_label} · {field['label']}",
        "fieldLabel": field["label"],
        "type": field["type"],
        "table": table,
        "tableLabel": table_label,
        "originalKey": field["name"],
        "sourceType": field.get("sourceType", field["type"]),
        "unique": field.get("unique", False),
        "normalized": field.get("normalized", False),
        # None for indicator fields, which have no CRE coalesce strategy.
        "coalesceStrategy": field.get("coalesceStrategy"),
        "multiValued": field.get("multiValued", False),
        # Absent means mappable: only the closed fields ever say otherwise.
        "mappable": field.get("mappable", True),
    }
    if field.get("valueType"):
        entry["valueType"] = field["valueType"]
    # nested_array/subKey drive the base table's ``sources`` $unwind handling, so
    # they are only meaningful for the base table (a joined right document is a
    # single sub-document, never an unwound array).
    if is_base and field.get("nested_array"):
        entry["nested_array"] = field["nested_array"]
        entry["subKey"] = field["subKey"]
    if field.get("fieldSettings"):
        entry["fieldSettings"] = field["fieldSettings"]
    # Forwarded so a rule's field mapping can be checked against it without
    # re-reading the entity document.
    if field.get("mapLabels") is not None:
        entry["mapLabels"] = field["mapLabels"]
    return entry


def get_unified_mapping_fields(
    mapping_doc: dict, include_nested_arrays: bool = True
) -> list[dict]:
    """Return the flattened field list for a stored mapping's tables.

    The single source of truth for "which fields does this mapping have": the
    base table's fields followed by every joined table's, in join order, each
    keyed by its flattened "table.field" name.

    Args:
        mapping_doc: A stored ``unified_mapping`` document (or any dict with
            ``baseTable`` and ``joins``).
        include_nested_arrays: Whether to include the base table's nested-array
            fields (the indicators ``sources.*`` set). Rendering a result table
            must exclude them unless the pipeline actually unwinds ``sources``,
            otherwise the columns cannot resolve — but a *filter* or *field
            mapping* builder should always offer them, since the query itself is
            what makes them resolvable. Defaults to including them, so a caller
            has to opt out deliberately.

    Returns:
        list[dict]: Field entries with ``key``, ``label``, ``fieldLabel``,
            ``type`` (filter widget), ``sourceType`` (stored data type),
            ``table``, ``originalKey``, ``unique``, ``normalized``,
            ``coalesceStrategy`` and, where applicable,
            ``nested_array``/``subKey``/``fieldSettings``.
    """
    base_table = mapping_doc.get("baseTable") or ""
    fields: list[dict] = []
    for field in get_unified_mapping_collection_fields(base_table):
        if field.get("nested_array") and not include_nested_arrays:
            continue
        fields.append(_mapping_field_entry(base_table, field, is_base=True))
    for join in mapping_doc.get("joins") or []:
        right_table = join.get("rightTable")
        if not right_table:
            continue
        for field in get_unified_mapping_collection_fields(right_table):
            fields.append(_mapping_field_entry(right_table, field, is_base=False))
    return fields


def cached_table_fields(
    table: str, fields_cache: Optional[dict[str, dict[str, dict]]]
) -> dict[str, dict]:
    """Return {field_name: field_meta} for a table, via ``fields_cache`` if given.

    A single request can look up the same table's field metadata from several
    independent places (``ensure_join_indexes`` in unified_mapping_indexes,
    ``build_pipeline`` in unified_mapping_exec, ``effective_join_field`` below, each
    possibly more than once for a chained join) — ``fields_cache`` lets all of
    them share one DB read per table instead of repeating it. Pass ``fields_cache``
    as ``None`` to always look up fresh (e.g. a one-off call with no request-scoped
    cache).
    """
    if fields_cache is not None and table in fields_cache:
        return fields_cache[table]
    fields = {
        field_meta["name"]: field_meta
        for field_meta in get_unified_mapping_collection_fields(table)
    }
    if fields_cache is not None:
        fields_cache[table] = fields
    return fields


def join_value_types_compatible(
    left: Optional[str], right: Optional[str]
) -> bool:
    """Whether two fields' stored primitives can ever compare equal in a $lookup.

    ``$lookup``'s localField/foreignField equality is BSON type-sensitive: "5"
    never matches 5, so a text↔number join is not "usually empty", it is
    *always* empty. Because the join still saves, indexes and executes, the only
    symptom is a silently empty result table -- hence a hard reject at
    validation time.

    An array field's ``valueType`` is its ELEMENT primitive (a List holds text),
    which is exactly right here -- Mongo matches a scalar against an array's
    elements, so a CRE List or indicators.sharedWith joins a plain string field
    fine, and neither can match a number.

    An absent ``valueType`` means the primitive is genuinely unknown (only a
    Reference now) and never blocks: for a hard reject a false positive that
    stops a legitimate join costs more than a miss.
    """
    if not left or not right:
        return True
    return left == right


def effective_join_field(
    table: str, field: str, fields_cache: Optional[dict[str, dict[str, dict]]] = None
) -> str:
    """Return the MongoDB field path to use as a $lookup key for the given field.

    CRE STRING fields with normalization enabled are stored as
    ``{value: "<normalized>", plugins: [...]}`` objects — not plain strings.
    A $lookup against the top-level field would compare a plain string to an
    object and silently return zero matches.  Using ``field.value`` as the
    foreignField (or localField) targets the nested plain-string sub-field so
    the join resolves correctly and the index on ``field.value`` is usable.

    Indicators fields never use CRE-style normalization so they always return
    the field name unchanged. ``fields_cache`` — see cached_table_fields.
    """
    fields = cached_table_fields(table, fields_cache)
    if fields.get(field, {}).get("normalized"):
        return f"{field}.value"
    return field
