"""Pipeline construction and execution for unified mappings.

Turning a mapping definition into a MongoDB aggregation — and turning the
documents it returns back into flat rows — is the core of the feature and is
independent of HTTP: the router
(``netskope/common/api/routers/unified_mapping.py``) validates and authorises a
request, then hands off to this module:

  * ``build_pipeline`` — the stored join pipeline for a mapping definition.
  * ``build_mapping_pipeline`` — the stored pipeline of an already-saved mapping,
    by id.  Forward-compat seam for business rules / sharing / action evaluation,
    which must not import the router module.
  * ``execute_mapping`` — run join → filter → match-mode → paginate and flatten
    one page of rows.
  * ``build_mapping_pipeline`` — the stored join pipeline of a saved mapping, by
    document or by id.  Sharing / action evaluation call this instead of the
    router, then append their own $match before executing.

Sharing and action evaluation additionally need stages the viewing path does not:

  * ``rule_match_stage`` / ``exception_match_stage`` — a unified mapping business
    rule's filter and its mute exceptions, as pipeline stages.
  * ``recency_match_stage`` / ``get_collection_recency_field`` — qualify joined rows
    by the CE-stamped ``lastUpdated`` field against a checkpoint.
  * ``matched_only_stage`` — sharing always forces MATCHED_ONLY, regardless of the
    mapping's stored (viewing-only) ``matchMode``.
  * ``build_sync_pipeline`` / ``row_identity_stage`` / ``row_ids`` / ``row_hash`` —
    joined-row identity primitives for the CRE act-once marker ledger (the stored
    pipeline strips joined ``_id``s, so these rebuild it without that $project).
  * ``unwrap_normalized_row_values`` / ``resolve_normalized_row_values`` —
    resolve CRE ``{value, plugins}`` wrappers, canonically or per destination
    configuration.

The ``*_stage`` helpers here operate on stored mapping *documents* (plain dicts), so
Celery callers never construct Pydantic models; the private ``_matched_expr`` /
``_match_mode_stage`` above take ``UnifiedMappingIn`` and serve the router path.

Filter parsing and column validation raise ``HTTPException`` (as the feature's
Pydantic models in ``netskope/common/models/unified_mapping.py`` already do), so a
non-HTTP caller such as a Celery task should catch it rather than let a 400 escape.
"""

import hashlib
import json
from typing import Any, Optional, Union

from bson import ObjectId
from fastapi import HTTPException

from . import Collections, DBConnector, PrefixedLogger, parse_dates
from ..models.unified_mapping import ExecuteResult, MatchMode, UnifiedMappingIn
from .unified_mapping_fields import (
    effective_join_field,
    get_unified_mapping_collection_fields,
)

logger = PrefixedLogger("[Unified Mapping]")

db_connector = DBConnector()


def _needs_sources_expansion(mapping: UnifiedMappingIn) -> bool:
    """Return True when base-table columns or joins use sources.* fields.

    Deliberately does NOT consider the filter. A filter on ``sources.source``
    matches a document if ANY element of the array matches, which Mongo does
    natively without an $unwind — expanding here would multiply every matched row
    by its source count as a side effect of filtering. What the filter does need is
    for "sources" to survive in the pipeline, which it does: see the (currently
    inert) strip in execute_mapping.
    """
    base = mapping.baseTable
    for col in mapping.columns:
        parts = col.split(".", 2)
        if len(parts) >= 2 and parts[0] == base and parts[1] == "sources":
            return True
    for join in mapping.joins:
        if join.leftTable == base and join.condition.leftField.startswith("sources."):
            return True
    return False


def _filter_references_sources(mapping: UnifiedMappingIn) -> bool:
    """Return True when the mapping's filter references a sources.* field on the base table.

    Reuses _translate_filter_keys so this checks the exact same key shape the
    $match stage will actually contain (base-table prefix stripped), instead of
    duplicating that prefix-handling logic here and risking it drifting out of sync.
    """
    raw = (mapping.filter.mongo or "").strip()
    if not raw or raw == "{}":
        return False
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return False
    return _tree_has_sources_key(_translate_filter_keys(parsed, mapping.baseTable))


def _tree_has_sources_key(node: Any) -> bool:
    """Return True if a translated filter tree contains a bare 'sources'/'sources.*' key."""
    if isinstance(node, list):
        return any(_tree_has_sources_key(child) for child in node)
    if not isinstance(node, dict):
        return False
    for key, value in node.items():
        if key in _LOGICAL_OPS or key == "$not":
            if _tree_has_sources_key(value):
                return True
        elif not key.startswith("$") and (key == "sources" or key.startswith("sources.")):
            return True
    return False


def build_pipeline(
    mapping: UnifiedMappingIn, fields_cache: Optional[dict[str, dict[str, dict]]] = None
) -> list[dict]:
    """Build the stored MongoDB aggregation pipeline (joins only) for a mapping.

    Uses ``localField``/``foreignField`` $lookup (NOT the let/pipeline/$expr form)
    so MongoDB can use the ``um_join_*`` index on the foreign field.  The
    let/pipeline/$expr form with ``$toLower``/``$toString`` makes the index
    unusable and degrades to O(N²) — do not reintroduce it.

    For case-insensitive joins the collation is applied at execute time via the
    aggregate ``collation`` option (not embedded in the pipeline), so the stored
    pipeline is identical for both case modes — only execution differs.

    The query-builder filter (mapping.filter.mongo) is applied at execute time via
    _filter_match_stage, not stored here, so relative-date conditions remain
    dynamic.  Pagination (skip/limit) and match-mode stages are also execute-time.
    """
    if fields_cache is None:
        fields_cache = {}
    pipeline: list[dict] = []
    sources_expanded = _needs_sources_expansion(mapping)

    if sources_expanded:
        pipeline.append({"$unwind": {"path": "$sources", "preserveNullAndEmptyArrays": True}})

    for join in mapping.joins:
        cond = join.condition
        right_field = effective_join_field(join.rightTable, cond.rightField, fields_cache)
        left_field = effective_join_field(join.leftTable, cond.leftField, fields_cache)
        if join.leftTable == mapping.baseTable:
            local_field = left_field
        else:
            local_field = f"{join.leftTable}.{left_field}"
        pipeline.append({
            "$lookup": {
                "from": join.rightTable,
                "localField": local_field,
                "foreignField": right_field,
                "as": join.rightTable,
            }
        })
        pipeline.append({
            "$unwind": {"path": f"${join.rightTable}", "preserveNullAndEmptyArrays": True}
        })

        pipeline.append({
            "$set": {
                join.rightTable: {
                    "$cond": [
                        {"$or": [
                            {"$eq": [f"${local_field}", None]},
                            {"$eq": [f"${join.rightTable}.{right_field}", None]},
                        ]},
                        "$$REMOVE",
                        f"${join.rightTable}",
                    ]
                }
            }
        })

    if mapping.joins:
        pipeline.append({
            "$project": {
                f"{joined_table}._id": 0
                for joined_table in {join.rightTable for join in mapping.joins}
            }
        })

    return pipeline


def _compute_all_fields(mapping: UnifiedMappingIn) -> list[dict]:
    """Build the flat field list (metadata only, no DB results) for a mapping.

    Nested-array fields (indicators ``sources.*``) are always listed, on the base
    table as well as on joined ones. This list IS the column catalogue the UI's
    column picker is built from, and selecting such a column is what turns
    _needs_sources_expansion on — so hiding them until the mapping already expands
    sources made them unreachable for a mapping based on indicators: they never
    appeared to be picked, so expansion never switched on.

    Deliberately narrower than ``get_unified_mapping_fields``: this feeds column
    RENDERING, so it carries no ``valueType``/``multiValued``/``unique``. Those
    answer "what may this field be joined or mapped to", which only the
    ``/collections`` and ``/rules/fields`` payloads are asked. Add them here only
    if a consumer of the preview/execute response actually needs them.
    """
    all_fields: list[dict] = []
    for field in get_unified_mapping_collection_fields(mapping.baseTable):
        entry: dict = {
            "key": f"{mapping.baseTable}.{field['name']}",
            "label": f"{mapping.baseTable} · {field['label']}",
            # fieldLabel is the column's own label (no table prefix); table is the
            # source collection. The UI groups columns by table (friendly entity
            # name on top) and shows fieldLabel underneath.
            "fieldLabel": field["label"],
            "type": field["type"],
            "table": mapping.baseTable,
            "originalKey": field["name"],
            "normalized": field.get("normalized", False),
        }
        if field.get("nested_array"):
            entry["nested_array"] = field["nested_array"]
            entry["subKey"] = field["subKey"]
        all_fields.append(entry)

    for join in mapping.joins:
        for field in get_unified_mapping_collection_fields(join.rightTable):
            entry = {
                "key": f"{join.rightTable}.{field['name']}",
                "label": f"{join.rightTable} · {field['label']}",
                "fieldLabel": field["label"],
                "type": field["type"],
                "table": join.rightTable,
                "originalKey": field["name"],
                "normalized": field.get("normalized", False),
            }

            if field.get("nested_array"):
                entry["nested_array"] = field["nested_array"]
                entry["subKey"] = field["subKey"]
            all_fields.append(entry)
    return all_fields


def _nested_array_values(container: dict, nested_array: str, sub_key: str) -> Any:
    """Read one sub-key out of a nested array (e.g. an indicator's ``sources``).

    Always a list, or None when nothing resolves — an $unwind-ed document yields a
    one-element list, a whole array one entry per element (duplicates kept). Keeps
    the column's type independent of whether the mapping expands sources.
    """
    entries = container.get(nested_array)
    if isinstance(entries, dict):
        value = entries.get(sub_key)
        return None if value is None else [value]
    if not isinstance(entries, list):
        return None
    values = [
        entry.get(sub_key)
        for entry in entries
        if isinstance(entry, dict) and entry.get(sub_key) is not None
    ]
    return values or None


def _flatten_doc(doc: dict, all_fields: list[dict], base_table: str) -> dict:
    """Flatten one aggregation document into a flat {key: value} row."""
    flat: dict = {}
    for field in all_fields:
        if field["table"] == base_table:
            nested_array = field.get("nested_array")
            if nested_array:
                flat[field["key"]] = _nested_array_values(doc, nested_array, field["subKey"])
            else:
                raw = doc.get(field["originalKey"])
                if field.get("normalized") and isinstance(raw, dict):
                    flat[field["key"]] = raw.get("value")
                else:
                    flat[field["key"]] = raw
        else:
            sub_doc = doc.get(field["table"])
            if not isinstance(sub_doc, dict):
                flat[field["key"]] = None
                continue
            nested_array = field.get("nested_array")
            if nested_array:
                flat[field["key"]] = _nested_array_values(sub_doc, nested_array, field["subKey"])
                continue
            raw = sub_doc.get(field["originalKey"])
            if field.get("normalized") and isinstance(raw, dict):
                flat[field["key"]] = raw.get("value")
            else:
                flat[field["key"]] = raw
    return flat


def _matched_expr(mapping: UnifiedMappingIn) -> dict:
    """Return the boolean expression that is True when every joined right doc is present."""
    joined_tables = [join.rightTable for join in mapping.joins]
    if not joined_tables:
        return {"$literal": True}
    present = [
        {"$ne": [{"$type": f"${joined_table}"}, "missing"]} for joined_table in joined_tables
    ]
    return present[0] if len(present) == 1 else {"$and": present}


def _match_mode_stage(mapping: UnifiedMappingIn, match_mode: MatchMode) -> list[dict]:
    """Return the $match stage(s) for the requested match mode (or [] for ALL)."""
    if match_mode == MatchMode.MATCHED_ONLY:
        return [{"$match": {"$expr": _matched_expr(mapping)}}]
    if match_mode == MatchMode.UNMATCHED_ONLY:
        return [{"$match": {"$expr": {"$not": _matched_expr(mapping)}}}]
    return []


_LOGICAL_OPS = {"$and", "$or", "$nor"}


def _translate_filter_keys(node: Any, base_table: str) -> Any:
    """Rewrite query-builder field keys to the joined-document paths.

    The query builder emits flattened keys ("tableName.fieldName"). After the
    join, base-table fields live at the top level (no prefix) while joined-table
    fields stay nested under the table name. So only the base-table prefix is
    stripped; joined-table keys are already the correct nested path. Recurses
    through $and / $or / $nor / $not.
    """
    if isinstance(node, list):
        return [_translate_filter_keys(child_node, base_table) for child_node in node]
    if not isinstance(node, dict):
        return node
    prefix = f"{base_table}."
    out: dict = {}
    for key, value in node.items():
        if key in _LOGICAL_OPS or key == "$not":
            out[key] = _translate_filter_keys(value, base_table)
        elif key.startswith("$"):
            out[key] = value
        else:
            new_key = key[len(prefix):] if key.startswith(prefix) else key
            out[new_key] = value
    return out


def _flatten_sources_elem_match(node: Any) -> Any:
    """Rewrite {"sources": {"$elemMatch": {...}}} into dotted keys.

    $elemMatch needs an array; after $unwind "sources" is a single sub-document, so
    the dotted form is the equivalent — and still correlated, one source per row.
    """
    if isinstance(node, list):
        return [_flatten_sources_elem_match(child) for child in node]
    if not isinstance(node, dict):
        return node
    out: dict = {}
    for key, value in node.items():
        if not (
            (key == "sources" or key.endswith(".sources"))
            and isinstance(value, dict)
            and set(value) == {"$elemMatch"}
        ):
            out[key] = _flatten_sources_elem_match(value)
            continue
        for sub_key, sub_value in value["$elemMatch"].items():
            if sub_key in _LOGICAL_OPS and isinstance(sub_value, list):
                # keep the operator, flatten each branch as its own elemMatch body
                branches = [
                    _flatten_sources_elem_match({key: {"$elemMatch": branch}})
                    for branch in sub_value
                ]
                out.setdefault(sub_key, []).extend(branches)
            else:
                out[f"{key}.{sub_key}"] = sub_value
    return out


def _filter_match_stage(mapping: UnifiedMappingIn, sources_expanded: bool = False) -> list[dict]:
    """Return the $match stage for the mapping's query-builder filter (or [])."""
    raw = (mapping.filter.mongo or "").strip()
    if not raw or raw == "{}":
        return []
    try:
        parsed = json.loads(raw, object_hook=parse_dates)
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, "Filter query is not valid.") from exc
    if not isinstance(parsed, dict) or not parsed:
        return []
    translated = _translate_filter_keys(parsed, mapping.baseTable)
    if sources_expanded:
        translated = _flatten_sources_elem_match(translated)
    return [{"$match": translated}]


def execute_mapping(
    mapping: UnifiedMappingIn,
    pipeline: Optional[list[dict]] = None,
    skip: int = 0,
    limit: int = 50,
    match_mode: MatchMode = MatchMode.MATCHED_ONLY,
    fields_cache: Optional[dict[str, dict[str, dict]]] = None,
    with_counts: bool = True,
) -> ExecuteResult:
    """Execute a mapping and return one paginated page plus match/unmatched totals.

    Order of operations is join → filter → match-mode → skip/limit, all inside
    MongoDB.  Two aggregations run: one $group for counts, one for the page.
    Flattening only ever touches the page rows.

    ``with_counts=False`` runs only the page aggregation and leaves the three totals
    None.  The count pass is ``joins + filter + $group`` — it has no match-mode stage
    and no skip/limit — so its result is identical for every page and every match mode
    of the same mapping and filter.  A caller paging through a result set it has
    already counted can therefore skip it and keep the numbers it holds, halving the
    aggregations per page.  Defaults to True, so a caller that does nothing gets the
    same response as before.

    ``fields_cache`` — see ``cached_table_fields`` in unified_mapping_fields; only
    relevant when ``pipeline`` is
    None (preview), since build_pipeline is the only place here that needs it —
    an already-built stored pipeline needs no field lookups to execute.
    """
    sources_expanded = _needs_sources_expansion(mapping)
    base_pipeline = pipeline if pipeline is not None else build_pipeline(mapping, fields_cache)
    all_fields = _compute_all_fields(mapping)

    if mapping.columns and not any(field["key"] in set(mapping.columns) for field in all_fields):
        logger.debug(
            f"None of the saved unified-mapping columns {sorted(mapping.columns)} matched available fields."
        )
    collection = db_connector.collection(mapping.baseTable)
    common = base_pipeline + _filter_match_stage(mapping, sources_expanded)

    sources_projected = any(
        field.get("nested_array") and field["table"] == mapping.baseTable
        for field in all_fields
    )
    # Inert as things stand: only indicator fields carry nested_array, so
    # sources_projected is true exactly when the base table is indicators — which
    # means the strip can only fire for a CRE base table, where there is no
    # "sources" field to strip. Kept as a guard for if _compute_all_fields stops
    # listing nested-array fields unconditionally.
    if not sources_expanded and not sources_projected and not _filter_references_sources(mapping):
        common = common + [{"$project": {"sources": 0}}]

    agg_opts: dict = {"allowDiskUse": True}
    if mapping.caseInsensitive:
        agg_opts["collation"] = {"locale": "en", "strength": 2}

    # --- Counts: total rows + matched (all joins present) in one pass ---
    # Count over the SAME pipeline the page uses (join + filter, including any
    # sources $unwind) so the totals always match the rows actually paginated.
    # Counting a source-expanded mapping over the un-unwound base would report one
    # row per base document while the page returns one row per source, breaking
    # pagination and the "X of Y matched" chip.
    total: Optional[int] = None
    total_matched: Optional[int] = None
    total_unmatched: Optional[int] = None
    if with_counts:
        count_pipeline = common + [{
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "matched": {"$sum": {"$cond": [_matched_expr(mapping), 1, 0]}},
            }
        }]
        count_docs = list(collection.aggregate(count_pipeline, **agg_opts))
        total_all = count_docs[0]["total"] if count_docs else 0
        total_matched = count_docs[0]["matched"] if count_docs else 0
        total_unmatched = total_all - total_matched

        if match_mode == MatchMode.MATCHED_ONLY:
            total = total_matched
        elif match_mode == MatchMode.UNMATCHED_ONLY:
            total = total_unmatched
        else:
            total = total_all

    # --- Page: match-mode filter + skip + limit ---
    page_pipeline = (
        common
        + _match_mode_stage(mapping, match_mode)
        + [{"$skip": skip}, {"$limit": limit}]
    )
    page_docs = collection.aggregate(page_pipeline, **agg_opts)
    rows = [_flatten_doc(doc, all_fields, mapping.baseTable) for doc in page_docs]
    response_fields = [
        {
            "key": field["key"],
            "label": field["label"],
            "fieldLabel": field["fieldLabel"],
            "table": field["table"],
            "type": field["type"],
            "nested_array": field.get("nested_array"),
        }
        for field in all_fields
    ]
    return ExecuteResult(
        fields=response_fields,
        rows=rows,
        skip=skip,
        limit=limit,
        total=total,
        total_matched=total_matched,
        total_unmatched=total_unmatched,
        sources_expanded=sources_expanded,
    )


def collect_filter_keys(node: Any, keys: set, prefix: str = "") -> None:
    """Collect every field key referenced by a mongo filter, into ``keys``.

    Recurses through ``$and``/``$or``/``$nor`` (list-valued) and any other
    operator (dict-valued) without trying to enumerate every Mongo operator by
    name — an unrecognised ``$op`` key is simply not added to ``keys`` (it isn't
    a field reference), and its value is still walked the same way a logical
    operator's is, so nested field references under it are still found.

    ``$elemMatch`` is the one operator whose *contents* are field references
    relative to the array it matches, so its body is walked with the array path
    as a prefix and the bare array path is not itself reported as a field: a
    correlated filter on the indicators sources array
    (``{"indicators.sources": {"$elemMatch": {"severity": ...}}}``, which is what
    a query builder emits for a ``!group``) yields
    ``indicators.sources.severity`` — a real field — rather than
    ``indicators.sources``, which is not one.
    """
    if isinstance(node, list):
        for item in node:
            collect_filter_keys(item, keys, prefix)
        return
    if not isinstance(node, dict):
        return
    for key, value in node.items():
        if key.startswith("$"):
            collect_filter_keys(value, keys, prefix)
        elif isinstance(value, dict) and "$elemMatch" in value:
            collect_filter_keys(value["$elemMatch"], keys, f"{prefix}{key}.")
        else:
            keys.add(f"{prefix}{key}")


def mapping_doc_joined_tables(mapping_doc: dict) -> list:
    """Return the joined (right-side) table names of a stored mapping document."""
    return [j["rightTable"] for j in mapping_doc.get("joins", []) if j.get("rightTable")]


def matched_expr(joined_tables: list) -> dict:
    """Return an aggregation boolean expression: True when every joined right doc is present."""
    if not joined_tables:
        return {"$literal": True}
    present = [{"$ne": [{"$type": f"${jt}"}, "missing"]} for jt in joined_tables]
    return present[0] if len(present) == 1 else {"$and": present}


def match_mode_stage(joined_tables: list, match_mode: MatchMode) -> list:
    """Return the $match stage(s) for the requested match mode (or [] for ALL)."""
    if match_mode == MatchMode.MATCHED_ONLY:
        return [{"$match": {"$expr": matched_expr(joined_tables)}}]
    if match_mode == MatchMode.UNMATCHED_ONLY:
        return [{"$match": {"$expr": {"$not": matched_expr(joined_tables)}}}]
    return []


def matched_only_stage(mapping_doc: dict) -> list:
    """Matched-only $match for a stored mapping document.

    Sharing always enforces MATCHED_ONLY regardless of the mapping's stored
    ``matchMode`` (a viewing-only preference), so field mappings that reference
    joined tables always resolve.
    """
    return match_mode_stage(mapping_doc_joined_tables(mapping_doc), MatchMode.MATCHED_ONLY)


def build_mapping_pipeline(mapping: Union[dict, str]) -> Optional[list]:
    """Return the stored join pipeline for a mapping document or mapping id.

    Args:
        mapping: A stored ``unified_mapping`` document (dict) or a mapping id string.

    Returns:
        The stored ``query`` pipeline (joins only), or ``None`` when the id is
        invalid or no such mapping exists. Task callers log-and-skip on ``None``;
        the router wraps this with HTTP error semantics.
    """
    if isinstance(mapping, dict):
        return mapping.get("query", [])
    try:
        oid = ObjectId(mapping)
    except Exception:
        return None
    doc = db_connector.collection(Collections.UNIFIED_MAPPING).find_one({"_id": oid})
    if not doc:
        return None
    return doc.get("query", [])


def _parse_rule_mongo(raw: str) -> Optional[dict]:
    """Parse a rule's mongo filter JSON string; None when empty or invalid."""
    raw = (raw or "").strip()
    if not raw or raw == "{}":
        return None
    try:
        # parse_dates resolves ISO date strings and __RELATIVE__ markers at
        # execute time, so relative-date filters stay dynamic (as in CREv2).
        parsed = json.loads(raw, object_hook=parse_dates)
    except (ValueError, TypeError):
        return None
    if not isinstance(parsed, dict) or not parsed:
        return None
    return parsed


def mapping_unwinds_sources(mapping_doc: dict) -> bool:
    """Whether a stored mapping's pipeline expands the indicators sources array.

    Read off the stored pipeline rather than recomputed from the mapping's
    columns/joins (``_needs_sources_expansion``, which needs a ``UnifiedMappingIn``):
    the pipeline is what actually runs, so it is the truth about whether
    ``sources`` is still an array by the time a filter is applied.
    """
    for stage in mapping_doc.get("query") or []:
        unwind = stage.get("$unwind") if isinstance(stage, dict) else None
        if isinstance(unwind, dict) and unwind.get("path") == "$sources":
            return True
        if unwind == "$sources":
            return True
    return False


def rule_match_stage(
    rule_doc: dict, base_table: str, sources_expanded: bool = False
) -> list:
    """Return the $match stage for a unified mapping rule's filter (or []).

    Rule filter mongo keys use the flattened unified keys ("table.field") —
    exactly what the mapping's own filter builder produces — so base-table keys
    are translated to top-level paths before matching.

    ``sources_expanded`` mirrors ``_filter_match_stage``: once the pipeline has
    unwound ``sources``, a correlated ``$elemMatch`` on it has no array left to
    match and must be rewritten to its dotted equivalent, or the filter silently
    matches nothing. Callers pass ``mapping_unwinds_sources(mapping_doc)``.
    """
    parsed = _parse_rule_mongo((rule_doc.get("filters") or {}).get("mongo"))
    if parsed is None:
        return []
    translated = _translate_filter_keys(parsed, base_table)
    if sources_expanded:
        translated = _flatten_sources_elem_match(translated)
    return [{"$match": translated}]


def exception_match_stage(
    rule_doc: dict, base_table: str, sources_expanded: bool = False
) -> list:
    """Return a $nor $match excluding rows matched by the rule's mute exceptions.

    Mirrors the CRE-entity share path: only filter-based exceptions apply
    (tag-based mutes are an indicator-source concept), and empty ``{}``
    exception filters are skipped — an empty clause in $nor matches every
    document and would silence the entire rule.

    ``sources_expanded`` is handled exactly as in ``rule_match_stage`` — an
    exception is just another filter, and one on ``sources`` would silently
    exclude nothing once the array has been unwound.
    """
    mute_queries = []
    for mute in rule_doc.get("exceptions") or []:
        parsed = _parse_rule_mongo((mute.get("filters") or {}).get("mongo"))
        if parsed:
            translated = _translate_filter_keys(parsed, base_table)
            if sources_expanded:
                translated = _flatten_sources_elem_match(translated)
            mute_queries.append(translated)
    if not mute_queries:
        return []
    return [{"$match": {"$nor": mute_queries}}]


def get_collection_recency_field(collection_name: str) -> str:
    """CE-stamped recency field for a unified mapping constituent collection.

    Both indicators (stamped in ``insert_or_update_indicator``) and
    ``crev2_entity_*`` records (stamped in CRE ``_store_records``) carry a
    top-level ``lastUpdated`` set at CE storage time. Do NOT use ``lastSeen``
    — it is plugin-supplied and max-merged, so a re-fetched-but-unchanged IOC
    does not move it.
    """
    return "lastUpdated"


def _recency_branch(collection_name: str, prefix: Optional[str], checkpoint) -> dict:
    """One table's recency clause, keyed on its CE-stamped recency field."""
    field = get_collection_recency_field(collection_name)
    path = f"{prefix}.{field}" if prefix else field
    if collection_name != Collections.INDICATORS.value:
        return {path: {"$gt": checkpoint}}
    # IOCs stored before lastUpdated existed fall back to lastSeen, so they
    # still qualify if the backfill migration did not reach them.
    seen = f"{prefix}.lastSeen" if prefix else "lastSeen"
    return {"$expr": {"$gt": [{"$ifNull": [f"${path}", f"${seen}"]}, checkpoint]}}


def recency_match_stage(mapping_doc: dict, checkpoint) -> list:
    """Return a $match qualifying rows updated after ``checkpoint`` (or []).

    A row qualifies when ANY constituent table's record moved: the base table's
    top-level ``lastUpdated`` or a joined table's nested
    ``<joinedTable>.lastUpdated`` is greater than the checkpoint.
    """
    if checkpoint is None:
        return []
    branches = [_recency_branch(mapping_doc.get("baseTable", ""), None, checkpoint)]
    for jt in mapping_doc_joined_tables(mapping_doc):
        branches.append(_recency_branch(jt, jt, checkpoint))
    return [{"$match": {"$or": branches}}]


def _is_joined_id_projection(stage: Any, joined_tables: list) -> bool:
    """Return whether ``stage`` is the stored pipeline's joined-``_id`` stripper.

    ``build_pipeline`` closes a joined mapping with
    ``{"$project": {"<joinedTable>._id": 0, ...}}``. Identified structurally
    (every key is a ``<joinedTable>._id`` exclusion) rather than by position, so
    an unrelated trailing ``$project`` is never dropped by mistake.
    """
    if not isinstance(stage, dict) or list(stage) != ["$project"]:
        return False
    spec = stage["$project"]
    if not isinstance(spec, dict) or not spec:
        return False
    expected = {f"{jt}._id" for jt in joined_tables}
    return all(key in expected and not value for key, value in spec.items())


def build_sync_pipeline(mapping_doc: dict) -> list:
    """Return the stored join pipeline with joined ``_id``s preserved.

    The stored ``query`` pipeline ends by projecting away every joined table's
    ``_id`` — they are noise for viewing. A joined row's identity, however, IS
    the tuple of its constituent ``_id``s (see ``row_hash``), so any consumer
    that must recognise the same row across cycles needs them kept. This
    returns the stored pipeline minus that final projection; everything else
    (the ``sources`` handling, the ``$lookup``/``$unwind`` chain and its
    index-friendly ``localField``/``foreignField`` form) is untouched.
    """
    pipeline = list(mapping_doc.get("query") or [])
    joined_tables = mapping_doc_joined_tables(mapping_doc)
    if pipeline and joined_tables and _is_joined_id_projection(pipeline[-1], joined_tables):
        pipeline.pop()
    return pipeline


def row_identity_stage(mapping_doc: dict) -> list:
    """Return a $project reducing joined rows to their identity ``_id``s only.

    Used by consumers that only need to recognise which rows a pipeline
    produced (not their content) — the projected documents are a few dozen
    bytes each regardless of how wide the mapping is, so a full identity sweep
    stays cheap.
    """
    spec: dict = {"_id": 1}
    for jt in mapping_doc_joined_tables(mapping_doc):
        spec[f"{jt}._id"] = 1
    return [{"$project": spec}]


def row_ids(row: dict, mapping_doc: dict) -> dict:
    """Return a joined row's constituent ``_id``s keyed by collection name.

    The base table's ``_id`` is at the row's top level; each joined table's is
    nested under its own name. A joined table missing from the row maps to
    ``None`` — callers enforcing MATCHED_ONLY never see that case.
    """
    ids = {mapping_doc.get("baseTable", ""): row.get("_id")}
    for jt in mapping_doc_joined_tables(mapping_doc):
        sub_doc = row.get(jt)
        ids[jt] = sub_doc.get("_id") if isinstance(sub_doc, dict) else None
    return ids


def row_hash(row: dict, mapping_doc: dict) -> str:
    """Return the stable identity hash of a joined row.

    Hashes the row's constituent ``_id``s — its IDENTITY, never its content —
    so a row keeps the same hash while its fields change. That is what makes an
    act-once ledger keyed on this hash fire once per distinct joined row rather
    than once per edit.

    Because MongoDB never reuses an ``_id``, a hash can never be reused by a
    different row either: a replacement document gets a fresh ``_id`` and so a
    fresh hash. A leftover ledger entry from a deleted row is therefore inert
    (it can never suppress a future row), which is why pruning it is
    housekeeping rather than a correctness requirement.

    SHA-256 is used as a collision-resistant identity digest, not as a security
    primitive; hashing (rather than concatenating the ``_id``s) bounds the key
    length so the ledger's unique index stays within MongoDB's index-key limit
    no matter how many tables a mapping joins.
    """
    ids = row_ids(row, mapping_doc)
    # Sorted by collection name so the digest never depends on dict ordering.
    material = "|".join(f"{table}={ids[table]}" for table in sorted(ids))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _normalized_fields_by_table(mapping_doc: dict) -> dict[str, set]:
    """Return each of a mapping's tables' normalized field names.

    Shared by ``unwrap_normalized_row_values`` (canonical resolution, for
    consumers with no destination configuration) and
    ``resolve_normalized_row_values`` (per-configuration resolution, for CRE
    action evaluation).
    """
    base_table = mapping_doc.get("baseTable", "")
    normalized_by_table = {}
    for table in [base_table] + mapping_doc_joined_tables(mapping_doc):
        if not table:
            continue
        normalized = {
            field["name"]
            for field in get_unified_mapping_collection_fields(table)
            if field.get("normalized")
        }
        if normalized:
            normalized_by_table[table] = normalized
    return normalized_by_table


def unwrap_normalized_row_values(rows: list, mapping_doc: dict) -> list:
    """Resolve CRE ``{value, plugins}`` wrappers on materialized joined rows.

    Normalized CRE STRING fields are stored as ``{value, plugins}`` objects, not
    plain strings. Consumers that resolve a field spec against a row with no
    destination configuration in hand (CTE field mappings; an indicator value
    must stay the same regardless of which destination it is pushed to) need
    the plain scalar — the same normalization the unified mapping router's
    ``_flatten_doc`` applies when it renders a row. Both the base table's
    top-level fields and every joined table's nested fields are unwrapped.

    A CRE action DOES have a destination configuration and must resolve each
    normalized field the way that configuration's own plugin would (matching
    ``cre.evaluate_records``) — use ``resolve_normalized_row_values`` there
    instead.

    Metadata-driven and applied in place, once per materialized batch, so the
    per-row cost is a dict lookup rather than a field-metadata query.
    """
    normalized_by_table = _normalized_fields_by_table(mapping_doc)
    if not normalized_by_table:
        return rows
    base_table = mapping_doc.get("baseTable", "")
    for row in rows:
        for table, field_names in normalized_by_table.items():
            # Base-table fields live at the row's top level; joined tables are
            # nested under their own name.
            sub_doc = row if table == base_table else row.get(table)
            if not isinstance(sub_doc, dict):
                continue
            for field_name in field_names:
                value = sub_doc.get(field_name)
                if isinstance(value, dict) and "value" in value:
                    sub_doc[field_name] = value.get("value")
    return rows


def resolve_normalized_field_value(normalized_value: Any, configuration: str) -> Any:
    """Return a normalized CRE field's value as ``configuration``'s plugin sees it.

    A normalized STRING field is stored as ``{"value": <canonical>, "plugins":
    [{"config": ..., "value": ...}, ...]}``. Each destination configuration may
    have pushed its own casing/formatting into ``plugins``; the canonical
    ``value`` is the fallback when no entry names this configuration. Moved
    here from ``crev2.tasks.evaluate_records`` (now re-exported there) so
    unified-mapping action evaluation can share it without ``common.utils``
    depending on the integrations package.
    """
    if isinstance(normalized_value, dict):
        for plugin_value_dict in normalized_value.get("plugins", []):
            if plugin_value_dict.get("config") == configuration:
                return plugin_value_dict.get("value")
        return normalized_value.get("value")
    return normalized_value


def resolve_normalized_row_values(rows: list, mapping_doc: dict, configuration: str) -> list:
    """Resolve CRE ``{value, plugins}`` wrappers for one destination configuration.

    Same traversal as ``unwrap_normalized_row_values``, but for a CRE action
    against a specific destination ``configuration`` — matching how
    ``cre.evaluate_records`` resolves entity-record action parameters and
    generated-alert ``rawData``, instead of always taking the canonical value.
    Use this wherever a joined row feeds a CRE action's parameters or its
    generated alert; use ``unwrap_normalized_row_values`` for the
    configuration-agnostic CTE field-mapping path.
    """
    normalized_by_table = _normalized_fields_by_table(mapping_doc)
    if not normalized_by_table:
        return rows
    base_table = mapping_doc.get("baseTable", "")
    for row in rows:
        for table, field_names in normalized_by_table.items():
            sub_doc = row if table == base_table else row.get(table)
            if not isinstance(sub_doc, dict):
                continue
            for field_name in field_names:
                value = sub_doc.get(field_name)
                if isinstance(value, dict) and "value" in value:
                    sub_doc[field_name] = resolve_normalized_field_value(
                        value, configuration
                    )
    return rows
