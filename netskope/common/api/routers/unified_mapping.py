"""Handles the unified mapping (multi-collection join) endpoints.

A unified mapping joins the CTE ``indicators`` collection with one or more CRE
``crev2_entity_<name>`` collections using ordered, two-table-at-a-time joins
(A→B, then AB→C, then ABC→D ...).  Only the join/filter pipeline is persisted;
the joined result is never stored — it is recomputed from the saved pipeline on
demand and paginated entirely inside MongoDB.

Key behaviours:
  * Joins are EXACT match only (no regex / contains).
  * One-to-many and many-to-one joins are allowed; many-to-many (both join sides
    non-unique) is rejected.
  * Consistent LEFT-OUTER join semantics at every level — the base/left row is
    always kept; the right sub-document is the match or absent.  "Matched" means
    every joined right document is present.
  * Pagination is join → filter → skip → limit, all inside the aggregation; only
    skip/limit change between pages.
  * Indexes for join fields are created on save (named ``um_join_<field>``) and
    reference-counted across saved mappings on delete.

This module holds the HTTP layer only — routes, scope/CTE access checks and
persistence of the mapping document.  Payload validation against live DB state
(allowed collections, join fields, name uniqueness) lives on the request models
in ``...models.unified_mapping``, as it does for the CTE and CREv2 business
rules.  The rest of the feature lives alongside the other ``unified_mapping_*``
utils:

  * ``...utils.unified_mapping_exec`` — build the aggregation pipeline, execute it
    and flatten one page of rows.
  * ``...utils.unified_mapping_indexes`` — create the ``um_join_*`` indexes a
    mapping needs and reference-count them on update/delete.
  * ``...utils.unified_mapping_fields`` — per-collection field metadata, and the
    join-key helpers that read it.
"""

from datetime import datetime, timezone
from typing import Annotated, Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, Security
from pymongo.errors import DuplicateKeyError

from ...models import User
from ...models.other import PollIntervalUnit
from ...models.unified_mapping import (
    CollectionMeta,
    ExecuteResult,
    JoinRule,
    MatchMode,
    UnifiedMappingCreate,
    UnifiedMappingIn,
    UnifiedMappingOut,
    UnifiedMappingPreview,
    UnifiedMappingUpdate,
    assert_unique_mapping_name,
    cte_platform_enabled,
)
from ...utils import Collections, DBConnector, PrefixedLogger, Scheduler
from ...utils.unified_mapping_exec import build_pipeline, execute_mapping
from ...utils.unified_mapping_fields import (
    get_all_unified_mapping_collections,
    get_collection_label,
)
from ...utils.unified_mapping_indexes import (
    drop_unused_join_indexes,
    ensure_join_indexes,
    ensure_unique_name_index,
)
from .auth import get_current_user

logger = PrefixedLogger("[Unified Mapping]")

router = APIRouter()
db_connector = DBConnector()
scheduler = Scheduler()

# Per-mapping schedules driven by a mapping's syncInterval. CTE sharing and CRE
# actions get one each, kept separate so either module can be disabled (or run
# late) without holding up the other — their Celery tasks are gated by
# @integration("cte") / @integration("cre") independently.
# Schedule document names follow the convention CREv2 already uses for its own
# per-object schedules — a verb naming the work, then the owning task's module
# prefix and the object it runs for ("Fetch cre.<config>" / "Update cre.<config>"
# in crev2/routers/configurations.py) — led by UM so a unified mapping's
# schedules are identifiable at a glance among a tenant's other per-object ones.
# The verbs match the tasks: one shares, one evaluates.
UM_SHARE_SCHEDULE_PREFIX = "UM Share cte."
UM_ACTIONS_SCHEDULE_PREFIX = "UM Evaluate cre."

UM_SHARE_TASK = "cte.um_share_indicators"
UM_ACTIONS_TASK = "cre.um_evaluate_records"


def _um_share_schedule_name(mapping_name: str) -> str:
    """Schedule document name for a mapping's recurring CTE share task."""
    return f"{UM_SHARE_SCHEDULE_PREFIX}{mapping_name}"


def _um_actions_schedule_name(mapping_name: str) -> str:
    """Schedule document name for a mapping's recurring CRE action task."""
    return f"{UM_ACTIONS_SCHEDULE_PREFIX}{mapping_name}"


def _um_schedule_targets(mapping_name: str) -> list[tuple]:
    """Return the ``(schedule name, task name)`` pairs owned by a mapping."""
    return [
        (_um_share_schedule_name(mapping_name), UM_SHARE_TASK),
        (_um_actions_schedule_name(mapping_name), UM_ACTIONS_TASK),
    ]


# Unified mappings live under CRE, and every mapping needs at least one CRE entity
# (the indicators collection cannot join to itself), so cre_* is the baseline scope
# for the feature. The indicators collection is CTE-owned: any mapping that reads or
# writes it additionally requires the matching cte_* scope, enforced per-mapping by
# assert_cte_access(). This keeps CRE-only users fully functional for CRE-to-CRE
# mappings without ever exposing Threat Exchange data to someone lacking CTE access.
READ_SCOPES = ["cre_read"]
WRITE_SCOPES = ["cre_write"]
CTE_READ_SCOPE = "cte_read"
CTE_WRITE_SCOPE = "cte_write"


def _mapping_in_collections(mapping: UnifiedMappingIn) -> set[str]:
    """Return the collection names referenced by an incoming mapping payload."""
    return {mapping.baseTable} | {join.rightTable for join in mapping.joins}


def has_cte_access(user: User, cte_enabled: bool, write: bool = False) -> bool:
    """Whether the caller may read/write CTE (indicators) data right now.

    True only when BOTH the tenant has the CTE platform enabled AND the user's
    token carries the matching cte_read/cte_write scope — the two gates are
    independent (tenant-wide feature flag vs. per-user permission) and both must
    pass. ``cte_enabled`` is computed once per request by the caller (via
    """
    if not cte_enabled:
        return False
    needed = CTE_WRITE_SCOPE if write else CTE_READ_SCOPE
    return needed in (user.scopes or [])


def assert_cte_access(user: User, collections: set[str], cte_enabled: bool, write: bool = False) -> None:
    """Require CTE access when a mapping involves the indicators collection.

    A CRE-only user can freely build and use CRE-to-CRE mappings, but the indicators
    collection is Threat Exchange data. Reading it (preview/execute) requires
    ``cte_read`` and persisting a mapping that uses it requires ``cte_write``, and
    the CTE platform must be enabled tenant-wide either way — so a joined mapping
    never becomes a way to reach CTE data without both CTE permission and the
    module being enabled. ``cte_enabled`` is threaded in from the caller — see
    has_cte_access.
    """
    if Collections.INDICATORS.value not in collections:
        return
    if not has_cte_access(user, cte_enabled, write=write):
        needed = CTE_WRITE_SCOPE if write else CTE_READ_SCOPE
        raise HTTPException(
            403,
            "This unified mapping uses Threat Exchange (Indicators) data, which "
            f"requires the Threat Exchange module to be enabled and the '{needed}' "
            "permission.",
        )


def mapping_doc_collections(doc: dict) -> set[str]:
    """Return the set of collection names referenced by a stored mapping doc."""
    cols: set[str] = set()
    if doc.get("baseTable"):
        cols.add(doc["baseTable"])
    for join in doc.get("joins", []):
        if join.get("rightTable"):
            cols.add(join["rightTable"])
    return cols


def _doc_to_out(doc: dict) -> UnifiedMappingOut:
    """Convert a raw MongoDB unified_mapping document to UnifiedMappingOut."""
    return UnifiedMappingOut(
        id=str(doc["_id"]),
        name=doc["name"],
        baseTable=doc["baseTable"],
        joins=[JoinRule(**join_doc) for join_doc in doc.get("joins", [])],
        columns=doc.get("columns", []),
        query=doc.get("query", []),
        createdAt=doc["createdAt"],
        updatedAt=doc["updatedAt"],
        caseInsensitive=doc.get("caseInsensitive", False),
        matchMode=MatchMode(doc.get("matchMode", MatchMode.ALL.value)),
        syncInterval=doc.get("syncInterval"),
        syncIntervalUnit=doc.get("syncIntervalUnit"),
        lastRunAt=doc.get("lastRunAt"),
        lastRunSuccess=doc.get("lastRunSuccess"),
        lastActionRunAt=doc.get("lastActionRunAt"),
        lastActionRunSuccess=doc.get("lastActionRunSuccess"),
    )


def _doc_to_mapping_in(doc: dict) -> UnifiedMappingIn:
    """Rebuild a UnifiedMappingIn from a stored mapping document.

    filter is intentionally omitted — it is never persisted (preview/execute-time
    only), so a rebuilt mapping always defaults to an empty ViewFilter.
    """
    return UnifiedMappingIn(
        name=doc["name"],
        baseTable=doc["baseTable"],
        joins=[JoinRule(**join_doc) for join_doc in doc.get("joins", [])],
        columns=doc.get("columns", []),
        caseInsensitive=doc.get("caseInsensitive", False),
        matchMode=MatchMode(doc.get("matchMode", MatchMode.ALL.value)),
        # syncInterval is required on UnifiedMappingIn; mappings saved before the
        # field existed fall back to the documented default of 12 hours.
        syncInterval=doc.get("syncInterval") or 12,
        syncIntervalUnit=doc.get("syncIntervalUnit") or PollIntervalUnit.HOURS,
    )


def _assert_collections_retained(existing: dict, mapping: UnifiedMappingIn) -> None:
    """Block dropping a table from a mapping a business rule is built on.

    A rule addresses the mapping's columns as "<table>.<field>" — in its filter,
    its ``fieldMapping`` and its CRE action parameters — and nothing rewrites those
    references, so dropping a table would leave them pointing at a column the
    mapping no longer produces. A mapping no rule uses has nothing to break, so its
    tables stay freely editable; adding a table is always allowed.

    Any rule on the mapping counts here, whether or not it is sharing or acting:
    a rule that does neither yet still carries a filter and a field mapping
    written against these tables, and editing it back into use must not find
    them gone. This guards editing only — deleting the whole mapping is refused
    outright when any rule references it (see ``_assert_mapping_not_referenced``),
    so there is nothing left to break there either.
    """
    removed = mapping_doc_collections(existing) - _mapping_in_collections(mapping)
    if not removed:
        return
    in_use = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).find_one(
        {"view": existing["name"]}, {"_id": 1}
    )
    if not in_use:
        return
    labels = ", ".join(sorted(get_collection_label(name) for name in removed))
    raise HTTPException(
        400,
        f"Cannot remove {labels} from unified mapping '{existing['name']}'. It is "
        "used by one or more business rules, whose filters, field mappings and "
        "actions may reference fields from it. Delete those business rules first "
        "to change this mapping's tables.",
    )


def _assert_mapping_not_referenced(mapping_name: str) -> None:
    """Block deletion of a mapping any business rule still references.

    Unlike CRE entity deletion (``delete_entity``, which cascades
    ``CREV2_BUSINESS_RULES``/``CTE_BUSINESS_RULES`` unconditionally), a unified
    mapping is costly to rebuild — its joins, filters and field mappings are
    hand-built — so losing one as a side effect of deleting a rule's mapping is
    not an acceptable trade. The caller must delete the referencing business
    rule(s) first.

    Any rule counts, not just one carrying CTE sharing or CRE actions — same
    breadth as ``_assert_collections_retained``'s guard on removing a table: an
    inert rule still carries a filter and field mapping written against this
    mapping. Existence is all that is needed: the 409 says only that the mapping
    is in use, naming no rule, so the message never leaks objects the caller may
    not be scoped to see and reads the same however many there are.
    """
    in_use = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).find_one(
        {"view": mapping_name}, {"_id": 1}
    )
    if in_use:
        raise HTTPException(
            409,
            "This Unified Mapping is in use by one or more business rules and "
            "cannot be deleted. Delete those business rules first.",
        )


def _cascade_delete_mapping_rules(mapping_name: str) -> list[str]:
    """Delete every unified mapping business rule (and its markers) built on ``mapping_name``.

    Used only when the mapping's own underlying collection is going away (see
    ``delete_mappings_for_collection``) — there the mapping cannot survive
    regardless, so cascading, not blocking, is correct: refusing would only
    leave it behind, broken. The direct delete endpoint uses
    ``_assert_mapping_not_referenced`` instead, since there the mapping itself
    is fine and only the caller's request is in question.

    Rules reference a mapping by name (immutable after creation), so the lookup
    is a name match. Their CTE sharing and CRE action configurations are
    embedded in the rule document, so they are dropped along with it.

    Args:
        mapping_name (str): Name of the mapping being deleted.

    Returns:
        list[str]: Names of the business rules that were deleted.
    """
    rules_collection = db_connector.collection(Collections.UNIFIED_MAPPING_RULES)
    markers_collection = db_connector.collection(Collections.UNIFIED_MAPPING_MARKERS)
    rule_names = [
        rule["name"] for rule in rules_collection.find({"view": mapping_name}, {"name": 1})
    ]
    if not rule_names:
        return []
    rules_collection.delete_many({"view": mapping_name})
    markers_collection.delete_many(
        {"$or": [{"mapping": mapping_name}, {"rule": {"$in": rule_names}}]}
    )
    return rule_names


def _persist_mapping(
    mapping: UnifiedMappingIn,
    oid: Optional[ObjectId] = None,
    fields_cache: Optional[dict[str, dict[str, dict]]] = None,
) -> dict:
    """Insert or replace a mapping document and return the stored doc.

    mapping.filter is intentionally not written here — filters are preview/execute-time
    only and are never part of the saved mapping definition.

    ``fields_cache`` — see ``cached_table_fields`` in unified_mapping_fields; passed
    through to ``build_pipeline`` in unified_mapping_exec.
    """
    pipeline = build_pipeline(mapping, fields_cache)
    now = datetime.now(tz=timezone.utc)
    unified_mapping_collection = db_connector.collection(Collections.UNIFIED_MAPPING)
    if oid is None:
        doc = {
            "name": mapping.name,
            "baseTable": mapping.baseTable,
            "joins": [join.model_dump() for join in mapping.joins],
            "columns": mapping.columns,
            "caseInsensitive": mapping.caseInsensitive,
            "matchMode": mapping.matchMode.value,
            "syncInterval": mapping.syncInterval,
            "syncIntervalUnit": mapping.syncIntervalUnit.value,
            "query": pipeline,
            "createdAt": now,
            "updatedAt": now,
        }
        try:
            doc["_id"] = unified_mapping_collection.insert_one(doc).inserted_id
        except DuplicateKeyError as exc:
            # Loser of a concurrent same-name create (unique index on `name`).
            raise HTTPException(
                400, f"A unified mapping named '{mapping.name}' already exists."
            ) from exc
        return doc

    unified_mapping_collection.update_one(
        {"_id": oid},
        {"$set": {
            "name": mapping.name,
            "baseTable": mapping.baseTable,
            "joins": [join.model_dump() for join in mapping.joins],
            "columns": mapping.columns,
            "caseInsensitive": mapping.caseInsensitive,
            "matchMode": mapping.matchMode.value,
            "syncInterval": mapping.syncInterval,
            "syncIntervalUnit": mapping.syncIntervalUnit.value,
            "query": pipeline,
            "updatedAt": now,
        }},
    )
    return unified_mapping_collection.find_one({"_id": oid})


def delete_mappings_for_collection(collection_name: str) -> list[str]:
    """Delete every saved mapping that reads ``collection_name``, and its rules.

    Called when the collection itself is going away — deleting a CRE entity drops
    its ``crev2_entity_<name>`` records, which leaves any mapping joining that
    table permanently unrunnable; loading such an orphan is what blanks the
    Unified Join Builder. Deleting the mapping cascades to everything built on
    it: the unified mapping business rules that name it (their CTE sharing and
    CRE action configurations are embedded in the rule document, so they go with
    it), those rules' act-once markers, and the mapping's own per-mapping share
    and action schedules. Freed ``um_join_*`` indexes are reclaimed once at the
    end, after every mapping is gone, so the reference-count scan sees the final
    state.

    Deliberately unconditional, unlike the direct delete endpoint: that one
    calls ``_assert_mapping_not_referenced`` because refusing there just tells
    the caller to delete the referencing rule(s) first — a mapping is costly to
    rebuild, so it should not be lost as a side effect. Here the collection is
    going away regardless, so the same refusal would only leave the mapping
    behind, orphaned and broken, for no benefit. Entity deletion cascades CRE
    and CTE business rules the same unconditional way.

    Args:
        collection_name (str): Collection being removed — for a CRE entity this
            is ``get_entity_collection(entity_name)``, not the entity name.

    Returns:
        list[str]: Names of the mappings that were deleted, in the order deleted.
    """
    # baseTable plus every rightTable is the full set of tables a mapping reads:
    # a join's leftTable is always the base table or a table an earlier join
    # already brought in, so it can never name a table the mapping does not
    # reach through one of these two keys (mapping_doc_collections/
    # _mapping_doc_tables read a stored mapping the same way).
    mapping_docs = list(
        db_connector.collection(Collections.UNIFIED_MAPPING).find(
            {
                "$or": [
                    {"baseTable": collection_name},
                    {"joins.rightTable": collection_name},
                ]
            }
        )
    )
    if not mapping_docs:
        return []

    affected_collections: set[str] = set()
    deleted_names: list[str] = []
    for doc in mapping_docs:
        mapping_name = doc["name"]
        # The other tables in the join need their join indexes reconsidered too —
        # this mapping may have been the only one keeping them alive.
        affected_collections |= mapping_doc_collections(doc)
        rule_names = _cascade_delete_mapping_rules(mapping_name)
        db_connector.collection(Collections.UNIFIED_MAPPING).delete_one({"_id": doc["_id"]})
        for schedule_name, _ in _um_schedule_targets(mapping_name):
            scheduler.delete(schedule_name)
        deleted_names.append(mapping_name)
        logger.info(
            f"Deleted unified mapping '{mapping_name}' and "
            f"{len(rule_names)} business rule(s) built on it because collection "
            f"'{collection_name}' is being removed."
        )

    drop_unused_join_indexes(affected_collections)
    return deleted_names


@router.get(
    "/unified-mapping/collections",
    tags=["Unified Mapping"],
    description="Return all collections available for unified mapping joining, with field metadata.",
)
async def list_collections(
    user: User = Security(get_current_user, scopes=READ_SCOPES),
) -> list[CollectionMeta]:
    """List all joinable collections and their fields (including the unique flag).

    The CTE indicators collection is offered only when the CTE platform is enabled
    tenant-wide AND the user has ``cte_read``; a CRE-only user, or any user on a
    tenant with CTE disabled, sees just the CRE entities they can join together.
    """
    metas = get_all_unified_mapping_collections()
    if not has_cte_access(user, cte_platform_enabled(), write=False):
        metas = [
            meta for meta in metas if meta["collection"] != Collections.INDICATORS.value
        ]
    return [CollectionMeta(**collection_meta) for collection_meta in metas]


@router.get(
    "/unified-mapping",
    tags=["Unified Mapping"],
    description="List all saved unified mappings.",
)
async def list_unified_mappings(
    user: User = Security(get_current_user, scopes=READ_SCOPES),
) -> list[UnifiedMappingOut]:
    """Return all saved unified mapping configurations.

    Mappings that join the CTE indicators collection are hidden unless the CTE
    platform is enabled tenant-wide AND the user has ``cte_read`` — they could not
    open such a mapping anyway (execute would read CTE data), so listing them would
    only expose un-usable entries.
    """
    docs = db_connector.collection(Collections.UNIFIED_MAPPING).find({})
    has_cte_read = has_cte_access(user, cte_platform_enabled(), write=False)
    return [
        _doc_to_out(doc)
        for doc in docs
        if has_cte_read
        or Collections.INDICATORS.value not in mapping_doc_collections(doc)
    ]


@router.post(
    "/unified-mapping/actions/preview",
    tags=["Unified Mapping"],
    description="Execute a draft unified mapping without saving it.",
)
async def preview_unified_mapping(
    mapping: UnifiedMappingPreview,
    skip: Annotated[int, Query(ge=0, description="Number of rows to skip (pagination).")] = 0,
    limit: Annotated[int, Query(ge=1, le=100, description="Rows per page (10-100).")] = 10,
    match_mode: Annotated[
        Optional[MatchMode],
        Query(
            description=(
                "Mapping mode: all | matched_only | unmatched_only. Defaults to matched_only "
                "when omitted. This is a display-time filter only — it is never persisted "
                "with the mapping."
            )
        ),
    ] = None,
    with_counts: Annotated[
        bool,
        Query(
            description=(
                "Whether to compute the row totals. The count pass depends only on the "
                "joins and filter — not on skip/limit/match_mode — so a caller paging "
                "through a result set it has already counted can pass false to skip it "
                "and reuse the totals it holds; the three total_* fields then come back "
                "null. Defaults to true."
            )
        ),
    ] = True,
    user: User = Security(get_current_user, scopes=READ_SCOPES),
) -> ExecuteResult:
    """Execute a draft mapping definition without persisting it.

    The draft's collections and join fields are validated by
    ``UnifiedMappingPreview``; the CTE platform flag and field metadata it read
    are reused here so the request still makes one read per table across
    validation, index creation and execution.
    """
    cte_enabled = mapping.cte_enabled
    fields_cache = mapping.fields_cache
    assert_cte_access(user, _mapping_in_collections(mapping), cte_enabled, write=False)
    # Create indexes at preview time so large-table joins are fast even before saving.
    # strict=False: a failure to create an index is non-fatal for preview — the query
    # still runs (just slower), so the user is never blocked from exploring a draft mapping.
    ensure_join_indexes(mapping, strict=False, fields_cache=fields_cache)
    effective_mode = match_mode if match_mode is not None else MatchMode.MATCHED_ONLY
    return execute_mapping(
        mapping,
        skip=skip,
        limit=limit,
        match_mode=effective_mode,
        fields_cache=fields_cache,
        with_counts=with_counts,
    )


@router.post(
    "/unified-mapping",
    tags=["Unified Mapping"],
    description=(
        "Create a new unified mapping: validate collections and fields, create join "
        "indexes, build the aggregation pipeline, and persist the configuration."
    ),
    status_code=201,
)
async def create_unified_mapping(
    mapping: UnifiedMappingCreate,
    user: User = Security(get_current_user, scopes=WRITE_SCOPES),
) -> UnifiedMappingOut:
    """Build pipeline, ensure indexes, save the mapping, and schedule its sync tasks.

    Collections, join fields and name uniqueness are validated by
    ``UnifiedMappingCreate``; what remains here is the per-user CTE access check
    (which needs ``user``) and persistence. ``ensure_unique_name_index`` still
    runs before the insert — it is the concurrency backstop behind the
    name check, whose DuplicateKeyError ``_persist_mapping`` translates.
    """
    fields_cache = mapping.fields_cache
    assert_cte_access(user, _mapping_in_collections(mapping), mapping.cte_enabled, write=True)
    ensure_unique_name_index()
    ensure_join_indexes(mapping, fields_cache=fields_cache)
    doc = _persist_mapping(mapping, fields_cache=fields_cache)
    for schedule_name, task_name in _um_schedule_targets(mapping.name):
        # upsert, not schedule(): the latter is a plain insert_one, so a leftover
        # schedule of the same name (a delete that half-failed, or a retry after
        # this loop threw) would silently become a duplicate PeriodicTask that
        # scheduler.delete's delete_one cannot fully remove. Scheduling also runs
        # after the mapping is already persisted, so a retried create must heal
        # rather than double-register.
        scheduler.upsert(
            name=schedule_name,
            task_name=task_name,
            poll_interval=mapping.syncInterval,
            poll_interval_unit=mapping.syncIntervalUnit,
            args=[mapping.name],
        )
    return _doc_to_out(doc)


@router.patch(
    "/unified-mapping/{mapping_id}",
    tags=["Unified Mapping"],
    description=(
        "Update a saved unified mapping: re-validate, rebuild pipeline, reconcile "
        "indexes. Tables may not be removed while a business rule uses the mapping."
    ),
)
async def update_unified_mapping(
    mapping_id: str,
    mapping: UnifiedMappingUpdate,
    user: User = Security(get_current_user, scopes=WRITE_SCOPES),
) -> UnifiedMappingOut:
    """Replace a saved mapping's definition and reconcile its join indexes.

    Collections and join fields are validated by ``UnifiedMappingUpdate``. Name
    uniqueness and table retention stay here because both need the stored
    document — the id to exclude, and the tables already saved.
    """
    try:
        oid = ObjectId(mapping_id)
    except Exception:
        raise HTTPException(400, f"Invalid mapping ID: '{mapping_id}'.")
    existing = db_connector.collection(Collections.UNIFIED_MAPPING).find_one({"_id": oid})
    if not existing:
        raise HTTPException(404, f"Unified mapping '{mapping_id}' not found.")

    # Read what validation established BEFORE any model_copy below: private
    # attributes surviving a copy is a pydantic implementation detail, and
    # cte_enabled gates an access check — it must never silently read False.
    cte_enabled = mapping.cte_enabled
    fields_cache = mapping.fields_cache

    # Mapping name is immutable after creation — same pattern as CRE configurations where
    # the name is the lookup key and cannot be changed via an update.  Silently restore
    # the stored name so the rest of the validation and persist path sees the right value.
    if mapping.name != existing["name"]:
        logger.debug(
            f"Mapping '{existing['name']}': ignoring requested rename to '{mapping.name}' — "
            "mapping name is immutable after creation."
        )
        mapping = mapping.model_copy(update={"name": existing["name"]})

    # cte_write is required if the stored OR the updated definition touches indicators
    # (you can't edit an existing CTE mapping, nor turn a CRE-only one into a CTE one,
    # without CTE write permission).
    assert_cte_access(
        user,
        mapping_doc_collections(existing) | _mapping_in_collections(mapping),
        cte_enabled,
        write=True,
    )
    _assert_collections_retained(existing, mapping)
    assert_unique_mapping_name(mapping.name, exclude_id=oid)

    ensure_join_indexes(mapping, fields_cache=fields_cache)
    doc = _persist_mapping(mapping, oid=oid, fields_cache=fields_cache)
    # Scan all um_join_* indexes on every collection this mapping touches (old + new)
    # and drop any not referenced by a remaining saved mapping. Scan-based so that
    # orphans from prior caseInsensitive changes are also caught.
    affected = (
        mapping_doc_collections(existing)
        | {mapping.baseTable}
        | {join.rightTable for join in mapping.joins}
    )
    drop_unused_join_indexes(affected)
    for schedule_name, task_name in _um_schedule_targets(mapping.name):
        scheduler.upsert(
            name=schedule_name,
            task_name=task_name,
            poll_interval=mapping.syncInterval,
            poll_interval_unit=mapping.syncIntervalUnit,
            args=[mapping.name],
        )
    return _doc_to_out(doc)


@router.delete(
    "/unified-mapping/{mapping_id}",
    tags=["Unified Mapping"],
    description="Delete a saved unified mapping and reclaim its unused join indexes.",
)
async def delete_unified_mapping(
    mapping_id: str,
    user: User = Security(get_current_user, scopes=WRITE_SCOPES),
) -> dict:
    """Delete a unified mapping by ID.

    Refused with a 409 while any business rule still references it — see
    ``_assert_mapping_not_referenced``.
    """
    try:
        oid = ObjectId(mapping_id)
    except Exception:
        raise HTTPException(400, f"Invalid mapping ID: '{mapping_id}'.")
    doc = db_connector.collection(Collections.UNIFIED_MAPPING).find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, f"Unified mapping '{mapping_id}' not found.")

    assert_cte_access(user, mapping_doc_collections(doc), cte_platform_enabled(), write=True)
    _assert_mapping_not_referenced(doc["name"])
    db_connector.collection(Collections.UNIFIED_MAPPING).delete_one({"_id": oid})
    drop_unused_join_indexes(mapping_doc_collections(doc))
    for schedule_name, _ in _um_schedule_targets(doc["name"]):
        scheduler.delete(schedule_name)
    return {"detail": f"Unified mapping '{doc['name']}' deleted successfully."}


@router.post(
    "/unified-mapping/{mapping_id}/execute",
    tags=["Unified Mapping"],
    description="Execute the stored pipeline for a unified mapping and return one merged page.",
)
async def execute_unified_mapping(
    mapping_id: str,
    skip: Annotated[int, Query(ge=0, description="Number of rows to skip (pagination).")] = 0,
    limit: Annotated[int, Query(ge=1, le=100, description="Rows per page (10-100).")] = 10,
    match_mode: Annotated[
        Optional[MatchMode],
        Query(
            description=(
                "Mapping mode: all | matched_only | unmatched_only. Defaults to matched_only "
                "when omitted. This is a display-time filter only — it is never persisted "
                "with the mapping."
            )
        ),
    ] = None,
    with_counts: Annotated[
        bool,
        Query(
            description=(
                "Whether to compute the row totals. The count pass depends only on the "
                "joins and filter — not on skip/limit/match_mode — so a caller paging "
                "through a result set it has already counted can pass false to skip it "
                "and reuse the totals it holds; the three total_* fields then come back "
                "null. Defaults to true."
            )
        ),
    ] = True,
    user: User = Security(get_current_user, scopes=READ_SCOPES),
) -> ExecuteResult:
    """Execute the stored pipeline and return one flattened, paginated page."""
    try:
        oid = ObjectId(mapping_id)
    except Exception:
        raise HTTPException(400, f"Invalid mapping ID: '{mapping_id}', Mapping does not exist.")

    doc = db_connector.collection(Collections.UNIFIED_MAPPING).find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, f"Unified mapping '{mapping_id}' not found.")

    # Rendering executes the join, so a mapping that includes indicators returns CTE
    # data — block it for users without cte_read even though cre_read reached here.
    assert_cte_access(user, mapping_doc_collections(doc), cte_platform_enabled(), write=False)

    mapping = _doc_to_mapping_in(doc)
    pipeline: list[dict] = doc.get("query", [])
    effective_mode = match_mode if match_mode is not None else MatchMode.MATCHED_ONLY
    return execute_mapping(
        mapping,
        pipeline=pipeline,
        skip=skip,
        limit=limit,
        match_mode=effective_mode,
        with_counts=with_counts,
    )
