"""Handles the unified mapping business rules endpoints.

A unified mapping business rule filters the joined (unified) records of a saved
unified mapping and carries a per-module sharing configuration: ``cteShare``
(destination configuration -> share actions) and ``creActions`` (CRE
configuration -> actions to perform). The two are independent — a rule may do
either, both, or neither. A single rule-level ``fieldMapping`` (IOC fields ->
unified row keys) turns this rule's joined rows into indicators; it is optional
at create and required once ``cteShare`` is non-empty (a CRE-action-only rule
never needs one, since a CRE action resolves its own parameters against the row
instead of building an indicator). Once complete — ``value`` and ``type`` both
set — it is immutable, mirroring the CTE business rule; an absent or partial one
can still be completed later.

Rules reference mappings by name (immutable after creation). The rule's mongo
filter uses the flattened unified keys ("table.field") emitted by the mapping's
own filter builder; base-table keys are translated to top-level paths at
execute time. Execution itself runs in Celery — ``cte.um_share_indicators`` for
sharing and ``cre.um_evaluate_records`` for actions — so these endpoints only
manage rule documents, dry-run counts, and manual sync dispatch.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query, Security
from pympler import asizeof

from ...models import User
from ...models.unified_mapping import (
    UmCreAction,
    UnifiedMappingRuleException,
    UnifiedMappingRuleIn,
    UnifiedMappingRuleOut,
    UnifiedMappingRuleUpdate,
    UmShareAction,
    ViewFilter,
    cte_platform_enabled,
    get_rule_mapping_doc,
)
from ...utils import Collections, DBConnector, PrefixedLogger, is_platform_enabled
from ...utils.unified_mapping_exec import (
    exception_match_stage,
    mapping_unwinds_sources,
    matched_only_stage,
    recency_match_stage,
    rule_match_stage,
)
from ...utils.unified_mapping_fields import get_unified_mapping_fields
from .auth import get_current_user
from .unified_mapping import (
    CTE_WRITE_SCOPE,
    READ_SCOPES,
    WRITE_SCOPES,
    assert_cte_access,
    has_cte_access,
    mapping_doc_collections,
)
from netskope.integrations.cte.models.indicator import IndicatorType
from netskope.integrations.cte.utils.constants import MAX_IOC_LIMIT

# Same split CTE's own /business_rules/test uses (business_rule.py) — derived
# from IndicatorType so the two stay in sync instead of duplicating a literal
# list that could drift from the enum.
_HASH_TYPES = [IndicatorType.SHA256.value, IndicatorType.MD5.value]
_URL_TYPES = [t.value for t in IndicatorType if t.value not in _HASH_TYPES]


def _field_spec_to_mongo_expr(spec: Any, base_table: str):
    """Translate a rule fieldMapping spec into a Mongo aggregation expression.

    Mirrors share_indicators.py's ``_resolve_field_spec`` /
    ``build_indicators_from_unified_mapping_rows`` convention (``$<path>`` /
    ``fixed:<v>`` / bare-literal, with the base-table prefix stripped since
    base-table fields live at the row's top level after the join) — but
    returns a Mongo expression instead of resolving against a materialized
    row, so IOC-type classification can run server-side without pulling
    every matching row into Python first.
    """
    if spec in (None, ""):
        return {"$literal": None}
    if isinstance(spec, str):
        if spec.startswith("$"):
            path = spec[1:]
            base_prefix = f"{base_table}."
            if path.startswith(base_prefix):
                path = path[len(base_prefix):]
            return f"${path}"
        if spec.startswith("fixed:"):
            return {"$literal": spec[len("fixed:"):]}
    return {"$literal": spec}

logger = PrefixedLogger("[Unified Mapping]")

router = APIRouter()
db_connector = DBConnector()

CRE_DISABLED_RULE_MESSAGE = (
    "This unified mapping business rule is disabled because the CRE module is "
    "turned off. Enable the CRE module to modify or use it."
)


def _assert_cre_share_access(cte_share: dict) -> None:
    """Require the CRE module for cteShare: it reads live CRE entity data."""
    if cte_share and not is_platform_enabled("cre"):
        raise HTTPException(
            400,
            "Cannot configure CTE sharing on a unified mapping business rule "
            "while the CRE module is disabled. Enable the CRE module first.",
        )


def _rule_mapping_collections(view: str) -> set[str]:
    """Collections the rule's mapping reads; empty if the mapping is gone."""
    doc = db_connector.collection(Collections.UNIFIED_MAPPING).find_one(
        {"name": view}, {"baseTable": 1, "joins": 1}
    )
    return mapping_doc_collections(doc or {})


def _assert_cte_share_access(user: User) -> None:
    """Require cte_write for cteShare: it pushes indicators to a destination plugin."""
    if not has_cte_access(user, cte_platform_enabled(), write=True):
        raise HTTPException(
            403,
            "Threat Exchange sharing requires the Threat Exchange module to be "
            f"enabled and the '{CTE_WRITE_SCOPE}' permission.",
        )


def _mapping_without_empties(field_mapping: dict) -> dict:
    """Drop cleared rows, which the rule form sends as ``None``/``""``."""
    return {
        key: value
        for key, value in (field_mapping or {}).items()
        if value is not None and value != ""
    }


def _mapping_is_complete(field_mapping: dict) -> bool:
    """Whether a stored mapping can already build indicators, and so locks.

    Only ``value`` + ``type`` make one shareable, so anything short of that stays
    editable: a partial mapping (or just the form's pre-filled ``expiresAt``)
    would otherwise strand the rule -- those two become mandatory the moment
    ``cteShare`` is set, and immutability would then refuse to add them.
    """
    mapping = _mapping_without_empties(field_mapping)
    return bool(mapping.get("value") and mapping.get("type"))


def _doc_to_rule_out(doc: dict) -> UnifiedMappingRuleOut:
    """Convert a raw MongoDB unified_mapping_rules document to UnifiedMappingRuleOut."""
    return UnifiedMappingRuleOut(
        id=str(doc["_id"]),
        name=doc["name"],
        view=doc["view"],
        filters=ViewFilter(**doc.get("filters", {})),
        exceptions=[
            UnifiedMappingRuleException(**e) for e in doc.get("exceptions", [])
        ],
        muted=doc.get("muted", False),
        unmuteAt=doc.get("unmuteAt"),
        disabledByCre=doc.get("disabledByCre", False),
        cteShare={
            dest: [UmShareAction(**a) for a in actions]
            for dest, actions in (doc.get("cteShare") or {}).items()
        },
        creActions={
            config: [UmCreAction(**a) for a in actions]
            for config, actions in (doc.get("creActions") or {}).items()
        },
        fieldMapping=doc.get("fieldMapping") or {},
        createdAt=doc["createdAt"],
        updatedAt=doc["updatedAt"],
        lastPerformed=doc.get("lastPerformed", {}),
    )


@router.get(
    "/unified-mapping/rules",
    tags=["Unified Mapping Rules"],
    description="List all unified mapping business rules.",
)
async def list_unified_mapping_rules(
    user: User = Security(get_current_user, scopes=READ_SCOPES),
) -> list[UnifiedMappingRuleOut]:
    """Return all saved unified mapping business rules.

    Rules on a mapping that joins the indicators collection are hidden without
    CTE read access — same filter list_unified_mappings applies to the mappings
    themselves, since such a rule could not be opened or tested anyway.
    """
    docs = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).find({})
    if has_cte_access(user, cte_platform_enabled()):
        return [_doc_to_rule_out(doc) for doc in docs]
    cte_views = {
        mapping_doc["name"]
        for mapping_doc in db_connector.collection(Collections.UNIFIED_MAPPING).find(
            {}, {"name": 1, "baseTable": 1, "joins": 1}
        )
        if Collections.INDICATORS.value in mapping_doc_collections(mapping_doc)
    }
    return [_doc_to_rule_out(doc) for doc in docs if doc.get("view") not in cte_views]


@router.get(
    "/unified-mapping/rules/fields",
    tags=["Unified Mapping Rules"],
    description="List every field a rule on this mapping can filter on or map from.",
)
async def list_unified_mapping_rule_fields(
    view: str = Query(..., description="Name of the unified mapping."),
    user: User = Security(get_current_user, scopes=READ_SCOPES),
) -> list[dict]:
    """Return the complete flattened field list for a mapping's joined tables.

    This is the field source for the rule builder (filter + field mapping) and
    is deliberately NOT the mapping's execute-response field list. That one
    describes the columns a saved mapping *renders*: it is narrowed to the
    mapping's selected columns and omits the indicators ``sources.*`` fields
    unless the pipeline unwinds them. A rule must be able to filter on anything
    the mapping's tables actually contain — which is exactly what the model's
    ``validate_rule_filters`` accepts — so both read the same list.

    Each entry carries ``table`` and ``fieldLabel`` alongside the prefixed
    ``label`` so the builder can show which entity a field came from.
    """
    mapping_doc = get_rule_mapping_doc(view)
    assert_cte_access(
        user, mapping_doc_collections(mapping_doc), cte_platform_enabled()
    )
    return get_unified_mapping_fields(mapping_doc)


@router.get(
    "/unified-mapping/rules/test",
    tags=["Unified Mapping Rules"],
    description=(
        "Dry-run a rule: total qualifying rows, plus the URL-like vs Filehash "
        "IOC split for CTE sharing."
    ),
)
async def test_unified_mapping_rule(
    name: str = Query(..., description="Name of the unified mapping business rule."),
    days: int = Query(
        ..., lt=366, gt=0, description="Only count rows updated in the last N days."
    ),
    user: User = Security(get_current_user, scopes=READ_SCOPES),
) -> dict:
    """Count rows matching the rule, split into URL-like vs Filehash IOC types.

    Mirrors CTE's own ``/business_rules/test`` (business_rule.py): a row's IOC
    type is resolved from the rule's ``fieldMapping["type"]`` spec (a rule
    without a usable type mapping shares nothing, matching
    ``build_indicators_from_unified_mapping_rows`` skipping such rows), and byte size
    is only measured for a bucket small enough to be worth measuring — same
    ``MAX_IOC_LIMIT`` gate CTE uses. Both counts and both size samples are
    computed in a single aggregation pass ($facet) over the joined pipeline
    rather than re-running the join once per bucket.
    """
    rule_doc = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).find_one(
        {"name": name}
    )
    if rule_doc is None:
        raise HTTPException(400, "Unified mapping business rule does not exist.")
    mapping_doc = get_rule_mapping_doc(rule_doc["view"])
    assert_cte_access(
        user, mapping_doc_collections(mapping_doc), cte_platform_enabled()
    )
    base_table = mapping_doc["baseTable"]
    field_mapping = rule_doc.get("fieldMapping") or {}
    type_expr = _field_spec_to_mongo_expr(field_mapping.get("type"), base_table)
    value_expr = _field_spec_to_mongo_expr(field_mapping.get("value"), base_table)
    checkpoint = datetime.now(tz=timezone.utc) - timedelta(days=days)
    sources_expanded = mapping_unwinds_sources(mapping_doc)
    pipeline = (
        (mapping_doc.get("query") or [])
        + rule_match_stage(rule_doc, base_table, sources_expanded)
        + exception_match_stage(rule_doc, base_table, sources_expanded)
        + recency_match_stage(mapping_doc, checkpoint)
        + matched_only_stage(mapping_doc)
        + [{"$addFields": {"__iocType__": type_expr, "__iocValue__": value_expr}}]
    )

    def _count_stage(type_values: list) -> list:
        return [{"$match": {"__iocType__": {"$in": type_values}}}, {"$count": "n"}]

    def _sample_stage(type_values: list) -> list:
        # $limit bounds the value sample regardless of true match count, so a
        # rule matching millions of rows can't blow up memory building the
        # $addToSet array — capped one past MAX_IOC_LIMIT is enough to know
        # the true count exceeded it without measuring the full set.
        return [
            {"$match": {"__iocType__": {"$in": type_values}}},
            {"$limit": MAX_IOC_LIMIT + 1},
            {"$group": {"_id": None, "values": {"$addToSet": "$__iocValue__"}}},
        ]

    # $facet stages cannot nest, so all four sub-pipelines are named branches
    # of one facet rather than a facet-of-facets — still a single pass over
    # the joined pipeline.
    pipeline += [{"$facet": {
        # Every qualifying joined row, before any IOC-type split: what a CRE
        # action operates on, since a rule configured only for creActions has no
        # fieldMapping and so no resolvable IOC type at all.
        "rowCount": [{"$count": "n"}],
        "urlCount": _count_stage(_URL_TYPES),
        "urlSample": _sample_stage(_URL_TYPES),
        "hashCount": _count_stage(_HASH_TYPES),
        "hashSample": _sample_stage(_HASH_TYPES),
    }}]
    agg_opts: dict = {"allowDiskUse": True}
    if mapping_doc.get("caseInsensitive"):
        agg_opts["collation"] = {"locale": "en", "strength": 2}
    result_docs = list(
        db_connector.collection(base_table).aggregate(pipeline, **agg_opts)
    )
    result = result_docs[0] if result_docs else {}

    def _count_and_size(count_key: str, sample_key: str) -> tuple:
        count_docs = result.get(count_key) or []
        count = count_docs[0]["n"] if count_docs else 0
        if count == 0 or count > MAX_IOC_LIMIT:
            return count, 0
        sample_docs = result.get(sample_key) or []
        values = sample_docs[0]["values"] if sample_docs else []
        return count, (asizeof.asizeof(values) if values else 0)

    url_count, url_size = _count_and_size("urlCount", "urlSample")
    hash_count, hash_size = _count_and_size("hashCount", "hashSample")
    row_docs = result.get("rowCount") or []
    return {
        # ``count`` is the CRE-side answer (rows), the url/hash pair the CTE-side
        # one (indicators). Both are returned so either sync dialog can read the
        # figure it needs from one call.
        "count": row_docs[0]["n"] if row_docs else 0,
        "url_count": url_count,
        "url_size": url_size,
        "hash_count": hash_count,
        "hash_size": hash_size,
    }


@router.post(
    "/unified-mapping/rules",
    tags=["Unified Mapping Rules"],
    description="Create a unified mapping business rule with its sharing configuration.",
    status_code=201,
)
async def create_unified_mapping_rule(
    rule: UnifiedMappingRuleIn,
    user: User = Security(get_current_user, scopes=WRITE_SCOPES),
) -> UnifiedMappingRuleOut:
    """Save a new unified mapping business rule.

    The payload — name uniqueness, the referenced mapping, filters, field
    mapping, ``cteShare`` and ``creActions`` — is fully validated by
    ``UnifiedMappingRuleIn`` before this runs, so there is nothing left to check
    here (same division as CTE's ``create_business_rule``).
    """
    assert_cte_access(
        user,
        _rule_mapping_collections(rule.view),
        cte_platform_enabled(),
        write=True,
    )
    if rule.cteShare:
        _assert_cte_share_access(user)
        _assert_cre_share_access(rule.cteShare)
    now = datetime.now(tz=timezone.utc)
    doc = {
        **rule.model_dump(),
        "lastPerformed": {},
        "createdAt": now,
        "updatedAt": now,
    }
    doc["_id"] = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).insert_one(
        doc
    ).inserted_id
    logger.debug(f"Business rule '{rule.name}' successfully created.")
    return _doc_to_rule_out(doc)


@router.patch(
    "/unified-mapping/rules",
    tags=["Unified Mapping Rules"],
    description="Update a unified mapping business rule (rule name is immutable).",
)
async def update_unified_mapping_rule(
    rule: UnifiedMappingRuleUpdate,
    name: str = Query(..., description="Name of the unified mapping business rule."),
    user: User = Security(get_current_user, scopes=WRITE_SCOPES),
) -> UnifiedMappingRuleOut:
    """Replace a rule's definition; the name and the mapping cannot change.

    The payload itself is validated by ``UnifiedMappingRuleUpdate`` (everything
    ``UnifiedMappingRuleIn`` checks except name uniqueness, which a rule being
    updated would always fail against itself). What is left here is resource
    identity: the rule addressed by ``name`` must exist, the stored name wins
    over any rename in the body, and the mapping is refused outright.
    """
    existing = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).find_one(
        {"name": name}
    )
    if existing is None:
        raise HTTPException(404, f"Unified mapping business rule '{name}' not found.")
    if existing.get("disabledByCre"):
        raise HTTPException(400, CRE_DISABLED_RULE_MESSAGE)
    assert_cte_access(
        user,
        _rule_mapping_collections(existing["view"]),
        cte_platform_enabled(),
        write=True,
    )
    # Either side needs cte_write: adding a share, or removing/redirecting one.
    if rule.cteShare or existing.get("cteShare"):
        _assert_cte_share_access(user)
        _assert_cre_share_access(rule.cteShare)

    if rule.view != existing["view"]:
        raise HTTPException(
            400,
            "The unified mapping of a business rule cannot be changed after "
            "creation. Create a new rule on the other mapping instead.",
        )

    stored_mapping = _mapping_without_empties(existing.get("fieldMapping"))
    incoming_mapping = _mapping_without_empties(rule.fieldMapping)
    if not incoming_mapping:
        # An absent mapping is a no-change, never a request to clear a stored
        # one — the CRE Actions page PATCHes the whole rule to swap only its
        # actions. A rule that already shares to CTE never reaches here with an
        # empty mapping: validate_cte_share runs during model validation and
        # rejects it first, so this branch only covers the no-cteShare callers.
        rule = rule.model_copy(
            update={"fieldMapping": existing.get("fieldMapping") or {}}
        )
    elif (
        _mapping_is_complete(existing.get("fieldMapping"))
        and incoming_mapping != stored_mapping
    ):
        raise HTTPException(
            400,
            "The Field Mapping of a business rule cannot be changed once it is "
            "set.",
        )

    if rule.name != name:
        logger.debug(
            f"Rule '{name}': ignoring requested rename to '{rule.name}' — rule "
            "name is immutable after creation."
        )
        rule = rule.model_copy(update={"name": name})
    db_connector.collection(Collections.UNIFIED_MAPPING_RULES).update_one(
        {"name": name},
        {"$set": {
            **rule.model_dump(),
            "updatedAt": datetime.now(tz=timezone.utc),
        }},
    )
    updated = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).find_one(
        {"name": name}
    )
    logger.debug(f"Business rule '{name}' updated.")
    return _doc_to_rule_out(updated)


@router.delete(
    "/unified-mapping/rules",
    tags=["Unified Mapping Rules"],
    description="Delete a unified mapping business rule.",
)
async def delete_unified_mapping_rule(
    name: str = Query(..., description="Name of the unified mapping business rule."),
    user: User = Security(get_current_user, scopes=WRITE_SCOPES),
) -> dict:
    """Delete a rule by name.

    Queued manualSync entries referencing the rule are left in place — the
    drain drops entries whose rule no longer resolves (same behavior as CTE
    business rules).

    The rule's CRE act-once markers are deleted with it so that re-creating a
    rule of the same name starts from a clean slate rather than silently
    skipping rows an earlier rule of that name had already acted on.
    """
    existing = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).find_one(
        {"name": name}
    )
    if existing is None:
        raise HTTPException(404, f"Unified mapping business rule '{name}' not found.")
    if existing.get("disabledByCre"):
        raise HTTPException(400, CRE_DISABLED_RULE_MESSAGE)
    assert_cte_access(
        user,
        _rule_mapping_collections(existing["view"]),
        cte_platform_enabled(),
        write=True,
    )
    if existing.get("cteShare"):
        _assert_cte_share_access(user)
    db_connector.collection(Collections.UNIFIED_MAPPING_RULES).delete_one({"name": name})
    db_connector.collection(Collections.UNIFIED_MAPPING_MARKERS).delete_many(
        {"rule": name}
    )
    logger.debug(f"Business rule '{name}' has been successfully deleted.")
    return {"detail": f"Unified mapping business rule '{name}' deleted successfully."}


@router.post(
    "/unified-mapping/rules/sync",
    tags=["Unified Mapping Rules"],
    description="Queue a one-time manual share for a rule/destination/action.",
)
async def sync_unified_mapping_rule(
    rule: str = Query(..., description="Name of the unified mapping business rule."),
    destinationConfiguration: str = Query(
        ..., description="Destination configuration to share to."
    ),
    days: int = Query(..., lt=366, gt=0, description="Share rows updated in the last N days."),
    action: UmShareAction = Body(...),
    user: User = Security(get_current_user, scopes=READ_SCOPES + [CTE_WRITE_SCOPE]),
) -> dict:
    """Queue a manual sync onto the destination configuration's ``manualSync``.

    The entry is drained by the destination's next scheduled
    ``cte.share_indicators`` run, which routes ``ruleType: "unified_mapping"``
    entries to the unified mapping share flow.

    Queueing this exports indicator data to a third-party destination, so it is
    a cte_write operation even though it only writes a queue entry here.
    """
    _assert_cte_share_access(user)
    stored_rule = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).find_one(
        {"name": rule}
    )
    if stored_rule is None:
        raise HTTPException(400, "Unified mapping business rule does not exist.")
    if stored_rule.get("disabledByCre"):
        raise HTTPException(400, CRE_DISABLED_RULE_MESSAGE)
    if destinationConfiguration not in (stored_rule.get("cteShare") or {}):
        raise HTTPException(
            400,
            f"Destination configuration '{destinationConfiguration}' is not "
            f"configured for sharing on rule '{rule}'.",
        )
    if (
        db_connector.collection(Collections.CONFIGURATIONS).find_one(
            {"name": destinationConfiguration}
        )
        is None
    ):
        raise HTTPException(
            400,
            f"Destination configuration '{destinationConfiguration}' does not exist.",
        )
    logger.debug(
        f"Sync with unified mapping business rule {rule} for destination "
        f"{destinationConfiguration} is triggered."
    )
    db_connector.collection(Collections.CONFIGURATIONS).update_one(
        {"name": destinationConfiguration},
        {"$push": {
            "manualSync": {
                "$each": [
                    {
                        "source": None,
                        "rule": rule,
                        "action": action.model_dump(),
                        "lastseen": days,
                        "ruleType": "unified_mapping",
                    }
                ]
            }
        }},
    )
    return {"success": True}


@router.post(
    "/unified-mapping/rules/actions/sync",
    tags=["Unified Mapping Rules"],
    description="Perform a rule's CRE action now on rows touched in the last N days.",
)
async def sync_unified_mapping_rule_action(
    rule: str = Query(..., description="Name of the unified mapping business rule."),
    configuration: str = Query(..., description="CRE configuration to act through."),
    action: str = Query(..., description="Action value to perform."),
    days: int = Query(
        ..., lt=366, gt=0, description="Act on rows updated in the last N days."
    ),
    user: User = Security(get_current_user, scopes=WRITE_SCOPES),
) -> dict:
    """Queue an immediate CRE action run for one rule/configuration/action.

    Dispatches ``cre.um_evaluate_records`` directly, mirroring CRE's own
    ``/business_rules/sync``. The run is marked manual, so — exactly like CRE's
    manual sync — it re-performs the action on every row in the window regardless
    of whether that row was already acted on, and leaves the scheduled run's
    checkpoint and act-once markers untouched.
    """
    from netskope.common.celery.scheduler import execute_celery_task
    from netskope.integrations.crev2.tasks.unified_mapping_actions import (
        um_evaluate_records,
    )

    stored_rule = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).find_one(
        {"name": rule}
    )
    if stored_rule is None:
        raise HTTPException(400, "Unified mapping business rule does not exist.")
    # The run reads the mapping's joined rows, so an indicators-joined mapping
    # needs cte_read even though the action itself is CRE-only.
    assert_cte_access(
        user, _rule_mapping_collections(stored_rule["view"]), cte_platform_enabled()
    )
    configured_actions = (stored_rule.get("creActions") or {}).get(configuration)
    if not configured_actions:
        raise HTTPException(
            400,
            f"CRE configuration '{configuration}' is not configured for actions "
            f"on rule '{rule}'.",
        )
    if action not in {stored.get("value") for stored in configured_actions}:
        raise HTTPException(
            400,
            f"Action '{action}' is not configured for CRE configuration "
            f"'{configuration}' on rule '{rule}'.",
        )
    logger.debug(
        f"Manual CRE action sync for unified mapping business rule {rule}, "
        f"configuration {configuration}, action {action} is triggered."
    )
    execute_celery_task(
        um_evaluate_records.apply_async,
        "cre.um_evaluate_records",
        args=[stored_rule["view"], rule, configuration, action, days, True],
    )
    return {"success": True}
