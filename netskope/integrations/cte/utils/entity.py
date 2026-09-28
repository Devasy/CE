"""Entity helpers for CTE business rules and sharing.

Lets a CTE business rule target either the native Threat Indicators data
(``indicators`` collection) or any CRE entity (``crev2_entity_<name>``). Mirrors
the CRE-side resolver helpers so a CTE rule's ``entity`` decides which collection
the rule reads from. The UI reads the CRE entity list/schema directly from the
existing ``/api/cre/*`` endpoints.
"""

import json
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional

from netskope.common.utils import Collections, parse_dates

from .schema import INDICATOR_STRING_FIELDS

if TYPE_CHECKING:  # pragma: no cover
    # cte.models.business_rule imports this module, so BusinessRuleDB may only
    # be referenced for typing -- never imported at runtime.
    from netskope.integrations.cte.models.business_rule import BusinessRuleDB

THREAT_INDICATORS_ENTITY = "Threat Indicators"

# Indicators persisted from CRE entity records are attributed to a derived
# source label of the form ``cre_<entity>_<ruleName>`` so each indicator source
# is attributable to the business rule that created it. This prefix is the
# single source of truth for that convention; the retraction flow keys off it
# to distinguish CRE-sourced indicators from plugin-sourced ones, so build and
# test the label only through ``cre_source_name``/``is_cre_source`` rather than
# hardcoding it. The label is never parsed back into its parts (all consumers
# are prefix checks), so rule names containing ``/`` or ``_`` are fine, and
# rule names are immutable so a label never goes stale.
CRE_SOURCE_PREFIX = "cre_"

# Prefixed so a view named after a plugin configuration cannot merge into that
# configuration's source entry. Same marker the UI uses for unified mapping names.
UM_SOURCE_PREFIX = "[Unified Mapping] "


def cre_source_name(entity: str, rule_name: str) -> str:
    """Derived indicator source label for a CRE-entity business rule.

    Format: ``cre_<entity>_<ruleName>``.
    """
    return f"{CRE_SOURCE_PREFIX}{entity}_{rule_name}"


def is_cre_source(source: str) -> bool:
    """Return True when an indicator source label denotes a CRE entity record."""
    return bool(source) and source.startswith(CRE_SOURCE_PREFIX)


def um_source_name(view: str) -> str:
    """Derived indicator source label for a unified mapping share.

    Format: ``[Unified Mapping] <view>``.
    """
    return f"{UM_SOURCE_PREFIX}{view}"


def is_threat_indicators_entity(entity: str) -> bool:
    """Return True when the entity is the native CTE Threat Indicators entity."""
    return (entity or THREAT_INDICATORS_ENTITY) == THREAT_INDICATORS_ENTITY


def get_entity_collection(entity: str) -> str:
    """Resolve the Mongo collection name for a CTE business-rule entity.

    Threat Indicators data is owned by CTE and stored in the ``indicators``
    collection; every other entity is a CRE entity living in
    ``crev2_entity_<name>``.
    """
    if is_threat_indicators_entity(entity):
        return Collections.INDICATORS.value
    return f"{Collections.CREV2_ENTITY_PREFIX.value}{entity}"


def get_entity_recency_field(entity: str) -> str:
    """Mongo field used for the days-window filter on entity records.

    CTE indicators use ``lastSeen``; CRE-ingested entities use ``lastUpdated``.
    """
    if is_threat_indicators_entity(entity):
        return "lastSeen"
    return "lastUpdated"


def load_mongo_filters(filters: Optional[str]) -> dict:
    """Parse a stored business-rule ``filters.mongo`` string into a Mongo query.

    The single place CTE turns a persisted filter string into a query. The
    ``parse_dates`` object_hook is load-bearing, not cosmetic: it converts
    ISO-8601 strings into ``datetime`` so they compare against BSON Dates
    (a plain ``json.loads`` here silently matches nothing, since BSON type
    ordering never equates a String with a Date), and bare strings into
    case-insensitive ``$regex``.
    """
    return json.loads(
        filters or "{}",
        object_hook=lambda pair: parse_dates(pair, INDICATOR_STRING_FIELDS),
    )


def build_cre_entity_match_query(
    rule: "BusinessRuleDB", lastseen: Optional[int] = None
) -> dict:
    """Mongo match for the CRE entity records a CTE business rule qualifies.

    Single source of truth shared by the ``GET /business_rules/test`` count
    endpoint and ``share_cre_entity_records``. The Test count must predict
    exactly what the share will match, so neither may build this query inline.

    Args:
        rule (BusinessRuleDB): CRE-entity CTE business rule.
        lastseen (Optional[int]): Days window. ``None`` means no recency bound
            (the recurring share); the Test endpoint always passes its
            ``days`` query parameter, which is validated to be greater than 0.

    Returns:
        dict: Mongo query for ``get_entity_collection(rule.entity)``.
    """
    clauses = []
    user_filter = load_mongo_filters(
        rule.filters.mongo if rule.filters else None
    )
    if user_filter:
        clauses.append(user_filter)
    if lastseen:
        recency_field = get_entity_recency_field(rule.entity)
        clauses.append(
            {recency_field: {"$gte": datetime.now() - timedelta(days=lastseen)}}
        )
    match_query = {"$and": clauses} if clauses else {}
    # Apply mute exceptions: exclude records matching any exception filter.
    # Tag-based exceptions do not apply to CRE entity records (no sources/tags).
    # Skip empty ({}) exception filters: an empty clause in $nor matches every
    # document and would silence the entire entity.
    mute_queries = []
    for mute in (rule.exceptions or []):
        if not (mute.filters and mute.filters.mongo):
            continue
        parsed = load_mongo_filters(mute.filters.mongo)
        if parsed:
            mute_queries.append(parsed)
    if mute_queries:
        match_query["$nor"] = mute_queries
    return match_query
