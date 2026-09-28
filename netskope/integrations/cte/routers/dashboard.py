"""Provides dashboard related endpoints."""

import traceback
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Security, Query
from netskope.common.utils import DBConnector, Collections, Logger
from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User

from netskope.integrations.cte.models.business_rule import BusinessRuleDB
from netskope.integrations.cte.tasks.share_indicators import (
    build_mongo_query,
)
from netskope.integrations.cte.utils.dashboard import (
    DESTINATION_STATUSES,
    derived_source_regex,
)
from netskope.integrations.cte.utils.entity import (
    cre_source_name,
    is_threat_indicators_entity,
    um_source_name,
)


router = APIRouter(prefix="/dashboard")
logger = Logger()
db_connector = DBConnector()


@router.get(
    "/pull",
    tags=["CTE Dashboard"],
)
async def pull_statistics(
    user: User = Security(get_current_user, scopes=["cte_read"]),
):
    """Get statistics for pulled indicators.

    Excludes indicators built from CRE entity records (synthetic
    ``cre_<entity>_<rule>`` sources) and from unified mappings (synthetic
    ``[Unified Mapping] <mapping>`` sources): neither was pulled by a CTE plugin
    and no plugin configuration exists for either source label, so the "Pulled
    IoCs" widget would render them as phantom plugins. They are reported by
    ``/dashboard/cre-entity-sharing`` and ``/dashboard/unified-mapping-sharing``
    respectively.
    """
    pipeline = [
        {
            "$match": {
                "sources": {
                    "$elemMatch": {"source": {"$not": derived_source_regex()}}
                }
            }
        },
        {"$unwind": "$sources"},
        {"$match": {"sources.source": {"$not": derived_source_regex()}}},
        {
            "$group": {
                "_id": {"type": "$type", "source": "$sources.source"},
                "retracted_count": {
                    "$sum": {"$cond": ["$sources.retracted", 1, 0]}
                },
                "unretracted_count": {
                    "$sum": {"$cond": ["$sources.retracted", 0, 1]}
                },
                "count": {"$sum": 1},
            }
        },
        {
            "$group": {
                "_id": "$_id.type",
                "sources": {
                    "$push": {
                        "k": "$_id.source",
                        "v": {
                            "retractedCount": "$retracted_count",
                            "unretractedCount": "$unretracted_count",
                            "allCount": "$count",
                        },
                    }
                },
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {
                    "$arrayToObject": {
                        "$concatArrays": [
                            [{"k": "$_id", "v": {
                                "$arrayToObject": "$sources"
                            }}]
                        ]
                    }
                }
            }
        },
    ]
    result = db_connector.collection(
        Collections.INDICATORS
    ).aggregate(pipeline)
    if not result:
        raise HTTPException(status_code=404, detail="No data found")
    return {
        list(item.keys())[0]: list(item.values())[0]
        for item in list(result)
    }


@router.get(
    "/sharing",
    tags=["CTE Dashboard"],
)
async def sharing_statistics(
    rule: str = Query(...),
    sourceConfiguration: str = Query(...),
    destinationConfiguration: str = Query(...),
    user: User = Security(get_current_user, scopes=["cte_read"]),
):
    """Get statistics for shared indicators."""
    rule = db_connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
        {"name": rule}
    )
    if rule is None:
        raise HTTPException(404, "CTE business rule does not exist.")
    rule = BusinessRuleDB(**rule)
    if not is_threat_indicators_entity(rule.entity):
        # A CRE-entity rule's filters target CRE entity record fields, so
        # ``build_mongo_query`` (which is indicator-shaped) would silently
        # produce meaningless counts against the indicators collection. Those
        # rules also have no source configuration at all.
        raise HTTPException(
            400,
            f"Business rule '{rule.name}' targets the CRE entity "
            f"'{rule.entity}' and has no source configuration.",
        )
    query = build_mongo_query(rule=rule, source=sourceConfiguration)
    pipeline = [
        {"$match": query},
        {
            "$group": {
                "_id": {"type": "$type"},
                "filtered_count": {"$sum": 1},
                "shared_count": {
                    "$sum": {
                        "$cond": [
                            {"$in": [destinationConfiguration, "$sharedWith"]},
                            1,
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "type": "$_id.type",
                "filteredCount": "$filtered_count",
                "sharedCount": "$shared_count",
                "misMatchCount": {
                    "$subtract": ["$filtered_count", "$shared_count"]
                },
            }
        },
    ]
    result = db_connector.collection(
        Collections.INDICATORS
    ).aggregate(pipeline)
    if not result:
        raise HTTPException(status_code=404, detail="No data found")
    return {
        item["type"]: {
            "filteredCount": item["filteredCount"],
            "sharedCount": item["sharedCount"],
            "misMatchCount": item["misMatchCount"],
        }
        for item in list(result)
    }


def _derived_source_statistics(
    source_name: str, destination_configuration: str, context: str
) -> Dict[str, Any]:
    """Per-type/per-status share counts for one derived source at one destination.

    Shared by ``/cre-entity-sharing`` and ``/unified-mapping-sharing``: both
    report on indicators the share flow *built* rather than pulled, and both
    persist them the same way (``persist_cre_entity_indicators``) — a
    ``sources[]`` entry under a synthetic label whose ``destinations[]`` carries
    the per-destination status. Only the label differs, so the aggregation does
    not.

    Args:
        source_name (str): The synthetic source label to report on.
        destination_configuration (str): The single destination to scope to.
        context (str): Human-readable subject for the error log/response, e.g.
            "CRE entity" or "unified mapping".

    Returns:
        Dict[str, Any]: ``{"builtByType": {...}, "statusesByType": {...},
            "statusTotals": {...}}``.

    Raises:
        HTTPException: 500 when either aggregation fails.
    """
    base_match = {"sources.source": source_name}
    pre_unwind_match = {"sources": {"$elemMatch": {"source": source_name}}}

    def _stages(extra=None):
        """Build the common unwind/match prefix for both aggregations."""
        stages = [
            {"$match": pre_unwind_match},
            {"$unwind": "$sources"},
            {"$match": base_match},
        ]
        stages.extend(extra or [])
        return stages

    try:
        built_rows = list(
            db_connector.collection(Collections.INDICATORS).aggregate(
                _stages(
                    [
                        {
                            "$match": {
                                "sources.destinations.name": (
                                    destination_configuration
                                )
                            }
                        },
                        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
                    ]
                ),
                allowDiskUse=True,
            )
        )

        status_rows = list(
            db_connector.collection(Collections.INDICATORS).aggregate(
                _stages(
                    [
                        {
                            "$match": {
                                "sources.destinations": {
                                    "$elemMatch": {
                                        "name": destination_configuration
                                    }
                                }
                            }
                        },
                        {"$unwind": "$sources.destinations"},
                        {
                            "$match": {
                                "sources.destinations.name": (
                                    destination_configuration
                                )
                            }
                        },
                        {
                            "$group": {
                                "_id": {
                                    "type": "$type",
                                    "status": "$sources.destinations.status",
                                },
                                "count": {"$sum": 1},
                            }
                        },
                    ]
                ),
                allowDiskUse=True,
            )
        )
    except Exception:
        logger.error(
            f"Could not aggregate {context} sharing statistics for source "
            f"'{source_name}'.",
            error_code="CTE_1106",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            status_code=500,
            detail=f"Could not aggregate {context} sharing statistics.",
        )

    built_by_type = {
        row["_id"]: row.get("count", 0) for row in built_rows if row.get("_id")
    }
    statuses_by_type: Dict[str, Dict[str, int]] = {}
    status_totals: Dict[str, int] = {
        status: 0 for status in DESTINATION_STATUSES
    }
    for row in status_rows:
        ioc_type = row["_id"].get("type")
        status = row["_id"].get("status")
        if not ioc_type or not status:
            continue
        count = row.get("count", 0)
        by_status = statuses_by_type.setdefault(ioc_type, {})
        by_status[status] = by_status.get(status, 0) + count
        status_totals[status] = status_totals.get(status, 0) + count

    return {
        "builtByType": built_by_type,
        "statusesByType": statuses_by_type,
        "statusTotals": status_totals,
    }


def _sharing_data_rows(statistics: Dict[str, Any]) -> list:
    """Reshape ``_derived_source_statistics`` output into the chart's rows."""
    statuses_by_type = statistics["statusesByType"]
    return [
        {
            "type": ioc_type,
            "builtCount": built_count,
            "statuses": [
                {"status": status, "count": count}
                for status, count in sorted(
                    statuses_by_type.get(ioc_type, {}).items()
                )
            ],
        }
        for ioc_type, built_count in sorted(statistics["builtByType"].items())
    ]


@router.get(
    "/cre-entity-sharing",
    tags=["CTE Dashboard"],
    description=(
        "Share status of the indicators one CRE-entity business rule built at "
        "one destination, broken down by indicator type."
    ),
)
async def cre_entity_sharing_statistics(
    rule: str = Query(...),
    destinationConfiguration: str = Query(...),
    user: User = Security(get_current_user, scopes=["cte_read"]),
) -> Dict[str, Any]:
    """Get share statistics for the indicators of one CRE-entity rule.

    Counts come from the persisted outcome, not from re-running the rule's
    filters: the rule's filters target CRE entity records while the indicators it
    produced live in the CTE ``indicators`` collection. Every such indicator
    carries a ``sources[]`` entry whose ``source`` is the rule's synthetic label
    and whose ``destinations[]`` records the per-destination status.

    ``destinationConfiguration`` is required, matching ``/dashboard/sharing``.
    The status counts come from a ``sources.destinations`` unwind, which emits
    one row per (indicator, destination) pair — so aggregating across several
    destinations counted an indicator shared to three of them three times, and
    the chart's bars added up to more than the indicators that exist. Scoping to
    a single destination is what makes the per-status counts real IoC counts.

    Both ``builtCount`` and the status counts are scoped to that destination, so
    they describe one population and "built minus shared" is meaningful.

    Args:
        rule (str): CRE-entity business rule name.
        destinationConfiguration (str): The destination to report on. Must be one
            the rule shares with.

    Returns:
        Dict[str, Any]: ``{"summary": {...}, "data": [...]}``.

    Raises:
        HTTPException: 404 when the rule does not exist or does not share with
            the requested destination, 400 when it is a Threat Indicators rule.
    """
    rule_doc = db_connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
        {"name": rule}
    )
    if rule_doc is None:
        raise HTTPException(
            status_code=404, detail=f"CTE business rule '{rule}' does not exist."
        )
    rule_model = BusinessRuleDB(**rule_doc)
    if is_threat_indicators_entity(rule_model.entity):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Business rule '{rule_model.name}' targets the Threat "
                f"Indicators entity and has no CRE source configuration."
            ),
        )

    source_name = cre_source_name(rule_model.entity, rule_model.name)
    # A rule with an empty ``creShare`` shares with nothing, so every destination
    # is unknown to it and this 404s — which is the honest answer, and matches
    # what the UI shows for such a rule ("No Data Found", no request sent).
    if destinationConfiguration not in (rule_model.creShare or {}):
        raise HTTPException(
            status_code=404,
            detail=(
                f"Business rule '{rule_model.name}' does not share with "
                f"destination '{destinationConfiguration}'."
            ),
        )

    statistics = _derived_source_statistics(
        source_name, destinationConfiguration, "CRE entity"
    )
    return {
        "summary": {
            "rule": rule_model.name,
            "entity": rule_model.entity,
            "destinations": [destinationConfiguration],
            "builtCount": sum(statistics["builtByType"].values()),
            "statusCounts": statistics["statusTotals"],
        },
        "data": _sharing_data_rows(statistics),
    }


@router.get(
    "/unified-mapping-sources",
    tags=["CTE Dashboard"],
    description=(
        "Unified mappings that have CTE sharing configured, with the "
        "destination configurations their rules share to."
    ),
)
async def unified_mapping_sources(
    user: User = Security(get_current_user, scopes=["cte_read"]),
) -> Dict[str, Any]:
    """List the unified mappings the "Unified Mapping IoCs" tab can chart.

    Read from the (small) unified mapping rules collection rather than by
    scanning ``indicators`` for distinct derived source labels: the rules are
    what is configured, so a mapping appears as soon as sharing is set up on it
    instead of only after its first successful share, and no full pass over the
    indicators store is needed just to populate two dropdowns.

    Several rules may target the same mapping, and every one of them shares
    under that mapping's single source label ``[Unified Mapping] <mapping>``
    (unlike CRE-entity sharing, whose label embeds the rule name). The
    destinations are therefore the union across the mapping's rules, and the
    counts this feeds are per mapping, not per rule — there is no rule
    attribution on a persisted indicator to filter by.

    Lives under ``/cte`` with a ``cte_read`` scope, like
    ``/dashboard/cre-entity-sharing``, so the widget keeps working when the CRE
    module is disabled or the user holds no ``cre_read``.

    Returns:
        Dict[str, Any]: ``{"data": [{"view", "source", "destinations"}, ...]}``
            sorted by mapping name, listing only mappings with at least one
            destination.
    """
    destinations_by_view: Dict[str, set] = {}
    try:
        rules = db_connector.collection(
            Collections.UNIFIED_MAPPING_RULES
        ).find({}, {"view": 1, "cteShare": 1, "_id": 0})
        for rule_doc in rules:
            view = rule_doc.get("view")
            if not view:
                continue
            # Muted rules are deliberately included: muting stops future shares,
            # it does not retract what a rule already shared, so the indicators
            # it built are still on the Threat IoCs page and still worth
            # charting.
            destinations = destinations_by_view.setdefault(view, set())
            destinations.update(
                name for name in (rule_doc.get("cteShare") or {}) if name
            )
    except Exception:
        logger.error(
            "Could not list the unified mappings with CTE sharing configured.",
            error_code="CTE_1111",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            status_code=500,
            detail="Could not list unified mapping sharing sources.",
        )

    return {
        "data": [
            {
                "view": view,
                "source": um_source_name(view),
                "destinations": sorted(destinations),
            }
            for view, destinations in sorted(destinations_by_view.items())
            if destinations
        ]
    }


@router.get(
    "/unified-mapping-sharing",
    tags=["CTE Dashboard"],
    description=(
        "Share status of the indicators one unified mapping built at one "
        "destination, broken down by indicator type."
    ),
)
async def unified_mapping_sharing_statistics(
    view: str = Query(...),
    destinationConfiguration: str = Query(...),
    user: User = Security(get_current_user, scopes=["cte_read"]),
) -> Dict[str, Any]:
    """Get share statistics for the indicators built from one unified mapping.

    Same contract as ``/dashboard/cre-entity-sharing`` — counts come from the
    persisted outcome, and ``destinationConfiguration`` is required so the
    ``sources.destinations`` unwind cannot count an indicator once per
    destination — with one difference: the unit is the *mapping*, not the rule.
    Unified mapping sharing attributes every indicator to
    ``[Unified Mapping] <mapping>`` regardless of which of the mapping's rules
    produced it, so a persisted indicator carries no rule attribution to filter
    on and a per-rule breakdown would be a guess. See
    ``/dashboard/unified-mapping-sources`` for the mapping/destination pairs
    this accepts.

    Args:
        view (str): Unified mapping name.
        destinationConfiguration (str): The destination to report on. Must be
            one that a rule on this mapping shares with.

    Returns:
        Dict[str, Any]: ``{"summary": {...}, "data": [...]}``, matching the
            CRE-entity endpoint's shape so the two charts share their reshaping.

    Raises:
        HTTPException: 404 when no rule on the mapping shares with the
            requested destination (which also covers a mapping that has no
            sharing configured, or does not exist).
    """
    shares_here = any(
        destinationConfiguration in (rule_doc.get("cteShare") or {})
        for rule_doc in db_connector.collection(
            Collections.UNIFIED_MAPPING_RULES
        ).find({"view": view}, {"cteShare": 1, "_id": 0})
    )
    if not shares_here:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No unified mapping business rule on '{view}' shares with "
                f"destination '{destinationConfiguration}'."
            ),
        )

    statistics = _derived_source_statistics(
        um_source_name(view), destinationConfiguration, "unified mapping"
    )
    return {
        "summary": {
            "view": view,
            "source": um_source_name(view),
            "destinations": [destinationConfiguration],
            "builtCount": sum(statistics["builtByType"].values()),
            "statusCounts": statistics["statusTotals"],
        },
        "data": _sharing_data_rows(statistics),
    }
