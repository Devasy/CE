"""Business rule related endpoints."""
import copy
import traceback
from typing import List, Optional
from fastapi.param_functions import Body
from fastapi import APIRouter, Security, Query, HTTPException
from datetime import datetime, timedelta
from pymongo.errors import OperationFailure
from starlette.responses import JSONResponse


from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import (
    Logger,
    DBConnector,
    Collections,
    is_platform_enabled,
)

from netskope.integrations.cte.models.business_rule import (
    BusinessRuleIn,
    BusinessRuleOut,
    BusinessRuleUpdate,
    BusinessRuleDelete,
    BusinessRuleDB,
    Action,
)
from netskope.integrations.cte.tasks.share_indicators import (
    build_mongo_query,
)
from netskope.integrations.cte.utils.entity import (
    THREAT_INDICATORS_ENTITY,
    build_cre_entity_match_query,
    get_entity_collection,
)
from netskope.integrations.cte.utils.constants import (
    BYTES_IN_MB,
    CTE_NO_ACTION_VALUE,
    MAX_IOC_LIMIT,
    NETSKOPE_ACTION_LIMITS,
    NETSKOPE_DEFAULT_ACTION_LIMITS,
    WARN_RATIO,
)

router = APIRouter()
logger = Logger()
connector = DBConnector()

CRE_DISABLED_RULE_MESSAGE = (
    "This business rule uses CRE entities and is disabled because the CRE module "
    "is turned off. Enable the CRE module to modify or use it."
)

# Bytes a value costs beyond its own length, mirroring the plugin's
# ``len(json.dumps(value)) + 3``. ASCII assumed: \uXXXX escapes estimate low.
JSON_VALUE_OVERHEAD = 5
SIZE_QUERY_FAILED_MESSAGE = (
    "Could not measure the IoCs qualified by this business rule. Reduce the "
    "number of days or narrow the business rule filter, then try again."
)


def _payload_size(query: dict, bucket: str) -> int:
    """Return the payload size, in bytes, of the values matching ``query``.

    Summed inside Mongo so only the total crosses the wire: ``distinct`` packs
    every value into one 16 MB BSON document and fails outright on a wide rule,
    and materialising the values here would cost the API process the same memory.
    One accumulator, so the pipeline runs in constant memory whatever it matches.

    Args:
        query: Mongo query for the bucket being measured.
        bucket: Bucket name used in the log line ("URL" or "file hash").

    Raises:
        HTTPException: 400 when the size query itself fails.
    """
    pipeline = [
        # Left exactly as ``count_documents`` runs it: a $type clause here would
        # be coalesced into the predicate, where it can win the planner's trial
        # with a full scan of the ``value`` index.
        {"$match": query},
        {
            "$group": {
                "_id": None,
                # ``value`` is unique-indexed, so every match is already a
                # distinct list entry. $cond charges nothing for a non-string,
                # which is not a value the destination could send.
                "size": {
                    "$sum": {
                        "$cond": [
                            {"$eq": [{"$type": "$value"}, "string"]},
                            {
                                "$add": [
                                    {"$strLenBytes": "$value"},
                                    JSON_VALUE_OVERHEAD,
                                ]
                            },
                            0,
                        ]
                    }
                },
            }
        },
    ]
    try:
        result = list(
            connector.collection(Collections.INDICATORS).aggregate(pipeline)
        )
    except OperationFailure:
        logger.error(
            f"Could not measure the {bucket} payload of the tested business rule.",
            error_code="CTE_1110",
            details=traceback.format_exc(),
        )
        raise HTTPException(400, SIZE_QUERY_FAILED_MESSAGE)
    return result[0]["size"] if result else 0


def _get_sharing_actions(
    rule: BusinessRuleDB,
    source: Optional[str],
    destination: Optional[str],
) -> List[Action]:
    """Collect the sharing actions whose limits apply to the tested totals.

    With ``source`` given, only that pairing's ``sharedWith[source][destination]``
    actions are relevant. ``source`` is optional on the test endpoint, and when
    it is omitted the counts are aggregated across every source -- so every
    action configured to ``destination`` from any source is returned instead,
    deduplicated by action value.
    """
    shared_with = rule.sharedWith or {}
    if source:
        return (shared_with.get(source) or {}).get(destination) or []
    actions = {}
    for destinations in shared_with.values():
        for action in (destinations or {}).get(destination) or []:
            actions.setdefault(action.value, action)
    return list(actions.values())


def _log_netskope_limit_usage(
    rule: BusinessRuleDB,
    source: Optional[str],
    destination: Optional[str],
    url_count: int,
    url_size: int,
    hash_count: int,
    hash_size: int,
) -> None:
    """Log how close the qualified IoCs are to Netskope's limits.

    The MAX_IOC_LIMIT indicator cap applies to every action. The payload limit
    differs per action and each action consumes only one of the two buckets, so
    the size is evaluated against that action's own bucket and limit rather than
    against the combined totals; actions with no documented payload limit
    ("Add to Private App", "Add to Destination Profile") are evaluated on count
    alone. The actions come from ``_get_sharing_actions``. "No Action" pushes
    nothing, so it is skipped entirely.
    """
    actions = _get_sharing_actions(rule, source, destination)
    warn_count = MAX_IOC_LIMIT * WARN_RATIO
    for action in actions:
        if action.value == CTE_NO_ACTION_VALUE:
            continue
        limits = NETSKOPE_ACTION_LIMITS.get(
            action.value, NETSKOPE_DEFAULT_ACTION_LIMITS
        )
        bucket = limits["bucket"]
        if bucket == "url":
            count, size = url_count, url_size
        elif bucket == "hash":
            count, size = hash_count, hash_size
        else:
            # Not tied to one bucket: either total can trip the indicator cap.
            count, size = max(url_count, hash_count), 0
        target = limits["target"]
        items = limits["item_label"]
        max_size = limits["max_size"]
        restriction = (
            f"Netskope {target} restriction" if target else "Netskope restriction"
        )
        if max_size and size > max_size:
            logger.debug(
                f"You are exceeding the {restriction} by attempting to share "
                f"{items} larger than {max_size / BYTES_IN_MB:g} MB."
            )
        elif count > MAX_IOC_LIMIT:
            logger.debug(
                f"You are exceeding the {restriction} by attempting to share "
                f"more than {MAX_IOC_LIMIT / 1000:g}k {items}."
            )
        elif (
            max_size and size > max_size * WARN_RATIO
        ) or count > warn_count:
            allotted = (
                f"{max_size * WARN_RATIO / BYTES_IN_MB:g} MB or "
                f"{warn_count / 1000:g}k {items}"
                if max_size
                else f"{warn_count / 1000:g}k {items}"
            )
            logger.info(
                "You have used 90% of Netskope's allotted space for "
                f"{target or 'IoC'} sharing, which is {allotted}."
            )


@router.get("/business_rules", tags=["CTE Business Rules"])
async def get_business_rule(
    user: User = Security(get_current_user, scopes=["cte_read"])
) -> List[BusinessRuleOut]:
    """Get list of business rules."""
    rules = []
    for rule in connector.collection(Collections.CTE_BUSINESS_RULES).find({}):
        rules.append(BusinessRuleOut(**rule))
    return rules


@router.post("/business_rules", tags=["CTE Business Rules"])
async def create_business_rule(
    rule: BusinessRuleIn,
    user: User = Security(get_current_user, scopes=["cte_write"]),
) -> BusinessRuleOut:
    """Create a business rule."""
    if (
        rule.entity not in (None, THREAT_INDICATORS_ENTITY)
        and not is_platform_enabled("cre")
    ):
        raise HTTPException(
            400,
            "Cannot create a CRE-entity business rule while the CRE module is "
            "disabled. Enable the CRE module first.",
        )
    connector.collection(Collections.CTE_BUSINESS_RULES).insert_one(
        rule.model_dump()
    )
    logger.debug(f"CTE business rule {rule.name} successfully created.")
    return rule


@router.patch("/business_rule", tags=["CTE Business Rules"])
async def update_business_rule(
    rule: BusinessRuleUpdate,
    user: User = Security(get_current_user, scopes=["cte_write"]),
) -> BusinessRuleOut:
    """Update an existing business rules."""
    stored_rule = connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
        {"name": rule.name}
    )
    if stored_rule and stored_rule.get("disabledByCre"):
        raise HTTPException(400, CRE_DISABLED_RULE_MESSAGE)
    connector.collection(Collections.CTE_BUSINESS_RULES).update_one(
        {"name": rule.name},
        {"$set": rule.model_dump(exclude_none=True)},
    )
    logger.debug(f"CTE business rule {rule.name} updated.")
    updated_rule = connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
        {"name": rule.name}
    )
    if updated_rule is None:
        raise HTTPException(404, "No business rule with this name exists.")
    return BusinessRuleOut(**updated_rule)


@router.delete("/business_rule", tags=["CTE Business Rules"])
async def delete_business_rule(
    rule: BusinessRuleDelete,
    user: User = Security(get_current_user, scopes=["cte_write"]),
):
    """Delete a business rule."""
    stored_rule = connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
        {"name": rule.name}
    )
    if stored_rule and stored_rule.get("disabledByCre"):
        raise HTTPException(400, CRE_DISABLED_RULE_MESSAGE)
    connector.collection(Collections.CTE_BUSINESS_RULES).delete_one(
        {"name": rule.name}
    )
    logger.debug(f"Business rule {rule.name} has been successfully deleted.")
    return {"success": True}


@router.post("/business_rules/sync", tags=["CTE Business Rules"])
async def sync_action(
    rule: str = Query(...),
    destinationConfiguration: str = Query(...),
    action: Action = Body(...),
    days: int = Query(..., lt=366, gt=0),
    sourceConfiguration: Optional[str] = Query(None),
    user: User = Security(get_current_user, scopes=["cte_read"]),
):
    """Queue a one-time manual share for a rule/destination/action.

    ``sourceConfiguration`` is required for Threat Indicators rules and omitted
    for CRE-entity rules (those have no CTE source; the share reads the CRE
    entity collection). The queued entry is drained by the destination's next
    scheduled share, which branches on the rule's entity.
    """
    stored_rule = connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
        {"name": rule}
    )
    if stored_rule is None:
        raise HTTPException(400, "CTE business rule does not exist.")
    if stored_rule.get("disabledByCre"):
        raise HTTPException(400, CRE_DISABLED_RULE_MESSAGE)
    if (
        stored_rule.get("entity", THREAT_INDICATORS_ENTITY)
        == THREAT_INDICATORS_ENTITY
        and not sourceConfiguration
    ):
        raise HTTPException(
            400,
            "sourceConfiguration is required for Threat Indicators business rules.",
        )
    logger.debug(
        f"Sync with CTE business rule {rule} for configuration {sourceConfiguration} is triggered."
    )
    connector.collection(Collections.CONFIGURATIONS).update_one(
        {"name": destinationConfiguration},
        {"$push": {
            "manualSync": {
                "$each": [
                    {
                        "source": sourceConfiguration,
                        "rule": rule,
                        "action": action.model_dump(),
                        "lastseen": days
                    }
                ]
            }
        }},
    )
    return {"success": True}


@router.get("/business_rules/test", tags=["CTE Business Rules"])
async def test_business_rules(
    rule: str = Query(...),
    days: int = Query(..., lt=366, gt=0),
    sourceConfiguration: Optional[str] = Query(None),
    destinationConfiguration: Optional[str] = Query(None),
    user: User = Security(get_current_user, scopes=["cte_read"]),
):
    """Test business rule."""
    rule = connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
        {"name": rule}
    )
    if rule is None:
        raise HTTPException(400, "CTE business rule does not exist.")
    if (
        sourceConfiguration
        and connector.collection(Collections.CONFIGURATIONS).find_one(
            {"name": sourceConfiguration}
        )
        is None
    ):
        raise HTTPException(
            400, f"CTE {sourceConfiguration} Configuration does not exist."
        )
    rule = BusinessRuleDB(**rule)
    last_seen = datetime.now() - timedelta(days=days)
    if rule.entity != THREAT_INDICATORS_ENTITY:
        # Counted with the helper the share uses, so this matches what
        # share_cre_entity_records will. Netskope's limits still apply at push.
        record_query = build_cre_entity_match_query(rule, days)
        count = connector.collection(
            get_entity_collection(rule.entity)
        ).count_documents(record_query)
        return JSONResponse(status_code=200, content={"count": count})
    if sourceConfiguration:
        query = build_mongo_query(
            rule=rule, source=sourceConfiguration, lastseen=last_seen
        )
    else:
        query = build_mongo_query(rule=rule, lastseen=last_seen)
    # Count of File Hashes and URLs from Filtered IoCs.
    hash_query = copy.deepcopy(query)
    hash_query["$and"].extend(
        [{"type": {"$in": ["sha256", "md5"]}}, {"active": True}]
    )
    url_query = copy.deepcopy(query)
    url_query["$and"].extend(
        [
            {
                "type": {
                    "$in": [
                        "url",
                        "ipv4",
                        "ipv6",
                        "ipv4_cidr",
                        "ipv6_cidr",
                        "hostname",
                        "domain",
                        "fqdn",
                    ]
                }
            },
            {"active": True},
        ]
    )
    url_count = connector.collection(Collections.INDICATORS).count_documents(
        url_query
    )
    hash_count = connector.collection(Collections.INDICATORS).count_documents(
        hash_query
    )
    # Above the count cap the destination truncates on count alone, so the size
    # is not measured -- the UI reports the count violation instead.
    url_size = (
        _payload_size(url_query, "URL")
        if 0 < url_count <= MAX_IOC_LIMIT
        else 0
    )
    hash_size = (
        _payload_size(hash_query, "file hash")
        if 0 < hash_count <= MAX_IOC_LIMIT
        else 0
    )
    # check if destination is a Netskope plugin.
    destination = None
    if destinationConfiguration:
        destination = list(
            connector.collection(Collections.CONFIGURATIONS).find(
                {"name": destinationConfiguration}
            )
        )[0]["plugin"]
    is_netskope = destination is not None and destination.split(".")[-2] == "netskope"
    if is_netskope:
        _log_netskope_limit_usage(
            rule=rule,
            source=sourceConfiguration,
            destination=destinationConfiguration,
            url_count=url_count,
            url_size=url_size,
            hash_count=hash_count,
            hash_size=hash_size,
        )
    return JSONResponse(
        status_code=200,
        content={
            "hash_count": hash_count,
            "hash_size": hash_size,
            "url_count": url_count,
            "url_size": url_size,
        },
    )
