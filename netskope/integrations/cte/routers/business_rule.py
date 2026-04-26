"""Business rule related endpoints."""
import copy
from typing import List, Optional
from fastapi.param_functions import Body
from fastapi import APIRouter, Security, Query, HTTPException
from datetime import datetime, timedelta
from starlette.responses import JSONResponse
from pympler import asizeof


from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import Logger, DBConnector, Collections

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
from netskope.integrations.cte.utils.constants import (
    MAX_IOC_LIMIT,
    WARN_IOC_LIMIT,
    MAX_SIZE_LIMIT,
    WARN_SIZE_LIMIT,
)

router = APIRouter()
logger = Logger()
connector = DBConnector()


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
    connector.collection(Collections.CTE_BUSINESS_RULES).update_one(
        {"name": rule.name},
        {"$set": rule.model_dump(exclude_none=True)},
    )
    logger.debug(f"CTE business rule {rule.name} updated.")
    return rule


@router.delete("/business_rule", tags=["CTE Business Rules"])
async def delete_business_rule(
    rule: BusinessRuleDelete,
    user: User = Security(get_current_user, scopes=["cte_write"]),
):
    """Delete a business rule."""
    connector.collection(Collections.CTE_BUSINESS_RULES).delete_one(
        {"name": rule.name}
    )
    logger.debug(f"Business rule {rule.name} has been successfully deleted.")
    return {"success": True}


@router.post("/business_rules/sync", tags=["CTE Business Rules"])
async def sync_action(
    rule: str = Query(...),
    sourceConfiguration: str = Query(...),
    destinationConfiguration: str = Query(...),
    action: Action = Body(...),
    days: int = Query(..., lt=366, gt=0),
    user: User = Security(get_current_user, scopes=["cte_read"]),
):
    """Test business rule."""
    if (
        connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
            {"name": rule}
        )
        is None
    ):
        raise HTTPException(400, "CTE business rule does not exist.")
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
    if sourceConfiguration:
        query = build_mongo_query(
            rule=rule, source=sourceConfiguration, lastseen=last_seen
        )
    else:
        query = build_mongo_query(rule=rule, lastseen=last_seen)
    # Count of File Hashes and URLs from Filtered IoCs.
    url_list = []
    hash_list = []
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
    if url_count <= 300000:
        url_list = list(
            connector.collection(Collections.INDICATORS).distinct(
                "value", url_query,
            )
        )
    hash_count = connector.collection(Collections.INDICATORS).count_documents(
        hash_query
    )

    if hash_count <= 300000:
        hash_list = list(
            connector.collection(Collections.INDICATORS).distinct(
                "value", hash_query,
            )
        )
    hash_size = asizeof.asizeof(hash_list) if hash_list else 0
    url_size = asizeof.asizeof(url_list) if url_list else 0
    # check if destination is a Netskope plugin.
    destination = None
    if destinationConfiguration:
        destination = list(
            connector.collection(Collections.CONFIGURATIONS).find(
                {"name": destinationConfiguration}
            )
        )[0]["plugin"]
    if destination == "netskope.plugins.Default.netskope.main":
        if (
            MAX_IOC_LIMIT > url_count > WARN_IOC_LIMIT
            or MAX_SIZE_LIMIT > url_size > WARN_SIZE_LIMIT
            or MAX_IOC_LIMIT > hash_count > WARN_IOC_LIMIT
            or MAX_SIZE_LIMIT > hash_size > WARN_SIZE_LIMIT
        ):
            logger.info(
                "You have used 90% of Netskope's allotted space for IoCs "
                "sharing, which is 6.3 MB or 270k urls/filehashes."
            )
        elif (
            url_count > MAX_IOC_LIMIT
            or url_size > MAX_SIZE_LIMIT
            or hash_count > MAX_IOC_LIMIT
            or hash_size > MAX_SIZE_LIMIT
        ):
            logger.debug(
                "You are exceeding the Netskope URL List/File restriction by "
                "attempting to share files larger than 7MB or 300k IoCs."
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
