"""Business rule related routes."""

import json
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Security
from pymongo import ReturnDocument
from starlette.responses import JSONResponse

from netskope.common.api.routers.auth import get_current_user
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger, parse_dates

from ..models import (
    BusinessRuleDB,
    BusinessRuleDelete,
    BusinessRuleIn,
    BusinessRuleOut,
    BusinessRuleUpdate,
    get_entity_by_name,
)
from ..tasks.evaluate_records import evaluate_records
from ..utils import build_pipeline_from_entity

connector = DBConnector()
logger = Logger()
router = APIRouter()


@router.get("/business_rules", tags=["CREv2 Business Rules"])
async def list_business_rules(
    _: User = Security(get_current_user, scopes=["cre_read"])
) -> list[BusinessRuleOut]:
    """Get list of business rules."""
    return [
        BusinessRuleOut(**rule)
        for rule in connector.collection(
            Collections.CREV2_BUSINESS_RULES
        ).find({})
    ]


@router.post("/business_rules", tags=["CREv2 Business Rules"])
async def create_business_rule(
    rule: BusinessRuleIn,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> BusinessRuleOut:
    """Create a business rule."""
    connector.collection(Collections.CREV2_BUSINESS_RULES).insert_one(
        rule.model_dump()
    )
    return rule


@router.patch("/business_rules", tags=["CREv2 Business Rules"])
async def update_business_rule(
    rule: BusinessRuleUpdate,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> BusinessRuleOut:
    """Update a business rule."""
    result = connector.collection(
        Collections.CREV2_BUSINESS_RULES
    ).find_one_and_update(
        {"name": rule.name},
        {"$set": rule.model_dump(exclude_unset=True)},
        return_document=ReturnDocument.AFTER,
    )
    return result


@router.delete("/business_rules", tags=["CREv2 Business Rules"])
async def delete_business_rule(
    rule: BusinessRuleDelete,
    _: User = Security(get_current_user, scopes=["cre_write"]),
):
    """Delete a business rule."""
    result = connector.collection(Collections.CREV2_BUSINESS_RULES).delete_one(
        {"name": rule.name}
    )
    if result.deleted_count == 0:
        raise HTTPException(
            404, f"Could not find business rule with name {rule.name}."
        )
    return {"success": True}


@router.get("/business_rules/test", tags=["CREv2 Business Rules"])
async def test_business_rules(
    rule: str = Query(...),
    days: int = Query(..., lt=366, gt=0),
    _: User = Security(get_current_user, scopes=["cre_read"]),
):
    """Test business rule."""
    rule = connector.collection(Collections.CREV2_BUSINESS_RULES).find_one(
        {"name": rule}
    )
    if rule is None:
        raise ValueError(400, "Business rule does not exist.")
    rule: BusinessRuleDB = BusinessRuleDB(**rule)
    query = json.loads(rule.entityFilters.mongo, object_hook=parse_dates)
    result = connector.collection(
        f"{Collections.CREV2_ENTITY_PREFIX.value}{rule.entity}"
    ).aggregate(
        build_pipeline_from_entity(get_entity_by_name(rule.entity))
        + [
            {
                "$match": {
                    "lastUpdated": {
                        "$gte": datetime.now() - timedelta(days=days)
                    }
                }
            }
        ]
        + [{"$match": query}]
        + [{"$group": {"_id": None, "count": {"$sum": 1}}}],
        allowDiskUse=True,
    )
    result = list(result)
    if len(result) == 0:
        return JSONResponse(status_code=200, content={"count": 0})
    else:
        return JSONResponse(
            status_code=200, content={"count": result.pop()["count"]}
        )


@router.post("/business_rules/sync", tags=["CREv2 Business Rules"])
async def sync_action(
    rule: str = Query(...),
    config: str = Query(...),
    action: str = Query(...),
    days: int = Query(..., lt=366, gt=0),
    _: User = Security(get_current_user, scopes=["cre_write"]),
):
    """Test business rule."""
    logger.debug(
        f"Sync with CRE business rule {rule} for configuration {config} is triggered."
    )
    rule = connector.collection(Collections.CREV2_BUSINESS_RULES).find_one(
        {"name": rule}
    )
    if rule is None:
        raise ValueError(400, "Business rule does not exist.")
    rule: BusinessRuleDB = BusinessRuleDB(**rule)
    execute_celery_task(
        evaluate_records.apply_async,
        "cre.evaluate_records",
        args=[
            rule.entity,
            ...,  # for all users..
            [rule.name],  # for this rule
            config,  # for this config
            action,  # perform this action
            days,  # ..only the users created after
            True,  # for manual sync, avoid flap suppression
        ],
    )
    return {"success": True}
