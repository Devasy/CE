"""Provides dashboard related endpoints."""

from fastapi import APIRouter, HTTPException, Security, Query
from netskope.common.utils import DBConnector, Collections, Logger
from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User

from netskope.integrations.cte.models.business_rule import BusinessRuleDB
from netskope.integrations.cte.tasks.share_indicators import (
    build_mongo_query,
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
    """Get statistics for pulled indicators."""
    pipeline = [
        {"$unwind": "$sources"},
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
        raise HTTPException(400, "CTE business rule does not exist.")
    rule = BusinessRuleDB(**rule)
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
