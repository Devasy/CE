"""ITSM Dashboard related endpoints."""
from fastapi import APIRouter, HTTPException, Query, Security
from datetime import datetime, timedelta
from typing import Optional
from enum import Enum
from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
)
from netskope.common.models import User
from netskope.common.api.routers.auth import get_current_user
from ..models.task import TaskRequestStatus, DataType

router = APIRouter(prefix="/dashboard", tags=["ITSM Dashboard"])
logger = Logger()
connector = DBConnector()


class TimeRange(Enum):
    """Time Range Enum."""

    ALL_TIME = "all_time"
    LAST_10_MINUTES = "last_10_minutes"
    LAST_30_MINUTES = "last_30_minutes"
    LAST_60_MINUTES = "last_60_minutes"
    CUSTOM = "custom"


@router.get(
    "/statistics",
    tags=["ITSM Dashboard"],
    description=(
        "Get the count of total number of tickets "
        "by status, type and configuration."
    ),
)
async def get_statistics(
    type: DataType = Query(
        DataType.ALERT,
        description=(
            "Type of data to be fetched. Options: 'event', 'alert'"
        )
    ),
    time_range: TimeRange = Query(
        TimeRange.LAST_10_MINUTES,
        description=(
            "Time range for the logs. Options: 'all_time',"
            " 'last_10_minutes', 'last_30_minutes', "
            "'last_60_minutes', 'custom'"),
    ),
    start_date: Optional[datetime] = Query(
        None,
        description=(
            "Start date for custom date range. "
            "Required if time_range is 'custom'."),
    ),
    end_date: Optional[datetime] = Query(
        None,
        description="End date for custom date range. Required if time_range is 'custom'.",
    ),
    user: User = Security(get_current_user, scopes=["cto_read"]),
):
    """Get the count of total number of logs and bytes ingested.

    Returns:
        Dict: Return dict of count of logs and bytes ingested.
    """
    # Determine the time range for the query
    if time_range == TimeRange.ALL_TIME:
        start_time = None
    elif time_range == TimeRange.LAST_10_MINUTES:
        start_time = datetime.now() - timedelta(minutes=10)
    elif time_range == TimeRange.LAST_30_MINUTES:
        start_time = datetime.now() - timedelta(minutes=30)
    elif time_range == TimeRange.LAST_60_MINUTES:
        start_time = datetime.now() - timedelta(minutes=60)
    elif time_range == TimeRange.CUSTOM:
        if not start_date or not end_date:
            raise HTTPException(
                status_code=400,
                detail="Both start_date and end_date are required for custom time range.",
            )
        if (end_date - start_date).days > 30:
            raise HTTPException(
                status_code=400, detail="Custom date range cannot exceed 30 days."
            )
        start_time = start_date

    # Adjust the query based on the time range
    query = {
        "dataType": type.value,
        "approvalStatus": {
            "$in": [
                TaskRequestStatus.APPROVED.value,
                TaskRequestStatus.NOT_REQUIRED.value
            ]
        },
    }
    if start_time:
        query["lastUpdatedAt"] = {"$gt": start_time}
    if time_range == TimeRange.CUSTOM:
        query["lastUpdatedAt"]["$lt"] = end_date
    pipeline = [
        {"$match": query},
        {
            "$group": {
                "_id": {"configuration": "$configuration", "status": "$status", "dataSubType": "$dataSubType"},
                "count": {"$sum": 1}
            }
        },
        {
            "$group": {
                "_id": {"configuration": "$_id.configuration", "status": "$_id.status"},
                "types": {"$push": {"k": "$_id.dataSubType", "v": "$count"}}
            }
        },
        {
            "$project": {
                "_id": 0,
                "configuration": "$_id.configuration",
                "status": "$_id.status",
                "types": {"$arrayToObject": "$types"}
            }
        },
        {
            "$group": {
                "_id": "$configuration",
                "statuses": {"$push": {"k": "$status", "v": "$types"}}
            }
        },
        {
            "$project": {
                "_id": 0,
                "configuration": "$_id",
                "statuses": {"$arrayToObject": "$statuses"}
            }
        }
    ]
    result = connector.collection(Collections.ITSM_TASKS).aggregate(
        pipeline,
        allowDiskUse=True,
    )
    # reformat result
    formatted_result = {}
    result = list(result)
    for item in result:
        formatted_result[item["configuration"]] = item["statuses"]
    return formatted_result
