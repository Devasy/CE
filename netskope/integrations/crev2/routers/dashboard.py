"""CREv2 Dashboard related endpoints."""
from fastapi import APIRouter, HTTPException, Query, Security
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
)
from netskope.common.models import User
from netskope.common.api.routers.auth import get_current_user

router = APIRouter(prefix="/dashboard", tags=["CREv2 Dashboard"])
logger = Logger()
connector = DBConnector()


class TimeRange(Enum):
    """Supported time range options for dashboard queries."""

    ALL_TIME = "all_time"
    LAST_10_MINUTES = "last_10_minutes"
    LAST_30_MINUTES = "last_30_minutes"
    LAST_60_MINUTES = "last_60_minutes"
    CUSTOM = "custom"


def _get_time_bounds(
    time_range: TimeRange,
    start_date: Optional[datetime],
    end_date: Optional[datetime]
) -> Tuple[Optional[datetime], Optional[datetime]]:
    if time_range == TimeRange.ALL_TIME:
        return None, None

    now = datetime.now()

    if time_range == TimeRange.LAST_10_MINUTES:
        return now - timedelta(minutes=10), now
    elif time_range == TimeRange.LAST_30_MINUTES:
        return now - timedelta(minutes=30), now
    elif time_range == TimeRange.LAST_60_MINUTES:
        return now - timedelta(minutes=60), now
    elif time_range == TimeRange.CUSTOM:
        if not start_date or not end_date:
            raise HTTPException(
                status_code=400,
                detail="Both start_date and end_date are required for custom time range."
            )
        if start_date >= end_date:
            raise HTTPException(
                status_code=400,
                detail="start_date must be earlier than end_date."
            )
        if (end_date - start_date).days > 30:
            raise HTTPException(
                status_code=400,
                detail="Custom date range cannot exceed 30 days."
            )
        return start_date, end_date

    return None, None


def _get_entity_names(entity_filter: Optional[str]) -> List[str]:
    """Get validated entity names based on filter."""
    if not entity_filter or entity_filter.lower() == "all":
        # Get all entities
        entities = connector.collection(Collections.CREV2_ENTITIES).find(
            {}, {"name": 1, "_id": 0}
        )
        return [entity["name"] for entity in entities]

    # Get specific entities and validate they exist
    requested_entities = [item.strip() for item in entity_filter.split(",") if item.strip()]
    entities = list(
        connector.collection(Collections.CREV2_ENTITIES).find(
            {"name": {"$in": requested_entities}},
            {"name": 1, "_id": 0},
        )
    )

    found_entities = [entity["name"] for entity in entities]
    missing_entities = set(requested_entities) - set(found_entities)

    if missing_entities:
        raise HTTPException(
            status_code=404,
            detail=f"Entities not found: {', '.join(sorted(missing_entities))}"
        )

    return found_entities


def _get_entity_record_counts(
    entity_names: List[str],
    start_time: Optional[datetime],
    end_time: Optional[datetime]
) -> Tuple[List[Dict[str, Any]], int]:
    """Get formatted result data and total record count for entities within time range."""
    time_query = {}
    if start_time and end_time:
        time_query["lastUpdated"] = {"$gte": start_time, "$lte": end_time}
    elif start_time:
        time_query["lastUpdated"] = {"$gte": start_time}
    elif end_time:
        time_query["lastUpdated"] = {"$lte": end_time}

    result_data = []
    total_records = 0

    for entity_name in entity_names:
        try:
            entity_collection = f"{Collections.CREV2_ENTITY_PREFIX.value}{entity_name}"
            count = connector.collection(entity_collection).count_documents(time_query)
            result_data.append({
                "entity": entity_name,
                "record_count": count
            })
            total_records += count
        except Exception as e:
            logger.error(
                f"Error counting records for entity {entity_name}",
                details=str(e)
            )
            result_data.append({
                "entity": entity_name,
                "record_count": 0
            })

    # Sort by entity name for consistent output
    result_data.sort(key=lambda x: x["entity"])

    return result_data, total_records


@router.get(
    "/pulled-data-by-entity",
    tags=["CREv2 Dashboard"],
    description="Get the count of pulled data records per entity with time range filtering."
)
async def get_pulled_data_by_entity(
    entity: Optional[str] = Query(
        None,
        description=(
            "Entity names to filter (comma-separated, e.g., 'Users,Devices'). "
            "Use 'All' or leave empty for all entities."
        ),
    ),
    time_range: TimeRange = Query(
        TimeRange.ALL_TIME,
        description=(
            "Time range for the data. Options: 'all_time', 'last_10_minutes', "
            "'last_30_minutes', 'last_60_minutes', 'custom'"
        )
    ),
    start_date: Optional[datetime] = Query(
        None,
        description="Start date for custom date range. Required if time_range is 'custom'."
    ),
    end_date: Optional[datetime] = Query(
        None,
        description="End date for custom date range. Required if time_range is 'custom'."
    ),
    user: User = Security(get_current_user, scopes=["cre_read"]),
) -> Dict[str, Any]:
    """Get pulled data record counts per entity with optional time filtering."""
    start_time, end_time = _get_time_bounds(time_range, start_date, end_date)

    entity_names = _get_entity_names(entity)

    if not entity_names:
        return {
            "summary": {
                "total_records": 0,
                "total_entities": 0,
                "time_range": time_range.value,
                "start_date": start_time.isoformat() if start_time else None,
                "end_date": end_time.isoformat() if end_time else None,
            },
            "data": []
        }

    result_data, total_records = _get_entity_record_counts(entity_names, start_time, end_time)

    return {
        "summary": {
            "total_records": total_records,
            "total_entities": len(result_data),
            "time_range": time_range.value,
            "start_date": start_time.isoformat() if start_time else None,
            "end_date": end_time.isoformat() if end_time else None,
        },
        "data": result_data
    }


@router.get(
    "/action-records",
    tags=["CREv2 Dashboard"],
    description="Get action records statistics based on business rule, destination_plugin, and time range."
)
async def get_action_records(
    business_rule: str = Query(
        ...,
        description="Business rule name to filter action records."
    ),
    destination_plugin: str = Query(
        ...,
        description="Target destination plugin name to filter action records."
    ),
    time_range: TimeRange = Query(
        TimeRange.ALL_TIME,
        description=(
            "Time range for the data. Options: 'all_time', 'last_10_minutes', "
            "'last_30_minutes', 'last_60_minutes', 'custom'"
        )
    ),
    start_date: Optional[datetime] = Query(
        None,
        description="Start date for custom date range. Required if time_range is 'custom'."
    ),
    end_date: Optional[datetime] = Query(
        None,
        description="End date for custom date range. Required if time_range is 'custom'."
    ),
    user: User = Security(get_current_user, scopes=["cre_read"]),
) -> Dict[str, Any]:
    """Get action records statistics based on business rule, destination_plugin, and time range.

    Returns:
        Dict: Statistics of action records grouped by status and action type.
    """
    # Verify business rule exists
    business_rule_doc = connector.collection(Collections.CREV2_BUSINESS_RULES).find_one(
        {"name": business_rule},
        {"_id": 1},
    )
    if not business_rule_doc:
        raise HTTPException(
            status_code=404,
            detail=f"Business rule '{business_rule}' not found."
        )

    # Verify destination_plugin exists
    destination_plugin_doc = connector.collection(Collections.CREV2_CONFIGURATIONS).find_one(
        {"name": destination_plugin},
        {"_id": 1},
    )
    if not destination_plugin_doc:
        raise HTTPException(
            status_code=404,
            detail=f"Destination Plugin '{destination_plugin}' not found."
        )

    # Get time bounds
    start_time, end_time = _get_time_bounds(time_range, start_date, end_date)

    # Build query for action logs
    query = {
        "rule": business_rule,
        "configuration": destination_plugin
    }

    # Add time range filter
    if start_time and end_time:
        query["performedAt"] = {"$gte": start_time, "$lte": end_time}
    elif start_time:
        query["performedAt"] = {"$gte": start_time}
    elif end_time:
        query["performedAt"] = {"$lte": end_time}

    # Aggregate action logs by status and action type
    pipeline = [
        {"$match": query},
        {
            "$group": {
                "_id": {
                    "action_label": "$action.label",
                    "action_value": "$action.value",
                    "status": "$status"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$group": {
                "_id": {
                    "action_label": "$_id.action_label",
                    "action_value": "$_id.action_value"
                },
                "statuses": {
                    "$push": {
                        "status": "$_id.status",
                        "count": "$count"
                    }
                },
                "total_count": {"$sum": "$count"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "action_label": "$_id.action_label",
                "action_value": "$_id.action_value",
                "statuses": 1,
                "total_count": 1
            }
        },
        {"$sort": {"action_label": 1}}
    ]

    result = list(
        connector.collection(Collections.CREV2_ACTION_LOGS).aggregate(
            pipeline,
            allowDiskUse=True
        )
    )

    # Calculate overall statistics
    total_actions = sum(item["total_count"] for item in result)
    status_counts = {}
    for item in result:
        for status_info in item.get("statuses", []):
            status = status_info.get("status")
            count = status_info.get("count", 0)
            status_counts[status] = status_counts.get(status, 0) + count

    return {
        "summary": {
            "business_rule": business_rule,
            "destination_plugin": destination_plugin,
            "total_actions": total_actions,
            "time_range": time_range.value,
            "start_date": start_time.isoformat() if start_time else None,
            "end_date": end_time.isoformat() if end_time else None,
            "status_counts": status_counts
        },
        "data": result
    }
