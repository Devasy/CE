"""CLS Dashboard."""

from enum import Enum
from typing import Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, Security, Query, HTTPException
from netskope.common.models import User, BatchDataType
from netskope.common.api.routers.auth import get_current_user
from netskope.common.utils import (
    DBConnector,
    DataBatchManager,
    Logger,
    Collections,
    PluginHelper,
)
from netskope.integrations.cls.utils.webtx_metrics_collector import get_webtx_metrics

plugin_helper = PluginHelper()


class TimeRange(Enum):
    """Time Range Enum."""

    ALL_TIME = "all_time"
    LAST_10_MINUTES = "last_10_minutes"
    LAST_30_MINUTES = "last_30_minutes"
    LAST_60_MINUTES = "last_60_minutes"
    CUSTOM = "custom"


router = APIRouter(prefix="/dashboard", tags=["CLS Dashboard"])
db_connector = DBConnector()
batch_manager = DataBatchManager()
logger = Logger()


def set_count_of_logs_bytes(logs_bytes_count_configuration, key_name, minutes=None):
    """Find total number of logs and bytes ingested.

    Args:
        minutes (int): Filter out logs or bytes data based on minutes
        parameter.
        logs_bytes_count_configuration (dict): Dictionary of logs and bytes
        ingested.
        key_name (str): key name like Last 10 min, Last 30 min, Last 60 min.
    """
    # Find total number of logs and bytes ingested. from data batches.
    if minutes:
        query = {"createdAt": {"$gt": datetime.now() - timedelta(minutes=minutes)}}
    else:
        query = {}
    pipelines = [
        {"$match": query},
        {"$unwind": {"path": "$cls"}},
        {
            "$lookup": {
                "from": Collections.CLS_CONFIGURATIONS,
                "localField": "cls.destination",
                "foreignField": "name",
                "as": "destinationConfig",
            }
        },
        {"$match": {"destinationConfig": {"$ne": []}}},
        {
            "$group": {
                "_id": None,  # Combine all documents into a single group
                "logsIngested": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$type", BatchDataType.WEBTX_BLOBS.value]},
                            0,
                            "$cls.count.ingested",
                        ]
                    }
                },
                "bytesIngested": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$type", BatchDataType.WEBTX_BLOBS.value]},
                            "$cls.size.ingested",
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "logsIngested": 1,
                "bytesIngested": 1,
            }
        },
    ]
    result = batch_manager.aggregate(pipelines)
    result = list(result)
    if result:
        result = result[0]
        logs_bytes_count_configuration[key_name]["logsIngested"] = result[
            "logsIngested"
        ]
        logs_bytes_count_configuration[key_name]["bytesIngested"] = result[
            "bytesIngested"
        ]


@router.get(
    "/summary",
    tags=["CLS Dashboard"],
    description="Get the count of total number of logs/bytes ingested.",
)
async def get_count_of_logs_and_bytes(
    user: User = Security(get_current_user, scopes=["cls_read"]),
):
    """Get the count of total number of logs and bytes ingested.

    Returns:
        Dict: Return dict of count of logs and bytes ingested.
    """
    logs_bytes_count_configuration = {
        "All Time": {
            "logsIngested": 0,
            "bytesIngested": 0,
        },
        "Last 10 Minutes": {
            "logsIngested": 0,
            "bytesIngested": 0,
        },
        "Last 30 Minutes": {
            "logsIngested": 0,
            "bytesIngested": 0,
        },
        "Last 60 Minutes": {
            "logsIngested": 0,
            "bytesIngested": 0,
        },
    }
    set_count_of_logs_bytes(logs_bytes_count_configuration, "Last 10 Minutes", 10)
    set_count_of_logs_bytes(logs_bytes_count_configuration, "Last 30 Minutes", 30)
    set_count_of_logs_bytes(logs_bytes_count_configuration, "Last 60 Minutes", 60)
    set_count_of_logs_bytes(logs_bytes_count_configuration, "All Time")

    webtx_metrics = get_webtx_metrics()
    if webtx_metrics:
        webtx_metrics["last_updated_at"] = (
            webtx_metrics["last_updated_at"].isoformat() + "Z"
        )
        logs_bytes_count_configuration.update({"webtx_metrics": webtx_metrics})
    return logs_bytes_count_configuration


@router.get(
    "/statistics",
    tags=["CLS Dashboard"],
    description=(
        "Get the count of total number of logs pulled and ingested by type."
    ),
)
async def get_stastics_by_type(
    type: BatchDataType = Query(
        BatchDataType.ALERTS, description="Type of data to be fetched"
    ),
    time_range: TimeRange = Query(
        TimeRange.LAST_10_MINUTES,
        description=(
            "Time range for the logs. Options: 'all_time',"
            " 'last_10_minutes', 'last_30_minutes', "
            "'last_60_minutes', 'custom'"
        ),
    ),
    start_date: Optional[datetime] = Query(
        None,
        description=(
            "Start date for custom date range. Required if time_range is 'custom'."
        ),
    ),
    end_date: Optional[datetime] = Query(
        None,
        description="End date for custom date range. Required if time_range is 'custom'.",
    ),
    user: User = Security(get_current_user, scopes=["cls_read"]),
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
    else:
        raise HTTPException(status_code=400, detail="Invalid time_range value.")

    # Adjust the query based on the time range
    query = {"type": type.value}
    if start_time:
        query["createdAt"] = {"$gt": start_time}
    if time_range == TimeRange.CUSTOM:
        query["createdAt"]["$lt"] = end_date

    result = batch_manager.aggregate(
        [
            {"$match": query},
            {"$unwind": "$cls"},
            {
                "$lookup": {
                    "from": Collections.CLS_CONFIGURATIONS,
                    "localField": "cls.destination",
                    "foreignField": "name",
                    "as": "destinationConfig",
                }
            },
            {"$match": {"destinationConfig": {"$ne": []}}},
            {
                "$group": {
                    "_id": {"sub_type": "$sub_type", "destination": "$cls.destination"},
                    "pulled": {
                        "$sum": (
                            "$cls.count.filtered"
                            if type != BatchDataType.WEBTX_BLOBS
                            else "$cls.size.filtered"
                        )
                    },
                    "ingested": {
                        "$sum": (
                            "$cls.count.ingested"
                            if type != BatchDataType.WEBTX_BLOBS
                            else "$cls.size.ingested"
                        )
                    },
                }
            },
            {
                "$group": {
                    "_id": "$_id.sub_type",
                    "destinations": {
                        "$push": {
                            "destination": "$_id.destination",
                            "pulled": "$pulled",
                            "ingested": "$ingested",
                        }
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "sub_type": "$_id",
                    "destinations": {
                        "$arrayToObject": {
                            "$map": {
                                "input": "$destinations",
                                "as": "dest",
                                "in": {
                                    "k": "$$dest.destination",
                                    "v": {
                                        "pulled": "$$dest.pulled",
                                        "ingested": "$$dest.ingested",
                                    },
                                },
                            }
                        }
                    },
                }
            },
        ]
    )
    result_list = list(result)

    all_plugin_destinations = {}
    plugins = db_connector.collection(Collections.CLS_CONFIGURATIONS).find()
    for plugin in plugins:
        PluginClass = plugin_helper.find_by_id(plugin.get("plugin"))
        if PluginClass:
            metadata = PluginClass.metadata
            if not (
                metadata.get("netskope")
                or metadata.get("pulling_supported")
                or metadata.get("pull_supported")
            ):
                all_plugin_destinations[plugin.get("name")] = {
                    "pulled": 0,
                    "ingested": 0,
                }

    response = []

    if result_list:
        for item in result_list:
            completed_destinations = all_plugin_destinations.copy()
            for dest, stats in item["destinations"].items():
                if dest in completed_destinations:
                    completed_destinations[dest] = stats
            item["destinations"] = completed_destinations
            response.append(item)
    else:
        response = [{"sub_type": "", "destinations": all_plugin_destinations}]

    return response


@router.get(
    "/statistics/{sub_type}",
    tags=["CLS Dashboard"],
    description=(
        "Get subtype specific count of alerts/events/logs pulled and ingested."
    ),
)
async def get_count_of_logs_and_bytes_by_sub_type(
    sub_type: str,
    destination: str,
    page: int = 1,
    page_size: int = 25,
    type: BatchDataType = Query(
        BatchDataType.ALERTS, description="Type of data to be fetched"
    ),
    time_range: TimeRange = Query(
        TimeRange.LAST_10_MINUTES,
        description=(
            "Time range for the logs. Options: 'all_time', "
            "'last_10_minutes', 'last_30_minutes', "
            "'last_60_minutes', 'custom'"
        ),
    ),
    start_date: Optional[datetime] = Query(
        None,
        description="Start date for custom date range. Required if time_range is 'custom'.",
    ),
    end_date: Optional[datetime] = Query(
        None,
        description="End date for custom date range. Required if time_range is 'custom'.",
    ),
    show_inconsistent_only: bool = Query(
        False,
        description="Show only inconsistent data where pulled and ingested numbers do not match",
    ),
    user: User = Security(get_current_user, scopes=["cls_read"]),
):
    """Get subtype specific count of alerts/events/logs pulled and ingested.

    Returns:
        List: Return array of data batches.
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
    else:
        raise HTTPException(status_code=400, detail="Invalid time_range value.")
    # Adjust the query based on the time range
    query = {
        "type": type.value,
        "sub_type": {"$regex": f"^{sub_type}$", "$options": "i"},
        "cls.destination": destination,
    }
    if start_time:
        query["createdAt"] = {"$gt": start_time}
    if time_range == TimeRange.CUSTOM:
        query["createdAt"]["$lt"] = end_date
    pipeline = [
        {"$match": query},
        {"$unwind": "$cls"},
        {
            "$lookup": {
                "from": Collections.CLS_CONFIGURATIONS,
                "localField": "cls.destination",
                "foreignField": "name",
                "as": "destinationConfig",
            }
        },
        {"$match": {"destinationConfig": {"$ne": []}}},
        {
            "$project": {
                "_id": 0,
                "id": {"$toString": "$_id"},
                "pulled": {
                    "$sum": (
                        "$cls.count.filtered"
                        if type != BatchDataType.WEBTX_BLOBS
                        else "$cls.size.filtered"
                    )
                },
                "ingested": {
                    "$sum": (
                        "$cls.count.ingested"
                        if type != BatchDataType.WEBTX_BLOBS
                        else "$cls.size.ingested"
                    )
                },
                "createdAt": 1,
                "destination": "$cls.destination",
            }
        },
        {"$sort": {"createdAt": -1}},
        {
            "$facet": {
                "stage1": [{"$group": {"_id": None, "count": {"$sum": 1}}}],
                "stage2": [
                    {"$skip": page_size * (page - 1)},
                    {"$limit": page_size},
                ],
            }
        },
        {"$unwind": "$stage1"},
        {"$project": {"count": "$stage1.count", "data": "$stage2"}},
    ]
    if show_inconsistent_only:
        pipeline.insert(
            2,
            {
                "$match": {
                    "$expr": {
                        "$ne": (
                            ["$cls.count.filtered", "$cls.count.ingested"]
                            if type != BatchDataType.WEBTX_BLOBS
                            else ["$cls.size.filtered", "$cls.size.ingested"]
                        )
                    }
                }
            },
        )
    result = batch_manager.aggregate(pipeline)
    result = list(result)

    return result[0] if result else {"count": 0, "data": []}
