"""Containers status related endpoints."""

import os
import traceback
from datetime import datetime
from pytz import UTC
from copy import deepcopy


from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from netskope.common.utils import Collections, DBConnector, Logger
from netskope.common.utils.db_connector import check_mongo_service, mongo_connection
from netskope.common.utils.rabbitmq_helper import make_rabbitmq_api_call

logger = Logger()
connector = DBConnector()
router = APIRouter()


@router.get(
    "/monitoring",
    tags=["Cloud Exchange monitoring stats"],
    description="Cloud Exchange monitoring stats.",
    status_code=200,
)
def get_status():
    """Cloud Exchange monitoring stats."""
    status_list = {
        "mongodb": dict(),
        "rabbitmq": dict(),
    }
    if os.environ.get("HA_IP_LIST"):
        try:
            # Use root user and admin database. Cluster status will not be accessible.
            connection_string = (
                os.environ["MONGO_CONNECTION_STRING"]
                .replace("://cteadmin", "://root")
                .replace("/cte?", "/admin?")
            )
            client = MongoClient(connection_string)
            cluster_status = client.admin.command({"replSetGetStatus": 1})
            for instance in cluster_status.get("members", []):
                ip = instance["name"].split(":")[0]
                status_list["mongodb"][ip] = {
                    "name": instance["name"],
                    "status": instance["stateStr"],
                    "message": instance["lastHeartbeatMessage"],
                    "infoMessage": instance["infoMessage"],
                }
            client.close()
        except Exception:
            logger.error(
                "Error occurred while connecting to MongoDB.",
                error_code="CE_1126",
                details=traceback.format_exc(),
            )
    else:
        status_list["mongodb"]["mongodb-primary"] = dict()
        try:
            _ = mongo_connection(
                check_mongo_service, os.environ["MONGO_CONNECTION_STRING"]
            )
            status_list["mongodb"]["mongodb-primary"]["status"] = True
        except Exception:
            logger.error(
                "Error occurred while connecting to mongodb.",
                details=traceback.format_exc(),
                error_code="CE_1084",
            )
            # Not able to connect with MongoDB.
            status_list["mongodb"]["mongodb-primary"]["status"] = False
            status_list["mongodb"]["mongodb-primary"][
                "error"
            ] = "Error occurred while connecting to mongodb."

    try:
        queue_stats = make_rabbitmq_api_call(
            "/api/queues/%2F/?page=1&page_size=500&name=cloudexchange_%5B369%5D&"
            "columns=messages_unacknowledged,messages_ready,message_bytes_unacknowledged,message_bytes_ready"
            ",name,leader,members,messages,state,memory&use_regex=true"
        )
        status_list["rabbitmq"]["queues"] = []
        for instance in queue_stats.get("items", []):
            status_list["rabbitmq"]["queues"].append(
                {
                    "name": instance.get("name", "unknown"),
                    "leader": instance.get("leader", "unknown"),
                    "members": instance.get("members", [instance.get("name", "unknown"),]),
                    "queue_messages": instance.get("messages", 0),
                    "queue_messages_ready": instance.get("messages_ready", 0),
                    "queue_messages_unacknowledged": instance.get(
                        "messages_unacknowledged", 0
                    ),
                    "memory": instance.get("memory", 0),
                    "message_bytes_ready": instance.get("message_bytes_ready", 0),
                    "message_bytes_unacknowledged": instance.get(
                        "message_bytes_unacknowledged", 0
                    ),
                    "state": instance.get("state", "unknown"),
                }
            )
    except Exception:
        logger.error(
            "Error occurred while processing the response from RabbitMQ",
            error_code="CE_1127",
            details=traceback.format_exc(),
        )
    return JSONResponse(content=status_list, status_code=200)


@router.get(
    "/system-monitoring",
    tags=["Cloud Exchange monitoring stats"],
    description="Machine monitoring stats",
    status_code=200,
)
def get_system_stats(node: str = None):
    """Machine monitoring stats.

    Args:
        node (str, optional): nodes to query for, incase of HA, it can be comma separated node list. Defaults to None.

    Returns:
        list: list of system stats response fetched from database.
    """
    if node:
        node_list = [node.strip() for node in node.split(",")]
    else:
        node_list = ["localhost"]
    try:
        pipeline = [
            {
                "$match": {
                    "systemStats": {"$exists": True, "$ne": {}, "$type": "object"}
                }
            },
            {"$sort": {"check_time": -1}},
            {
                "$project": {
                    "check_time": 1,
                    "_id": 0,
                    "systemStats": {
                        "$arrayToObject": {
                            "$filter": {
                                "input": {"$objectToArray": "$systemStats"},
                                "as": "kv",
                                "cond": {"$in": ["$$kv.k", node_list]},
                            }
                        }
                    },
                }
            },
            {"$match": {"systemStats": {"$ne": {}}}},
        ]

        response = {
            "cpu": {
                "processors": "",
                "load_avg_1min_percentage": [],
                "load_avg_5min_percentage": [],
                "load_avg_15min_percentage": [],
                "timestamps": [],
            },
            "memory": {
                "total_GB": "",
                "used_GB": [],
                "percent": [],
                "timestamps": [],
            },
            "disk": {
                "total_GB": "",
                "used_GB": [],
                "available_GB": [],
                "percent_used": [],
                "timestamps": [],
            },
        }
        node_response = {}
        datapoints = list(
            connector.collection(Collections.CLUSTER_HEALTH).aggregate(pipeline)
        )
        datapoints.reverse()  # oldest first order.
        for datapoint in datapoints:
            timestamp = datapoint.get("check_time")
            if timestamp is None:
                timestamp = datetime.now(UTC)
            elif timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)

            system_stats = datapoint.get("systemStats", {})
            if not system_stats or (not isinstance(system_stats, dict)):
                continue
            for node, node_data in system_stats.items():
                if not isinstance(node_data, dict):
                    continue
                if not node_response.get(node):
                    node_response[node] = deepcopy(response)
                cpu = node_data.get("cpu")
                if isinstance(cpu, dict):
                    node_response[node]["cpu"]["load_avg_1min_percentage"].append(
                        cpu.get("load_avg_1min_percentage")
                    )
                    node_response[node]["cpu"]["load_avg_5min_percentage"].append(
                        cpu.get("load_avg_5min_percentage")
                    )
                    node_response[node]["cpu"]["load_avg_15min_percentage"].append(
                        cpu.get("load_avg_15min_percentage")
                    )
                    if (
                        not node_response[node]["cpu"]["processors"]
                        and cpu.get("processors") is not None
                    ):
                        node_response[node]["cpu"]["processors"] = cpu["processors"]
                    node_response[node]["cpu"]["timestamps"].append(timestamp)

                memory = node_data.get("memory")
                if isinstance(memory, dict):
                    node_response[node]["memory"]["used_GB"].append(
                        memory.get("used_GB")
                    )
                    node_response[node]["memory"]["percent"].append(
                        memory.get("percent")
                    )
                    if (
                        not node_response[node]["memory"]["total_GB"]
                        and memory.get("total_GB") is not None
                    ):
                        node_response[node]["memory"]["total_GB"] = memory["total_GB"]
                    node_response[node]["memory"]["timestamps"].append(timestamp)

                disk = node_data.get("disk")
                if isinstance(disk, dict):
                    node_response[node]["disk"]["used_GB"].append(disk.get("used_GB"))
                    node_response[node]["disk"]["available_GB"].append(
                        disk.get("available_GB")
                    )
                    node_response[node]["disk"]["percent_used"].append(
                        disk.get("percent_used")
                    )
                    if (
                        not node_response[node]["disk"]["total_GB"]
                        and disk.get("total_GB") is not None
                    ):
                        node_response[node]["disk"]["total_GB"] = disk["total_GB"]
                    node_response[node]["disk"]["timestamps"].append(timestamp)

        return node_response
    except OperationFailure as ex:
        raise HTTPException(400, f"{ex}")
    except Exception:
        logger.error(
            "Error occurred while processing the query.",
            details=traceback.format_exc(),
            error_code="CE_1001",
        )
        raise HTTPException(400, "Error occurred while processing the query.")


@router.get(
    "/ce-details",
    tags=["Cloud Exchange monitoring stats"],
    description="Cloud Exchange deployment details",
    status_code=200,
)
def get_ce_details():
    """Cloud Exchange deployment details.

    Returns:
        dict: Cloud Exchange deployment details.
    """
    return {
        "Platform Provider": os.environ.get("PLATFORM_PROVIDER", "custom")
        .strip()
        .strip('"'),
        "Host OS": os.environ.get("HOST_OS", "Unknown").strip().strip('"'),
        "Deployment Type": "HA" if os.environ.get("HA_IP_LIST") else "Standalone",
        "Flavor": (
            "VM"
            if os.environ.get("CE_AS_VM", "False").strip().strip('"').lower() == "true"
            else "Container"
        ),
    }
