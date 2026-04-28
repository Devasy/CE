"""Containers status related endpoints."""

import os
from datetime import timezone
from fastapi import APIRouter, Response, HTTPException, Security
from netskope.common.utils import Logger, DBConnector, Collections
from netskope.common.utils.disk_free_alarm import check_certs_validity
from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User

from netskope.common.utils.service_health_check import (
    check_node_services,
    check_standalone_services,
)

connector = DBConnector()
router = APIRouter()
logger = Logger()


@router.get(
    "/containers/status",
    tags=["Containers Status"],
    description="Read docker containers status.",
    status_code=200,
)
def read_status(resp: Response):
    """Read docker containers status."""
    check_certs_validity()
    if os.environ.get("HA_IP_LIST"):
        object_cursor = (
            connector.collection(Collections.CLUSTER_HEALTH)
            .find({}, {"_id": 0})
            .sort("_id", -1)
            .limit(1)
        )
        health_records = list(object_cursor)

        if not health_records:
            # No records yet, return empty structure
            return {
                "success": False,
                "serviceStatus": [],
                "message": "No cluster status data available yet.",
            }

        health_record = health_records[0]
        return {"success": False, "serviceStatus": health_record.get("serviceStatus"), "message": "HA is enabled."}

    # Use utility function to check standalone services
    services = check_standalone_services()

    out = {
        "serviceStatus": [
            {"name": "Core", "active": services["Core"]},
            {"name": "UI", "active": services["UI"]},
            {"name": "MongoDB", "active": services["MongoDB"]},
            {"name": "RabbitMQ", "active": services["RabbitMQ"]},
        ],
        # Keeping none value for backword compatibility of UI
        "diskStats": [
            {"name": "Available Disk", "count": None},
            {"name": "Low Watermark", "count": None},
        ],
    }

    # set status code of API to 503 if one of service is down
    for status in out.get("serviceStatus"):
        if not status.get("active"):
            resp.status_code = 503
    return out


@router.get(
    "/cluster/nodes",
    tags=["Cluster Status"],
    description="Get list of cluster nodes quickly without blocking.",
    status_code=200,
)
def get_cluster_nodes(
    user: User = Security(get_current_user, scopes=[]),
):
    """Return list of cluster nodes from HA_IP_LIST environment variable."""
    ha_ip_list = os.environ.get("HA_IP_LIST", "")
    if not ha_ip_list:
        return {"success": False, "nodes": [], "message": "HA is not enabled."}

    nodes = [ip.strip() for ip in ha_ip_list.split(",") if ip.strip()]
    return {"success": True, "nodes": nodes}


@router.get(
    "/cluster_status/node/{node_ip}",
    tags=["Cluster Status"],
    description="Get status for a single node.",
    status_code=200,
)
def get_node_status(node_ip: str, user: User = Security(get_current_user, scopes=[])):
    """Get health status for a single node by IP address."""
    if not os.environ.get("HA_IP_LIST"):
        return {"success": False, "message": "HA is not enabled."}

    # Validate node IP is in HA_IP_LIST
    ha_ip_list = os.environ.get("HA_IP_LIST", "").split(",")
    if node_ip not in [ip.strip() for ip in ha_ip_list]:
        raise HTTPException(
            status_code=400, detail=f"Node IP {node_ip} is not in the HA cluster."
        )

    # Use utility function to check all services for this node
    node_status = check_node_services(node_ip, verify=False)

    return {"success": True, "status": node_status}


@router.get(
    "/cluster_status",
    tags=["Cluster Status"],
    description="Fetch the heartbeat logs from database (cached data).",
    status_code=200,
)
def cluster_status(
    user: User = Security(get_current_user, scopes=[]),
):
    """Read cluster status from the database."""
    if not os.environ.get("HA_IP_LIST"):
        return {"success": False, "message": "HA is not enabled."}

    # Get the latest record from database
    object_cursor = (
        connector.collection(Collections.CLUSTER_HEALTH)
        .find({}, {"_id": 0})
        .sort("_id", -1)
        .limit(1)
    )
    health_records = list(object_cursor)

    if not health_records:
        # No records yet, return empty structure
        return {
            "success": True,
            "status": [],
            "serviceStatus": [],
            "check_time": None,
            "message": "No cluster status data available yet.",
        }

    health_record = health_records[0]
    health_record["check_time"] = (
        health_record["check_time"].replace(tzinfo=timezone.utc).isoformat()
    )

    # set status code of API to 503 if one of service is down
    for status in health_record.get("serviceStatus", []):
        if not status.get("active"):
            raise HTTPException(
                status_code=503,
                detail="Services are currently unavailable. To restore functionality, "
                "majority of the HA nodes need to be operational.",
            )
    return health_record
