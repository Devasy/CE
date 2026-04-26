"""Containers status related endpoints."""
import os
import traceback
from datetime import timezone
from fastapi import APIRouter, Response, HTTPException
from netskope.common.utils import Logger, DBConnector, Collections
from netskope.common.utils.db_connector import mongo_connection, check_mongo_service
from netskope.common.utils.rabbitmq_helper import make_rabbitmq_api_call
from netskope.common.utils.disk_free_alarm import check_certs_validity
from netskope.common.celery.heartbeat import get_cluster_stats


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
        return {"success": False, "serviceStatus": [], "message": "HA is enabled."}

    out = {
        "serviceStatus": [
            {"name": "Core", "active": True},
            {"name": "UI", "active": True},
        ],
        # Keeping none value for backword compatibility of UI
        "diskStats": [
            {"name": "Available Disk", "count": None},
            {"name": "Low Watermark", "count": None},
        ],
    }
    try:
        _ = mongo_connection(check_mongo_service, os.environ["MONGO_CONNECTION_STRING"])
        out["serviceStatus"].append({"name": "MongoDB", "active": True})
    except Exception:
        logger.error(
            "Error occurred while connecting to mongodb.",
            details=traceback.format_exc(),
            error_code="CE_1004",
        )
        out["serviceStatus"].append({"name": "MongoDB", "active": False})
    try:
        response = make_rabbitmq_api_call("/api/nodes")
        response = response[0]
        if response["running"] is True:
            out["serviceStatus"].append({"name": "RabbitMQ", "active": True})
    except Exception:
        logger.error(
            "Error occurred while processing the response from rabbitmq",
            error_code="CE_1042",
            details=traceback.format_exc(),
        )
        out["serviceStatus"].append({"name": "RabbitMQ", "active": False})
    # set status code of API to 503 if one of service is down
    for status in out.get("serviceStatus"):
        if not status.get("active"):
            resp.status_code = 503
    return out


@router.get(
    "/cluster_status",
    tags=["Cluster Status"],
    description="Fetch the heartbeat logs from database.",
    status_code=200,
)
def cluster_status():
    """Read cluster status from the database."""
    if not os.environ.get("HA_IP_LIST"):
        return {"success": False, "message": "HA is not enabled."}
    get_cluster_stats()
    object_cursor = connector.collection(Collections.CLUSTER_HEALTH).find({}, {'_id': 0}).sort('_id', -1).limit(1)

    health_record = list(object_cursor)[0]
    health_record["check_time"] = health_record["check_time"].replace(tzinfo=timezone.utc).isoformat()

    # set status code of API to 503 if one of service is down
    for status in health_record.get("serviceStatus"):
        if not status.get("active"):
            raise HTTPException(
                status_code=503,
                detail="Services are currently unavailable. To restore functionality, "
                       "majority of the HA nodes need to be operational."
            )
    return health_record
