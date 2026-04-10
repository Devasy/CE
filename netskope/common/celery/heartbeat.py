"""Heartbeat Logs for Monitoring the Status of CE Services."""

import math
import os
import socket
import traceback
import warnings
from datetime import UTC, datetime

import requests
from pymongo import MongoClient
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.util.retry import Retry

from netskope.common.celery.analytics import collect_analytics_details
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    track,
)
from netskope.common.utils.back_pressure import back_pressure_mechanism
from netskope.common.utils.db_connector import check_mongo_service, mongo_connection
from netskope.common.utils.handle_exception import (
    handle_exception,
    handle_status_code,
)
from netskope.common.utils.rabbitmq_helper import make_rabbitmq_api_call

from .main import APP

UI_SERVICE_NAME = os.environ.get("UI_SERVICE_NAME", "ui")
UI_PROTOCOL = os.environ.get("UI_PROTOCOL", "http")

logger = Logger()
connector = DBConnector()
UI_SERVICE_NAME = os.environ.get("UI_SERVICE_NAME", "ui")


@APP.task(name="common.heartbeat")
@track()
def heartbeat_logs():
    """Heartbeat logs for CE."""
    back_pressure_mechanism(True)

    # Core
    # Works Fine. If this method is called.
    # MongoDB
    collect_analytics_details()
    if os.environ.get("HA_IP_LIST"):
        return get_cluster_stats()

    try:
        _ = mongo_connection(check_mongo_service, os.environ["MONGO_CONNECTION_STRING"])
    except Exception:
        logger.error(
            "Error occurred while connecting to mongodb.",
            details=traceback.format_exc(),
            error_code="CE_1084",
        )
        # Not able to connect with MongoDB.
        return {"success": False}

    # RabbitMQ
    try:
        response = make_rabbitmq_api_call("/api/nodes")
        if isinstance(response, list) and len(response) and not response[0].get("running", False):
            return {"success": False}
    except Exception:
        logger.error(
            "Error occurred while processing the response from rabbitmq",
            error_code="CE_1086",
            details=traceback.format_exc(),
        )
        # Not able to connect with RabbitMQ.
        return {"success": False}

    # UI
    ui_status = True
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((UI_SERVICE_NAME, 3000))
        if result != 0:
            ui_status = False
    except Exception:
        logger.error(
            "Could not verify Port information of the machine",
            error_code="CE_1083",
            details=traceback.format_exc(),
        )
        ui_status = False
    if ui_status:
        logger.info("UI, MongoDB, Core and RabbitMQ containers are in active state.")
    else:
        logger.error(
            "MongoDB, Core and RabbitMQ containers are in active state. UI container is down.",
            error_code="CE_1122",
        )
    try:
        params = {"skip_cluster": True}
        timestamp = datetime.now(UTC)
        system_stats = make_management_server_call(endpoint="/system-stats", params=params, require_auth=False)
        connector.collection(Collections.CLUSTER_HEALTH).insert_one(
            {
                "check_time": timestamp,
                "systemStats": {"localhost": system_stats},
            }
        )
    except Exception:
        logger.error(
            "Error occurred while processing response from management server.",
            error_code="CE_1042",
            details=traceback.format_exc(),
        )
    return {"success": True}


def prepare_string(down_list):
    """Prepare a service status string."""
    if len(down_list) == 1:
        return f"{down_list[0]} service is down"
    string = down_list[0]
    for i in range(1, len(down_list) - 1):
        string += ", " + down_list[i]
    string = f"{string} and {down_list[-1]}"
    string = f"{string} services are down"
    return string


def make_management_server_call(endpoint=None, token=None, params=None, payload=None, require_auth=True):
    """
    Make a call to the management server.

    Args:
        endpoint (str, optional): API endpoint to call. Defaults to None.
        token (str, optional): Authentication token. Defaults to None.
        params (dict, optional): Query parameters. Defaults to None.
        payload (dict, optional): Request payload. Defaults to None.
        require_auth (bool, optional): Whether authentication is required. Defaults to True.

    Returns:
        dict: Response from the management server.
    """
    url = f"{UI_PROTOCOL}://{UI_SERVICE_NAME}:3000/api/management{endpoint}"
    proxies = {
        "http": None,
        "https": None,
    }
    if not token and require_auth:
        logger.error("Could not connect to Management server. Token not found.")
        return False

    if require_auth:
        headers = {"Authorization": f"Bearer {token}"}
    else:
        headers = None
    if not (params and isinstance(params, dict)):
        params = None

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.1)
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))  # NOSONAR

    success, response = handle_exception(
        session.get,
        custom_message="Error encountered while fetching system statistics from management server.",
        log_level="error",
        url=url,
        headers=headers,
        proxies=proxies,
        timeout=30,
        verify=False,
        params=params,
        json=payload,
    )
    if not success:
        raise response

    response = handle_status_code(
        response,
        custom_message="Error encountered while fetching system statistics from management server.",
        log_level="error",
        log=True,
    )
    return response


# Health check for HA environment.
def get_cluster_stats(verify=False):
    """Cluster Stats for HA."""
    status_list = {
        "mongodb": dict(),
        "rabbitmq": dict(),
        "core": dict(),
        "ui": dict(),
    }

    host_list = os.environ.get("HA_IP_LIST", "").split(",")
    timestamp = datetime.now(UTC)
    try:
        # Use root user and admin database. Cluster status will not be accessible.
        connection_string = (
            os.environ["MONGO_CONNECTION_STRING"].replace("://cteadmin", "://root").replace("/cte?", "/admin?")
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

    try:
        cluster_status = make_rabbitmq_api_call("/api/nodes")
        for instance in cluster_status:
            hostname = instance["name"].split("@")[1]
            status_list["rabbitmq"][hostname] = {
                "name": instance["name"],
                "status": "Active" if instance["running"] else "Inactive",
            }
    except Exception:
        logger.error(
            "Error occurred while processing the response from RabbitMQ",
            error_code="CE_1127",
            details=traceback.format_exc(),
        )

    with warnings.catch_warnings():
        proxies = {"http": None, "https": None}
        warnings.simplefilter("ignore", InsecureRequestWarning)
        for ip in host_list:
            try:
                ui_url = f"{UI_PROTOCOL}://{ip}:{os.environ['UI_PORT']}"
                response = requests.get(f"{ui_url}/login", verify=verify, proxies=proxies)
                if response.status_code != 200:
                    status_list["ui"][ip] = {"status": "Inactive"}
                    status_list["core"][ip] = {"status": "Unknown"}
                    response.raise_for_status()
                status_list["ui"][ip] = {"status": "Active"}

                try:
                    response = requests.get(f"{ui_url}/api/healthcheck", verify=verify, proxies=proxies)
                    if response.status_code != 200:
                        status_list["core"][ip] = {"status": "Inactive"}
                        response.raise_for_status()
                    status_list["core"][ip] = {"status": "Active"}
                except Exception:
                    logger.error(
                        f"Error occurred while checking the CORE status for '{ip}' node",
                        error_code="CE_1128",
                        details=traceback.format_exc(),
                    )
                    status_list["core"][ip] = {"status": "Inactive"}
            except Exception:
                status_list["ui"][ip] = {"status": "Inactive"}
                status_list["core"][ip] = {"status": "Unknown"}
                logger.error(
                    f"Error occurred while checking the UI status for '{ip}' node",
                    error_code="CE_1129",
                    details=traceback.format_exc(),
                )

    total = len(status_list["mongodb"])

    mongo_count = 0
    for stat in status_list["mongodb"].values():
        if stat.get("status", "").upper() in ["PRIMARY", "SECONDARY"]:
            mongo_count += 1

    rmq_count = 0
    for stat in status_list["rabbitmq"].values():
        if stat.get("status", "") == "Active":
            rmq_count += 1

    ui_count = 0
    for stat in status_list["ui"].values():
        if stat.get("status", "") == "Active":
            ui_count += 1

    core_count = 0
    for stat in status_list["core"].values():
        if stat.get("status", "") == "Active":
            core_count += 1

    log_string = (
        "UI({1}/{0}), MongoDB({2}/{0}), Core({3}/{0}) and RabbitMQ({4}/{0}) containers are in active state.".format(
            total, ui_count, mongo_count, core_count, rmq_count
        )
    )

    service_status = [
        {"name": "Core", "active": True if core_count > 0 else False},
        {"name": "UI", "active": True if ui_count > 0 else False},
        {"name": "MongoDB", "active": True if mongo_count >= math.floor(total / 2 + 1) else False},
        {"name": "RabbitMQ", "active": True if rmq_count >= math.floor(total / 2 + 1) else False},
    ]

    transformed = {}
    for key, value in status_list.items():
        for instance in value:
            if instance in transformed:
                transformed[instance].update({key: value[instance]})
            else:
                transformed[instance] = {"instance": instance, key: value[instance]}

    for instance, value in transformed.items():
        down_list = []
        if value.get("mongodb", {}).get("status", "").upper() not in ["PRIMARY", "SECONDARY"]:
            down_list.append("MongoDB")

        if value.get("rabbitmq", {}).get("status", "") != "Active":
            down_list.append("RabbitMQ")
        if value.get("core", {}).get("status", "") != "Active":
            down_list.append("Core")
        if value.get("ui", {}).get("status", "") != "Active":
            down_list.append("UI")

        if down_list:
            string = f"{prepare_string(down_list)} for node {instance}."
            logger.info(string)

    system_stats = {}
    params = {"skip_cluster": False}
    try:
        system_stats = make_management_server_call(params=params, endpoint="/system-stats", require_auth=False)
    except Exception:
        logger.error(
            "Error occurred while processing response from management server.",
            error_code="CE_1042",
            details=traceback.format_exc(),
        )
    connector.collection(Collections.CLUSTER_HEALTH).insert_one(
        {
            "check_time": timestamp,
            "status": list(transformed.values()),
            "serviceStatus": service_status,
            "systemStats": system_stats,
        }
    )

    logger.info(log_string)
    return {"success": True}
