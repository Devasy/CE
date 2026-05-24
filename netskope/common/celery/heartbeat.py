"""Heartbeat Logs for Monitoring the Status of CE Services."""

import math
import os
import socket
import traceback
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import requests
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
from netskope.common.utils.service_health_check import (
    check_mongodb_service,
    check_rabbitmq_service,
    check_node_services,
)

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
        if (
            isinstance(response, list)
            and len(response)
            and not response[0].get("running", False)
        ):
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
        system_stats = make_management_server_call(
            endpoint="/system-stats", params=params, require_auth=False
        )
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
            resolution="The Management Server is not reachable, please verify the following steps:\n Step 1: Check if the Management Server Service is Running by executing below mentioned command. If it's not running, try re-running the setup process to start it again.\n$ systemctl status cloud-exchange\n Step 2: Ensure that port 8000 is allowed in the firewall, as the Management Server runs on this port.",  # noqa
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


def make_management_server_call(
    endpoint=None, token=None, params=None, payload=None, require_auth=True
):
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

    # Get MongoDB status for all nodes using utility function
    mongodb_status = check_mongodb_service()
    status_list["mongodb"] = mongodb_status

    # Get RabbitMQ status for all nodes using utility function
    rabbitmq_status = check_rabbitmq_service()
    status_list["rabbitmq"] = rabbitmq_status

    def check_node_health(ip):
        """Check health status for a single node using utility function."""
        node_status_dict = check_node_services(ip, verify=verify)
        return ip, {"ui": node_status_dict["ui"], "core": node_status_dict["core"]}

    # Parallel execution of node health checks
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", InsecureRequestWarning)
        # Use ThreadPoolExecutor for parallel node health checks
        # Max workers set to number of nodes, but cap at reasonable limit
        max_workers = min(len(host_list), 10)  # Cap at 10 concurrent requests
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all node health check tasks
            future_to_ip = {
                executor.submit(check_node_health, ip): ip
                for ip in host_list
                if ip.strip()
            }

            # Collect results as they complete
            for future in as_completed(future_to_ip):
                ip = future_to_ip[future]
                try:
                    result_ip, node_status = future.result()
                    status_list["ui"][result_ip] = node_status["ui"]
                    status_list["core"][result_ip] = node_status["core"]
                except Exception as exc:
                    logger.error(
                        f"Node health check generated an exception for '{ip}': {exc}",
                        error_code="CE_1130",
                        details=traceback.format_exc(),
                    )
                    status_list["ui"][ip] = {"status": "Inactive"}
                    status_list["core"][ip] = {"status": "Unknown"}

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
        "UI({1}/{0}), MongoDB({2}/{0}), Core({3}/{0}) and RabbitMQ({4}/{0}) containers are in active state."
    ).format(total, ui_count, mongo_count, core_count, rmq_count)

    service_status = [
        {"name": "Core", "active": True if core_count > 0 else False},
        {"name": "UI", "active": True if ui_count > 0 else False},
        {
            "name": "MongoDB",
            "active": True if mongo_count >= math.floor(total / 2 + 1) else False,
        },
        {
            "name": "RabbitMQ",
            "active": True if rmq_count >= math.floor(total / 2 + 1) else False,
        },
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
        if value.get("mongodb", {}).get("status", "").upper() not in [
            "PRIMARY",
            "SECONDARY",
        ]:
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
        system_stats = make_management_server_call(
            params=params, endpoint="/system-stats", require_auth=False
        )
    except Exception:
        logger.error(
            "Error occurred while processing response from management server.",
            error_code="CE_1042",
            details=traceback.format_exc(),
            resolution="The Management Server is not reachable, please verify the following steps:\n Step 1: Check if the Management Server Service is Running by executing below mentioned command. If it's not running, try re-running the setup process to start it again.\n$ systemctl status cloud-exchange\n Step 2: Ensure that port 8000 is allowed in the firewall, as the Management Server runs on this port.",  # noqa
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
