"""Service health check utilities for reusable service status checking."""

import os
import traceback
import warnings
from typing import Dict, Optional, Tuple
import requests
from pymongo import MongoClient
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from netskope.common.utils import Logger
from netskope.common.utils.rabbitmq_helper import make_rabbitmq_api_call
from netskope.common.utils.db_connector import mongo_connection, check_mongo_service

logger = Logger()

UI_PROTOCOL = os.environ.get("UI_PROTOCOL", "http")
UI_PORT = os.environ.get("UI_PORT", "3000")


def check_ui_service(
    node_ip: str, verify: bool = False, timeout: int = 10
) -> Tuple[bool, Optional[str]]:
    """
    Check UI service status for a given node IP.

    Args:
        node_ip: IP address of the node to check
        verify: Whether to verify SSL certificates
        timeout: Request timeout in seconds

    Returns:
        Tuple of (is_active: bool, error_message: Optional[str])
    """
    try:
        ui_url = f"{UI_PROTOCOL}://{node_ip}:{UI_PORT}"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", InsecureRequestWarning)
            proxies = {"http": None, "https": None}
            response = requests.get(
                f"{ui_url}/login", verify=verify, proxies=proxies, timeout=timeout
            )
            if response.status_code == 200:
                return True, None
            return False, f"UI service returned status code {response.status_code}"
    except requests.exceptions.Timeout:
        error_msg = f"Timeout checking UI service for node {node_ip}"
        logger.error(error_msg, error_code="CE_1135")
        return False, error_msg
    except Exception as e:
        error_msg = f"Error checking UI service for node {node_ip}: {str(e)}"
        logger.error(
            error_msg,
            error_code="CE_1129",
            details=traceback.format_exc(),
        )
        return False, error_msg


def check_core_service(
    node_ip: str, verify: bool = False, timeout: int = 5
) -> Tuple[bool, Optional[str]]:
    """
    Check Core service status for a given node IP.

    Args:
        node_ip: IP address of the node to check
        verify: Whether to verify SSL certificates
        timeout: Request timeout in seconds

    Returns:
        Tuple of (is_active: bool, error_message: Optional[str])
    """
    try:
        ui_url = f"{UI_PROTOCOL}://{node_ip}:{UI_PORT}"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", InsecureRequestWarning)
            proxies = {"http": None, "https": None}
            response = requests.get(
                f"{ui_url}/api/healthcheck",
                verify=verify,
                proxies=proxies,
                timeout=timeout,
            )
            if response.status_code == 200:
                return True, None
            return False, f"Core service returned status code {response.status_code}"
    except requests.exceptions.Timeout:
        error_msg = f"Timeout checking Core service for node {node_ip}"
        logger.error(error_msg, error_code="CE_1136")
        return False, error_msg
    except Exception as e:
        error_msg = f"Error checking Core service for node {node_ip}: {str(e)}"
        logger.error(
            error_msg,
            error_code="CE_1128",
            details=traceback.format_exc(),
        )
        return False, error_msg


def check_mongodb_service(node_ip: Optional[str] = None) -> Dict[str, any]:
    """
    Check MongoDB service status.

    Args:
        node_ip: Optional specific node IP to check. If None, checks all nodes in replica set.

    Returns:
        Dictionary mapping node IPs to their MongoDB status information.
        Format: {ip: {"name": str, "status": str, "message": str, "infoMessage": str}}
    """
    status_dict = {}
    try:
        connection_string = (
            os.environ["MONGO_CONNECTION_STRING"]
            .replace("://cteadmin", "://root")
            .replace("/cte?", "/admin?")
        )
        client = MongoClient(connection_string)
        cluster_status = client.admin.command({"replSetGetStatus": 1})

        for instance in cluster_status.get("members", []):
            ip = instance["name"].split(":")[0]
            # If node_ip is specified, only return status for that node
            if node_ip is None or ip == node_ip:
                status_dict[ip] = {
                    "name": instance["name"],
                    "status": instance["stateStr"],
                    "message": instance.get("lastHeartbeatMessage", ""),
                    "infoMessage": instance.get("infoMessage", ""),
                }

        client.close()
    except Exception:
        logger.error(
            "Error occurred while connecting to MongoDB.",
            error_code="CE_1126",
            details=traceback.format_exc(),
        )

    return status_dict


def check_rabbitmq_service(node_ip: Optional[str] = None) -> Dict[str, any]:
    """
    Check RabbitMQ service status.

    Args:
        node_ip: Optional specific node IP to check. If None, checks all nodes.

    Returns:
        Dictionary mapping node IPs to their RabbitMQ status information.
        Format: {ip: {"name": str, "status": str}}
    """
    status_dict = {}
    try:
        cluster_status = make_rabbitmq_api_call("/api/nodes")
        for instance in cluster_status:
            hostname = instance["name"].split("@")[1]
            # If node_ip is specified, only return status for that node
            if node_ip is None or hostname == node_ip:
                status_dict[hostname] = {
                    "name": instance["name"],
                    "status": "Active" if instance["running"] else "Inactive",
                }
    except Exception:
        logger.error(
            "Error occurred while processing the response from RabbitMQ",
            error_code="CE_1127",
            details=traceback.format_exc(),
        )

    return status_dict


def check_node_services(node_ip: str, verify: bool = False) -> Dict[str, any]:
    """
    Check all services (UI, Core, MongoDB, RabbitMQ) for a single node.

    Args:
        node_ip: IP address of the node to check
        verify: Whether to verify SSL certificates

    Returns:
        Dictionary with service statuses:
        {
            "instance": str,
            "ui": {"status": str},
            "core": {"status": str},
            "mongodb": dict,
            "rabbitmq": dict
        }
    """
    node_status = {
        "instance": node_ip,
        "ui": {"status": "Inactive"},
        "core": {"status": "Unknown"},
        "mongodb": {},
        "rabbitmq": {},
    }

    # Check UI service
    ui_active, ui_error = check_ui_service(node_ip, verify=verify, timeout=10)
    if ui_active:
        node_status["ui"] = {"status": "Active"}
        # Only check Core if UI is active
        core_active, core_error = check_core_service(node_ip, verify=verify, timeout=5)
        if core_active:
            node_status["core"] = {"status": "Active"}
        else:
            node_status["core"] = {"status": "Inactive"}
    else:
        # UI is inactive, Core status is Unknown
        node_status["ui"] = {"status": "Inactive"}
        node_status["core"] = {"status": "Unknown"}

    # Check MongoDB status for this node
    mongodb_status = check_mongodb_service(node_ip=node_ip)
    if node_ip in mongodb_status:
        node_status["mongodb"] = mongodb_status[node_ip]

    # Check RabbitMQ status for this node
    rabbitmq_status = check_rabbitmq_service(node_ip=node_ip)
    if node_ip in rabbitmq_status:
        node_status["rabbitmq"] = rabbitmq_status[node_ip]

    return node_status


def check_standalone_services() -> Dict[str, bool]:
    """
    Check services for standalone (non-HA) deployment.

    Returns:
        Dictionary with service statuses:
        {
            "Core": bool,
            "UI": bool,
            "MongoDB": bool,
            "RabbitMQ": bool
        }
    """
    services = {
        "Core": True,  # Assumed active if this function is called
        "UI": True,  # Assumed active if this function is called
        "MongoDB": False,
        "RabbitMQ": False,
    }

    # Check MongoDB
    try:
        _ = mongo_connection(check_mongo_service, os.environ["MONGO_CONNECTION_STRING"])
        services["MongoDB"] = True
    except Exception:
        logger.error(
            "Error occurred while connecting to mongodb.",
            details=traceback.format_exc(),
            error_code="CE_1004",
        )
        services["MongoDB"] = False

    # Check RabbitMQ
    try:
        response = make_rabbitmq_api_call("/api/nodes")
        if isinstance(response, list) and len(response) > 0:
            services["RabbitMQ"] = response[0].get("running", False)
    except Exception:
        logger.error(
            "Error occurred while processing the response from rabbitmq",
            error_code="CE_1042",
            details=traceback.format_exc(),
        )
        services["RabbitMQ"] = False

    return services
