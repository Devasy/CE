"""Analytics related task."""

import json
import hashlib
import math
import os
import shutil
import time
import traceback
from datetime import datetime, timedelta
from urllib.parse import urlparse

import re
from packaging.version import Version, InvalidVersion
from packaging.specifiers import SpecifierSet, InvalidSpecifier
import psutil
import requests
from netskope_api.iterator.netskope_iterator import NetskopeIterator
from netskope.common.api import __version__ as CE_VERSION
from netskope.common.models.settings import SettingsDB
from netskope.common.utils.analytics_mappings import (
    HOST_PLATFORM_MAPPING,
    MODULES_MAPPING_NUMBERS,
    OS_MAPPING,
    PLUGINS_STATE_MAPPING,
    REPOSITORY_MAPPING,
)
from netskope.common.utils.disk_free_alarm import (
    get_available_disk_space,
    check_certs_validity,
)
from netskope.common.utils.handle_exception import handle_exception, handle_status_code
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.common.utils.proxy import get_proxy_params
from netskope.common.utils import has_source_info_args

from .. import api
from ..utils import (
    Collections,
    DBConnector,
    Logger,
    add_user_agent,
    get_installation_id,
    track,
)
from .main import APP
from netskope.common.utils.const import MAX_ANALYTICS_LENGTH

PROMOTION_BANNERS_FILE_LOCATION = os.getenv("PROMOTION_BANNERS_FILE_LOCATION")
core_version = api.__version__
ui_version = api.__version__
connector = DBConnector()
logger = Logger()
plugin_helper = PluginHelper()
ALERT = "alert"


def convert_to_hex(data, length=2) -> str:
    """Convert to hex."""
    try:
        data = int(data)
        data = min(data, 16**length - 1)
        data = hex(data)[2:]
        if len(data) < length:
            data = "0" * (length - len(data)) + data
    except ValueError:
        data = "0" * length
    return data


def convert_size(size_bytes, length=2, base=1024):
    """Convert bytes to human readable format."""
    try:
        if size_bytes == 0:
            return "0" * length + "K"
        size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
        if base == 1000:
            size_name = ("A", "K", "M", "B", "T", "Q", "W", "Z", "Y")
        i = int(math.floor(math.log(size_bytes, base)))
        p = math.pow(base, i)
        s = size_bytes // p
        s = convert_to_hex(s, length)
        return "%s%s" % (s, size_name[i])
    except Exception as e:
        logger.debug(f"Error occurred while converting size: {e}")
        return "0" * length + "K"


def convert_email_to_hex(email_id):
    """Convert email to Hex format."""
    try:
        encoded_email = "".join([convert_to_hex(ord(i), 2) for i in email_id[:100]])
        return encoded_email
    except Exception as e:
        logger.debug(f"Error occurred while converting email to hex: {e}")


def get_stack_details() -> dict:
    """Collect stack details."""
    data = {"stack_details": ""}
    try:
        host_platform_type = (
            os.environ.get("PLATFORM_PROVIDER", "custom").strip().strip('"')
        )
        host_platform = HOST_PLATFORM_MAPPING.get(host_platform_type, "f")
        host_os_env = os.environ.get("HOST_OS", "Unknown").strip().strip('"')
        host_os = OS_MAPPING.get(host_os_env, "f")
        cpu = min(psutil.cpu_count(), 255)
        cpu = convert_to_hex(cpu, 2)
        ram = min(psutil.virtual_memory().total / (1024 * 1024 * 1024), 255)
        ram = convert_to_hex(int(ram), 2)
        total_storage = shutil.disk_usage("/var/lib/rabbitmq").total
        total_storage = convert_size(total_storage, length=3)
        free_storage_percentage = get_available_disk_space()
        free_storage_percentage = convert_to_hex(free_storage_percentage, 2)
        analytics_data = f"{host_platform}{host_os}{cpu}{ram}{total_storage}{free_storage_percentage}"
        data["stack_details"] = analytics_data
    except Exception as e:
        logger.debug(f"Error occurred while getting stack details: {e}")
    return data


def get_ce_details() -> dict:
    """Get ce details."""
    data = {"ce_details": ""}
    try:
        ce_as_vm = 1 if os.environ.get("CE_AS_VM", "").lower() == "true" else 0
        ha = 2 if os.environ.get("HA_IP_LIST") else 0
        ce_as_vm_ha = str(ce_as_vm | ha)
        settings = connector.collection(Collections.SETTINGS).find_one({})
        settingdb = SettingsDB(**settings)
        modules = settingdb.platforms
        modules_enabled = 0
        cls_enabled = modules.get("cls", False)
        modules_enabled = modules_enabled | (
            MODULES_MAPPING_NUMBERS["CLS"] if cls_enabled else 0
        )
        cto_enabled = modules.get("itsm", False)
        modules_enabled = modules_enabled | (
            MODULES_MAPPING_NUMBERS["CTO"] if cto_enabled else 0
        )
        cte_enabled = modules.get("cte", False)
        modules_enabled = modules_enabled | (
            MODULES_MAPPING_NUMBERS["CTE"] if cte_enabled else 0
        )
        crev2_enabled = modules.get("cre", False)
        modules_enabled = modules_enabled | (
            MODULES_MAPPING_NUMBERS["CREV2"] if crev2_enabled else 0
        )
        edm_enabled = modules.get("edm", False)
        modules_enabled = modules_enabled | (
            MODULES_MAPPING_NUMBERS["EDM"] if edm_enabled else 0
        )
        cfc_enabled = modules.get("cfc", False)
        modules_enabled = modules_enabled | (
            MODULES_MAPPING_NUMBERS["CFC"] if cfc_enabled else 0
        )
        modules_enabled_hex = convert_to_hex(modules_enabled, length=2)
        proxy_configured = 1 if settingdb.proxy.server else 0
        number_of_tenants = convert_to_hex(
            connector.collection(Collections.NETSKOPE_TENANTS).count_documents({}),
            length=1,
        )
        analytics_data = (
            f"{ce_as_vm_ha}{modules_enabled_hex}{proxy_configured}{number_of_tenants}"
        )
        data["ce_details"] = analytics_data
    except Exception as e:
        logger.debug(f"Failed to get ce details: {e}")
    return data


def get_platform_from_webhook(url: str) -> str:
    """Identify a platform based on its incoming webhook URL structure.

    This function can identify platforms that provide a unique URL for users
    to send data to (e.g., Slack, Discord). It cannot identify platforms
    that require the user to provide their own endpoint URL (e.g., GitHub, Stripe).

    Args:
        url: The webhook URL as a string.

    Returns:
        The name of the identified platform or 'other' if it's not recognized
        or the URL is invalid.
    """
    if not isinstance(url, str) or not url.strip():
        return "other"

    try:
        # Parse the URL to safely access its components
        parsed_url = urlparse(url.lower())
        domain = parsed_url.netloc

        # Slack
        if "hooks.slack.com" in domain:
            return "Slack"

        # Discord
        if "discord.com" in domain:
            return "Discord"

        # Microsoft Teams (handles multiple URL formats)
        if "webhook.office.com" in domain or "outlook.office.com" in domain:
            return "MicrosoftTeams"

        # Zapier
        if "hooks.zapier.com" in domain:
            return "Zapier"

        # Google Chat
        if "chat.googleapis.com" in domain:
            return "GoogleChat"

    except (ValueError, AttributeError):
        # Handle malformed URLs
        return "other"

    return "other"


def update_folder_id(configuration, folder_id):
    """Update folder id."""
    if folder_id == "notifier_itsm":
        platform_name = (
            configuration.get("parameters", {}).get("platform", {}).get("name", "")
        )
        if platform_name:
            platform_name = platform_name.replace(" ", "_")
            folder_id += "_" + platform_name.lower()
    elif folder_id == "webhook_cto":
        webhook_url = (
            configuration.get("parameters", {}).get("params", {}).get("webhook_url", "")
        )
        if webhook_url:
            platform_name = get_platform_from_webhook(webhook_url)
            folder_id += "_" + platform_name.lower()
    return folder_id


def get_active_plugins_data(
    plugin_configurations, is_cls=False, provider=False
) -> list:
    """Get active plugins data."""
    active_plugins_data = []

    try:
        logs_ingested = 0
        bytes_ingested = 0
        addtional_cls_data = {}
        for configuration in plugin_configurations:
            plugin_id = configuration["plugin"]
            plugin = plugin_helper.find_by_id(plugin_id)
            folder_id = plugin_id.split(".")[-2]
            folder_id = update_folder_id(configuration, folder_id)
            repo_name = plugin_id.split(".")[-3]
            repo_id = "f"
            if repo_name in ["Netskope", "Default"]:
                repo_id = REPOSITORY_MAPPING["Default"]
            elif repo_name == "custom_plugins":
                repo_id = REPOSITORY_MAPPING["Custom Plugins"]
            else:
                repo = connector.collection(Collections.PLUGIN_REPOS).find_one(
                    {"name": repo_name}
                )
                if repo and repo.get("url"):
                    if repo["url"].startswith(
                        "https://github.com/netskopeoss/ta_cloud_exchange_plugins"
                    ):
                        repo_id = REPOSITORY_MAPPING.get("Default")
                    elif repo["url"].startswith(
                        "https://github.com/netskopeoss/ta_cloud_exchange_beta_plugins"
                    ):
                        repo_id = REPOSITORY_MAPPING.get("Beta")
                    elif repo["url"].startswith(
                        "https://github.com/crestdatasystems/ta_cloud_exchange_plugins_beta"
                    ):
                        repo_id = REPOSITORY_MAPPING.get("Crest Hotfix 1")
                    elif repo["url"].startswith(
                        "https://github.com/crestdatasystems/ta_cloud_exchange_plugins_hotfix_repo"
                    ):
                        repo_id = REPOSITORY_MAPPING.get("Crest Hotfix 2")
                    else:
                        repo_id = REPOSITORY_MAPPING.get("Custom Repo")

            plugin_version_string = plugin.metadata.get("version", "0.0.0").lower()
            match = re.match(r"(\d+\.\d+\.\d+)", plugin_version_string)
            if match:
                plugin_version = match.group(1)
            else:
                plugin_version = "0.0.0"
            plugin_version_hex = ""
            for number in plugin_version.split("."):
                plugin_version = convert_to_hex(int(number), 1)
                plugin_version_hex += plugin_version
            plugin_version_hex += "b" if "-beta" in plugin_version_string else "r"
            hashed_plugin_id = hashlib.sha256(folder_id.encode()).hexdigest()[:4]
            plugin_state = 0
            if isinstance(configuration.get("lastRunSuccess"), dict):
                last_run_success = configuration.get("lastRunSuccess")
                # CTE and CTO common field
                pull_success = last_run_success.get("pull", False)
                plugin_state = plugin_state | (
                    PLUGINS_STATE_MAPPING["PULL"] if pull_success else 0
                )
                # CTE field
                share_success = last_run_success.get("share", False)
                plugin_state = plugin_state | (
                    PLUGINS_STATE_MAPPING["SHARE"] if share_success else 0
                )
                # CTO fields
                sync_success = last_run_success.get("sync", False)
                plugin_state = plugin_state | (
                    PLUGINS_STATE_MAPPING["SYNC"] if sync_success else 0
                )
                update_success = last_run_success.get("update", False)
                plugin_state = plugin_state | (
                    PLUGINS_STATE_MAPPING["UPDATE"] if update_success else 0
                )
                plugin_state = convert_to_hex(plugin_state, length=1)
            if not provider:
                active_plugins_data.append(
                    f"{repo_id}{hashed_plugin_id}{plugin_version_hex}{plugin_state}"
                )
            else:
                active_plugins_data.append(
                    f"{repo_id}{hashed_plugin_id}{plugin_version_hex}"
                )
            if is_cls:
                logs_ingested += configuration.get("logsIngested", 0)
                addtional_cls_data["is_netskope_cls_enabled"] = True
                bytes_ingested += configuration.get("bytesIngested", 0)
                addtional_cls_data["is_webtx_enabled"] = True
        if is_cls:
            addtional_cls_data["bytes_ingested"] = bytes_ingested
            addtional_cls_data["logs_ingested"] = logs_ingested
            return active_plugins_data, addtional_cls_data
    except Exception as e:
        logger.debug(
            f"Error occured while collecting active plugin details, {e}",
            details=traceback.format_exc(),
        )
    if is_cls:
        return active_plugins_data, addtional_cls_data
    return active_plugins_data


def get_provider_plugins_details() -> dict:
    """Get provider plugins details."""
    data = {"provider": {}}
    try:
        provider_plugins_configurations = connector.collection(
            Collections.NETSKOPE_TENANTS
        ).find({})
        active_plugins_data = get_active_plugins_data(
            provider_plugins_configurations, provider=True
        )
        provider_configurations_count = connector.collection(
            Collections.NETSKOPE_TENANTS
        ).count_documents({})
        analytics_data = convert_to_hex(provider_configurations_count, length=1)
        data["provider"] = {
            "basics": analytics_data,
            "plugins": active_plugins_data,
        }
    except Exception as e:
        logger.debug(f"Failed to get cto details: {e}", details=traceback.format_exc())
    return data


def get_cte_details() -> dict:
    """Get cte details."""
    data = {"cte": {}}
    try:
        active_plugins = connector.collection(
            Collections.CONFIGURATIONS
        ).count_documents({"active": True})
        inactive_plugins = connector.collection(
            Collections.CONFIGURATIONS
        ).count_documents({"active": False})
        plugin_configurations = connector.collection(Collections.CONFIGURATIONS).find(
            {"active": True}
        )
        sharing_configurations = connector.collection(
            Collections.CTE_BUSINESS_RULES
        ).find(
            {"sharedWith": {"$exists": True, "$ne": {}}}, {"sharedWith": 1, "_id": 0}
        )
        sharing_configuration_count = 0
        for sharing_configuration in sharing_configurations:
            for key in sharing_configuration["sharedWith"]:
                sharing_configuration_count += len(
                    sharing_configuration["sharedWith"][key]
                )
        indicators_count = connector.collection(Collections.INDICATORS).count_documents(
            {}
        )
        active_plugins_data = get_active_plugins_data(plugin_configurations)
        analytics_data = (
            convert_to_hex(inactive_plugins, length=1)
            + convert_to_hex(active_plugins, length=1)
            + convert_to_hex(sharing_configuration_count, length=1)
            + convert_size(indicators_count, length=3, base=1000)
        )
        data["cte"] = {"basics": analytics_data, "plugins": active_plugins_data}
    except Exception as e:
        logger.debug(f"Failed to get cto details: {e}", details=traceback.format_exc())
    return data


def get_cto_details() -> dict:
    """Get cto details."""
    data = {"cto": {}}
    try:
        active_plugins = connector.collection(
            Collections.ITSM_CONFIGURATIONS
        ).count_documents({"active": True})
        inactive_plugins = connector.collection(
            Collections.ITSM_CONFIGURATIONS
        ).count_documents({"active": False})
        plugin_configurations = connector.collection(
            Collections.ITSM_CONFIGURATIONS
        ).find({"active": True})
        queues_configurations = connector.collection(
            Collections.ITSM_BUSINESS_RULES
        ).find({"queues": {"$exists": True, "$ne": {}}}, {"queues": 1, "_id": 0})
        queues_configuration_count = 0
        for queues_configuration in queues_configurations:
            queues_configuration_count += len(queues_configuration["queues"])
        tickets_count = connector.collection(Collections.ITSM_TASKS).count_documents({})
        active_plugins_data = get_active_plugins_data(plugin_configurations)
        analytics_data = (
            convert_to_hex(inactive_plugins, length=1)
            + convert_to_hex(active_plugins, length=1)
            + convert_to_hex(queues_configuration_count, length=1)
            + convert_size(tickets_count, length=3, base=1000)
        )
        data["cto"] = {"basics": analytics_data, "plugins": active_plugins_data}
    except Exception as e:
        logger.debug(f"Failed to get cto details: {e}", details=traceback.format_exc())
    return data


def get_cls_details() -> dict:
    """Get cls details."""
    data = {"cls": {}}
    try:
        active_plugins = connector.collection(
            Collections.CLS_CONFIGURATIONS
        ).count_documents({"active": True})
        inactive_plugins = connector.collection(
            Collections.CLS_CONFIGURATIONS
        ).count_documents({"active": False})
        plugin_configurations = connector.collection(
            Collections.CLS_CONFIGURATIONS
        ).find({"active": True})
        siem_mappings = connector.collection(Collections.CLS_BUSINESS_RULES).find(
            {"siemMappings": {"$exists": True, "$ne": {}}},
            {"siemMappings": 1, "_id": 0},
        )
        siem_mapping_counts = 0
        for siem_mapping in siem_mappings:
            for key in siem_mapping["siemMappings"]:
                siem_mapping_counts += len(siem_mapping["siemMappings"][key])
        active_plugins_data, addtional_data = get_active_plugins_data(
            plugin_configurations, is_cls=True
        )
        logs_ingested = addtional_data.get("logs_ingested", 0)
        bytes_ingested = addtional_data.get("bytes_ingested", 0)
        cls_data = (
            convert_to_hex(inactive_plugins, length=1)
            + convert_to_hex(active_plugins, length=1)
            + convert_to_hex(siem_mapping_counts, length=1)
            + convert_size(logs_ingested, length=3, base=1000)
            + convert_size(bytes_ingested, length=3, base=1024)
        )
        data = {"cls": {"basics": cls_data, "plugins": active_plugins_data}}
    except Exception as e:
        logger.debug(f"Failed to get cls details: {e}", details=traceback.format_exc())
    return data


def get_crev2_details() -> dict:
    """Get crev2 details."""
    data = {"cre": {}}
    try:
        inactive_plugins = connector.collection(
            Collections.CREV2_CONFIGURATIONS
        ).count_documents({"active": False})
        active_plugins = connector.collection(
            Collections.CREV2_CONFIGURATIONS
        ).count_documents({"active": True})
        plugin_configurations = connector.collection(
            Collections.CREV2_CONFIGURATIONS
        ).find({"active": True})
        action_configurations = connector.collection(
            Collections.CREV2_BUSINESS_RULES
        ).find({"actions": {"$exists": True, "$ne": {}}}, {"actions": 1, "_id": 0})
        action_configuration_count = 0
        for action_configuration in action_configurations:
            for key in action_configuration["actions"]:
                action_configuration_count += len(action_configuration["actions"][key])
        active_plugins_data = get_active_plugins_data(plugin_configurations)
        entities = connector.collection(Collections.CREV2_ENTITIES).find({})
        total_users_count = 0
        for entity in entities:
            total_users_count += connector.collection(
                Collections.CREV2_ENTITY_PREFIX.value + entity.get("name")
            ).count_documents({})
        analytics_data = (
            convert_to_hex(inactive_plugins, length=1)
            + convert_to_hex(active_plugins, length=1)
            + convert_to_hex(action_configuration_count, length=1)
            + convert_size(total_users_count, length=3, base=1000)
        )
        data["cre"] = {
            "basics": analytics_data,
            "plugins": active_plugins_data,
        }
    except Exception as e:
        logger.debug(f"Failed to get cre details: {e}", details=traceback.format_exc())
    return data


def get_edm_details() -> dict:
    """Get edm details."""
    data = {"edm": {}}
    try:
        inactive_plugins = connector.collection(
            Collections.EDM_CONFIGURATIONS
        ).count_documents({"active": False})
        plugin_configurations = connector.collection(
            Collections.EDM_CONFIGURATIONS
        ).find({"active": True})
        sharing_configurations = connector.collection(
            Collections.EDM_BUSINESS_RULES
        ).count_documents({})
        manual_upload_configurations = connector.collection(
            Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS
        ).count_documents({})

        edm_statistics = connector.collection(Collections.EDM_STATISTICS).find_one({})

        if edm_statistics:
            hashes_shared_configurations = edm_statistics.get("sentHashes", 0)
            hashes_received_configurations = edm_statistics.get("receivedHashes", 0)
        else:
            hashes_shared_configurations = 0
            hashes_received_configurations = 0

        active_plugins_data = get_active_plugins_data(plugin_configurations)

        edm_data = (
            convert_to_hex(inactive_plugins, length=1)
            + convert_to_hex(len(active_plugins_data), length=1)
            + convert_to_hex(sharing_configurations, length=1)
            + convert_to_hex(manual_upload_configurations, length=1)
            + convert_to_hex(hashes_shared_configurations, length=1)
            + convert_to_hex(hashes_received_configurations, length=1)
        )
        data = {"edm": {"basics": edm_data, "plugins": active_plugins_data}}
    except Exception as e:
        logger.debug(f"Failed to get edm details: {e}", details=traceback.format_exc())
    return data


def get_cfc_details() -> dict:
    """Get cfc details."""
    data = {"cfc": {}}
    try:
        inactive_plugins = connector.collection(
            Collections.CFC_CONFIGURATIONS
        ).count_documents({"active": False})
        plugin_configurations = connector.collection(
            Collections.CFC_CONFIGURATIONS
        ).find({"active": True})
        sharing_configurations = connector.collection(
            Collections.CFC_SHARING
        ).count_documents({})
        business_rule_configurations = connector.collection(
            Collections.CFC_BUSINESS_RULES
        ).count_documents({})
        manual_upload_configurations = connector.collection(
            Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
        ).count_documents({})

        cfc_statistics = connector.collection(Collections.CFC_STATISTICS).find_one({})

        if cfc_statistics:
            sent_images = cfc_statistics.get("sentImages", 0)
        else:
            sent_images = 0

        active_plugins_data = get_active_plugins_data(plugin_configurations)

        cfc_data = (
            convert_to_hex(inactive_plugins, length=1)
            + convert_to_hex(len(active_plugins_data), length=1)
            + convert_to_hex(sharing_configurations, length=1)
            + convert_to_hex(business_rule_configurations, length=1)
            + convert_to_hex(sent_images, length=1)
            + convert_to_hex(manual_upload_configurations, length=1)
        )
        data = {"cfc": {"basics": cfc_data, "plugins": active_plugins_data}}
    except Exception as e:
        logger.debug(f"Failed to get cfc details: {e}", details=traceback.format_exc())
    return data


def truncate_plugins_data(analytics: str, analytics_raw: dict):
    """Truncate plugins data."""
    plugins_truncated = False
    plugins_data = "".join(analytics_raw["plugins"])
    while (
        len(analytics) + len("netskope-ce-" + api.__version__) + len(plugins_data)
        > MAX_ANALYTICS_LENGTH
    ):
        plugins_data_popped = None
        expected_length = (
            MAX_ANALYTICS_LENGTH
            - len("netskope-ce-" + api.__version__)
            - len(analytics)
        )
        for plugin_list in [analytics_raw["plugins"]]:
            if plugin_list:
                plugins_data_popped = plugin_list.pop()
                plugins_data = plugins_data.replace(plugins_data_popped, "", 1)
                plugins_truncated = True
                if len(plugins_data) < expected_length:
                    break
        if plugins_data_popped is None:
            break
    return plugins_truncated


def collect_analytics_details() -> dict:
    """Collect analytics details."""
    try:
        analytics_data_dict = {}
        analytics_data = ""
        stack_and_ce_details = {**get_stack_details(), **get_ce_details()}
        settings = connector.collection(Collections.SETTINGS).find_one({})
        current_up_time = settings.get("currentUpTime")
        if current_up_time:
            current_up_time = int(
                datetime.strptime(current_up_time, "%Y-%m-%d %H:%M:%S").timestamp()
            )
        else:
            current_up_time = int(time.time())
        settings = SettingsDB(**settings)
        data = {**get_provider_plugins_details()}
        if settings.platforms.get("cls", False):
            data.update(**get_cls_details())
        if settings.platforms.get("itsm", False):
            data.update(**get_cto_details())
        if settings.platforms.get("cte", False):
            data.update(**get_cte_details())
        if settings.platforms.get("cre", False):
            data.update(**get_crev2_details())
        if settings.platforms.get("edm", False):
            data.update(**get_edm_details())
        if settings.platforms.get("cfc", False):
            data.update(**get_cfc_details())

        for analytics_type, analytics_details in data.items():
            if analytics_type == "provider":
                analytics_data = "-" + "-".join(
                    [
                        stack_and_ce_details["stack_details"],
                        stack_and_ce_details["ce_details"] + "0",
                        analytics_details["basics"],
                    ]
                )
            else:
                analytics_data = "-0-" + analytics_details["basics"]
            plugins_truncated = truncate_plugins_data(analytics_data, analytics_details)
            if analytics_type == "provider":
                analytics_data = "-" + "-".join(
                    [
                        stack_and_ce_details["stack_details"],
                        stack_and_ce_details["ce_details"]
                        + str(int(plugins_truncated)),
                        str(current_up_time),
                        analytics_details["basics"]
                        + "".join(analytics_details["plugins"]),
                    ]
                )
            else:
                analytics_data = (
                    "-"
                    + str(int(plugins_truncated))
                    + "-"
                    + analytics_details["basics"]
                    + "".join(analytics_details["plugins"])
                )

            analytics_data_dict[analytics_type] = analytics_data

        connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"analytics": analytics_data_dict}}
        )
        return analytics_data_dict
    except Exception as e:
        logger.debug(
            f"Failed to collect analytics details: {e}", details=traceback.format_exc()
        )
    return analytics_data_dict


def get_basic_analytics():
    """Get basic analytics."""
    try:
        settings = connector.collection(Collections.SETTINGS).find_one({})
        email_id = ""
        if "emailAddress" in settings:
            email_id = settings["emailAddress"]
        analytics = "-" + get_installation_id() + "-" + convert_email_to_hex(email_id)
        return analytics
    except Exception as e:
        logger.debug(
            f"Failed to get basic analytics: {e}", details=traceback.format_exc()
        )
    return ""


def create_user_agent(analytics):
    """Create User-Agent string.

    Returns:
        str: return user agent as string
    """
    try:
        user_agent_header = add_user_agent()
        updated_ce_version = CE_VERSION.replace("-", "_")
        user_agent_header["User-Agent"] = user_agent_header["User-Agent"].replace(
            CE_VERSION, updated_ce_version
        )
        user_agent = user_agent_header["User-Agent"] + analytics
        return str(user_agent)
    except Exception as e:
        logger.debug(
            f"Failed to create user agent: {e}", details=traceback.format_exc()
        )
        return ""


def call_provider_api(tenant, analytics_type, user_agent):
    """Call provider API."""
    from netskope_api.iterator.const import Const
    from netskope.common.utils import resolve_secret
    from netskope.common.utils.plugin_provider_helper import PluginProviderHelper

    future_time = int((datetime.now() + timedelta(minutes=60)).timestamp())
    provider = PluginProviderHelper().get_provider(tenant.get("name"))
    if not provider:
        logger.debug(
            f"Skipping {analytics_type} analytics sharing for {tenant.get('name')} as"
            " it is not a valid provider plugin."
        )
        return
    is_netskope_tenant = plugin_helper.is_netskope_provider_plugin(
        tenant.get("plugin", "")
    )
    try:
        if has_source_info_args(
            provider,
            "share_analytics_in_user_agent",
            ["tenant_name", "user_agent_analytics", "analytics_type"],
        ):
            provider.share_analytics_in_user_agent(
                tenant.get("name"), user_agent, analytics_type
            )
        else:
            raise NotImplementedError
    except NotImplementedError:
        if not is_netskope_tenant:
            logger.debug(
                (
                    f"Skipping sharing {analytics_type} analytics for {tenant.get('name')} "
                    "as it is not a Netskope Tenant plugin and "
                    "share_analytics_in_user_agent method is not implemented."
                )
            )
            return
        params = {
            Const.NSKP_TOKEN: resolve_secret(tenant["parameters"].get("v2token")),
            Const.NSKP_TENANT_HOSTNAME: tenant["parameters"]
            .get("tenantName")
            .strip()
            .strip("/")
            .removeprefix("https://"),
            Const.NSKP_USER_AGENT: user_agent,
            Const.NSKP_ITERATOR_NAME: f"analytics_{analytics_type}_{tenant.get('name')}".replace(
                " ", ""
            ),
            Const.NSKP_EVENT_TYPE: Const.EVENT_TYPE_ALERT,
            Const.NSKP_ALERT_TYPE: None,
        }
        iterator = NetskopeIterator(params)
        response = iterator.download(future_time)
        if response.status_code == 200:
            logger.info(
                f"{analytics_type.title()} analytics shared successfully for {tenant.get('name')}"
            )
            return True
        else:
            response = handle_status_code(
                response,
                error_code="CE_1044",
                custom_message=(
                    f"Error occurred while sharing {analytics_type} analytics for {tenant.get('name')}"
                    f" in User-Agent with Netskope"
                ),
            )
            return False
    except Exception:
        logger.error(
            f"Error occurred while sharing {analytics_type} analytics for {tenant.get('name')} in User-Agent",
            details=traceback.format_exc(),
        )
        return False


@APP.task(name="common.share_analytics_in_user_agent")
@track()
def share_analytics_in_user_agent():
    """Share User-Agent with Netskope.

    Returns:None
    """
    try:
        from netskope.common.utils.plugin_provider_helper import PluginProviderHelper

        plugin_provider_helper = PluginProviderHelper()
        tenants = plugin_provider_helper.list_tenants()
        analytics = collect_analytics_details()
        analytics["basic"] = get_basic_analytics()

        for tenant in tenants:
            for analytics_type, analytics_details in analytics.items():
                user_agent = create_user_agent(analytics_details)
                call_provider_api(tenant, analytics_type, user_agent)
                time.sleep(1)
    except Exception:
        logger.error(
            "Error occurred while sharing analytics using User-Agent with Netskope",
            details=traceback.format_exc(),
        )


def is_banner_applicable_for_version(ce_versions_spec, current_version):
    """Check if a banner should be displayed for the current CE version.

    Args:
        ce_versions_spec: Version specifier string from the banner JSON (e.g. '>=5.0.0,<7.0.0').
                          If None or empty, the banner applies to all versions.
        current_version:  The running CE version string (e.g. '6.1.0', '7.0.1-beta').

    Returns:
        bool: True if the banner should be displayed, False otherwise.
    """
    if not ce_versions_spec:
        return True

    def normalize_version(ver_str):
        """Normalize a version string, converting beta notation to PEP 440 pre-release."""
        ver_str = ver_str.strip()
        base_match = re.match(r"^(\d+\.\d+\.\d+)", ver_str)
        if base_match and "beta" in ver_str.lower():
            base = base_match.group(1)
            beta_num = re.search(r"beta[.\-]?(\d+)", ver_str.lower())
            return f"{base}b{beta_num.group(1) if beta_num else '0'}"
        return ver_str

    def normalize_specifier(spec_str):
        """Normalize all version strings inside a specifier expression."""
        # Match operator + version pairs, e.g. '>=6.1.0-beta.1'
        return re.sub(
            r"(>=|<=|==|!=|~=|>|<)(\S+?)(?=,|$)",
            lambda m: m.group(1) + normalize_version(m.group(2)),
            spec_str,
        )

    try:
        normalized_version = normalize_version(current_version)
        normalized_spec_str = normalize_specifier(ce_versions_spec)
        spec = SpecifierSet(normalized_spec_str, prereleases=True)
        version = Version(normalized_version)
        return version in spec
    except (InvalidSpecifier, InvalidVersion, Exception) as e:
        logger.info(
            f"Invalid ce_versions specifier '{ce_versions_spec}' in promotional banner: {e}. "
            "Defaulting to showing the banner for all versions."
        )
        return True


def pull_cloud_exchange_banners():
    """Get banners from GitHub.

    Raises:
        response: Github Connectivy errors.
    """
    try:
        if not PROMOTION_BANNERS_FILE_LOCATION:
            logger.error(
                "Error occurred while getting file location for promotion banners."
            )
            return
        settings = connector.collection(Collections.SETTINGS).find_one({})
        settingdb = SettingsDB(**settings)

        success, response = handle_exception(
            requests.get,
            error_code="CE_1045",
            custom_message="Unable to pull promotion banners from Github",
            url=PROMOTION_BANNERS_FILE_LOCATION,
            log_level="info",
            proxies=get_proxy_params(settingdb),
        )
        if not success:
            raise response
        if response.status_code == 404:
            logger.info("No promotional banners are available on GitHub to pull.")
            return
        else:
            response = handle_status_code(
                response,
                error_code="CE_1046",
                custom_message="Unable to pull promotion banners from Github",
                log_level="info",
            )
        if isinstance(response, bytes):
            response = json.loads(response)
        list_of_banner_ids = []
        for banner in response:
            is_applicable_to_ce = is_banner_applicable_for_version(
                banner.get("ce_versions"), CE_VERSION
            )
            list_of_banner_ids.append(banner.get("id"))
            already_exist_banner = connector.collection(
                Collections.NOTIFICATIONS
            ).find_one({"id": banner.get("id")})

            if not is_applicable_to_ce:
                if not already_exist_banner:
                    # Not applicable to this CE version and not in DB — skip entirely.
                    logger.info(f"Banner {banner.get('id')} is not applicable to this CE version.")
                    continue
                else:
                    # Not applicable but already exists — just mark it acknowledged.
                    connector.collection(Collections.NOTIFICATIONS).update_one(
                        {"id": banner.get("id")},
                        {"$set": {"acknowledged": True}},
                    )
                    continue

            # Banner IS applicable to this CE version — upsert with full fields.
            connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": banner.get("id")},
                {
                    "$set": {
                        "id": banner.get("id"),
                        "message": banner.get("message"),
                        "type": banner.get("type"),
                        "acknowledged": (
                            False
                            if not already_exist_banner
                            else already_exist_banner.get("acknowledged", False)
                        ),
                        "createdAt": datetime.now(),
                        "is_promotion": True,
                    },
                },
                upsert=True,
            )
        connector.collection(Collections.NOTIFICATIONS).delete_many(
            {"is_promotion": True, "id": {"$nin": list_of_banner_ids}}
        )
    except Exception as e:
        logger.debug(f"Unable to pull promotion banners from Github: {e}")


@APP.task(name="common.share_usage_analytics")
@track()
def share_usage_analytics():
    """Share usage analytics with Netskope.

    Returns:
        dict: Dictionary with success result.
    """
    pull_cloud_exchange_banners()
    check_certs_validity()
    return {"success": True}
