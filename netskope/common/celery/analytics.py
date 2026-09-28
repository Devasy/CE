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
from netskope.common.models.ai_copilot.ai_usage import AIFeature
from netskope.common.utils.analytics_mappings import (
    AI_DOWN_REASON_ORDER,
    AI_EFFORT_MAPPING,
    AI_ERROR_BUCKETS,
    AI_FINDING_KIND_ORDER,
    AI_FINDING_MODULE_NUMBERS,
    AI_MODEL_MAPPING,
    AI_PROVIDER_MAPPING,
    HOST_PLATFORM_MAPPING,
    MODULES_MAPPING_NUMBERS,
    OS_MAPPING,
    PLUGINS_STATE_MAPPING,
    REPOSITORY_MAPPING,
    PLUGIN_STATS,
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
    plugin_id_to_provider,
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
            else:
                plugin_state = PLUGIN_STATS.get(configuration.get("lastRunSuccess"))
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


# --------------------------------------------------------------------------------------
# AI Copilot analytics — the "ai" analytics type.
#
#   -{T}-{V}-{P}-{G}-{L}-{C}-{A}-{N}
#     T  truncation flag: 1 = provider blocks were dropped, by EITHER limit on segment P
#        (the _AI_MAX_PROVIDER_BLOCKS cap or the 255-char fit)
#     V  format version
#     P  provider config     2 header chars + 4 per configured provider (active first)
#     G  copilot sessions    3
#     L  log analyzer        27 core + 5 ext
#     C  copilot bot         27 core + 30 ext
#     A  automapper          27 core + 3 ext
#     N  proactive findings  26
#
# Groups are '-' separated so a field added to one feature's extension never shifts
# another group's offsets. Within a group the fields are FIXED-WIDTH and POSITIONAL:
# widening, reordering or dropping one silently reinterprets every historical report, so
# treat the layout and the AI_* mapping tables as a wire format, not an internal detail.
# --------------------------------------------------------------------------------------
AI_ANALYTICS_VERSION = "1"

# Feature key the CRE auto-mapper stamps on its AIUsageRecord. Must match that value
# exactly — segment A is looked up by this string, so a mismatch reports the feature as
# entirely unused rather than failing.
AI_FEATURE_AUTOMAPPER = "cre_auto_mapper"

# Ceiling on the repeating provider blocks in segment P. This only stops a deployment with
# a long tail of provider configs from crowding the rest of the payload out of the 255-char
# cap; hitting it raises the truncation flag exactly like the length-driven drop does, so a
# decoder never has to infer a shortfall from P1 vs the block count (and cannot be misled
# by P1 saturating at 'f' once 16+ configs exist).
_AI_MAX_PROVIDER_BLOCKS = 8


def _ai_pct(numerator: int, denominator: int) -> str:
    """Percentage as 2 hex chars (00-64). 'ff' means n/a — nothing to divide by.

    'ff' is distinct from '00' so an empty denominator cannot be read as a real 0%.
    """
    if not denominator:
        return "ff"
    return convert_to_hex(round(100 * numerator / denominator), 2)


def _ai_count3(value: int) -> str:
    """Accumulating counter as 3 chars — EXACT 0-4095, 'fff' meaning 4095+.

    convert_to_hex, NOT convert_size(n, 2, base=1000): a 2-digit mantissa against a
    1000-wide decade saturates for every value in 256-999 (they all encode as 'ffA'),
    so a deployment sitting in that band reports an unchanging number and its
    day-over-day delta reads as ZERO. An exact counter keeps deltas correct across the
    whole realistic range, in the same 3 characters.
    """
    return convert_to_hex(value, 3)


def _ai_tokens(value: int) -> str:
    """Encode a token total as 7 chars — EXACT thousands, 'fffffff' meaning 268B+.

    Not convert_size: that helper computes ``n // 1000**i``, so its mantissa never
    exceeds 999 no matter how wide it is. A total gets 1-3 significant decimal digits,
    and only ONE right after a decade boundary — 1,000,000 and 1,999,999 both encode as
    '001M', a 2x uncertainty on the number cost analysis is built on.

    Exact thousands gives 1,000-token resolution (0.1% at a million) across a 268-billion
    range, well beyond what a 365-day retention window accumulates. Totals under 1,000
    tokens floor to 0, which a single turn exceeds.
    """
    return convert_to_hex(value // 1000, 7)


def _ai_provider_segment(configured: int) -> tuple:
    """Build segment P as ``(header, blocks, capped)`` — 2 header chars + a 4-char block each.

    Returned unjoined so ``get_ai_details`` can pop blocks off the tail to fit the
    255-char cap, the same way ``truncate_plugins_data`` pops plugin entries. ``capped``
    reports whether _AI_MAX_PROVIDER_BLOCKS already dropped configs before that loop ever
    runs, so the caller raises the truncation flag for BOTH ways segment P falls short —
    the cap fires on deployments that sit well under the length limit, where the loop
    below never executes.

    Header: P1 configured count · P2 one-active boolean.
    Block:  provider type (1) · model (2) · effort (1), repeated.

    One block per configured provider, not just the enabled one: which vendors and models
    an admin has set up is itself the signal. Blocks are ordered active first, so when P2
    is 1 the first block is the live configuration and the rest follow sorted by name.

    Capped at _AI_MAX_PROVIDER_BLOCKS so a long tail of configs cannot crowd out the rest
    of the payload. P1 still carries the configured count (saturating at 15), so a
    shortfall is also visible by comparing it against the number of blocks present.

    Every field is POINT-IN-TIME: it describes config as it stands at collection, while
    the token counters beside it span the whole retention window.
    """
    configs = list(
        connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find(
            {}, {"_id": 0, "name": 1, "plugin": 1, "active": 1, "parameters": 1}
        )
    )
    configs.sort(key=lambda c: (not c.get("active"), c.get("name") or ""))

    blocks = []
    for config in configs[:_AI_MAX_PROVIDER_BLOCKS]:
        provider = plugin_id_to_provider(config.get("plugin", "")).value
        parameters = config.get("parameters") or {}
        model = parameters.get("model")
        effort = parameters.get("agentic_effort_calibration")
        blocks.append(
            AI_PROVIDER_MAPPING.get(provider, AI_PROVIDER_MAPPING[None])
            + AI_MODEL_MAPPING.get(model, "fe" if model else "ff")
            + (AI_EFFORT_MAPPING.get(effort, "e") if effort else "f")
        )

    header = convert_to_hex(configured, length=1) + (
        # Product rule: at most ONE provider is active globally, so this is a boolean.
        "1" if any(c.get("active") for c in configs) else "0"
    )
    # Measured against the list that was actually sliced, not the count_documents value in
    # ``configured``: a config written between those two queries must not flip the flag.
    return header, blocks, len(configs) > _AI_MAX_PROVIDER_BLOCKS


def _ai_usage_stats() -> dict:
    """Per-feature rollup of ai_usage_metrics, keyed by feature value.

    One aggregation: the error buckets are expressible as $in tests, so there is no need
    to post-process in Python. Feedback stubs are excluded here (they are not turns and
    carry no tokens) — the thumbs counters read them separately, on purpose.
    """
    api_token_types = AI_ERROR_BUCKETS["api_token"]
    network_types = AI_ERROR_BUCKETS["network"]
    pipeline = [
        {"$match": {"feedbackStub": {"$ne": True}}},
        {
            "$group": {
                "_id": "$feature",
                "turns": {"$sum": 1},
                "inputTokens": {"$sum": {"$ifNull": ["$inputTokens", 0]}},
                "outputTokens": {"$sum": {"$ifNull": ["$outputTokens", 0]}},
                # No success counter: success = turns - errCode - errApiToken - errNetwork
                # - cancelled, exactly, because every ERROR record carries an errorType and
                # every errorType resolves to exactly one bucket below.
                # Abandonment, not failure — kept out of any success-rate denominator.
                "cancelled": {
                    "$sum": {"$cond": [{"$eq": ["$errorType", "client_disconnected"]}, 1, 0]}
                },
                "errApiToken": {
                    "$sum": {"$cond": [{"$in": ["$errorType", api_token_types]}, 1, 0]}
                },
                "errNetwork": {
                    "$sum": {"$cond": [{"$in": ["$errorType", network_types]}, 1, 0]}
                },
                # "code" is the DEFAULT bucket: anything classified that is not
                # api/token and not network. A negative test, so an unlisted LLMErrorType
                # lands here rather than in no bucket at all — success is turns minus the
                # three buckets, so an unbucketed error would read as a success.
                "errCode": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": [{"$ifNull": ["$errorType", None]}, None]},
                                    {"$not": {"$in": ["$errorType", api_token_types]}},
                                    {"$not": {"$in": ["$errorType", network_types]}},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                "iterLimit": {
                    "$sum": {"$cond": [{"$eq": ["$errorType", "iteration_limit"]}, 1, 0]}
                },
                "iterSum": {"$sum": {"$ifNull": ["$metadata.iterationsUsed", 0]}},
                "iterCount": {
                    "$sum": {
                        "$cond": [
                            {"$ne": [{"$type": "$metadata.iterationsUsed"}, "missing"]}, 1, 0
                        ]
                    }
                },
                # Grounding denominator: ONLY turns that actually carry citationCount.
                # Errored turns produced no answer, and records written before the
                # grounding instrumentation shipped have no such field — counting them
                # would permanently depress every grounding percentage.
                "groundDen": {
                    "$sum": {
                        "$cond": [
                            {"$ne": [{"$type": "$metadata.citationCount"}, "missing"]}, 1, 0
                        ]
                    }
                },
                # webUsed and kbUsed MUST be scoped to the same population as groundDen.
                # The gateway writes webSearchEnriched on EVERY record including errors,
                # but citationCount is only stamped after a successful answer is
                # normalised — so an unscoped numerator over a success-only denominator
                # can exceed it and encode a percentage above 0x64 (an impossible value).
                # An errored turn produced no answer, so it belongs in neither.
                "webUsed": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$eq": ["$metadata.webSearchEnriched", True]},
                                    {"$ne": [{"$type": "$metadata.citationCount"}, "missing"]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                # Inherently scoped: the field must exist to equal 0.
                "zeroCitation": {
                    "$sum": {"$cond": [{"$eq": ["$metadata.citationCount", 0]}, 1, 0]}
                },
                # KB grounding produces NO citations, so it is a separate signal — an
                # answer grounded purely in the local knowledge packs legitimately has
                # zero citations. Copilot-only (the log analyzer has no knowledge tool).
                "kbUsed": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$eq": ["$metadata.kbGrounded", True]},
                                    {"$ne": [{"$type": "$metadata.citationCount"}, "missing"]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                "degRepaired": {
                    "$sum": {"$cond": [{"$eq": ["$metadata.degraded", "repaired_structured"]}, 1, 0]}
                },
                "degNoStruct": {
                    "$sum": {
                        "$cond": [{"$eq": ["$metadata.degraded", "no_structured_response"]}, 1, 0]
                    }
                },
            }
        },
    ]
    stats = {}
    for row in connector.collection(Collections.AI_USAGE_METRICS).aggregate(pipeline):
        stats[row.pop("_id")] = row
    return stats


def _ai_feedback_stats() -> dict:
    """Thumbs counts + the down-reason histogram.

    Feedback stubs are INCLUDED here: a stub exists precisely to preserve a rating whose
    turn never persisted a usage record, so excluding them would silently drop
    thumbs-down on failed turns — the most important feedback there is.
    """
    pipeline = [
        {"$match": {"feedback.rating": {"$in": ["up", "down"]}}},
        {
            "$group": {
                "_id": {"rating": "$feedback.rating", "comment": "$feedback.comment"},
                "n": {"$sum": 1},
            }
        },
    ]
    up = down = 0
    reasons = {reason: 0 for reason in AI_DOWN_REASON_ORDER}
    for row in connector.collection(Collections.AI_USAGE_METRICS).aggregate(pipeline):
        key = row["_id"]
        count = row["n"]
        if key.get("rating") == "up":
            up += count
            continue
        down += count
        comment = key.get("comment")
        if comment in reasons:
            reasons[comment] += count
    return {"up": up, "down": down, "reasons": reasons}


def _ai_reason_histogram(reasons: dict, down_total: int) -> str:
    """Render the 5-nibble down-reason histogram — SHARES of thumbs-down, quantised to 1/15.

    Shares rather than raw counts because a nibble caps at 15 and the counts accumulate
    over the retention window, so raw counts would saturate almost immediately. Absolute
    counts stay recoverable as ``down_total * nibble / 15``.

    Largest-remainder rounding keeps the sum <= 15, which leaves ``15 - sum`` as the
    share of thumbs-down where the user dismissed the chip picker without choosing a
    reason — that bucket comes free, with no extra field.
    """
    if not down_total:
        return "0" * len(AI_DOWN_REASON_ORDER)

    exact = [15 * reasons[reason] / down_total for reason in AI_DOWN_REASON_ORDER]
    nibbles = [int(value) for value in exact]
    remaining = 15 - sum(nibbles)
    # Hand out what integer truncation dropped, largest fractional part first, but never
    # more than the reasons actually account for (the residual belongs to "no reason").
    remainders = sorted(
        range(len(exact)), key=lambda i: exact[i] - int(exact[i]), reverse=True
    )
    accounted = sum(reasons[reason] for reason in AI_DOWN_REASON_ORDER)
    spare = min(remaining, max(0, round(15 * accounted / down_total) - sum(nibbles)))
    for index in remainders[:spare]:
        nibbles[index] += 1
    return "".join(convert_to_hex(value, length=1) for value in nibbles)


def _ai_core_block(stats: dict) -> str:
    """Build the per-feature core block — 27 chars, identical for every feature.

    x1 turns · x2 input · x3 output · x4 code · x5 api/token · x6 network.

    Turns use `convert_size` (exact to 999, then 3 significant digits — a 1% error at
    100k turns); tokens use `_ai_tokens` instead.

    Abandoned turns are bucketed as network errors by AI_ERROR_BUCKETS, which keeps
    ``success = x1 - x4 - x5 - x6`` exact. Success count and rate are that subtraction,
    so neither is encoded.
    """
    stats = stats or {}
    return (
        convert_size(stats.get("turns", 0), length=3, base=1000)
        + _ai_tokens(stats.get("inputTokens", 0))
        + _ai_tokens(stats.get("outputTokens", 0))
        + _ai_count3(stats.get("errCode", 0))
        + _ai_count3(stats.get("errApiToken", 0))
        + _ai_count3(stats.get("errNetwork", 0))
    )


def _ai_web_grounded(stats: dict) -> str:
    """Web-grounded % over the turns that carry grounding instrumentation — 2 chars."""
    stats = stats or {}
    return _ai_pct(stats.get("webUsed", 0), stats.get("groundDen", 0))


def _ai_log_analyzer_extension(stats: dict) -> str:
    """Build segment L extension — 5 chars. L1 iteration-limit hits · L2 web-grounded %.

    No KB-grounded field on purpose: the log analyzer has no knowledge tool. Analyze runs
    [web_tool, get_ce_docs_keywords] and triage runs the four log tools plus the web tool,
    and get_ce_docs_keywords returns CE *vocabulary* to aim a web search — it is not a
    grounding source. A KB field here would be a constant '00'.
    """
    return _ai_count3((stats or {}).get("iterLimit", 0)) + _ai_web_grounded(stats)


def _ai_journey_stats() -> dict:
    """Journey counts from copilot_sessions (the active journey + any paused ones).

    A SNAPSHOT, not an accumulating counter: journeys live inside session documents, so
    they are destroyed by session delete and by the shorter aiDataCleanup window, and
    dismissing a paused journey removes it outright. Counts can go DOWN between reports.
    Dismissed-but-still-attached journeys are counted — they were created.
    """
    pipeline = [
        {
            "$project": {
                "journeys": {
                    "$concatArrays": [
                        {"$cond": [{"$ifNull": ["$journey", False]}, ["$journey"], []]},
                        {"$ifNull": ["$pausedJourneys", []]},
                    ]
                }
            }
        },
        {"$unwind": "$journeys"},
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                # "Started" = ANY step marked done, order-independent.
                "withProgress": {
                    "$sum": {
                        "$cond": [
                            {
                                "$gt": [
                                    {
                                        "$size": {
                                            "$filter": {
                                                "input": {"$ifNull": ["$journeys.steps", []]},
                                                "cond": {"$eq": ["$$this.state", "done"]},
                                            }
                                        }
                                    },
                                    0,
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                "playbook": {
                    "$sum": {"$cond": [{"$ifNull": ["$journeys.playbookId", False]}, 1, 0]}
                },
            }
        },
    ]
    rows = list(connector.collection(Collections.COPILOT_SESSIONS).aggregate(pipeline))
    return rows[0] if rows else {"total": 0, "withProgress": 0, "playbook": 0}


def _ai_copilot_extension(stats: dict, feedback: dict, journeys: dict) -> str:
    """Build segment C extension — 30 chars."""
    stats = stats or {}
    down_total = feedback["down"]
    return (
        _ai_count3(stats.get("iterLimit", 0))
        + _ai_web_grounded(stats)
        + _ai_pct(stats.get("kbUsed", 0), stats.get("groundDen", 0))
        # Two degrade rungs, counted separately: 'repaired_structured' keeps journeys and
        # insight cards, 'no_structured_response' loses them. Both persist as SUCCESS.
        + _ai_count3(stats.get("degRepaired", 0))
        + _ai_count3(stats.get("degNoStruct", 0))
        + _ai_count3(feedback["up"])
        + _ai_count3(down_total)
        + _ai_reason_histogram(feedback["reasons"], down_total)
        + convert_to_hex(journeys.get("total", 0), 2)
        + convert_to_hex(journeys.get("withProgress", 0), 2)
        + convert_to_hex(journeys.get("playbook", 0), 2)
    )


def _ai_automapper_suggested_fields() -> int:
    """Count entity fields the automapper suggested, across every CRE entity.

    Keys on ``fields[].metadata.ai_suggested`` being true — the marker the automapper
    sets. Matching on the presence of ``metadata`` alone would also count fields carrying
    it for unrelated reasons. Returns 0 when no field is marked, including on schemas
    that carry no such marker.
    """
    pipeline = [
        {"$unwind": "$fields"},
        {"$match": {"fields.metadata.ai_suggested": True}},
        {"$count": "n"},
    ]
    rows = list(connector.collection(Collections.CREV2_ENTITIES).aggregate(pipeline))
    return rows[0]["n"] if rows else 0


def _ai_findings_stats() -> dict:
    """Summarise copilot_findings by status/severity, kind and module.

    Shared by the encoded segment N and the diagnose report, so the two can never report
    different numbers. 'watching' findings are excluded — they are pre-threshold trackers
    and invisible by design. Auto-resolved counts undercount: findings carry an expireAt
    TTL, so resolved documents age out of the snapshot.
    """
    pipeline = [
        {"$match": {"status": {"$in": ["open", "acknowledged", "auto_resolved"]}}},
        {
            "$group": {
                "_id": {
                    "status": "$status",
                    "severity": "$severity",
                    "kind": "$kind",
                    "module": "$module",
                },
                "n": {"$sum": 1},
            }
        },
    ]
    by_status_severity: dict = {}
    open_by_kind: dict = {}
    acked_by_kind: dict = {}
    problematic_modules: set = set()
    for row in connector.collection(Collections.COPILOT_FINDINGS).aggregate(pipeline):
        key = row["_id"]
        status, severity, kind, module = (
            key.get("status"),
            key.get("severity"),
            key.get("kind"),
            key.get("module"),
        )
        count = row["n"]
        by_status_severity[(status, severity)] = (
            by_status_severity.get((status, severity), 0) + count
        )
        if status == "open":
            open_by_kind[kind] = open_by_kind.get(kind, 0) + count
            problematic_modules.add(module)
        elif status == "acknowledged":
            acked_by_kind[kind] = acked_by_kind.get(kind, 0) + count
    return {
        "byStatusSeverity": by_status_severity,
        "openByKind": open_by_kind,
        "ackedByKind": acked_by_kind,
        # Modules carrying at least one OPEN finding — how wide the problem is, not how deep.
        "problematicModules": sorted(m for m in problematic_modules if m),
    }


def _ai_proactive_segment(stats: dict) -> str:
    """Encode segment N — 24 chars, all snapshots, from _ai_findings_stats().

    N1 module bitmask · N2 per-kind OPEN histogram · N3 per-kind ACKNOWLEDGED histogram.

    WHICH rules fire and which get muted is the actionable signal, so the per-kind rows
    are what the payload keeps; the severity split and the auto-resolved counts are
    diagnose-only. Nibbles saturate at 'f' = "15 or more", so totals derived by summing a
    row are a LOWER BOUND — plugin_error_logs groups by errorCode and can realistically
    exceed 15 on a broken deployment.
    """
    module_mask = 0
    for module in stats["problematicModules"]:
        module_mask |= AI_FINDING_MODULE_NUMBERS.get(module, 0)

    def _kind_histogram(counts: dict) -> str:
        return "".join(
            convert_to_hex(counts.get(finding_kind, 0), length=1)
            for finding_kind in AI_FINDING_KIND_ORDER
        )

    return (
        convert_to_hex(module_mask, 2)
        + _kind_histogram(stats["openByKind"])
        + _kind_histogram(stats["ackedByKind"])
    )


def get_ai_details() -> str:
    """Build the complete 'ai' analytics string, or '' when AI was never configured.

    Layout: -{T}-{V}-{P}-{G}-{L}-{C}-{A}-{N}, where T is the truncation flag (1 when
    provider blocks were dropped — by the _AI_MAX_PROVIDER_BLOCKS cap, by the 255-char
    fit, or both). Groups are '-' separated so a field added to one feature's extension
    never shifts another group's offsets.

    Gated on an LLM provider config EXISTING rather than being active: AI is not a
    settings.platforms toggle, so the per-module platform check does not apply, and a
    configured-then-disabled deployment still has history worth reporting.
    """
    try:
        configured = connector.collection(
            Collections.LLM_PROVIDER_CONFIGURATIONS
        ).count_documents({})
        if configured == 0:
            return ""
        provider_header, provider_blocks, capped = _ai_provider_segment(configured)

        usage = _ai_usage_stats()
        log_stats = usage.get(AIFeature.POSTURE_ASSESSMENT.value, {})
        copilot_stats = usage.get(AIFeature.CONFIGURATION_COPILOT.value, {})
        automapper_stats = usage.get(AI_FEATURE_AUTOMAPPER, {})

        # Segment G carries sessions ONLY. Which features are in use (x1 > 0 per feature),
        # the turn total (sum of the three x1s) and turns-per-session (copilot x1 /
        # sessions) are all exactly derivable from what is already sent, and a second copy
        # of a derivable value can only ever disagree with its own inputs.
        sessions = connector.collection(Collections.COPILOT_SESSIONS).count_documents({})
        global_segment = convert_to_hex(sessions, 3)

        feedback = _ai_feedback_stats()
        journeys = _ai_journey_stats()

        tail = [
            global_segment,
            _ai_core_block(log_stats) + _ai_log_analyzer_extension(log_stats),
            _ai_core_block(copilot_stats)
            + _ai_copilot_extension(copilot_stats, feedback, journeys),
            _ai_core_block(automapper_stats)
            + convert_to_hex(_ai_automapper_suggested_fields(), 3),
            _ai_proactive_segment(_ai_findings_stats()),
        ]

        def _assemble(blocks: list, truncated: bool) -> str:
            return "-".join(
                [
                    "",  # leading '-' from the join
                    str(int(truncated)),
                    AI_ANALYTICS_VERSION,
                    provider_header + "".join(blocks),
                    *tail,
                ]
            )

        # Fit the cap the way truncate_plugins_data does: drop entries off the tail of
        # the only variable-length list until it fits, and raise the truncation flag.
        # Blocks are ordered active-first, so the live configuration is the last dropped.
        # Whole blocks only — the payload is positional, so clipping the string would
        # shift every field after the cut and decode wrong. The flag is one character in
        # either state, so dropping a block always shortens the result.
        #
        # Seeded from the hard cap rather than False: _AI_MAX_PROVIDER_BLOCKS may already
        # have dropped configs, and it fires INDEPENDENTLY of length — the fixed part is
        # ~182 chars, so a 9-provider payload sits ~50 characters under the cap and the
        # loop below never runs. The flag means "segment P is incomplete", so it has to
        # cover both limits; otherwise the report claims a complete P while withholding
        # configs, and only a decoder that re-derives the block count notices.
        prefix_length = len("netskope-ce-" + api.__version__)
        truncated = capped
        while provider_blocks and (
            prefix_length + len(_assemble(provider_blocks, truncated))
            > MAX_ANALYTICS_LENGTH
        ):
            provider_blocks.pop()
            truncated = True

        analytics = _assemble(provider_blocks, truncated)
        if prefix_length + len(analytics) > MAX_ANALYTICS_LENGTH:
            # Only reachable if the fixed-width part alone outgrows the cap. Mirrors
            # truncate_plugins_data, which also sends what it has rather than dropping the
            # report — logged loudly so the overflow is attributable.
            logger.error(
                "AI Copilot analytics exceeds the User-Agent limit even with every "
                f"provider block dropped ({prefix_length + len(analytics)}/"
                f"{MAX_ANALYTICS_LENGTH} characters).",
                details=f"raw={analytics}",
            )
        return analytics
    except Exception as e:
        logger.debug(
            f"Failed to get ai details: {e}", details=traceback.format_exc()
        )
        return ""


def _ai_rate(numerator: int, denominator: int):
    """Return a percentage rounded to 1dp, or None when there is nothing to divide by."""
    if not denominator:
        return None
    return round(100 * numerator / denominator, 1)


def _ai_feature_report(stats: dict, *, cancellable: bool = True, kb: bool = False) -> dict:
    """Expand one feature's raw stats into the readable diagnose shape.

    Values the encoded payload leaves to be derived are spelled out here: the diagnose
    bundle has no width budget, so nothing needs to be recomputed by hand.
    """
    stats = stats or {}
    turns = stats.get("turns", 0)
    err_code = stats.get("errCode", 0)
    err_api = stats.get("errApiToken", 0)
    err_net = stats.get("errNetwork", 0)
    cancelled = stats.get("cancelled", 0) if cancellable else 0
    # err_net already contains the cancelled turns (AI_ERROR_BUCKETS folds
    # client_disconnected into "network"), so cancelled is NOT subtracted again here.
    err_total = err_code + err_api + err_net
    success = turns - err_total
    # Real failures, with user abandonment taken back out — the honest success-rate
    # denominator. Heavy Stop-button use is not unreliability.
    real_errors = err_total - cancelled
    inp = stats.get("inputTokens", 0)
    out = stats.get("outputTokens", 0)
    iter_count = stats.get("iterCount", 0)
    ground_den = stats.get("groundDen", 0)
    degraded = stats.get("degRepaired", 0) + stats.get("degNoStruct", 0)

    report = {
        "turns": turns,
        "inputTokens": inp,
        "outputTokens": out,
        "totalTokens": inp + out,
        "errors": {
            "code": err_code,
            # "network" INCLUDES cancelled turns, since AI_ERROR_BUCKETS buckets
            # client_disconnected there. Both figures are reported so the two causes can
            # be separated.
            "network": err_net,
            "networkExcludingCancelled": err_net - cancelled,
            "apiToken": err_api,
            "total": err_total,
            "iterationLimitHits": stats.get("iterLimit", 0),
        },
        "success": success,
        "successRatePct": _ai_rate(success, success + real_errors),
        "errorRatePct": _ai_rate(real_errors, turns),
        "avgIterations": round(stats.get("iterSum", 0) / iter_count, 2) if iter_count else None,
        # Denominator is turns that produced an answer AND carry the grounding fields.
        # Records without them are excluded, so this can be 0 while turns is not.
        "grounding": {
            "measurableTurns": ground_den,
            "webGroundedPct": _ai_rate(stats.get("webUsed", 0), ground_den),
            "zeroCitationPct": _ai_rate(stats.get("zeroCitation", 0), ground_den),
        },
    }
    if cancellable:
        report["cancelledTurns"] = cancelled
        report["cancellationRatePct"] = _ai_rate(cancelled, turns)
    if kb:
        report["grounding"]["kbGroundedPct"] = _ai_rate(stats.get("kbUsed", 0), ground_den)
        # Both degrade rungs persist as SUCCESS: the user got an answer, at reduced
        # fidelity. noStructuredResponse is the worse of the two — it loses the turn's
        # journey and insight cards entirely.
        report["degraded"] = {
            "repairedStructured": stats.get("degRepaired", 0),
            "noStructuredResponse": stats.get("degNoStruct", 0),
            "ratePct": _ai_rate(degraded, turns),
        }
    return report


def collect_ai_analytics_report() -> dict:
    """Build the full-precision AI Copilot report for the diagnose bundle.

    Same underlying queries as get_ai_details(), so the two cannot disagree. With no
    255-char budget it reports exact values rather than quantised ones, spells out the
    derived fields, and adds operational context — retention settings, collection sizes,
    environment flags — that matters when debugging a live deployment.

    Contains NO conversation content and no secrets: counts, rates and non-secret config
    only. Keep it that way — ``aiDataCleanup`` is a privacy control over exactly the
    transcripts this report does not read.
    """
    settings = connector.collection(Collections.SETTINGS).find_one({}) or {}
    providers = list(
        connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find(
            {}, {"_id": 0, "name": 1, "plugin": 1, "active": 1, "sslValidation": 1,
                 "parameters.model": 1, "parameters.agentic_effort_calibration": 1}
        )
    )
    active = next((p for p in providers if p.get("active")), None)
    usage = _ai_usage_stats()
    feedback = _ai_feedback_stats()
    journeys = _ai_journey_stats()
    findings = _ai_findings_stats()

    def _sev(status: str) -> dict:
        pairs = findings["byStatusSeverity"]
        return {
            "warn": pairs.get((status, "warn"), 0),
            "error": pairs.get((status, "error"), 0),
        }

    open_by_kind = findings["openByKind"]
    acked_by_kind = findings["ackedByKind"]
    # Share of a rule's findings that were muted rather than resolved: a high rate marks
    # the rule as a false-positive generator.
    mute_rate = {
        kind: _ai_rate(acked_by_kind.get(kind, 0),
                       open_by_kind.get(kind, 0) + acked_by_kind.get(kind, 0))
        for kind in sorted(set(open_by_kind) | set(acked_by_kind))
    }

    journey_total = journeys.get("total", 0)
    thumbs_up = feedback["up"]
    thumbs_down = feedback["down"]
    copilot_turns = usage.get(AIFeature.CONFIGURATION_COPILOT.value, {}).get("turns", 0)
    sessions = connector.collection(Collections.COPILOT_SESSIONS).count_documents({})

    return {
        "provider": {
            "configured": len(providers),
            "activeName": active.get("name") if active else None,
            "activePlugin": active.get("plugin") if active else None,
            "providerType": plugin_id_to_provider(active.get("plugin", "")).value if active else None,
            "model": (active.get("parameters") or {}).get("model") if active else None,
            "effort": (active.get("parameters") or {}).get("agentic_effort_calibration") if active else None,
            "sslValidation": active.get("sslValidation") if active else None,
        },
        # Every counter below is bounded by these windows — a total is "since the oldest
        # surviving record", never lifetime.
        "retention": {
            "aiStatsCleanupDays": settings.get("aiStatsCleanup", 365),
            "aiDataCleanupDays": settings.get("aiDataCleanup", 90),
        },
        "collectionSizes": {
            "aiUsageMetrics": connector.collection(Collections.AI_USAGE_METRICS).count_documents({}),
            "copilotSessions": sessions,
            "copilotTurns": connector.collection(Collections.COPILOT_TURNS).count_documents({}),
            "copilotFindings": connector.collection(Collections.COPILOT_FINDINGS).count_documents({}),
        },
        "sessions": sessions,
        "turnsPerSession": round(copilot_turns / sessions, 2) if sessions else None,
        "features": {
            "postureAssessment": _ai_feature_report(
                usage.get(AIFeature.POSTURE_ASSESSMENT.value, {})
            ),
            "copilot": _ai_feature_report(
                usage.get(AIFeature.CONFIGURATION_COPILOT.value, {}), kb=True
            ),
            "automapper": _ai_feature_report(
                usage.get(AI_FEATURE_AUTOMAPPER, {}), cancellable=False
            ),
        },
        "feedback": {
            "thumbsUp": thumbs_up,
            "thumbsDown": thumbs_down,
            "satisfactionPct": _ai_rate(thumbs_up, thumbs_up + thumbs_down),
            "responseRatePct": _ai_rate(thumbs_up + thumbs_down, copilot_turns),
            # Exact counts here, unlike the /15 shares the User-Agent payload is forced into.
            "downReasons": dict(feedback["reasons"]),
            "downNoReasonGiven": thumbs_down - sum(feedback["reasons"].values()),
        },
        "journeys": {
            "total": journey_total,
            "withProgress": journeys.get("withProgress", 0),
            "fromPlaybook": journeys.get("playbook", 0),
            "freeForm": journey_total - journeys.get("playbook", 0),
            "engagementRatePct": _ai_rate(journeys.get("withProgress", 0), journey_total),
            "playbookRatePct": _ai_rate(journeys.get("playbook", 0), journey_total),
        },
        "findings": {
            "open": _sev("open"),
            "acknowledged": _sev("acknowledged"),
            "autoResolved": _sev("auto_resolved"),
            "problematicModules": findings["problematicModules"],
            "openByKind": dict(sorted(open_by_kind.items())),
            "acknowledgedByKind": dict(sorted(acked_by_kind.items())),
            "muteRatePctByKind": mute_rate,
        },
        "automapperSuggestedFields": _ai_automapper_suggested_fields(),
        "environment": {
            "AI_COPILOT_VERBOSITY": os.getenv("AI_COPILOT_VERBOSITY", "concise"),
            "AI_COPILOT_GATEWAY": os.getenv("AI_COPILOT_GATEWAY", "v1"),
            "AI_COPILOT_STREAM_STEPS": os.getenv("AI_COPILOT_STREAM_STEPS", "true"),
            "CE_ATTENTION_SCAN_DISABLED": os.getenv("CE_ATTENTION_SCAN_DISABLED", ""),
            # Registration is conditional on a provider existing, so its absence
            # explains an empty findings feed.
            "attentionScanScheduled": connector.collection(Collections.SCHEDULES).count_documents(
                {"name": "INTERNAL COPILOT ATTENTION SCAN"}, limit=1
            ) > 0,
        },
    }


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

        # AI Copilot analytics. Adding it to analytics_data_dict is all that is needed to
        # both SEND it (share_analytics_in_user_agent loops this dict, one request per
        # type) and PERSIST it (the settings write below), exactly like every other type.
        # It needs no truncate_plugins_data pass — it carries no plugin list, and its one
        # variable-width part (the per-provider blocks in segment P) is already capped.
        ai_analytics = get_ai_details()
        if ai_analytics:
            analytics_data_dict["ai"] = ai_analytics
            logger.debug(
                "AI Copilot analytics collected.",
                details=(
                    f"raw={ai_analytics} "
                    f"length={len(create_user_agent(ai_analytics))}/{MAX_ANALYTICS_LENGTH} "
                    f"segments={ai_analytics.split('-')}"
                ),
            )
        else:
            logger.debug(
                "AI Copilot analytics skipped: no LLM provider has ever been configured."
            )

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
        # The AI-provider banner is an internally-managed is_promotion banner (NOT sourced from
        # GitHub). Keep it out of this delete so the sync doesn't drop it and reset its
        # `acknowledged` state — its lifecycle is owned by ensure_ai_provider_banner.
        connector.collection(Collections.NOTIFICATIONS).delete_many(
            {
                "is_promotion": True,
                "id": {"$nin": list_of_banner_ids},
            }
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
    # Periodic backstop for the "configure an AI provider" banner: seeds it on an unconfigured
    # deployment where the migration/CRUD hooks didn't (e.g. a deployment already past the
    # 7.0.0-beta.1 migration). No-ops when a provider exists or the banner already exists, so it
    # never resets a user's acknowledgement. Provider CRUD is the immediate path; this is the
    # 24h safety net. Same standard banner pipeline.
    from netskope.common.utils.notifier import ensure_ai_provider_banner
    ensure_ai_provider_banner()
    check_certs_validity()
    return {"success": True}
