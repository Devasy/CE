"""Forbidden error banner operations."""
from typing import List

from netskope.common.utils import Logger, DBConnector, Collections
from netskope.common.utils.notifier import Notifier
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.utils import const
import traceback

provider_helper = PluginProviderHelper()
logger = Logger()
connector = DBConnector()
notifier = Notifier()
DOCS_URL = "https://docs.netskope.com/en/netskope-help/integrations-439794/netskope-cloud-exchange/get-started-with-cloud-exchange/configure-netskope-tenants/#v2-rest-api-scopes"  # NOQA


def gather_alert_and_event_types(tenant_name: str) -> List[str]:
    """Gather all the event types to fetch from a tenant.

    Args:
        tenant_name (str): Name of the tenant.

    Returns:
        List[str]: Types of alerts and events.
    """
    configured_types = provider_helper.get_all_configured_subtypes(tenant_name)
    return configured_types["events"] + configured_types["alerts"]


def create_or_ack_unauthorized_error_banner():
    """Create or acknowledge unauthorized error banner."""
    try:
        tenant_error_messages = []
        for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find({"storage.is_v2_token_expired": True}):
            message = f"[{tenant.get('name')}]({tenant.get('parameters', {}).get('tenantName').strip().strip('/')}"
            message += "/ns#/settings)"
            tenant_error_messages.append(message)
        if tenant_error_messages:
            message = (
                "The Netskope tenant API token has expired for %s. "
                "To resume communication between Netskope Tenant and Cloud Exchange,"
                " please take action based on your token type. "
                "For V2 tokens, generate a new token or re-issue the existing one,"
                " then update the tenant configuration. "
                "For RBAC V3 tokens, either generate a new token or extend the expiration date of the current one."
                % ("**" + ", ".join(tenant_error_messages) + "**")
            )
            notifier.banner_error(
                id=const.UNAUTHORIZED_BANNER_ID,
                message=message
            )
            connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": const.UNAUTHORIZED_BANNER_ID},
                {
                    "$set": {
                        "acknowledged": False,
                    },
                },
                upsert=True,
            )
        else:
            connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": const.UNAUTHORIZED_BANNER_ID},
                {
                    "$set": {
                        "acknowledged": True,
                    },
                },
                upsert=True,
            )
    except Exception as e:
        logger.error(
            f"Error occred while upserting banner for unauthorized error. {e}.",
            details=traceback.format_exc()
        )


def create_or_ack_forbidden_error_banner(print_message=False):
    """Create or acknowledge forbidden error banner."""
    try:
        create_or_ack_unauthorized_error_banner()
        tenant_error_messages = []
        forbidden_endpoints_msg = []
        for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find({"forbidden_endpoints": {"$exists": 1}, "forbidden_endpoints": {"$not": {"$eq": {}}}}):  # NOQA
            if tenant.get("storage", {}).get("is_v2_token_expired"):
                continue
            alerts_events = gather_alert_and_event_types(tenant["name"])
            forbidden_endpoints = tenant.get("storage", {}).get("forbidden_endpoints", {})
            filtered_forbidden_endpoints = [
                forbidden_endpoints[type] for type in alerts_events if type in forbidden_endpoints
            ]
            if filtered_forbidden_endpoints:
                message = f"[{tenant.get('name')}]({tenant.get('parameters', {}).get('tenantName').strip().strip('/')}"
                message += "/ns#/settings)"
                tenant_error_messages.append(message)
                forbidden_endpoints_msg.append(message + " [" + "\n".join(filtered_forbidden_endpoints) + "]")

        message = ("Netskope API token of tenant %s has been revoked, deleted or has insufficient privileges "
                   "to continue pulling alerts and events from Netskope."
                   " Please check the **[required privileges](%s)** and ensure that your API token has "
                   "the necessary permissions to access the required resources.")
        if tenant_error_messages:
            message = message % ("**" + ", ".join(tenant_error_messages) + "**", DOCS_URL)
            if print_message:
                print(
                    "Please grant Read access to following endpoint(s) from your Netskope "
                    f"tenant(s) {', '.join(forbidden_endpoints_msg)} (Settings > Tools"
                    " > Rest API v2; Edit your token) to continue alert and event pull."
                )
            notifier.banner_error(
                id=const.FORBIDDEN_ERROR_BANNER_ID,
                message=message
            )
            connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": const.FORBIDDEN_ERROR_BANNER_ID},
                {
                    "$set": {
                        "acknowledged": False,
                    },
                },
                upsert=True,
            )
        else:
            connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": const.FORBIDDEN_ERROR_BANNER_ID},
                {
                    "$set": {
                        "acknowledged": True,
                    },
                },
                upsert=True,
            )
    except Exception as e:
        logger.error(
            f"Error occred while upserting banner for forbidden error. {e}.",
            details=traceback.format_exc()
        )
