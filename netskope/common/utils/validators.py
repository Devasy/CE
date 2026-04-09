"""Commonly used validators."""

from typing import Dict
from ..models import PollIntervalUnit
from netskope.common.models import TenantDB
from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
)
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper

connector = DBConnector()
logger = Logger()
plugin_provider_helper = PluginProviderHelper()


def validate_poll_interval(cls, v: int, values: Dict, **kwargs):
    """Determine if the provided configuration has valid pollInterval set.

    Args:
        configuration (ConfigurationIn): Configuration to be validated.
    """
    values = values.data
    from netskope.integrations.cls.models.plugin import ConfigurationUpdate
    from netskope.integrations.cte.models.plugin import ConfigurationIn, ConfigurationUpdate as CTEConfigUpdate

    if cls is ConfigurationUpdate and values["active"] is False:
        return None
    if v is None:
        return None
    multiplyer = {
        PollIntervalUnit.SECONDS: 1,
        PollIntervalUnit.MINUTES: 60,
        PollIntervalUnit.HOURS: 60 * 60,
        PollIntervalUnit.DAYS: 60 * 60 * 24,
    }
    if "pollIntervalUnit" not in values:
        raise ValueError("Invalid Sync Interval Unit provided.")
    interval_in_seconds = v * multiplyer[values["pollIntervalUnit"]]
    if (
        cls in [ConfigurationIn, CTEConfigUpdate]
        and values["tenant"]
        and not 10 * 60 <= interval_in_seconds <= (60 * 60 * 24 * 365)
    ):
        raise ValueError("Sharing sync interval must be between 10 minutes and 1 year.")
    if not 10 <= interval_in_seconds <= (60 * 60 * 24 * 365):
        raise ValueError("Sync interval must be between 10 seconds and 1 year.")
    return v


def permissions_check_validator(cls, v, values, **kwargs):
    """Permission check validator for API requests."""
    # ADD 403 CHECK FOR CLS
    values = values.data
    from netskope.integrations.cls.models.plugin import ConfigurationUpdate

    if cls is ConfigurationUpdate and values["active"] is False:
        return v
    if not isinstance(v, dict):
        return v

    if not values.get("tenant"):
        return v

    event_type = v.get("event_type", [])
    from netskope.integrations.crev2.models.configurations import (
        ConfigurationIn,
    )

    if cls in [ConfigurationIn, ConfigurationUpdate]:
        event_type = ["application"]

    if not event_type:
        return v

    tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
        {"name": values.get("tenant")}
    )
    tenant = TenantDB(**tenant)
    forbidden_endpoints = []
    iterator_name = f"{tenant.name}_event_%s_validate_token"
    for sub_type in event_type:
        it_name = iterator_name % (sub_type)
        provider_obj = plugin_provider_helper.get_provider(tenant.name)
        success, uri = provider_obj.call_api_endpoint_for_validation(tenant, "event", sub_type, it_name)
        if not success:
            forbidden_endpoints.append(uri)
            break
    if forbidden_endpoints:
        # TODO : Shold we add new error code here?
        logger.debug(
            f"For {tenant.name}, received 403 error for following endpoint(s)",
            details=",\n".join(forbidden_endpoints)
        )
        raise ValueError(
            "The Netskope tenant API V2 token does not have the necessary permissions configured."
            " Refer to the list of required permissions."
        )
    return v
