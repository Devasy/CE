"""Common validation for tenant."""
from netskope.common.utils import Collections, DBConnector, Logger
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper

connector = DBConnector()
logger = Logger()
plugin_provider_helper = PluginProviderHelper()


def validate_tenant(tenant_name, check_v2_token=True):
    """Validate tenant configuration and token."""
    tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
        {"name": tenant_name}
    )

    if not tenant:
        logger.debug(f"Tenant with name {tenant_name} no longer exists.", error_code="CE_1029")
        return False, {"success": False, "message": f"Tenant {tenant_name} does not exist", "error_code": "CE_1029"}

    if not tenant.get("parameters", {}).get("v2token"):
        message = f"V2 token is not configured for Tenant {tenant_name}. " \
                  f"Pulling data from Tenant {tenant_name} stopped."
        logger.error(message, error_code="CE_1096")
        return False, {"success": False, "message": message, "error_code": "CE_1096"}
    if not check_v2_token:
        return True, tenant
    provider_obj = plugin_provider_helper.get_provider(tenant_name)
    if not provider_obj.validate_v2token(tenant)[0]:
        message = f"V2 token does not have access to all the alert and event dataexport endpoints. " \
                  f"Pulling alerts from Tenant {tenant_name} stopped."
        logger.error(message, error_code="CE_1105")
        return False, {"success": False, "message": message, "error_code": "CE_1105"}

    return True, tenant
