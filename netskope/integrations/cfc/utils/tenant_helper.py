"""Tenant Helper for CFC module."""
from netskope.common.models import TenantDB
from netskope.common.utils import Collections, Logger, DBConnector
from netskope.common.utils.validate_tenant import validate_tenant


class TenantHelper():
    """Netskope tenant helper class."""

    def __init__(self):
        """Initialize tenant helper."""
        self.logger = Logger()
        self.connector = DBConnector()

    def get_proxy(self) -> dict:
        """Get proxy dict."""
        return {}

    def _get_tenant_from_name(self, name: str) -> TenantDB:
        """Get Tenant object with tenant name.

        Args:
            name (str): name of tenant

        Returns:
            TenantDB: TenantDB object for provided tenant name.
        """
        tenant = self.connector.collection(
            Collections.NETSKOPE_TENANTS
        ).find_one({"name": name})
        if tenant is None:
            raise Exception("Could not find the associated tenant.")  # NOSONAR
        return TenantDB(**tenant)

    def get_tenant_cfc(self, name: str) -> TenantDB:
        """Get tenant associated with the CFC config.

        Args:
            name (str): Name of configuration

        Raises:
            Exception: Error if tenant is not found.

        Returns:
            TenantDB: Tenant accosiated with provided plugin configuration
        """
        try:
            config = self.connector.collection(
                Collections.CFC_CONFIGURATIONS
            ).find_one({"name": name})
            tenant_name = config.get("tenant")
            return self._get_tenant_from_name(tenant_name)
        except TypeError:
            raise Exception("Could not find the associated tenant.")  # NOSONAR

    def validate_tenant_cfc(self, name: str):
        """Validate the token of teh tenant."""
        success, content = validate_tenant(name, check_v2_token=True)
        if not success:
            raise ValueError(content["message"])
        return TenantDB(**content)
