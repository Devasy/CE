"""Alerts helper class."""
from typing import List

from netskope.common.models.other import PollIntervalUnit
from netskope.common.models.settings import SettingsDB
from netskope.common.models.tenant import TenantDB
from netskope.common.utils import Collections, DBConnector, Logger, Singleton
from netskope.common.utils.plugin_helper import PluginHelper

from .proxy import get_proxy_params


helper = PluginHelper()


class AlertsHelper(metaclass=Singleton):
    """Netskope alerts helper class."""

    def __init__(self):
        """Initialize alerts helper."""
        self.logger = Logger()
        self.connector = DBConnector()
        self._loaded = False
        self._alerts = []

    @property
    def alerts(self) -> List[dict]:
        """Get alerts list."""
        if isinstance(self._alerts, bytes):
            from netskope.common.utils import parse_events
            self._alerts = parse_events(self._alerts)

        return self._alerts.get("result", []) if isinstance(self._alerts, dict) else self._alerts

    @alerts.setter
    def alerts(self, alerts) -> None:
        """Set alerts list."""
        self._alerts = alerts

    def get_proxy(self) -> dict:
        """Get proxy dict."""
        settings = self.connector.collection(Collections.SETTINGS).find_one({})
        return get_proxy_params(SettingsDB(**settings))

    def _get_tenant_from_name(self, name: str) -> TenantDB:
        tenant = self.connector.collection(
            Collections.NETSKOPE_TENANTS
        ).find_one({"name": name})
        if tenant is None:
            raise Exception("Could not find the associated tenant.")  # NOSONAR
        return TenantDB(**tenant)

    def get_tenant_itsm(self, name: str) -> TenantDB:
        """Get tenant associated with the CTO config."""
        try:
            config = self.connector.collection(
                Collections.ITSM_CONFIGURATIONS
            ).find_one({"name": name})
            if not config:
                raise Exception(f"Configuration {name} does not exist.")
            tenant_name = config.get("tenant")
            return self._get_tenant_from_name(tenant_name)
        except TypeError:
            raise Exception("Could not find the associated tenant.")  # NOSONAR

    def get_tenant_cte(self, name: str) -> TenantDB:
        """Get tenant associated with the CTE config."""
        try:
            config = self.connector.collection(
                Collections.CONFIGURATIONS
            ).find_one({"name": name})
            if not config:
                raise Exception(f"Configuration {name} does not exist.")
            tenant_name = config.get("tenant")
            return self._get_tenant_from_name(tenant_name)
        except TypeError:
            raise Exception("Could not find the associated tenant.")  # NOSONAR

    def get_tenant_cls(self, name: str) -> TenantDB:
        """Get tenant associated with the CTE config."""
        try:
            config = self.connector.collection(
                Collections.CLS_CONFIGURATIONS
            ).find_one({"name": name})
            if not config:
                raise Exception(f"Configuration {name} does not exist.")
            if helper.find_by_id(config["plugin"]).metadata.get(
                "pull_supported", False
            ):
                return TenantDB(
                    name=config["name"],
                    plugin=config["plugin"],
                    parameters=config["parameters"],
                )
            if PluginHelper.is_syslog_service_plugin(config.get("plugin")):
                return TenantDB(
                    name="Netskope CE Logs",
                    plugin="netskope.plugins.Default.netskope_provider.main",
                    parameters={
                        "tenantName": "Netskope",
                        "token": "17267652656182",
                        "pollInterval": 30,
                        "pollIntervalUnit": PollIntervalUnit.SECONDS,
                        "parameters": {
                            "days": 7,
                        },
                    }
                )
            tenant_name = config.get("tenant")
            return self._get_tenant_from_name(tenant_name)
        except TypeError:
            raise Exception("Could not find the associated tenant.")  # NOSONAR

    def get_tenant(self, tenant_name: str) -> TenantDB:
        """Get tenant associated with the tenant name."""
        try:
            tenant = self.connector.collection(
                Collections.NETSKOPE_TENANTS
            ).find_one({"name": tenant_name})
            if tenant is None:
                raise Exception(
                    "Could not find the associated tenant."
                )  # NOSONAR
            return TenantDB(**tenant)
        except TypeError:
            raise Exception("Could not find the associated tenant.")  # NOSONAR

    def get_tenant_crev2(self, name: str) -> TenantDB:
        """Get tenant associated with the CRE config."""
        try:
            config = self.connector.collection(
                Collections.CREV2_CONFIGURATIONS
            ).find_one({"name": name})
            tenant_name = config.get("tenant")
            tenant = self.connector.collection(
                Collections.NETSKOPE_TENANTS
            ).find_one({"name": tenant_name})
            if tenant is None:
                raise Exception(
                    "Could not find the associated tenant."
                )  # NOSONAR
            return TenantDB(**tenant)
        except TypeError:
            raise Exception("Could not find the associated tenant.")  # NOSONAR

    def get_tenant_edm(self, name: str) -> TenantDB:
        """Get tenant associated with the EDM config."""
        try:
            config = self.connector.collection(
                Collections.EDM_CONFIGURATIONS
            ).find_one({"name": name})
            tenant_name = config.get("tenant")
            tenant = self.connector.collection(
                Collections.NETSKOPE_TENANTS
            ).find_one({"name": tenant_name})
            if tenant is None:
                raise Exception(
                    "Could not find the associated tenant."
                )  # NOSONAR
            return TenantDB(**tenant)
        except TypeError:
            raise Exception("Could not find the associated tenant.")  # NOSONAR

    def get_tenant_cfc(self, name: str) -> TenantDB:
        """Get tenant associated with the CFC config."""
        try:
            config = self.connector.collection(
                Collections.CFC_CONFIGURATIONS
            ).find_one({"name": name})
            tenant_name = config.get("tenant")
            tenant = self.connector.collection(
                Collections.NETSKOPE_TENANTS
            ).find_one({"name": tenant_name})
            if tenant is None:
                raise Exception(
                    "Could not find the associated tenant."
                )  # NOSONAR
            return TenantDB(**tenant)
        except TypeError:
            raise Exception("Could not find the associated tenant.")  # NOSONAR
