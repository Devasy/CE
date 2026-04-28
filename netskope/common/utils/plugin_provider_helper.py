"""Plugin provider helper class."""
from datetime import datetime, timezone
import json
import traceback
from typing import List
from netskope.common.models import NetskopeField, FieldDataType
from netskope.common.models.tenant import TenantDB
from netskope.common.utils import (
    Singleton,
    Collections,
    Logger,
    DBConnector,
    PluginHelper,
    SecretDict
)

plugin_helper = PluginHelper()
ALERTS = "alerts"
EVENTS = "events"

modules = [
    {
        "collection": Collections.CONFIGURATIONS,
    },
    {
        "collection": Collections.CLS_CONFIGURATIONS,
    },
    {
        "collection": Collections.ITSM_CONFIGURATIONS,
    },
    {
        "collection": Collections.CREV2_CONFIGURATIONS,
    },
]


class PluginProviderHelper(metaclass=Singleton):
    """Netskope alerts helper class."""

    def __init__(self):
        """Initialize alerts helper."""
        self.logger = Logger()
        self.connector = DBConnector()

    def list_tenants(self) -> List:
        """Get list of tenants."""
        tenants = self.connector.collection(Collections.NETSKOPE_TENANTS).find({})

        return [tenant for tenant in tenants]

    def get_tenant_details(self, tenant_name: str, data_type=None) -> dict:
        """Get tenant details of the given tenant name."""
        tenant = self.connector.collection(Collections.NETSKOPE_TENANTS).find_one(
            {"name": tenant_name}
        )
        if tenant is None:
            raise Exception("Could not find the associated tenant.")  # NOSONAR

        if not plugin_helper.find_by_id(tenant.get("plugin")):
            raise Exception("Could not find the associated plugin.")  # NOSONAR

        tenant = TenantDB(**tenant)

        if data_type and tenant.checkpoint.get(data_type):
            checkpoint = tenant.checkpoint.get(data_type)
        else:
            checkpoint = tenant.checkpoint

        tenant_details = {
            "name": tenant.name,
            "parameters": tenant.parameters,
            "storage": tenant.storage,
            "checkpoint": checkpoint,
        }

        return tenant_details

    def update_tenant_storage(self, tenant_name: str, update_set={}, update_unset={}, return_document=False):
        """Update the storage of the give tenant."""
        try:
            update_set_storage = {}
            update_unset_storage = {}
            tenant_update = {}

            for key, value in update_set.items():
                update_set_storage[f"storage.{key}"] = value

            for key, value in update_unset.items():
                update_unset_storage[f"storage.{key}"] = value

            if update_set_storage:
                tenant_update["$set"] = update_set_storage

            if update_unset_storage:
                tenant_update["$unset"] = update_unset_storage

            return self.connector.collection(Collections.NETSKOPE_TENANTS).find_one_and_update(
                {"name": tenant_name},
                tenant_update,
                return_document=return_document,
            )
        except Exception as err:  # NOSONAR
            self.logger.error(
                f"Error occurred while updating tenant storage: {err}",
                details=traceback.format_exc(),
            )

    def replace_webtx_metrics(self, tenant: str, tenant_name: str, metrics: dict):
        """Replace the webtx metrics of the provided tenant."""
        return self.connector.collection(Collections.WEBTX_METRICS).replace_one(
            {"name": tenant_name},
            {"name": tenant_name, "tenant": tenant, **metrics,
                "last_updated_at": datetime.now(timezone.utc)},
            upsert=True,
        )

    def get_webtx_metrics(self, tenant_name: str):
        """Get the webtx metrics of the provided tenant."""
        return self.connector.collection(Collections.WEBTX_METRICS).find_one({"name": tenant_name}, {"_id": 0})

    def get_stored_field(self, field):
        """Get the stored field based on the provided field name.

        Args:
            field: The name of the field to retrieve from the NETSKOPE_FIELDS collection.

        Returns:
            The stored field document matching the provided field name.
        """
        return self.connector.collection(Collections.NETSKOPE_FIELDS).find_one(
            {"name": field}
        )

    def store_new_field(self, field, type_of_field, data_type: FieldDataType = FieldDataType.TEXT):
        """Update or insert a new field in the NETSKOPE_FIELDS collection.

        Args:
            field: The name of the field to be updated or inserted.
            type_of_field: The type of the field to be updated or inserted.

        Returns:
            None
        """
        self.connector.collection(Collections.NETSKOPE_FIELDS).update_one(
            {"name": field},
            {
                "$set": NetskopeField(
                    name=field,
                    label=field
                    if field.startswith("_")
                    else field.replace("_", " ").title(),
                    type=type_of_field,
                    dataType=data_type,
                ).model_dump()
            },
            upsert=True,
        )

    def is_netskope_plugin_enabled(self, tenant_name) -> bool:
        """Return true if any netskope plugin is enabled."""
        return any(
            self.connector.collection(module["collection"]).find_one(
                {"tenant": tenant_name, "active": True}
            )
            for module in modules
        )

    def is_module_enabled(self) -> bool:
        """Return true if any module is enabled."""
        return any(
            v
            for _, v in self.connector.collection(Collections.SETTINGS)
            .find_one({})["platforms"]
            .items()
        )

    def get_provider(self, tenant_name):
        """Create a provider object for the given tenant name.

        Args:
            tenant_name (str): The name of the tenant.

        Returns:
            The provider for the given tenant name or None if not found.
        """
        try:
            tenant = self.connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                {"name": tenant_name}
            )

            if tenant is None:
                raise Exception("Could not find the associated tenant.")  # NOSONAR

            ProviderClass = plugin_helper.find_by_id(tenant.get("plugin"))
            if not ProviderClass:
                self.logger.debug("Unable to find the provider class.")
                return None

            provider = ProviderClass(
                tenant["name"],
                SecretDict(tenant["parameters"]),
                tenant["storage"],
                None,
                self.logger,
            )

            return provider
        except Exception:
            import traceback
            self.logger.warn(
                "Error occurred while trying to create the provider object.", details=traceback.format_exc()
            )
            return None

    def get_all_configured_subtypes(self, tenant_name):
        """Get the subtypes for the given tenant name."""
        try:
            settings = self.connector.collection(Collections.SETTINGS).find_one({})
            tenant = self.connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                {"name": tenant_name}
            )
            if tenant is None:
                raise ValueError("Could not find the associated tenant.")

            modules = [
                {
                    "platform": "cls",
                    "collection": Collections.CLS_CONFIGURATIONS,
                    "collection_filter_query": {},
                    "data_types": [ALERTS, EVENTS],
                    "br_collection": Collections.CLS_BUSINESS_RULES,
                    "br_query": '{"siemMappings.%s": {"$exists": true}}'
                },
                {
                    "platform": "cte",
                    "collection": Collections.CONFIGURATIONS,
                    "collection_filter_query": {},
                    "data_types": [ALERTS],
                },
                {
                    "platform": "itsm",
                    "collection": Collections.ITSM_CONFIGURATIONS,
                    "collection_filter_query": {},
                    "data_types": [ALERTS, EVENTS],
                },
                {
                    "platform": "cre",
                    "collection": Collections.CREV2_CONFIGURATIONS,
                    "collection_filter_query": {},
                    "data_types": [ALERTS, EVENTS],
                }
            ]
            configured_sub_types = {ALERTS: [], EVENTS: []}
            for module in modules:
                if not settings.get("platforms", {}).get(module["platform"], False):
                    continue
                plugins = self.connector.collection(module["collection"]).find(
                    {"tenant": tenant_name, "active": True, **module["collection_filter_query"]}
                )
                for plugin in plugins:
                    if "br_collection" in module and not self.connector.collection(module["br_collection"]).find_one(
                        json.loads(module["br_query"] % plugin["name"])
                    ):
                        continue
                    plugin_class = plugin_helper.find_by_id(plugin.get("plugin"))
                    if not plugin_class:
                        continue
                    plugin_class_obj = plugin_class(
                        plugin.get("name"),
                        SecretDict(plugin.get("parameters")),
                        None,
                        None,
                        self.logger,
                    )
                    if plugin.get("mappedEntities"):
                        plugin_class_obj.mappedEntities = plugin.get("mappedEntities")
                    for data_type in module["data_types"]:
                        configured_sub_types[data_type].extend(
                            plugin_class_obj.get_types_to_pull(data_type)
                        )
            configured_sub_types[ALERTS] = list(set(configured_sub_types[ALERTS]))
            configured_sub_types[EVENTS] = list(set(configured_sub_types[EVENTS]))
            return configured_sub_types
        except Exception:
            self.logger.error(
                "Error occurred while trying to get the subtypes.",
                error_code="CE_1135",
                details=traceback.format_exc()
            )

    def check_and_update_forbidden_endpoints(self, tenant_name):
        """Check and update forbidden endpoints."""
        from .forbidden_notifier import create_or_ack_forbidden_error_banner
        if not tenant_name:
            return
        configured_types = self.get_all_configured_subtypes(tenant_name)
        configured_types = configured_types["events"] + configured_types["alerts"]
        tenant_details = self.get_tenant_details(tenant_name)
        update_unset = {}
        if tenant_details.get("storage", {}).get("forbidden_endpoints"):
            forbidden_alerts_events = tenant_details["storage"]["forbidden_endpoints"]
            for sub_type in forbidden_alerts_events:
                if sub_type not in configured_types:
                    update_unset[f"forbidden_endpoints.{sub_type}"] = ""
        if update_unset:
            self.update_tenant_storage(tenant_name, update_unset=update_unset)
            create_or_ack_forbidden_error_banner()


def convert_checkpoint(tenant_checkpoint):
    """Convert Checkpoint."""
    checkpoint = {}
    if ALERTS in tenant_checkpoint:
        checkpoint[ALERTS] = tenant_checkpoint[ALERTS]
    if EVENTS in tenant_checkpoint:
        checkpoint[EVENTS] = tenant_checkpoint[EVENTS]
    if "alert" in tenant_checkpoint:  # 3.x format of checkpoint
        checkpoint[ALERTS] = tenant_checkpoint["alert"]
    if "event" in tenant_checkpoint:  # 3.x format of checkpoint
        checkpoint[EVENTS] = tenant_checkpoint["event"]
    return checkpoint


def convert_tenant_format(tenant):
    """Convert the format of the given 'tenant' data into a new structure.

    Parameters:
        tenant (dict): A dictionary containing information about a tenant.

    Returns:
        dict: A dictionary with the converted format.
    """
    return {
        "name": tenant["name"],
        "plugin": tenant.get(
            "plugin", "netskope.plugins.Default.netskope_provider.main"
        ),
        "parameters": {
            "tenantName": (
                f"https://{tenant['tenantName'].strip()}.goskope.com"
                if "https" not in tenant["tenantName"]
                else tenant["tenantName"].strip().strip("/")
            ),
            "token": tenant.get("token"),
            "v2token": tenant.get("v2token"),
        },
        "pollInterval": tenant.get("pollInterval", 30),
        "pollIntervalUnit": tenant.get("pollIntervalUnit", "seconds"),
        "checkpoint": convert_checkpoint(tenant.get("checkpoint", {})),
        "lockedAt": tenant.get("lockedAt", {}),
        "storage": {
            "first_event_pull": tenant.get("first_event_pull", {}),
            "disabled_event_pull": tenant.get("disabled_event_pull", {}),
            "first_alert_pull": tenant.get("first_alert_pull", {}),
            "disabled_alert_pull": tenant.get("disabled_alert_pull", {}),
            "forbidden_endpoints": tenant.get("forbidden_endpoints", {}),
            "is_v2_token_expired": tenant.get("is_v2_token_expired", False),
        },
        "task": tenant.get("task", {}),
    }
