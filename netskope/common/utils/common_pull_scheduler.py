"""Common pull scheduler."""
import traceback
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.utils.scheduler import Scheduler
from netskope.common.utils.db_connector import DBConnector, Collections
from netskope.common.utils import Logger

connector = DBConnector()
plugin_provider_helper = PluginProviderHelper()
scheduler = Scheduler()
logger = Logger()


def schedule_or_delete_common_pull_tasks(tenant_name=None):
    """Schedule or delete common pull tasks for alerts and events."""
    try:
        tenant_filter = {}
        if tenant_name:
            tenant_filter = {"name": tenant_name}
        for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find(tenant_filter):
            try:
                data = plugin_provider_helper.get_all_configured_subtypes(tenant["name"])
                for data_type, sub_types in data.items():
                    if sub_types:
                        scheduler.upsert(
                            name=f"tenant.{tenant['name']}.{data_type}",
                            task_name="common.pull",
                            poll_interval=tenant["pollInterval"],
                            poll_interval_unit=tenant["pollIntervalUnit"],
                            args=[data_type, tenant["name"]],
                        )
                    else:
                        scheduler.delete(f"tenant.{tenant['name']}.{data_type}")
                        logger.debug(f"Skipped creating {data_type} pull tasks for tenant '{tenant['name']}'")
            except Exception:
                logger.error(
                    f"Error occurred while creating common pull tasks for {tenant['name']}",
                    error_code="CE_1133",
                    details=traceback.format_exc()
                )
    except Exception as error:
        logger.error(
            "Error occurred while creating common pull tasks",
            error_code="CE_1134",
            details=traceback.format_exc()
        )
        raise error


if __name__ == "__main__":
    schedule_or_delete_common_pull_tasks()
