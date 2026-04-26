"""Tenant pull Historical alerts task."""
from .main import APP
from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
    PluginHelper,
    track,
    Notifier,
)

from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.utils.back_pressure import back_pressure_mechanism
from netskope.common.models import TenantDB

from netskope.integrations.cte.tasks.plugin_lifecycle_task import (
    execute_plugin,
)
from netskope.integrations.cls.tasks.plugin_lifecycle_task import (
    execute_plugin as cls_execute_plugin,
)
from netskope.integrations.itsm.tasks.pull_data_items import (
    pull_data_items as pull_itsm_data_items,
)
from netskope.integrations.crev2.tasks.fetch_records import fetch_records
from datetime import datetime, timedelta

from ..utils.validate_tenant import validate_tenant

helper = PluginHelper()
connector = DBConnector()
logger = Logger()
notifier = Notifier()


def get_nested_value(obj, key_string):
    """Get nested value from dictionary."""
    keys = key_string.split(".")
    current_value = obj
    for key in keys:
        if isinstance(current_value, dict) and key in current_value:
            current_value = current_value[key]
        else:
            return None
    return current_value


@APP.task(name="common.historical_alerts", acks_late=False)
@track()
def historical_alerts(
    tenant_name: str,
    collection: str,
    configuration_name: str,
    end_time: datetime,
    required_subtypes=None,
):
    """Pull historical alerts and execute the given Netskope configurations."""
    if not back_pressure_mechanism():
        return {"success": False}  # TODO: Need to add logs

    success, content = validate_tenant(tenant_name, check_v2_token=False)
    if not success:
        return content

    tenant = TenantDB(**content)

    modules = {
        Collections.CONFIGURATIONS: {
            "lifecycle": execute_plugin,
            "task": "cte.execute_plugin",
            "path": "parameters.days",
        },
        Collections.CLS_CONFIGURATIONS: {
            "lifecycle": cls_execute_plugin,
            "task": "cls.execute_plugin",
            "path": "parameters.days",
        },
        Collections.ITSM_CONFIGURATIONS: {
            "lifecycle": pull_itsm_data_items,
            "task": "itsm.pull_data_items",
            "path": "parameters.params.days",
        },
        Collections.CREV2_CONFIGURATIONS: {
            "lifecycle": fetch_records,
            "task": "cre.fetch_records",
            "path": "parameters.days",
        },
    }

    config_dict = connector.collection(collection).find_one({
        "name": configuration_name
    })
    days = get_nested_value(config_dict, f'{modules[collection]["path"]}')

    start_time = end_time - timedelta(days=days)
    if start_time == end_time:
        logger.info(f"Historical data pull for alerts has been skipped for '{configuration_name}' plugin,"
                    " because it is disabled from the configuration.")
        return {"success": True}
    if not required_subtypes:
        logger.info(
            f"Historical data pull for alerts has been skipped for '{configuration_name}' plugin,"
            f" because no alerts are selected in the configuration."
        )
        return {"success": True}

    iterator_name = f"{tenant_name}_{configuration_name}_{collection}_%s_historical"

    ProviderClass = helper.find_by_id(tenant.plugin)
    provider = ProviderClass(
        tenant.name, tenant.parameters, tenant.storage, datetime.now(), logger
    )

    pulled_data = provider.pull(
        "alerts",
        iterator_name,
        pull_type="historical_pulling",
        configuration_name=configuration_name,
        start_time=start_time,
        end_time=end_time,
        override_subtypes=required_subtypes,
        handle_forbidden=True,
        compress_historical_data=True
    )

    for data, sub_type, _, _ in pulled_data:
        if data:
            execute_celery_task(modules[collection]["lifecycle"].apply_async, modules[collection]["task"],
                                args=[configuration_name],
                                kwargs={"data": data, "data_type": "alerts", "sub_type": sub_type})

    return {"success": True}
