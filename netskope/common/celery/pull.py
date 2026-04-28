"""Tenant pull events task."""

import traceback
import time
from datetime import datetime
import os
from .main import APP
from celery.exceptions import SoftTimeLimitExceeded
from netskope.common.utils import (
    DBConnector,
    Collections,
    DataBatchManager,
    Logger,
    PluginHelper,
    track,
    get_sub_type_config_mapping,
    parse_events,
)
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.utils.back_pressure import back_pressure_mechanism
from netskope.common.utils.const import SOFT_TIME_LIMIT, TASK_TIME_LIMIT
from netskope.common.models import TenantDB, BatchDataSourceType
from netskope.common.celery.scheduler import execute_celery_task
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


MAX_MAINTENANCE_WINDOW_MINUTES = int(os.getenv("MAX_MAINTENANCE_WINDOW_MINUTES", 15))
EXPLICIT_PULLING_TASK_EXECUTION_FOR_HOUR = (
    os.environ.get("EXPLICIT_PULLING_TASK_EXECUTION_FOR_HOUR", "true").lower() == "true"
)

helper = PluginHelper()
connector = DBConnector()
batch_manager = DataBatchManager()
logger = Logger()
plugin_provider_helper = PluginProviderHelper()

modules = {  # create constant
    Collections.CONFIGURATIONS: {
        "lifecycle": execute_plugin,
        "task": "cte.execute_plugin",
        "lastrunAt": "lastRunAt.pull",
        "lastRunSuccess": "lastRunSuccess.pull",
        "batch_id_supported": False,
    },
    Collections.CLS_CONFIGURATIONS: {
        "lifecycle": cls_execute_plugin,
        "task": "cls.execute_plugin",
        "batch_id_supported": True,
    },
    Collections.ITSM_CONFIGURATIONS: {
        "lifecycle": pull_itsm_data_items,
        "task": "itsm.pull_data_items",
        "lastrunAt": "lastRunAt.pull",
        "lastRunSuccess": "lastRunSuccess.pull",
        "batch_id_supported": False,
    },
    Collections.CREV2_CONFIGURATIONS: {
        "lifecycle": fetch_records,
        "task": "cre.fetch_records",
        "batch_id_supported": False,
    },
}


@APP.task(name="common.pull", acks_late=False)
@track()
def pull(data_type: str, tenant_name: str, configuration_name: str = None):
    """Pull alerts and execute all the Netskope configurations."""
    logger.info(f"Initiating pull task for data type: '{data_type}'")
    if not back_pressure_mechanism():
        return {"success": False}

    start_time = time.time()

    tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
        {"name": tenant_name}
    )

    if not tenant:
        logger.debug(
            f"Tenant with name {tenant_name} no longer exists.", error_code="CE_1029"
        )
        return {
            "success": False,
            "message": f"Tenant {tenant_name} does not exist",
            "error_code": "CE_1029",
        }

    tenant = TenantDB(**tenant)

    if data_type == "webtx":
        try:
            provider = plugin_provider_helper.get_provider(tenant_name)
            for _ in provider.pull(data_type, configuration_name=configuration_name):
                pass
            return {"success": True}
        except SoftTimeLimitExceeded:
            raise
        except Exception:
            logger.error("Failed to pull data from webtx.", details=traceback.format_exc())
            raise

    sub_type_config_mapping, latest_checked = get_sub_type_config_mapping(
        tenant_name, data_type
    )

    if not sub_type_config_mapping:
        logger.info(
            f"No {data_type} will be pulled from tenant {tenant_name} because no {data_type} are selected."
        )
        return {"success": True}

    checkpoints = tenant.checkpoint
    data_type_checkpoint = checkpoints.get(data_type)

    ProviderClass = helper.find_by_id(tenant.plugin)
    provider = ProviderClass(
        tenant.name, tenant.parameters, tenant.storage, data_type_checkpoint, logger
    )

    try:
        pulled_data = provider.pull(data_type)
        should_apply_expo_backoff = False
        for data, data_sub_type, sub_type_config_mapping, is_expo_backoff in pulled_data:
            should_apply_expo_backoff = should_apply_expo_backoff or is_expo_backoff
            temp_data = data
            if isinstance(data, bytes):
                temp_data = parse_events(data)
            data_count = len(
                temp_data.get("result", [])
                if isinstance(temp_data, dict)
                else temp_data
            )
            batch = batch_manager.create(
                data_type,
                data_sub_type,
                data_count,
                tenant_name,
                BatchDataSourceType.TENANT,
            )
            for module in modules:
                for configuration in connector.collection(module).find(
                    {"tenant": tenant_name}
                ):
                    if configuration["name"] not in sub_type_config_mapping.get(
                        data_sub_type, set()
                    ):
                        continue
                    PluginClass = helper.find_by_id(configuration["plugin"])  # NOSONAR
                    if not PluginClass or not configuration["active"]:
                        continue
                    kwargs = {
                        "data": data,
                        "data_type": data_type,
                        "sub_type": data_sub_type,
                    }
                    if modules[module]["batch_id_supported"]:
                        kwargs["batch_id"] = batch["_id"]
                    if data:
                        logger.debug(
                            f"Executing plugin lifecycle for {configuration['name']} configuration."
                        )
                        execute_celery_task(
                            modules[module]["lifecycle"].apply_async,
                            modules[module]["task"],
                            args=[configuration["name"]],
                            kwargs=kwargs,
                            soft_time_limit=SOFT_TIME_LIMIT,
                            time_limit=TASK_TIME_LIMIT,
                        )
                    else:
                        connector.collection(module).update_one(
                            {"name": configuration["name"]},
                            {
                                "$set": {
                                    modules[module].get("lastrunAt", "lastRunAt"): datetime.now(),
                                    modules[module].get("lastRunSuccess", "lastRunSuccess"): True,
                                }
                            },
                        )

        end_time = time.time()
        time_delta = 3600 - (end_time - start_time)
        # if EXPLICIT_PULLING_TASK_EXECUTION_FOR_HOUR and client.pulling_started and time_delta > 0:
        #     time.sleep(time_delta)
        if EXPLICIT_PULLING_TASK_EXECUTION_FOR_HOUR and time_delta > 0 and should_apply_expo_backoff:
            time.sleep(time_delta)
        logger.info(f"The pull task completed for data type: '{data_type}'")
        return {"success": True}
    except SoftTimeLimitExceeded:
        logger.error(
            "Terminating the common.pull task; the time limit exceeded.",
            error_code="CE_1113",
        )
        return {"success": False}
