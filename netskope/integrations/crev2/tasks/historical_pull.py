"""Fetch historical app data."""
import gzip
import json
import time
from datetime import datetime

from py_expression_eval import Parser

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import TenantDB
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    integration,
    track,
)
from netskope.common.utils.back_pressure import back_pressure_mechanism

from ..tasks.fetch_records import fetch_records

connector = DBConnector()
helper = PluginHelper()
logger = Logger()
parser = Parser()


def _end_life(name: str, success: bool):
    """Update the lastRunSuccess and lastRunAt.

    Args:
        name (str): Name of the configuration.
        success (bool): lastRunSuccess value to be updated.
    """
    connector.collection(Collections.CREV2_CONFIGURATIONS).update_one(
        {"name": name},
        {
            "$set": {
                "lastRunAt": datetime.now(),
                "lastRunSuccess": success,
            }
        },
    )


@APP.task(name="cre.historical_appdata", acks_late=False)
@integration("cre")
@track()
def historical_appdata(
    tenant_name: str,
    configuration_name: str,
    start_time: datetime,
    end_time: datetime,
    retries_remaining: int = 3,
):
    """Pull historical application data, execute the given Netskope configurations and share the applications."""
    if not back_pressure_mechanism():
        logger.error(
            "Backpressure mechanism triggered. Skipping historical appdata pulling task."
        )
        return {"success": False, "message": "Back pressure triggered."}

    worker_start_time = int(time.time())
    tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
        {"name": tenant_name}
    )
    if tenant is None:
        logger.error(
            f"Tenant with name '{tenant_name}' no longer exists. Skipping historical appdata pulling task.",
            error_code="ARE_1023",
        )
        return {
            "success": False,
            "message": f"Tenant {tenant_name} does not exist",
        }
    tenant = TenantDB(**tenant)

    configuration = connector.collection(
        Collections.CREV2_CONFIGURATIONS
    ).find_one({"name": configuration_name})
    PluginClass = helper.find_by_id(configuration["plugin"])  # NOSONAR
    if (
        not PluginClass
        or not PluginClass.metadata.get("netskope", False)
        or configuration["active"] is False
    ):
        logger.info(
            f"Configuration with name '{configuration_name}' does not exist or is disabled."
        )
        return {"success": True}

    event_types = ["application"]
    total_events = 0
    iterator_name = f"{tenant.name}_{configuration_name}_ARE_%s_historical"

    ProviderClass = helper.find_by_id(tenant.plugin)
    provider = ProviderClass(
        tenant.name, tenant.parameters, tenant.storage, datetime.now(), logger
    )

    pulled_data = provider.pull(
        "events",
        iterator_name,
        pull_type="historical_pulling",
        configuration_name=configuration_name,
        start_time=start_time,
        end_time=end_time,
        override_subtypes=event_types,
        compress_historical_data=False,
    )

    for events, _, _, _ in pulled_data:
        if not events:
            continue
        total_events += len(events)
        execute_celery_task(
            fetch_records.apply_async,
            "cre.fetch_records",
            args=[configuration_name],
            kwargs={
                "data": gzip.compress(
                    json.dumps({"result": events}).encode("utf-8"),
                    compresslevel=3,
                ),
                "data_type": "events",
                "sub_type": "application",
            },
        )

    _end_life(configuration_name, True)
    worker_end_time_iterator = int(time.time())

    logger.info(
        f"Historical event pull has been completed for the window for {configuration_name}. "
        f"Total Events: {total_events} Time taken: {worker_end_time_iterator-worker_start_time} seconds."
    )
    return {"success": True}
