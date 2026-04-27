"""Provides task for updating incidents."""
import re
import traceback
from datetime import datetime
from typing import List

from pymongo import UpdateOne

from netskope.common.celery.main import APP
from netskope.common.utils import (
    DBConnector,
    Collections,
    integration,
    PluginHelper,
    Logger,
    track,
    SecretDict,
)
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.utils.alerts_helper import AlertsHelper
from netskope.integrations.itsm.models import (
    ConfigurationDB, Task, SyncStatus, TaskStatus, TaskRequestStatus
)
from netskope.integrations.itsm.plugin_base import PushResult
from netskope.integrations.itsm.utils.custom_mapping_utils import ce_to_tenant_task_map


connector = DBConnector()
logger = Logger()
helper = PluginHelper()
alerts_helper = AlertsHelper()
plugin_provider_helper = PluginProviderHelper()


def _end_life(name, success):
    """End the life.

    Args:
        name (str): Name of the configuration.
        success (bool): Whether it was a success or not.
    """
    connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
        {"name": name},
        {
            "$set": {
                "lastRunAt": datetime.now(),
                "lastRunSuccess": success,
            }
        },
    )
    return success


def _get_incidents_to_update(
    configuration: ConfigurationDB
) -> List[Task]:
    tasks = connector.collection(Collections.ITSM_TASKS).find(
        {
            "dataType": "event",
            "status": {"$ne": TaskStatus.FAILED},
            "approvalStatus": {"$in": [TaskRequestStatus.APPROVED, TaskRequestStatus.NOT_REQUIRED]},
            "dataItem.configuration": configuration.name,
            "dataItem.eventType": "incident",
            "$expr": {
                "$or": [
                    {
                        "$and": [
                            {"$ne": ["$lastUpdatedAt", None]},
                            {
                                "$or": [
                                    {"$eq": ["$lastSyncedAt", None]},
                                    {"$gte": ["$lastUpdatedAt", "$lastSyncedAt"]}
                                ]
                            }
                        ]
                    },
                    {"$eq": ["$syncStatus", SyncStatus.FAILED]},
                ]
            }
        }
    )
    return [Task(**task) for task in tasks]


def _update_timestamp_in_results(
    tasks: List[Task], timestamp: datetime, status: SyncStatus = SyncStatus.SUCCESS
):
    update_queries = []
    for task in tasks:
        update_queries.append(
            UpdateOne(
                {"id": task.id},
                {"$set": {"lastSyncedAt": timestamp, "syncStatus": status}},
            )
        )
    if update_queries:
        return (connector.collection(Collections.ITSM_TASKS).bulk_write(update_queries)).modified_count
    return 0


@APP.task(name="itsm.update_incidents", acks_late=False)
@integration("itsm")
@track()
def update_incidents(
    configuration_name: str
):
    """Update back the incidents for the given configuration.

    Args:
        configuration_name (str): Name of the configuration.
    """
    logger.update_level()
    configuration = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
        {"name": configuration_name}
    )
    if configuration is None:
        return False
    configuration = ConfigurationDB(**configuration)
    if configuration.active is False:
        return False

    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR
    if PluginClass is None:
        logger.info(
            f"Plugin with ID {configuration.plugin} does not exist. Skipping itsm.update_incidents task.",
            error_code="CTO_1034",
        )
        return _end_life(configuration.name, False)

    plugin = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        configuration.storage,
        configuration.checkpoint,
        logger,
    )
    checkpoint = datetime.now()

    try:
        tasks = _get_incidents_to_update(configuration)
        if not tasks:
            logger.info(
                f"No incidents to update for the configuration {configuration.name}."
            )
            return _end_life(configuration.name, True)
        mapped_tasks = ce_to_tenant_task_map(tasks, configuration)
        results: PushResult = plugin.sync_incidents(mapped_tasks)
        total_count = len(mapped_tasks)
        success_count = 0
        failed_count = 0
        if results is None or not results.success:
            logger.info(
                f"Could not update incidents for configuration {configuration.name}. "
                f"{re.sub(r'token=([0-9a-zA-Z]*)', 'token=********&', results.message)}",
                details=re.sub(
                    r"token=([0-9a-zA-Z]*)", "token=********&", results.message
                ),
                error_code="CTO_1045",
            )
            return _end_life(configuration.name, False)

        if results.results["failed"] and results.results["success"]:
            failed_set = {task.id for task in results.results["failed"]}
            cleaned_success_list = [
                task for task in results.results["success"] if task.id not in failed_set
            ]
            results.results["success"] = cleaned_success_list

        if results.results["success"]:
            success_count = _update_timestamp_in_results(
                results.results["success"], checkpoint, status=SyncStatus.SUCCESS
            )
        if results.results["failed"]:
            failed_count = _update_timestamp_in_results(
                results.results["failed"], checkpoint, status=SyncStatus.FAILED
            )
        connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
            {"name": configuration.name}, {"$set": {"checkpoint": checkpoint, "storage": plugin.storage or {}}}
        )
        if success_count:
            logger.info(
                f"{success_count} incident(s) successfully updated out of "
                f"{total_count} for configuration {configuration.name}."
            )
        if failed_count:
            logger.info(
                f"{failed_count} incident(s) update failed out of "
                f"{total_count} for configuration {configuration.name}."
            )
    except NotImplementedError:
        logger.error(
            f"Could not update incidents for configuration {configuration_name}. "
            "Plugin does not implement sync_incidents method.",
            details=traceback.format_exc(),
            error_code="CTO_1035",
        )
        return _end_life(configuration.name, False)
    except Exception:
        logger.error(
            "An exception occurred while updating incidents"
            f" for configuration {configuration_name}.",
            details=traceback.format_exc(),
            error_code="CTO_1036",
        )
        return _end_life(configuration.name, False)
    return _end_life(configuration.name, True)
