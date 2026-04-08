"""Provides task for syncing states."""

from __future__ import absolute_import, unicode_literals
from datetime import datetime
from typing import List
import traceback
from pymongo import UpdateOne
from netskope.common.celery.main import APP
from netskope.integrations.itsm.utils.custom_mapping_utils import plugin_to_ce_task_map
from netskope.common.utils import (
    DBConnector,
    Collections,
    integration,
    Logger,
    PluginHelper,
    track,
    SecretDict,
)


import netskope.plugins.Default
from netskope.integrations.itsm.models import (
    ConfigurationDB,
    Task,
    TaskStatus,
    SyncStatus,
    TaskRequestStatus,
)

connector = DBConnector()
logger = Logger()
helper = PluginHelper().add_packages([netskope.plugins.Default]).refresh()
helper = PluginHelper()

TASK_UPDATE_CHECK_FIELDS = [
    "status",
    "severity",
    "assignee"
]


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


def _check_if_task_is_updated(old_task, new_task):
    if old_task:
        for key in TASK_UPDATE_CHECK_FIELDS:
            if (
                old_task.get("updatedValues")
                and new_task.get("updatedValues")
                and old_task.get("updatedValues", {}).get(key) != new_task.get("updatedValues", {}).get(key)
            ):
                return True
    return False


def _update_tasks(tasks: List[Task], configuration_name) -> None:
    BATCH_SIZE = 1000
    for start_idx in range(0, len(tasks), BATCH_SIZE):
        task_batch = tasks[start_idx:start_idx+BATCH_SIZE]
        try:
            updates = []
            old_tasks = connector.collection(Collections.ITSM_TASKS).find(
                {"id": {"$in": [task.id for task in task_batch]}}
            )
            old_tasks = {task["id"]: task for task in old_tasks}

            for task in task_batch:
                old_task = old_tasks.get(task.id)
                is_updated = _check_if_task_is_updated(old_task, task.model_dump())

                if task.status == TaskStatus.DELETED and task.deletedAt is None:
                    updates.append(
                        UpdateOne(
                            {"id": task.id},
                            {"$set": {
                                "status": task.status,
                                "severity": task.severity,
                                "deletedAt": datetime.now(),
                                **(
                                    {
                                        "lastUpdatedAt": datetime.now(),
                                        "syncStatus": SyncStatus.PENDING,
                                        "updatedValues": task.updatedValues.model_dump() if task.updatedValues else None
                                    }
                                    if is_updated
                                    else {}
                                ),
                            }},
                        )
                    )
                elif task.status != TaskStatus.DELETED:
                    updates.append(
                        UpdateOne(
                            {"id": task.id},
                            {"$set": {
                                "status": task.status,
                                "severity": task.severity,
                                "deletedAt": None,
                                **(
                                    {
                                        "lastUpdatedAt": datetime.now(),
                                        "syncStatus": SyncStatus.PENDING,
                                        "updatedValues": task.updatedValues.model_dump() if task.updatedValues else None
                                    }
                                    if is_updated
                                    else {}
                                )
                            }},
                        )
                    )

            if updates:
                connector.collection(Collections.ITSM_TASKS).bulk_write(updates)
            if len(tasks) > BATCH_SIZE:
                logger.info(
                    f"Updated tasks {len(task_batch)} out of {len(tasks)}"
                    f" for configuration '{configuration_name}'."
                )
        except Exception as error:
            logger.error(
                f"Error while updating tasks {len(task_batch)} out of "
                f"{len(tasks)} for configuration '{configuration_name}'. {error}",
                error_code="CTO_1019",
                details=traceback.format_exc(),
            )


@APP.task(name="itsm.sync_states")
@integration("itsm")
@track()
def sync_states(name: str):
    """Synchronize task states."""
    configuration = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        return False
    configuration = ConfigurationDB(**configuration)

    if configuration.active is False:
        return False

    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR
    if PluginClass is None:
        logger.info(
            f"Plugin with ID {configuration.plugin} does not exist. Skipping itsm.sync_states task.",
            error_code="CTO_1018",
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
    tasks = [
        Task(**t)
        for t in connector.collection(Collections.ITSM_TASKS).find(
            {
                "configuration": configuration.name,
                "approvalStatus": {
                    "$in": [TaskRequestStatus.APPROVED.value, TaskRequestStatus.NOT_REQUIRED.value]
                },
                "status": {"$ne": TaskStatus.FAILED.value},
            }
        )
    ]
    if not tasks:
        logger.info(f"No tasks to be updated for configuration {configuration.name}.")
        return _end_life(configuration.name, True)
    logger.info(f"Syncing states for tasks with configuration {configuration.name}.")
    try:
        tasks = [task for task in tasks if task.id is not None]
        updated_tasks = plugin.sync_states(tasks)
        if not updated_tasks:
            logger.info(
                f"sync_states returned invalid result for configuration {configuration.name}."
            )
            return _end_life(configuration.name, False)
        updated_tasks = [
            plugin_to_ce_task_map(task, configuration)
            for task in updated_tasks
        ]

        connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
            {"name": configuration.name}, {"$set": {"checkpoint": checkpoint, "storage": plugin.storage}}
        )
    except NotImplementedError:
        logger.error(
            "Could not sync states. Plugin does not implement sync_states method.",
            details=traceback.format_exc(),
            error_code="CTO_1044",
        )
        return _end_life(configuration.name, False)
    except Exception:
        logger.error(
            "Could not sync states. An exception occurred.",
            details=traceback.format_exc(),
            error_code="CTO_1020",
        )
        return _end_life(configuration.name, False)
    _update_tasks(updated_tasks, configuration.name)
    logger.info(
        f"{len(updated_tasks)} tasks successfully updated for configuration {configuration.name}."
    )
    return _end_life(configuration.name, True)
