"""Perform action on sync interval or specific time."""

import traceback
from functools import partial
from datetime import datetime, timezone
from netskope.common.utils import (
    DBConnector,
    Collections,
    track,
    integration,
    Logger,
)
from netskope.common.models import SettingsDB
from netskope.common.celery.main import APP
from netskope.integrations.crev2.models import (
    ActionLogStatus,
    RevertActionStatus,
    ActionLogDB,
    get_plugin_from_configuration_name,
)
from ..plugin_base import PluginBase, ActionResult

connector = DBConnector()
logger = Logger()


def _set_status(_id, updates={}):
    """Set the fields of an action log."""
    connector.collection(Collections.CREV2_ACTION_LOGS).update_one(
        {"_id": _id},
        {
            "$set": updates,
        },
    )


@APP.task(name="cre.perform_action")
@integration("cre")
@track()
def perform_action():
    """Perform action on sync interval.

    Args:
        syncInterval (int, optional): sync interval for schedular. Defaults to None.
        syncIntervalUnit (str, optional): syncIntervalUnit for schedular. Defaults to None.
        performAt (datetime, optional): date to start performing action. Defaults to None.
    """
    find_query = {
        "$or": [{"status": ActionLogStatus.SCHEDULED}, {"action.performRevert": True}]
    }
    cursor = connector.collection(Collections.CREV2_ACTION_LOGS).find(find_query)
    settings = connector.collection(Collections.SETTINGS).find_one({})
    settings = SettingsDB(**settings)
    actions_batch = {}

    for log in cursor:
        action_log = ActionLogDB(**log)

        plugin = get_plugin_from_configuration_name(action_log.configuration)
        # get current day of the week
        today = datetime.now(timezone.utc).weekday()
        if (
            today in settings.cre.maintenanceDays
            and (
                settings.cre.startTime.strftime("%H:%M:%S")
                < datetime.now(timezone.utc).strftime("%H:%M:%S")
                < settings.cre.endTime.strftime("%H:%M:%S")
            )
            or (
                settings.cre.endTime.strftime("%H:%M:%S")
                < settings.cre.startTime.strftime("%H:%M:%S")
                and not (
                    settings.cre.endTime.strftime("%H:%M:%S")
                    < datetime.now(timezone.utc).strftime("%H:%M:%S")
                    < settings.cre.startTime.strftime("%H:%M:%S")
                )
            )
        ):
            log_func = partial(_set_status, log["_id"])
            actions_batch.setdefault(
                (
                    action_log.configuration,
                    action_log.action.value,
                ),
                [],
            ).append(
                {
                    "params": action_log.action,
                    "id": str(log["_id"]),
                    "log_func": log_func,
                    "record": action_log.record,
                    "error_log_message": (
                        f"Error occurred while performing the {action_log.action.value} action on "
                        f"record with id {action_log.record['_id']}."
                    ),
                }
            )
        else:
            logger.info(
                f"Action will be perform on next cycle for record with id {action_log.record['_id']}."
            )

    def _execute_single_action(plugin, action):
        """Execute a single action."""
        is_revert = getattr(action["params"], "performRevert", False)
        try:
            plugin.execute_action(action["params"])
            if is_revert:
                logger.info(
                    f"Successfully reverted {action['params'].value} action on record "
                    f"with id {action['record'].get('_id', 'unknown')}."
                )
                action["log_func"](
                    updates={
                        "revertActionParameters": {
                            "revertActionStatus": RevertActionStatus.SUCCESS,
                            "revertPerformedAt": datetime.now(),
                        },
                        "action.performRevert": False,
                        "action.parameters": action["params"].model_dump().get("parameters", {}),
                    }
                )
            else:
                logger.info(
                    f"Successfully performed {action['params'].value} action on"
                    f" record with id {action['record'].get('_id', 'unknown')}."
                )
                action["log_func"](
                    updates={
                        "status": ActionLogStatus.SUCCESS,
                        "performedAt": datetime.now(),
                        "action.parameters": action["params"].model_dump().get("parameters", {}),
                    }
                )
        except Exception:
            if is_revert:
                logger.error(
                    f"Failed to revert {action['params'].value} action on "
                    f"record with id {action['record'].get('_id', 'unknown')}.",
                    details=traceback.format_exc(),
                )
            else:
                logger.error(
                    action["error_log_message"],
                    details=traceback.format_exc(),
                )
            if is_revert:
                action["log_func"](
                    updates={
                        "revertActionParameters": {
                            "revertActionStatus": RevertActionStatus.FAILED,
                            "revertPerformedAt": datetime.now(),
                        },
                        "action.performRevert": False
                    }
                )
            else:
                action["log_func"](
                    updates={
                        "status": ActionLogStatus.FAILED,
                        "performedAt": datetime.now(),
                    }
                )

    for metadata, actions in actions_batch.items():
        configuration, _ = metadata
        plugin = get_plugin_from_configuration_name(configuration)
        if plugin.execute_actions == PluginBase.execute_actions:
            # method has not been implemented in the plugin
            # execute the actions individually instead
            for action in actions:
                _execute_single_action(plugin, action)
        else:
            try:
                action_type = actions[0]["params"].value
                logger.info(
                    f"Performing {action_type} action on batch with {len(actions)} records."
                )
                # Execute actions with either full action objects or just params based on plugin flag
                if hasattr(plugin, "provide_action_id") and plugin.provide_action_id:
                    result = plugin.execute_actions(
                        [{"params": action["params"], "id": action["id"]} for action in actions]
                    )
                else:
                    result = plugin.execute_actions([action["params"] for action in actions])

                # Handle partial success reporting
                if result and isinstance(result, ActionResult):
                    for action in actions:
                        is_revert = getattr(action["params"], "performRevert", False)
                        action_failed = not result.success or action["id"] in result.failed_action_ids
                        if is_revert:
                            action["log_func"](
                                updates={
                                    "revertActionParameters": {
                                        "revertActionStatus": (
                                            RevertActionStatus.FAILED
                                            if action_failed
                                            else RevertActionStatus.SUCCESS
                                        ),
                                        "revertPerformedAt": datetime.now(),
                                    },
                                    "action.performRevert": False,
                                    "action.parameters": action["params"].model_dump().get("parameters", {}),
                                }
                            )
                        else:
                            action["log_func"](
                                updates={
                                    "status": ActionLogStatus.FAILED if action_failed else ActionLogStatus.SUCCESS,
                                    "performedAt": datetime.now(),
                                    "action.parameters": action["params"].model_dump().get("parameters", {}),
                                }
                            )
                    logger.info(
                        f"Successfully {'performed' if not is_revert else 'reverted'} {action_type} action on batch "
                        f"with {len(actions) - len(result.failed_action_ids)} records. "
                        f"{len(result.failed_action_ids)} records failed out of {len(actions)}."
                    )
                else:
                    # All succeeded (result is None or success=True with no failed_actions)
                    for action in actions:
                        is_revert = getattr(action["params"], "performRevert", False)
                        if is_revert:
                            action["log_func"](
                                updates={
                                    "revertActionParameters": {
                                        "revertActionStatus": RevertActionStatus.SUCCESS,
                                        "revertPerformedAt": datetime.now(),
                                    },
                                    "action.performRevert": False,
                                    "action.parameters": action["params"].model_dump().get("parameters", {}),
                                }
                            )
                        else:
                            action["log_func"](
                                updates={
                                    "status": ActionLogStatus.SUCCESS,
                                    "performedAt": datetime.now(),
                                    "action.parameters": action["params"].model_dump().get("parameters", {}),
                                }
                            )
                    logger.info(
                        f"Successfully {'performed' if not is_revert else 'reverted'} {action_type} action on "
                        f"batch with {len(actions)} records."
                    )
            except NotImplementedError:
                for action in actions:
                    _execute_single_action(plugin, action)
            except Exception:
                action_type = actions[0]["params"].value
                logger.error(
                    f"Failed to perform batch {action_type} action on {len(actions)} records.",
                    error_code="CRE_1037",
                    details=traceback.format_exc(),
                )
                for action in actions:  # mark all as failed if there is an error
                    is_revert = getattr(action["params"], "performRevert", False)
                    if is_revert:
                        action["log_func"](
                            updates={
                                "revertActionParameters": {
                                    "revertActionStatus": RevertActionStatus.FAILED,
                                    "revertPerformedAt": datetime.now(),
                                },
                                "action.performRevert": False
                            }
                        )
                    else:
                        action["log_func"](
                            updates={
                                "status": ActionLogStatus.FAILED,
                                "performedAt": datetime.now(),
                            }
                        )
