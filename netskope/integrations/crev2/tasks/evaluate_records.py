"""Evaluate fetched records."""

import copy
import json
import random
import string
import traceback
from functools import partial
from bson.objectid import ObjectId
from datetime import datetime, timedelta, UTC
from typing import Any

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import SettingsDB
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    integration,
    parse_dates,
    track,
)
from netskope.integrations.itsm.models import Alert
from netskope.integrations.itsm.tasks.pull_data_items import store_cre_alerts

from ..models import (
    Action,
    ActionLogDB,
    ActionLogStatus,
    BusinessRuleDB,
    get_entity_by_name,
    get_plugin_from_configuration_name,
)
from ..utils import build_pipeline_from_entity
from ..plugin_base import PluginBase, ActionResult

connector = DBConnector()
helper = PluginHelper()
logger = Logger()
RECORD_BATCH_SIZE = 1000


def _evaluate_records(
    records: list,
    rules: list,
    only_configuration: str,
    only_action: str,
    is_manual: bool,
) -> dict:
    actions_batch = {}
    settings = connector.collection(Collections.SETTINGS).find_one({})
    settings = SettingsDB(**settings)
    for rule in rules:
        logger.debug(f"Evaluating {rule.name} for {len(records)} records.")
        entity = get_entity_by_name(rule.entity)
        query = json.loads(
            rule.entityFilters.mongo,
            object_hook=parse_dates,
        )
        pipeline = (
            [
                {"$match": {"_id": {"$in": records}}}
            ]  # only apply on selected records
        ) + build_pipeline_from_entity(
            entity
        )  # perform the joins
        pipeline_with_filters = pipeline + [
            {"$match": query}
        ]  # apply the query
        matched_records = []
        all_eval_actions = set()
        for configuration, actions in rule.actions.items():
            for action in actions:
                all_eval_actions.add(
                    f"{rule.name}-{configuration}-{action.value}"
                )
        alerts = []
        for record in connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.name}"
        ).aggregate(pipeline_with_filters):
            for configuration, actions in rule.actions.items():
                if only_configuration and configuration != only_configuration:
                    continue
                plugin = get_plugin_from_configuration_name(configuration)
                for action in actions:
                    if only_action and action.value != only_action:
                        continue
                    # remember that this record is matching to un-mark the non-matching records later
                    matched_records.append(record["_id"])
                    # remember them to unmark these actions later
                    if (
                        not is_manual  # if it's manual, always perform
                        and f"{rule.name}-{configuration}-{action.value}"
                        in record.get("lastEvals", [])
                    ):
                        # this record previously matched this business rule which
                        # resulted in this action being performed. Do not execute this action
                        # again.
                        logger.debug(
                            f"Record with _id {str(record['_id'])} matched the "
                            f"rule {rule.name} during last eval as well. Will not be performing action again."
                        )
                        continue
                    try:
                        params = _map_params(action, record)
                        if action.requireApproval:
                            logger.info(
                                f"Scheduling the {action.value} action for record with id {record['_id']} for approval."
                            )
                            _log_action(
                                entity.name,
                                record,
                                rule.name,
                                configuration,
                                params,
                                status=ActionLogStatus.PENDING_APPROVAL,
                                performedAt=datetime.now(),
                            )
                        elif (
                            not (
                                (
                                    settings.cre.startTime.strftime("%H:%M:%S")
                                    < datetime.now(UTC).strftime("%H:%M:%S")
                                    < settings.cre.endTime.strftime("%H:%M:%S")
                                )
                                or (
                                    settings.cre.endTime.strftime("%H:%M:%S")
                                    < settings.cre.startTime.strftime(
                                        "%H:%M:%S"
                                    )
                                    and not (
                                        settings.cre.endTime.strftime(
                                            "%H:%M:%S"
                                        )
                                        < datetime.now(UTC).strftime(
                                            "%H:%M:%S"
                                        )
                                        < settings.cre.startTime.strftime(
                                            "%H:%M:%S"
                                        )
                                    )
                                )
                            )
                            and action.performLater
                        ):  # action should be performed later
                            logger.info(
                                f"Scheduling the {action.value} action for record with id {record['_id']},"
                                f" which will be performed during maintenance window"
                            )
                            _log_action(
                                entity.name,
                                record,
                                rule.name,
                                configuration,
                                params,
                                status=ActionLogStatus.SCHEDULED,
                                performedAt=datetime.now(),
                            )
                        else:
                            actions_batch.setdefault(
                                (
                                    configuration,
                                    action.value,
                                ),
                                [],
                            ).append(
                                {
                                    "params": params,
                                    "id": str(ObjectId()),
                                    "log_func": partial(
                                        _log_action,
                                        entity.name,
                                        record,
                                        rule.name,
                                        configuration,
                                    ),
                                    "before_log_message": (
                                        f"Performing the {action.value} action on record with "
                                        f"id {record['_id']}"
                                    ),
                                    "after_log_message": (
                                        f"Successfully performed the {action.value} action on record with "
                                        f"id {record['_id']}"
                                    ),
                                    "error_log_message": (
                                        f"Error occurred while performing the {action.value} action "
                                        f"on record with id {record['_id']}."
                                    ),
                                }
                            )
                    except Exception:
                        logger.error(
                            f"Error occurred while performing the {action.value} action "
                            f"on record with id {record['_id']}.",
                            details=traceback.format_exc(),
                        )
                        _log_action(
                            entity.name,
                            record,
                            rule.name,
                            configuration,
                            action,
                            status=ActionLogStatus.FAILED,
                            performedAt=datetime.now(),
                        )
                    finally:
                        if action.generateAlert:
                            alerts.append(
                                Alert(
                                    id="".join(
                                        random.SystemRandom().choice(
                                            string.hexdigits
                                        )
                                        for _ in range(24)
                                    ),
                                    configuration="CRE",
                                    alertName=rule.name,
                                    alertType="CRE",
                                    app="CRE",
                                    appCategory="CRE",
                                    type="CRE",
                                    timestamp=datetime.now(),
                                    rawData={
                                        "plugin": plugin.metadata.get("name"),
                                        "action": action.label,
                                    }
                                    | params.parameters,
                                )
                            )
                    # TODO: update storage
        # mark all the records that did match
        if matched_records:
            connector.collection(
                f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.name}"
            ).update_many(
                {"_id": {"$in": matched_records}},
                {
                    "$addToSet": {
                        "lastEvals": {"$each": list(all_eval_actions)}
                    }
                },
            )
        # unmark all the records that previously matched but now did not
        unmark_records = set(records) - set(matched_records)
        records_to_unmark = []
        for item in all_eval_actions:
            records_to_unmark += connector.collection(
                f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.name}"
            ).distinct(
                "_id",
                {
                    "_id": {"$in": list(unmark_records)},
                    "lastEvals": item,
                },
            )

        if records_to_unmark:
            result = connector.collection(
                f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.name}"
            ).update_many(
                {"_id": {"$in": list(records_to_unmark)}},
                {"$pull": {"lastEvals": item}},
            )
            logger.debug(
                f"Unmarked {result.modified_count} record(s) as they do not match the business rule {rule.name} now."
            )
            _execute_undos(list(records_to_unmark), entity, rule)
        else:
            logger.debug(f"No records to unmark for the business rule {rule.name}.")
        if alerts:
            execute_celery_task(
                store_cre_alerts.apply_async,
                "itsm.store_cre_alerts",
                args=[alerts],
            )
    return actions_batch


def _execute_undos(records: list[str], entity, rule: BusinessRuleDB):
    """Execute undo actions on the given records.

    Args:
        records (list[str]): List of records.
        entity (EntityDB): Entity of the records.
        rule (BusinessRuleDB): Business rule.
    """
    unmatched_pipeline = (
        [
            {"$match": {"_id": {"$in": records}}}
        ]  # only apply on selected records
    ) + build_pipeline_from_entity(
        entity
    )  # perform the joins
    for record in connector.collection(
        f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.name}"
    ).aggregate(unmatched_pipeline):
        for configuration, actions in rule.actions.items():
            for action in actions:
                params = _map_params(action, record)
                plugin = get_plugin_from_configuration_name(configuration)
                try:
                    plugin.revert_action(params)
                except NotImplementedError:
                    logger.debug(
                        f"Revert action is not implemented for {action.value}. It will not be reverted."
                    )
                except Exception:
                    logger.error(
                        f"Error occurred while reverting the {action.value} action.",
                        details=traceback.format_exc(),
                    )


def _dot_walk(obj: dict, key: str) -> Any:
    keys = key.split(".")
    while keys:
        k = keys.pop(0)
        obj = obj.get(k, None)
        if obj is None:
            return obj
    return obj


def _map_params(action: Action, record: dict) -> Action:
    action = action.model_copy(deep=True)
    for key, value in action.parameters.items():
        if isinstance(value, str) and value.startswith("$"):
            action.parameters[key] = _dot_walk(record, value[1:])
    return action


@APP.task(name="cre.evaluate_records", acks_late=False)
@integration("cre")
@track()
def evaluate_records(
    entity: str,
    records: list = ...,
    rules: list = ...,
    configuration: str = None,
    action: str = None,
    days: int = ...,
    is_manual: bool = False,
) -> list[dict]:
    """Evaluate fetched records."""
    business_rules = _get_business_rules_with_action(entity, rules)
    if not business_rules:
        return {"success": True}
    actions = {}
    if records is ...:
        records = []
        for record in connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{entity}"
        ).find(
            {}
            | (
                {}
                if days is ...
                else {
                    "lastUpdated": {
                        "$gt": datetime.now() - timedelta(days=days)
                    }
                }
            ),
            {"_id": True},
        ):
            records.append(record["_id"])
            if len(records) == RECORD_BATCH_SIZE:
                for key, value in _evaluate_records(
                    records,
                    business_rules,
                    configuration,
                    action,
                    is_manual,
                ).items():
                    actions.setdefault(key, []).extend(value)
                records = []
        if records:
            for key, value in _evaluate_records(
                records,
                business_rules,
                configuration,
                action,
                is_manual,
            ).items():
                actions.setdefault(key, []).extend(value)
    else:
        for key, value in _evaluate_records(
            records,
            business_rules,
            configuration,
            action,
            is_manual,
        ).items():
            actions.setdefault(key, []).extend(value)
    if not actions:
        return {"success": True}

    def _execute_single_action(plugin, action):
        """Execute a single action."""
        try:
            logger.info(action["before_log_message"])
            plugin.execute_action(action["params"])
            logger.info(action["after_log_message"])
            action["log_func"](
                status=ActionLogStatus.SUCCESS,
                action=action["params"],
                performedAt=datetime.now()
            )
        except Exception:
            logger.error(
                action["error_log_message"],
                error_code="CRE_1037",
                details=traceback.format_exc(),
            )
            action["log_func"](
                status=ActionLogStatus.FAILED,
                action=action["params"],
                performedAt=datetime.now()
            )

    for metadata, actions in actions.items():
        configuration, _ = metadata
        plugin = get_plugin_from_configuration_name(configuration)
        if plugin.execute_actions == PluginBase.execute_actions:
            # method has not been implemented in the plugin
            # execute the actions individually instead
            for action in actions:
                _execute_single_action(plugin, action)
        else:
            try:
                action_type = actions[0]['params'].value
                logger.info(
                    f"Performing {action_type} action on batch with {len(actions)} records."
                )
                if hasattr(plugin, "provide_action_id") and plugin.provide_action_id:
                    result = plugin.execute_actions(
                        [{"params": action["params"], "id": action["id"]} for action in actions]
                    )
                else:
                    result = plugin.execute_actions([action["params"] for action in actions])

                # Handle partial success reporting
                if result and isinstance(result, ActionResult):
                    for action in actions:
                        action_failed = not result.success or action["id"] in result.failed_action_ids
                        action["log_func"](
                            status=ActionLogStatus.FAILED if action_failed else ActionLogStatus.SUCCESS,
                            performedAt=datetime.now(),
                            action=action["params"],
                        )
                    logger.info(
                        f"Successfully performed {action_type} action on batch "
                        f"with {len(actions) - len(result.failed_action_ids)} records. "
                        f"{len(result.failed_action_ids)} records failed out of {len(actions)}."
                    )
                else:
                    # All succeeded (result is None)
                    for action in actions:
                        action["log_func"](
                            status=ActionLogStatus.SUCCESS,
                            performedAt=datetime.now(),
                            action=action["params"],
                        )
                    logger.info(
                        f"Successfully performed {action_type} action on batch with {len(actions)} records."
                    )
            except NotImplementedError:
                for action in actions:
                    _execute_single_action(plugin, action)
            except Exception:
                action_type = actions[0]['params'].value
                logger.error(
                    f"Failed to perform batch {action_type} action on {len(actions)} records.",
                    error_code="CRE_1037",
                    details=traceback.format_exc(),
                )
                for action in actions:  # mark all as failed if there is an error
                    action["log_func"](
                        status=ActionLogStatus.FAILED,
                        action=action["params"],
                        performedAt=datetime.now()
                    )
    return {"success": True}


def _log_action(
    entity: str,
    record: dict,
    rule: str,
    configuration: str,
    action: Action,
    status: ActionLogStatus = ActionLogStatus.SUCCESS,
    performedAt: datetime = datetime.now(),
) -> ActionLogDB:
    if "lastEvals" in record:
        record_without_lastevals = copy.deepcopy(record)
        record_without_lastevals.pop("lastEvals")
    else:
        record_without_lastevals = record
    log = ActionLogDB(
        entity=entity,
        record=record_without_lastevals,
        rule=rule,
        status=status,
        performedAt=performedAt,
        configuration=configuration,
        action=action,
    )
    document = connector.collection(Collections.CREV2_ACTION_LOGS).insert_one(
        log.model_dump(),
    )
    return document.inserted_id


def _get_business_rules_with_action(
    entity: str, rules: list = ...
) -> list[BusinessRuleDB]:
    """Get business rules with actions."""
    return [
        BusinessRuleDB(**rule)
        for rule in connector.collection(
            Collections.CREV2_BUSINESS_RULES
        ).find(
            {
                "entity": entity,
                "actions": {"$ne": {}},
                **({} if rules else {"muted": False}),
                **({} if rules is ... else {"name": {"$in": rules}}),
            },
        )
    ]
