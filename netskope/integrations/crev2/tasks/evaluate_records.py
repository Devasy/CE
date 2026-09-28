"""Evaluate fetched records."""

import copy
import json
import random
import string
import traceback
from functools import partial
from bson.objectid import ObjectId
from datetime import datetime, timedelta, UTC
from typing import Any, Optional

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import SettingsDB
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    cto_alerts_enabled,
    integration,
    is_platform_enabled,
    log_cto_alerts_skipped,
    parse_dates,
    track,
)
from netskope.common.utils.unified_mapping_exec import resolve_normalized_field_value
from netskope.integrations.itsm.models import Alert
from netskope.integrations.itsm.tasks.pull_data_items import store_cre_alerts

from ..models import (
    Action,
    ActionLogDB,
    ActionLogStatus,
    BusinessRuleDB,
    EntityFieldType,
    get_entity_by_name,
    get_plugin_from_configuration_name,
)
from ..utils import (
    THREAT_INDICATORS_ENTITY,
    build_pipeline_from_entity,
    build_threat_indicator_business_rule_match,
    display_action_parameters,
    get_entity_collection,
    get_entity_recency_field,
)
from ..utils.threat_indicators_entity import TI_GROUP_LABELS
from ..plugin_base import PluginBase, ActionResult

connector = DBConnector()
helper = PluginHelper()
logger = Logger()
RECORD_BATCH_SIZE = 1000


def _is_within_action_window(settings: SettingsDB) -> bool:
    """Return whether now (UTC) falls inside the configured CRE action window.

    Actions flagged ``performLater`` are only executed inline while inside the
    window; outside it they are logged as SCHEDULED for ``cre.perform_action``
    to pick up. The window may wrap past midnight (``endTime`` < ``startTime``),
    in which case everything except the complementary daytime gap is inside it.

    Note this intentionally does NOT consider ``settings.cre.maintenanceDays`` —
    ``cre.perform_action`` applies that day-of-week gate when it drains
    SCHEDULED logs, and duplicating it here would defer actions twice.
    """
    start = settings.cre.startTime.strftime("%H:%M:%S")
    end = settings.cre.endTime.strftime("%H:%M:%S")
    now = datetime.now(UTC).strftime("%H:%M:%S")
    if start < now < end:
        return True
    return end < start and not (end < now < start)


def _execute_single_action(plugin, action):
    """Execute a single action and log its outcome."""
    try:
        logger.info(action["before_log_message"])
        plugin.execute_action(action["params"])
        logger.info(action["after_log_message"])
        action["log_func"](
            status=ActionLogStatus.SUCCESS,
            action=action["params"],
            performedAt=datetime.now(),
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
            performedAt=datetime.now(),
        )


def execute_actions_batch(actions_batch: dict) -> None:
    """Execute a batch of routed actions, grouped per (configuration, action).

    The single action-execution path for CRE evaluation: batched
    ``plugin.execute_actions`` is preferred and ``plugin.execute_action`` is the
    per-action fallback for plugins that do not implement it. Every action is
    logged SUCCESS or FAILED (honouring ``ActionResult.failed_action_ids`` for
    partial batch failures), and the plugin's storage is persisted once per
    configuration afterwards regardless of outcome.

    Shared with the unified-mapping action task so both evaluation paths route
    actions through identical plugin, partial-failure and storage semantics.

    Args:
        actions_batch (dict): ``(configuration, action value)`` -> list of
            action descriptors, each carrying ``params``, ``id``, ``log_func``
            and the before/after/error log messages.
    """
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
                if hasattr(plugin, "provide_action_id") and plugin.provide_action_id:
                    result = plugin.execute_actions(
                        [
                            {"params": action["params"], "id": action["id"]}
                            for action in actions
                        ]
                    )
                else:
                    result = plugin.execute_actions(
                        [action["params"] for action in actions]
                    )

                # Handle partial success reporting
                if result and isinstance(result, ActionResult):
                    failed_ids = set(result.failed_action_ids or [])
                    batch_failed_ids = {
                        action["id"] for action in actions if action["id"] in failed_ids
                    }
                    # If the plugin reported overall failure but gave no usable
                    # ids, fall back to marking every action in the batch failed.
                    all_failed = not result.success and not batch_failed_ids
                    for action in actions:
                        action_failed = all_failed or action["id"] in batch_failed_ids
                        action["log_func"](
                            status=(
                                ActionLogStatus.FAILED
                                if action_failed
                                else ActionLogStatus.SUCCESS
                            ),
                            performedAt=datetime.now(),
                            action=action["params"],
                        )
                    failed_count = len(actions) if all_failed else len(batch_failed_ids)
                    logger.info(
                        f"Successfully performed {action_type} action on batch "
                        f"with {len(actions) - failed_count} records. "
                        f"{failed_count} records failed out of {len(actions)}."
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
                action_type = actions[0]["params"].value
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
        # Persist the plugin's storage once per plugin instance, after all
        # actions for this configuration have been executed (covers both the
        # batched execute_actions path and the per-action execute_action
        # fallback, and runs regardless of success/failure).
        try:
            connector.collection(Collections.CREV2_CONFIGURATIONS).update_one(
                {"name": configuration},
                {"$set": {"storage": plugin.storage or {}}},
            )
        except Exception:
            logger.error(
                f"Error occurred while updating storage for configuration {configuration}.",
                details=traceback.format_exc(),
            )


def _get_normalized_fields(entity):
    """Get normalized fields list."""
    return [
        field.name
        for field in entity.fields
        if field.type == EntityFieldType.STRING and field.params and field.params.normalization
    ]


def _flatten_matched_source(record_fields: dict, entity_name: str, source_configuration: Optional[str]) -> dict:
    """Flatten a Threat Indicators record's matching source onto its top level.

    Business rules on the Threat Indicators entity are scoped to a single
    source configuration (enforced by build_threat_indicator_source_match's
    $elemMatch, so a matching source is always present here), so CTO alerts
    generated from them should surface that one source's own fields
    (destinations, retracted, retractionDestinations, etc.) directly instead
    of the raw "sources" list - mirroring how the CRE Action Logs page
    already displays these records (NCTE-242). Fields already present on
    the record (e.g. the reconciled internalHits/externalHits) are never
    overwritten.

    Args:
        record_fields: The record dict as copied for the alert's rawData.
        entity_name: The business rule's entity name.
        source_configuration: The business rule's sourceConfiguration, if any.

    Returns:
        dict: record_fields, with "sources" removed and the matched
        source's fields hoisted onto the top level - unchanged if the
        entity isn't Threat Indicators or no source actually matches.
    """
    if entity_name != THREAT_INDICATORS_ENTITY:
        return record_fields
    sources = record_fields.get("sources")
    if not source_configuration or not isinstance(sources, list):
        return record_fields
    matched_source = next(
        (
            source
            for source in sources
            if isinstance(source, dict) and source.get("source") == source_configuration
        ),
        None,
    )
    if not matched_source:
        return record_fields
    del record_fields["sources"]
    for key, value in matched_source.items():
        if key == "derived":
            continue
        record_fields.setdefault(key, value)
    return record_fields

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
    module_alerts_on = bool(settings.cre and settings.cre.generateAlerts)
    cto_on = cto_alerts_enabled(settings)
    alerts_enabled = module_alerts_on and cto_on
    # Reported once per run, and only for a rule that actually asked for alerts,
    # so a disabled CTO module does not log on every evaluation cycle.
    cto_skip_pending = module_alerts_on and not cto_on
    for rule in rules:
        logger.debug(f"Evaluating {rule.name} for {len(records)} records.")
        if cto_skip_pending and any(
            action.generateAlert
            for rule_actions in (rule.actions or {}).values()
            for action in rule_actions
        ):
            log_cto_alerts_skipped(f"business rule '{rule.name}' on '{rule.entity}'")
            cto_skip_pending = False
        entity = get_entity_by_name(rule.entity)
        # The entity's own field catalog already carries curated display
        # labels (e.g. "internalHits" -> "Total Netskope Hits") - the same
        # ones the CRE Action Logs page shows via GET /cre/entities. Passed
        # through to store_cre_alerts so a CTO alert's rawData keys get
        # registered with these labels on first sight, instead of ITSM's
        # generic camelCase-to-Title-Case fallback.
        entity_field_labels = {field.name: field.label for field in entity.fields}
        if entity.name == THREAT_INDICATORS_ENTITY:
            # entity.fields only names the destinations/retractionDestinations
            # sub-fields in dot-notation (e.g. "sources.destinations.name"),
            # never the group itself, so the map above has no entry for the
            # flat "destinations"/"retractionDestinations" keys
            # _flatten_matched_source hoists onto the alert - add their
            # curated group labels here instead of falling back to humanizing
            # the raw key name.
            entity_field_labels.update(
                {
                    group[1]: label
                    for group, label in TI_GROUP_LABELS.items()
                    if len(group) == 2 and group[0] == "sources"
                }
            )
            query = build_threat_indicator_business_rule_match(
                rule.entityFilters.mongo,
                rule.sourceConfiguration,
            )
        else:
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
        # Get normalized field list
        normalized_entity_fields = _get_normalized_fields(entity)
        alerts = []
        for record in connector.collection(
            get_entity_collection(entity.name)
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
                        params = _map_params(
                            action,
                            record,
                            configuration,
                            normalized_entity_fields,
                            source_configuration=rule.sourceConfiguration
                            if entity.name == THREAT_INDICATORS_ENTITY
                            else None,
                        )
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
                            not _is_within_action_window(settings)
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
                        if action.generateAlert and alerts_enabled:
                            record_fields = {
                                k: (
                                    resolve_normalized_field_value(
                                        v, configuration
                                    )
                                    if k in normalized_entity_fields
                                    else v
                                )
                                for k, v in record.items()
                                if k
                                not in (
                                    "_id",
                                    "lastEvals",
                                    "lastUpdated",
                                )
                            }
                            record_fields = _flatten_matched_source(
                                record_fields,
                                entity.name,
                                rule.sourceConfiguration,
                            )
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
                                    rawData=record_fields
                                    | {
                                        "plugin": plugin.metadata.get(
                                            "name"
                                        ),
                                        "action": action.label,
                                        "businessRule": rule.name,
                                    }
                                    | display_action_parameters(params),
                                )
                            )
                    # TODO: update storage
        # Mark all the records that did match
        if matched_records:
            connector.collection(
                get_entity_collection(entity.name)
            ).update_many(
                {"_id": {"$in": matched_records}},
                {
                    "$addToSet": {
                        "lastEvals": {"$each": list(all_eval_actions)}
                    }
                },
            )
        # Unmark all the records that previously matched but now did not
        unmark_records = set(records) - set(matched_records)
        records_to_unmark = []
        for item in all_eval_actions:
            records_to_unmark += connector.collection(
                get_entity_collection(entity.name)
            ).distinct(
                "_id",
                {
                    "_id": {"$in": list(unmark_records)},
                    "lastEvals": item,
                },
            )

        if records_to_unmark:
            result = connector.collection(
                get_entity_collection(entity.name)
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
                kwargs={"field_labels": entity_field_labels},
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
    normalized_fields = _get_normalized_fields(entity)
    for record in connector.collection(
        get_entity_collection(entity.name)
    ).aggregate(unmatched_pipeline):
        for configuration, actions in rule.actions.items():
            for action in actions:
                params = _map_params(
                    action,
                    record,
                    configuration,
                    normalized_fields,
                    source_configuration=rule.sourceConfiguration
                    if entity.name == THREAT_INDICATORS_ENTITY
                    else None,
                )
                plugin = get_plugin_from_configuration_name(configuration)
                try:
                    plugin.revert_action(params)
                    connector.collection(
                        Collections.CREV2_CONFIGURATIONS
                    ).update_one(
                        {"name": configuration},
                        {"$set": {"storage": plugin.storage or {}}},
                    )
                except NotImplementedError:
                    logger.debug(
                        f"Revert action is not implemented for {action.value}. It will not be reverted."
                    )
                except Exception:
                    logger.error(
                        f"Error occurred while reverting the {action.value} action.",
                        details=traceback.format_exc(),
                    )


def _dot_walk(
    obj: Any, key: str, source_filter: Optional[str] = None
) -> Any:
    """Resolve a dotted path on a record (e.g. ``sources.severity``).

    CRE entity fields such as Threat Indicator ``sources`` are stored as an
    array of objects in MongoDB. When ``source_filter`` is set (the rule's
    ``sourceConfiguration``), nested ``sources.*`` lookups prefer that entry.
    """
    if not key:
        return obj
    keys = key.split(".")
    for index, segment in enumerate(keys):
        if obj is None:
            return None
        if isinstance(obj, list):
            remainder = ".".join(keys[index:])
            candidates = [item for item in obj if isinstance(item, dict)]
            if source_filter:
                filtered = [
                    item
                    for item in candidates
                    if item.get("source") == source_filter
                ]
                if filtered:
                    candidates = filtered
            for item in candidates:
                result = _dot_walk(item, remainder)
                if result is not None:
                    return result
            return None
        if isinstance(obj, dict):
            obj = obj.get(segment, None)
        else:
            return None
    return obj


def _has_resolved_param_value(value) -> bool:
    """Return whether a resolved action parameter value should be kept.

    None, empty strings, whitespace-only strings, and collections with no
    such items are treated as unset so _map_params can store None instead.
    """
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set)):
        return any(_has_resolved_param_value(item) for item in value)
    return True


def _map_params(
    action: Action,
    record: dict,
    configuration: str,
    normalize_fields: list = [],
    source_configuration: Optional[str] = None,
) -> Action:
    action = action.model_copy(deep=True)
    for key, value in action.parameters.items():
        if isinstance(value, str) and value.startswith("$"):
            resolved_value = _dot_walk(
                record, value[1:], source_filter=source_configuration
            )
            if value[1:] in normalize_fields:
                resolved_value = resolve_normalized_field_value(
                    resolved_value, configuration
                )
            if not _has_resolved_param_value(resolved_value):
                action.parameters[key] = None
            else:
                action.parameters[key] = resolved_value
        elif (
            isinstance(value, list)
            and len(value) > 0
            and all(isinstance(v, str) and v.startswith("$") for v in value)
        ):
            resolved = []
            for v in value:
                path = v[1:]
                resolved_value = _dot_walk(
                    record, path, source_filter=source_configuration
                )
                if path in normalize_fields:
                    resolved_value = resolve_normalized_field_value(
                        resolved_value, configuration
                    )
                resolved.append(resolved_value)
            if not any(_has_resolved_param_value(item) for item in resolved):
                action.parameters[key] = None
            else:
                resolved = [
                    item
                    for item in resolved
                    if _has_resolved_param_value(item)
                ]
                action.parameters[key] = (
                    resolved[0] if len(resolved) == 1 else resolved
                )
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
    if entity == THREAT_INDICATORS_ENTITY and not is_platform_enabled("cte"):
        logger.debug(
            "Skipped evaluating Threat Indicators business rule(s) because the "
            "CTE module is disabled. Enable the CTE module to resume evaluating "
            "these rules.",
        )
        return {"success": False, "message": "CTE module is currently disabled."}
    business_rules = _get_business_rules_with_action(entity, rules)
    if not business_rules:
        return {"success": True}
    actions = {}
    if records is ...:
        records = []
        recency_field = get_entity_recency_field(entity)
        for record in connector.collection(
            get_entity_collection(entity)
        ).find(
            {}
            | (
                {}
                if days is ...
                else {
                    recency_field: {
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
    execute_actions_batch(actions)
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
        for rule in connector.collection(Collections.CREV2_BUSINESS_RULES).find(
            {
                "entity": entity,
                "actions": {"$ne": {}},
                **({} if rules else {"muted": False}),
                **({} if rules is ... else {"name": {"$in": rules}}),
            },
        )
    ]
