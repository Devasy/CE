"""Retry mechanism for ITSM."""

from typing import Union, List
from bson.objectid import ObjectId
from datetime import datetime
from mongoquery import Query
from uuid import uuid4
import json
import re
import traceback

from netskope.integrations.itsm.models import (
    Alert,
    BusinessRuleDB,
    ConfigurationDB,
    DedupeRule,
    DataType,
    Event,
    FieldMapping,
    Task,
    TaskStatus,
    TaskRequestStatus,
)
from netskope.common.utils import (
    DBConnector,
    Collections,
    PluginHelper,
    Logger,
    parse_dates,
)
from .constants import ALERTS_EVENT_UNIQUE_CRITERIA
from .schemas import has_source_info_args, alert_event_query_schema
from netskope.integrations.itsm.models.custom_fields import MappingDirection
from netskope.integrations.itsm.utils.custom_mapping_utils import apply_custom_mapping, plugin_to_ce_task_map

MAIN_ATTRS = [
    "id",
    "alertName",
    "alertType",
    "eventType",
    "app",
    "appCategory",
    "user",
    "type",
    "timestamp",
    "configuration",
]

connector = DBConnector()
logger = Logger()
helper = PluginHelper()


def data_fields():
    """Alert/Event fields."""
    STATIC_DICT, RAW_DICT, _ = alert_event_query_schema()
    STRING_FIELDS = list(STATIC_DICT.keys()) + list(RAW_DICT.keys())
    return STRING_FIELDS


def _substitute_vars(message: str, data_item: Union[Alert, Event]) -> str:
    """Replace variables in a string with values from alert/event."""

    def get_value(match):
        """Resolve variable name."""
        raw_key = "rawData"
        if match.group(1) in (MAIN_ATTRS):
            return getattr(data_item, match.group(1), "value_unavailable")
        else:
            return getattr(data_item, raw_key).get(match.group(1), "value_unavailable")

    var_regex = r"(?<!\\)\$([a-zA-Z0-9_]+)"
    return re.sub(
        var_regex,
        lambda match: str(get_value(match)),
        message,
    )


def _map_values(data_item: Union[Alert, Event], mappings: List[FieldMapping]) -> dict:
    """Generate a mapped dictionary based on the given alert/event and field mappings."""
    result = {}
    raw_key = "rawData"
    for mapping in mappings:
        if mapping.extracted_field not in [None, "custom_message"]:
            if mapping.extracted_field in (MAIN_ATTRS):
                result[mapping.destination_field] = getattr(
                    data_item, mapping.extracted_field, None
                )
            else:
                result[mapping.destination_field] = getattr(data_item, raw_key).get(
                    mapping.extracted_field, None
                )
        else:
            result[mapping.destination_field] = _substitute_vars(
                mapping.custom_message, data_item
            )
        if result[mapping.destination_field] is None:
            result.pop(mapping.destination_field)
        if mapping.destination_field in result and result.get(
            mapping.destination_field, None
        ):
            result[mapping.destination_field] = str(result[mapping.destination_field])
    return result


def call_plugin(
    plugin,
    data_item,
    queues,
    configuration,
    business_rule,
    data_type: DataType = DataType.ALERT,
    ignore_approval=False,
):
    """Call the plugin method."""
    if queues[0].requireApproval and not ignore_approval:
        task = Task(id=uuid4().hex, approvalStatus=TaskRequestStatus.PENDING)
    else:
        mapped_data_item = apply_custom_mapping(
            data_item,
            configuration,
            direction=MappingDirection.REVERSE
        )
        task: Task = plugin.create_task(
            mapped_data_item,
            _map_values(mapped_data_item, queues[0].defaultMappings.get("mappings", [])),
            queues[0],  # queues are limited to one
        )
        task.approvalStatus = (
            TaskRequestStatus.APPROVED
            if queues[0].requireApproval
            else TaskRequestStatus.NOT_REQUIRED
        )
        if type(task) is not Task:
            raise ValueError("Invalid task returned.")
    # overwrite mandatory fields
    task.configuration = configuration.name
    task.dataItem = data_item
    task.businessRule = business_rule.name  # for muting
    task.createdAt = datetime.now()
    task.lastUpdatedAt = datetime.now()
    task.dataType = data_type
    task.dataSubType = (
        data_item.alertType if data_type == DataType.ALERT else data_item.eventType
    )
    task = plugin_to_ce_task_map(task, configuration)
    return task, queues[0].requireApproval


def create_ticket(
    create_check,
    plugin,
    data_item,
    queues,
    configuration,
    business_rule,
    _id=None,
    data_type: DataType = DataType.ALERT,
    ignore_approval=False,
):
    """Create or update ticket based on status."""
    if (
        create_check
        and connector.collection(Collections.ITSM_TASKS).count_documents(
            {
                "dataItem.id": data_item.id,
                "configuration": configuration.name,
                "businessRule": business_rule.name,
                "dataType": data_type.value,
                "dataSubType": (
                    data_item.alertType
                    if data_type == DataType.ALERT
                    else data_item.eventType
                ),
            }
        )
        >= 1
    ):  # No need to create multiple tickets for failed ticket.
        if _id:
            task, sent_for_request = call_plugin(
                plugin,
                data_item,
                queues,
                configuration,
                business_rule,
                data_type=data_type,
                ignore_approval=ignore_approval,
            )
            connector.collection(Collections.ITSM_TASKS).update_one(
                {"_id": _id},
                {
                    "$set": task.model_dump(
                        exclude=[
                            "dedupeCount",
                            "deletedAt",
                            "lastSyncedAt",
                            "syncStatus",
                        ],
                    )
                },
            )
            logger.info(
                f"Successfully created {'request' if sent_for_request and not ignore_approval else 'task'} "
                f"for {data_type.value}"
                f" id {data_item.id} "
                f"{'while retrying' if sent_for_request and not ignore_approval else 'from the request'}."
            )
        else:
            return None  # No need to do anything when manual sync is triggered and dup is a Failed ticket.
    else:
        task, sent_for_request = call_plugin(
            plugin, data_item, queues, configuration, business_rule, data_type=data_type
        )
        connector.collection(Collections.ITSM_TASKS).insert_one(task.model_dump())
    return sent_for_request


def _process_dedupe_rules(
    name: str,
    data_item: Union[Alert, Event],
    rules: List[DedupeRule],
    data_type: DataType = DataType.ALERT,
) -> List[dict]:
    """Process dedupe rules so they can be used for building mongo queries."""

    def _add_prefix(pair):
        raw_key = "rawData"
        for key, value in list(pair.items()):
            if not key.startswith("dataItem.") and not key.startswith("$"):
                pair[f"dataItem.{key.replace(f'{raw_key}_', f'{raw_key}.')}"] = (
                    pair.pop(key)
                )
        return parse_dates(
            pair,
            data_fields()
            + [
                f"dataItem.{field.replace(f'{raw_key}_', f'{raw_key}.')}"
                for field in data_fields()
            ],
        )

    out = []
    raw_key = "rawData"
    for rule in rules:
        if rule.filters is not None:
            rule_query = rule.filters.mongo
        elif rule.dedupeFields is not None:
            rule_query = {}
            for key in rule.dedupeFields:
                if key.startswith(f"{raw_key}_"):
                    value = getattr(data_item, raw_key).get(
                        key[len(raw_key) + 1:]
                    )
                else:
                    value = getattr(data_item, key, None)
                if value:
                    rule_query[key] = {"$eq": value}
                else:
                    rule_query["$or"] = [
                        *(rule_query.get("$or") or []),
                        {key: {"$eq": value}},
                        {key: {"$exists": False}},
                    ]
            rule_query = json.dumps(rule_query)
        out.append(
            (
                json.loads(  # to apply on local object
                    rule_query,
                    object_hook=lambda pair: parse_dates(
                        pair, data_fields(), ignore_regex=True
                    ),
                ),
                (
                    {
                        **json.loads(  # to run on mongo
                            rule_query,
                            object_hook=lambda pair: _add_prefix(pair),
                        ),
                        "configuration": name,
                    }
                    if name
                    else {
                        **json.loads(  # to run on mongo
                            rule_query,
                            object_hook=lambda pair: _add_prefix(pair),
                        )
                    }
                ),
            )
        )

    # Add additional dedup rules for incidents
    additional_rule_exists = False
    if ALERTS_EVENT_UNIQUE_CRITERIA.get(data_type.value):
        sub_type = getattr(
            data_item, "alertType" if data_type == DataType.ALERT else "eventType"
        )
        additional_unique_fields = ALERTS_EVENT_UNIQUE_CRITERIA[data_type.value].get(
            sub_type
        )
        if additional_unique_fields:
            rule_query = {}
            for field in additional_unique_fields:
                if field.startswith(f"{raw_key}_"):
                    value = getattr(data_item, raw_key).get(
                        field[len(raw_key) + 1:]
                    )
                else:
                    value = getattr(data_item, field, None)
                if value:
                    rule_query[field] = {"$eq": value}
                else:
                    rule_query["$or"] = [
                        *(rule_query.get("$or") or []),
                        {field: {"$eq": value}},
                        {field: {"$exists": False}},
                    ]
            rule_query = json.dumps(rule_query)
            out.append(
                (
                    json.loads(  # to apply on local object
                        rule_query,
                        object_hook=lambda pair: parse_dates(
                            pair, data_fields(), ignore_regex=True
                        ),
                    ),
                    (
                        {
                            **json.loads(  # to run on mongo
                                rule_query,
                                object_hook=lambda pair: _add_prefix(pair),
                            ),
                            "configuration": name,
                        }
                        if name
                        else {
                            **json.loads(  # to run on mongo
                                rule_query,
                                object_hook=lambda pair: _add_prefix(pair),
                            )
                        }
                    ),
                )
            )
            additional_rule_exists = True
    return out, additional_rule_exists


def _filter_data_items(
    data_items: Union[List[Alert], List[Event]],
    filters: Union[str, dict],
) -> Union[List[Alert], List[Event]]:
    """Filter alert/event based on the query."""
    if type(filters) is str:
        query = json.loads(
            filters,
            object_hook=lambda pair: parse_dates(
                pair, data_fields(), ignore_regex=True
            ),
        )
    else:
        query = filters
    filter_query = Query(query)
    return list(
        filter(lambda data_item: filter_query.match(data_item.model_dump()), data_items)
    )


def _get_duplicate(
    data_item: Union[Alert, Event],
    rules: List[tuple],
    data_type: DataType,
    ignore_requests: bool = False,
    ignore_failed: bool = True,
) -> Task:
    """Get duplicate task if it is already created."""
    for rule in rules:
        on_local, on_db = rule
        data_items = _filter_data_items([data_item], on_local)
        if not data_items:  # alert/event does not match the dedupe rule
            continue
        # check if there is already a task whose alert/event matches this dedupe rule
        on_db["dataType"] = data_type.value
        if ignore_failed:
            on_db["status"] = {"$ne": TaskStatus.FAILED.value}
        match = None
        if not ignore_requests:
            on_db["approvalStatus"] = {
                "$nin": [
                    TaskRequestStatus.APPROVED.value,
                    TaskRequestStatus.NOT_REQUIRED.value,
                ]
            }
            match = connector.collection(Collections.ITSM_TASKS).find_one(on_db)
        if not match:
            on_db["approvalStatus"] = {
                "$in": [
                    TaskRequestStatus.APPROVED.value,
                    TaskRequestStatus.NOT_REQUIRED.value,
                ]
            }
            match = connector.collection(Collections.ITSM_TASKS).find_one(on_db)
        if not match:
            continue
        return Task(**match)
    return None


def create_tickets_or_requests(
    plugin,
    data_item: Union[Alert, Event],
    rule: BusinessRuleDB,
    queues,
    configuration: ConfigurationDB,
    data_type: DataType = DataType.ALERT,
    audit_request: bool = False,
    retry: bool = False,
    _id: ObjectId = None,
):
    """Create tickets for alerts/events for a specific business rule."""
    dedupe_rules, additional_rule_exists = _process_dedupe_rules(
        configuration.name, data_item, rule.dedupeRules, data_type=data_type
    )
    duplicate_task = _get_duplicate(
        data_item,
        dedupe_rules,
        data_type,
        ignore_requests=(audit_request or retry),
        ignore_failed=retry,
    )
    requests, tickets, failed = 0, 0, 0
    try:
        if duplicate_task is None:
            # No need to check if it creates duplicate in manual sync.
            sent_for_request = create_ticket(
                audit_request or retry,
                plugin,
                data_item,
                queues,
                configuration,
                rule,
                _id if (audit_request or retry) else None,
                data_type=data_type,
                ignore_approval=(audit_request or retry),
            )
            if sent_for_request is True:
                requests += 1
            else:
                tickets += 1
        else:
            # Try to create the ticket if dup is failed.
            _task = None
            update_dict = {}
            if (
                not retry
                and not audit_request
                and duplicate_task.approvalStatus == TaskRequestStatus.DECLINED
            ):
                if queues[0].requireApproval:
                    update_dict["approvalStatus"] = TaskRequestStatus.PENDING.value
                    _task = duplicate_task
                    requests += 1
                else:
                    sent_for_request = create_ticket(
                        True,
                        plugin,
                        data_item,
                        queues,
                        configuration,
                        rule,
                        _id=ObjectId(duplicate_task.internalId),
                        data_type=data_type,
                    )
                    _task = duplicate_task
                    if sent_for_request is True:
                        requests += 1
                    else:
                        tickets += 1
            elif not retry and duplicate_task.status == TaskStatus.FAILED:
                sent_for_request = create_ticket(
                    True,
                    plugin,
                    data_item,
                    queues,
                    configuration,
                    rule,
                    ObjectId(duplicate_task.internalId),
                    data_type=data_type,
                    ignore_approval=True,
                )
                if sent_for_request is True:
                    requests += 1
                else:
                    tickets += 1
                _task = duplicate_task
            else:
                _task = duplicate_task
                if duplicate_task.approvalStatus in [
                    TaskRequestStatus.APPROVED,
                    TaskRequestStatus.NOT_REQUIRED,
                ]:
                    update_all_details = has_source_info_args(
                        plugin, "update_task", ["upsert_task"]
                    )
                    mapped_data_item = apply_custom_mapping(
                        data_item,
                        configuration,
                        direction=MappingDirection.REVERSE
                    )
                    task = (
                        plugin.update_task(
                            duplicate_task,
                            mapped_data_item,
                            _map_values(
                                mapped_data_item,
                                (
                                    queues[0].defaultMappings.get("mappings", [])
                                    if additional_rule_exists
                                    else queues[0].defaultMappings.get("dedup", [])
                                ),
                            ),
                            queues[0],  # queues are limited to one
                            additional_rule_exists,
                        )
                        if update_all_details
                        else plugin.update_task(
                            duplicate_task,
                            mapped_data_item,
                            _map_values(
                                mapped_data_item,
                                queues[0].defaultMappings.get("dedup", []),
                            ),
                            queues[0],  # queues are limited to one
                        )
                    )
                    task.dataItem = data_item
                    task = plugin_to_ce_task_map(task, configuration)
                    update_dict["updatedValues"] = (
                        task.updatedValues.model_dump() if task.updatedValues else None
                    )
                    tickets += 1
                else:
                    requests += 1
                update_dict["dataItem"] = data_item.model_dump()
                update_dict["dataSubType"] = (
                    data_item.alertType
                    if data_type == DataType.ALERT
                    else data_item.eventType
                )
            if _task:
                inc = 0
                if (retry or audit_request) and _id:
                    current_task = connector.collection(
                        Collections.ITSM_TASKS
                    ).find_one({"_id": _id})
                    if current_task:
                        inc = current_task.get("dedupeCount", 0)
                    connector.collection(Collections.ITSM_TASKS).delete_one(
                        {"_id": _id}
                    )
                connector.collection(Collections.ITSM_TASKS).update_one(
                    {"_id": ObjectId(_task.internalId)},
                    (
                        ({"$inc": {"dedupeCount": inc + 1}, "$set": update_dict})
                        if update_dict
                        else ({"$inc": {"dedupeCount": inc + 1}})
                    ),
                )
    except Exception as ex:
        failed += 1
        logger.error(
            f"Could not create/update task for {data_type.value} with ID {data_item.id} "
            f"for configuration {configuration.name}. {repr(ex)}.",
            error_code="CTO_1024",
            details=traceback.format_exc(),
        )
        if not retry and not audit_request and duplicate_task:
            if (
                duplicate_task.approvalStatus == TaskRequestStatus.DECLINED
                and not queues[0].requireApproval
            ):
                connector.collection(Collections.ITSM_TASKS).update_one(
                    {"_id": ObjectId(duplicate_task.internalId)},
                    {
                        "$set": {
                            "approvalStatus": TaskRequestStatus.NOT_REQUIRED.value,
                            "status": TaskStatus.FAILED,
                            "lastUpdatedAt": datetime.now(),
                        }
                    },
                )
            elif duplicate_task.status == TaskStatus.FAILED:
                connector.collection(Collections.ITSM_TASKS).update_one(
                    {"_id": ObjectId(duplicate_task.internalId)},
                    {"$inc": {"dedupeCount": 1}},
                )
        elif retry:
            connector.collection(Collections.ITSM_TASKS).update_one(
                {"_id": _id},
                {
                    "$set": {
                        "status": TaskStatus.FAILED,
                        "lastUpdatedAt": datetime.now(),
                    }
                },
            )
        elif audit_request:
            connector.collection(Collections.ITSM_TASKS).update_one(
                {"_id": _id},
                {
                    "$set": {
                        "status": TaskStatus.FAILED.value,
                        "lastUpdatedAt": datetime.now(),
                        "approvalStatus": TaskRequestStatus.APPROVED.value,
                    }
                },
            )
        else:
            task = {
                "id": None,
                "dataItem": data_item.model_dump(),
                "businessRule": rule.name,
                "configuration": configuration.name,
                "status": TaskStatus.FAILED,
                "approvalStatus": (
                    TaskRequestStatus.APPROVED
                    if queues[0].requireApproval
                    else TaskRequestStatus.NOT_REQUIRED
                ),
                "createdAt": datetime.now(),
                "lastUpdatedAt": datetime.now(),
                "dataType": data_type,
                "dataSubType": (
                    data_item.alertType
                    if data_type == DataType.ALERT
                    else data_item.eventType
                ),
            }
            connector.collection(Collections.ITSM_TASKS).insert_one(task)
    return {"requests": requests, "tickets": tickets, "failed": failed}
