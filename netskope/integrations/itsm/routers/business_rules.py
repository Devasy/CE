"""Plugin related endpoints."""

import json
from typing import List
from datetime import datetime, timedelta
from fastapi import APIRouter, Security, Query, HTTPException
from starlette.responses import JSONResponse
import traceback
from pydantic import StringConstraints
from typing_extensions import Annotated

from netskope.common.api.routers.auth import get_current_user
from netskope.integrations.itsm.utils.custom_mapping_utils import apply_custom_mapping
from netskope.common.models import User
from netskope.integrations.itsm.models.custom_fields import MappingDirection
from netskope.common.utils import (
    Logger,
    DBConnector,
    Collections,
    parse_dates,
    PluginHelper,
    SecretDict,
)
from netskope.integrations.itsm.models.data_item import Alert, Event, DataType
from netskope.common.celery.scheduler import execute_celery_task

from ..models import (
    BusinessRuleIn,
    BusinessRuleOut,
    BusinessRuleDB,
    BusinessRuleUpdate,
    BusinessRuleDelete,
    FieldMapping,
    ConfigurationDB,
    Task,
    Queue,
)
from ..utils import filter_out_none_values, alert_event_query_schema
from ..utils.tickets import (
    _process_dedupe_rules,
    _map_values,
    _get_duplicate,
    _filter_data_items,
)
from .configurations import _get_available_mapping_fields
from ..tasks.pull_data_items import sync_alerts_and_events

router = APIRouter()
logger = Logger()
helper = PluginHelper()
connector = DBConnector()


@router.get("/business_rules", tags=["ITSM Business Rules"])
async def get_business_rules(user: User = Security(get_current_user, scopes=["cto_read"])) -> List[BusinessRuleOut]:
    """Get list of business rules."""
    rules = []
    for rule in connector.collection(Collections.ITSM_BUSINESS_RULES).find({}):
        rules.append(BusinessRuleOut(**rule))
    return rules


@router.post("/business_rules", tags=["ITSM Business Rules"])
async def post_business_rules(
    rule: BusinessRuleIn,
    user: User = Security(get_current_user, scopes=["cto_write"]),
) -> BusinessRuleOut:
    """Create a business rule."""
    connector.collection(Collections.ITSM_BUSINESS_RULES).insert_one(rule.model_dump())
    logger.debug(f"Business rule {rule.name} successfully created.")
    return rule


@router.post("/business_rules/sync", tags=["ITSM Business Rules"])
async def sync_queue(
    name: str = Query(...),
    configuration: str = Query(...),
    days: int = Query(7, lt=366, gt=0),
    user: User = Security(get_current_user, scopes=["cto_write"]),
):
    """Sync data with queue."""
    if connector.collection(Collections.ITSM_BUSINESS_RULES).find_one({"name": name}) is None:
        raise ValueError(400, "ITSM business rule does not exist.")
    logger.debug(f"Sync with business rule {name} for configuration {configuration} is triggered.")
    execute_celery_task(
        sync_alerts_and_events.apply_async,
        "itsm.sync_alerts_and_events",
        args=[name, configuration, days],
    )
    return {"success": True}


@router.patch("/business_rule", tags=["ITSM Business Rules"])
async def patch_business_rules(
    rule: BusinessRuleUpdate,
    user: User = Security(get_current_user, scopes=["cto_write"]),
) -> BusinessRuleOut:
    """Update a business rule."""
    existing_dict = connector.collection(Collections.ITSM_BUSINESS_RULES).find_one({"name": rule.name})
    existing_rule = BusinessRuleDB(**existing_dict)
    updated_rule = BusinessRuleDB(**{**existing_dict, **filter_out_none_values(rule.model_dump())})
    if (
        updated_rule.unmuteAt is not None  # unmute time has been set on the rule
        and existing_rule.unmuteAt != updated_rule.unmuteAt  # it is different than previous time
    ):
        pass
    connector.collection(Collections.ITSM_BUSINESS_RULES).update_one(
        {"name": rule.name}, {"$set": updated_rule.model_dump()}
    )
    logger.debug(f"Business rule {rule.name} has been successfully updated.")
    return updated_rule


@router.delete("/business_rule", tags=["ITSM Business Rules"])
async def delete_business_rule(
    rule: BusinessRuleDelete,
    user: User = Security(get_current_user, scopes=["cto_write"]),
):
    """Delete a business rule."""
    connector.collection(Collections.ITSM_BUSINESS_RULES).delete_one({"name": rule.name})
    logger.debug(f"Business rule {rule.name} has been successfully deleted.")
    return {}


def data_fields():
    """Alert/Event fields."""
    STATIC_DICT, RAW_DICT, _ = alert_event_query_schema()
    STRING_FIELDS = list(STATIC_DICT.keys()) + list(RAW_DICT.keys())
    return STRING_FIELDS


def get_total_tickets_creation(rule_query, rule, data_type: DataType = DataType.ALERT):
    """Get total tickets creation."""
    count = 0
    skip = 0
    collection_name = (
        Collections.ITSM_EVENTS if data_type == DataType.EVENT else Collections.ITSM_ALERTS
    )
    Model = Event if data_type == DataType.EVENT else Alert
    in_memory = []
    while True:
        cursor = (
            connector.collection(collection_name)
            .find(rule_query)
            .skip(skip)
            .limit(10000)
        )
        for item in cursor:
            skip += 1
            item = Model(**item)
            dedupe_rules, _ = _process_dedupe_rules(
                None, item, rule.dedupeRules, data_type=data_type
            )
            duplicate_task_found = _get_duplicate(
                item,
                dedupe_rules,
                data_type,
                ignore_requests=False,
                ignore_failed=False,
            )
            if duplicate_task_found:
                continue

            for on_local, _ in dedupe_rules:
                matched = _filter_data_items(in_memory, on_local)
                if not matched:
                    in_memory.append(item)
                    count += 1
        if skip % 10000 == 0:
            continue
        else:
            break
    return count


def get_filtered_data(rule, start_time, end_time, data_type: DataType):
    """Get filtered data."""
    data_collection = Collections.ITSM_ALERTS if data_type == DataType.ALERT else Collections.ITSM_EVENTS
    query = {
        "$and": [
            json.loads(
                rule.filters.mongo,
                object_hook=lambda pair: parse_dates(pair, data_fields()),
            ),
            {"timestamp": {"$lte": end_time, "$gte": start_time}},
        ]
    }
    if rule.muteRules:
        query["$and"].append(
            {
                "$nor": [
                    json.loads(
                        r.filters.mongo,
                        object_hook=lambda pair: parse_dates(pair, data_fields()),
                    )
                    for r in rule.muteRules
                ]
            }
        )
    result = connector.collection(data_collection).aggregate(
        [{"$match": query}, {"$group": {"_id": None, "count": {"$sum": 1}}}],
        allowDiskUse=True,
    )
    result = list(result)
    if len(result) == 0:
        return 0, 0
    elif rule.dedupeRules:
        ticket_count = get_total_tickets_creation(query, rule, data_type=data_type)
        return result.pop()["count"], ticket_count
    return result.pop()["count"], None


@router.get("/business_rule/test", tags=["ITSM Business Rules"])
async def test_business_rule(
    rule: str = Query(...),
    days: int = Query(..., lt=366, gt=0),
    user: User = Security(get_current_user, scopes=["cto_read"]),
):
    """Test a business rule."""
    rule = connector.collection(Collections.ITSM_BUSINESS_RULES).find_one(
        {"name": rule}
    )
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    if rule is None:
        raise HTTPException(400, f"No business rule with name {rule} found.")
    rule = BusinessRuleDB(**rule)
    alert_count, alert_ticket_count = get_filtered_data(rule, start_time, end_time, DataType.ALERT)
    event_count, event_ticket_count = get_filtered_data(rule, start_time, end_time, DataType.EVENT)
    response = {
        "alerts_count": alert_count,
        "events_count": event_count
    }
    if alert_ticket_count is not None and event_ticket_count is not None:
        response["ticket_count"] = alert_ticket_count+event_ticket_count
    return JSONResponse(
        status_code=200,
        content=response
    )


def get_data_item(query):
    """Get the data item for the given query."""
    data_collection = Collections.ITSM_ALERTS
    data_item = connector.collection(data_collection).find_one(query)
    if not data_item:
        data_collection = Collections.ITSM_EVENTS
        data_item = connector.collection(data_collection).find_one(query)
    if not data_item:
        raise HTTPException(
            400, "No Alert or Event matches the selected business rule."
        )
    data_item = (
        Alert(**data_item)
        if data_collection == Collections.ITSM_ALERTS
        else Event(**data_item)
    )
    return data_item


@router.post("/business_rule/testQueue", tags=["ITSM Business Rules"])
async def test_queue_business_rule(
    configuration: str = Query(...),
    rule: str = Query(...),
    label: Annotated[str, StringConstraints(strip_whitespace=True)] = "",
    value: Annotated[str, StringConstraints(strip_whitespace=True)] = "",
    mappings: List[FieldMapping] = [],
    user: User = Security(get_current_user, scopes=["cto_read"]),
):
    """Test the queue configuration."""
    available_fields = _get_available_mapping_fields(configuration)
    supports_field_mappings = len(available_fields) > 0

    if supports_field_mappings and not mappings:
        raise HTTPException(status_code=400, detail="Please provide mapping information.")
    elif not supports_field_mappings and not mappings:
        raise HTTPException(status_code=400, detail="Test Queue is not supported.")
    rule_db = connector.collection(Collections.ITSM_BUSINESS_RULES).find_one(
        {"name": rule}
    )
    if rule_db is None:
        raise HTTPException(
            400, f"Ticket Orchestrator business rule {rule} no longer exists."
        )
    rule_db = BusinessRuleDB(**rule_db)
    configuration_db = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
        {"name": configuration}
    )
    if configuration_db is None:
        raise HTTPException(
            400,
            f"Ticket Orchestrator configuration {configuration} no longer exists.",
        )
    configuration_db = ConfigurationDB(**configuration_db)
    query = json.loads(
        rule_db.filters.mongo,
        object_hook=lambda pair: parse_dates(pair, data_fields()),
    )
    data_item = get_data_item(query)
    PluginClass = helper.find_by_id(configuration_db.plugin)
    if not PluginClass:
        raise ValueError("Invalid plugin provided.")
    plugin = PluginClass(
        configuration_db.name,
        SecretDict(configuration_db.parameters),
        configuration_db.storage or {},
        configuration_db.checkpoint,
        logger,
    )

    try:
        queue = Queue(
            **{"label": label, "value": value, "defaultMappings": {"mappings": mappings}}
        )
    except Exception as e:
        raise HTTPException(400, e)
    try:
        mapped_data_item = apply_custom_mapping(
            data_item,
            configuration_db,
            direction=MappingDirection.REVERSE
        )
        task: Task = plugin.create_task(
            mapped_data_item,
            _map_values(mapped_data_item, mappings),
            queue,
        )
        if not isinstance(task, Task):
            raise ValueError("Invalid task returned.")
        if task.link:
            return {"link": task.link}
        else:
            return {}
    except Exception as exp:
        logger.error(
            message=f"Error while sending test notification. Error: {exp}.",
            details=traceback.format_exc(),
            error_code="CTO_1043",
        )
        raise HTTPException(
            400, "Error while sending test notification, check logs for more details."
        )
