"""Provides task for pulling alerts/events."""

from __future__ import absolute_import, unicode_literals
import gzip
import json
from datetime import datetime, timedelta
from typing import List, Union
from mongoquery import Query
import traceback
import threading
from queue import Queue
import os
from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.integrations.itsm.utils.custom_mapping_utils import apply_custom_mapping
from netskope.common.models import NetskopeFieldType, TenantDB
from netskope.integrations.itsm.models.custom_fields import MappingDirection
from netskope.common.utils import (
    DBConnector,
    Collections,
    integration,
    PluginHelper,
    Logger,
    parse_dates,
    track,
    SecretDict,
    parse_events,
)
from netskope.common.utils.back_pressure import back_pressure_mechanism
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.utils.alerts_helper import AlertsHelper
from netskope.common.utils.validate_tenant import validate_tenant
from netskope.integrations.itsm.models import (
    ConfigurationDB,
    Alert,
    BusinessRuleDB,
    DedupeRule,
    Event,
    DataType,
)
from netskope.integrations.itsm.utils.tickets import create_tickets_or_requests, data_fields, _filter_data_items
from netskope.integrations.itsm.utils.constants import MAX_BATCH_SIZE


connector = DBConnector()
logger = Logger()
helper = PluginHelper()
alerts_helper = AlertsHelper()
plugin_provider_helper = PluginProviderHelper()

MAX_NUM_OF_THREADS = os.environ.get("MAX_NUM_OF_THREADS_FOR_CTO", 3)


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
                "lastRunAt.pull": datetime.now(),
                "lastRunSuccess.pull": success,
                # "lockedAt": None,
            }
        },
    )
    return success


def _remove_muted_data_items(
    data_items: Union[List[Alert], List[Event]],
    rules: List[DedupeRule],
):
    rule_queries = []
    for rule in rules:
        query = json.loads(
            rule.filters.mongo,
            object_hook=lambda pair: parse_dates(
                pair, data_fields(), ignore_regex=True
            ),
        )
        rule_queries.append(Query(query))
    return list(
        filter(
            lambda data_item: not any(
                map(lambda q: q.match(data_item.model_dump()), rule_queries)
            ),
            data_items,
        )
    )


def _create_tickets_for_rule(
    data_items: Union[List[Alert], List[Event]],
    rule: BusinessRuleDB,
    configuration=None,
    data_type: DataType = DataType.ALERT,
):
    """Create alerts/events for a specific business rule."""
    if not rule.queues:  # ignore if no queues are linked
        return
    if any(not rule.filters.isValid for rule in rule.muteRules):
        logger.error(
            f"Filtering and task(s) creation of {len(data_items)} {data_type}(s) "
            " have failed because one or more field data types"
            f" are incompatible for one of the mute rules in rule {rule.name}.",
            resolution="Reconfigure Mute rule using the Edit button in the CTO Module -> Business Rules page."
        )
        return
    filtered_data_items = _remove_muted_data_items(data_items, rule.muteRules)
    for name, queues in (
        rule.queues.items()
        if configuration is None
        else [(configuration, rule.queues[configuration])]
    ):
        try:
            configuration = connector.collection(
                Collections.ITSM_CONFIGURATIONS
            ).find_one({"name": name})
            if configuration is None:
                logger.error(
                    f"Ticket Orchestrator configuration {name} no longer exists.",
                    error_code="CTO_1010",
                )
                continue
            if configuration and not configuration.get("active"):
                logger.info(
                    f"CTO configuration {configuration.get('name')} is disabled. "
                    f"Skipping sync with business rule {rule.name}."
                )
                continue
            configuration = ConfigurationDB(**configuration)

            PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
            plugin = PluginClass(
                configuration.name,
                SecretDict(configuration.parameters),
                configuration.storage,
                configuration.checkpoint,
                logger,
            )
            failed, tickets, requests = 0, 0, 0
            for data_item in filtered_data_items:
                # Process the dedupe rules
                if any(rule.filters and not rule.filters.isValid for rule in rule.dedupeRules):
                    logger.error(
                        f"Filtering and task(s) creation of {len(data_items)} {data_type}(s) "
                        " have failed because one or more field data types"
                        f" are incompatible for one of the Deduplication rules in rule {rule.name}.",
                        resolution=(
                            "Reconfigure Deduplication rule using the Edit button in the",
                            " CTO Module -> Business Rules page."
                        )
                    )
                    return
                result = create_tickets_or_requests(
                    plugin=plugin,
                    data_item=data_item,
                    rule=rule,
                    queues=queues,
                    configuration=configuration,
                    data_type=data_type,
                )
                failed += result["failed"]
                tickets += result["tickets"]
                requests += result["requests"]
            if tickets or requests or failed:
                logger.info(
                    f"{tickets} task(s) and {requests} request(s) created successfully and {failed} task(s) failed "
                    f"for configuration {configuration.name}."
                )
            else:
                logger.info(
                    f"No tasks created for configuration {configuration.name}."
                )
            connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
                {"name": configuration.name},
                {"$set": {"storage": plugin.storage}},
            )
        except NotImplementedError:
            logger.error(
                f"Could not create tasks for the given {data_type}s with configuration {configuration.name}. "
                f"Plugin does not implement create_task method.",
                details=traceback.format_exc(),
                error_code="CTO_1025",
            )
        except Exception as ex:
            logger.error(
                f"Error occurred while creating tasks with configuration {configuration.name}. {repr(ex)}.",
                details=traceback.format_exc(),
                error_code="CTO_1026",
            )


def _create_tickets_async(jobs, data_type: DataType):
    """Create tickets according to mappings."""
    while not jobs.empty():
        value = jobs.get()
        data_items, rule, (name, queue) = value

        if not rule.filters.isValid:
            logger.error(
                f"Filtering and task(s) creation of {len(data_items)} {data_type}(s) "
                " have failed because one or more field data types"
                f" are incompatible for rule {rule.name}.",
                resolution="Reconfigure Business rule using the Edit button in the CTO Module -> Business Rules page."
            )
            continue
        filtered_data_items = _filter_data_items(data_items, rule.filters.mongo)
        if any(not rule.filters.isValid for rule in rule.muteRules):
            logger.error(
                f"Filtering and task(s) creation of {len(data_items)} {data_type}(s) "
                " have failed because one or more field data types"
                f" are incompatible for one of the mute rules in rule {rule.name}.",
                resolution="Reconfigure Mute rule using the Edit button in the CTO Module -> Business Rules page."
            )
            continue
        filtered_data_items = _remove_muted_data_items(
            filtered_data_items, rule.muteRules
        )
        try:
            configuration = connector.collection(
                Collections.ITSM_CONFIGURATIONS
            ).find_one({"name": name})
            if configuration is None:
                logger.debug(
                    f"Ticket Orchestrator configuration {name} no longer exists.",
                    error_code="CTO_1010",
                )
                continue
            if configuration and not configuration.get("active"):
                logger.info(
                    f"CTO configuration {name} is disabled. "
                    f"Skipping sync with business rule {rule.name}."
                )
                continue
            configuration = ConfigurationDB(**configuration)
            PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
            plugin = PluginClass(
                configuration.name,
                SecretDict(configuration.parameters),
                configuration.storage,
                configuration.checkpoint,
                logger,
            )
            failed_tasks = 0
            total_data_items = len(filtered_data_items)
            for data_item in filtered_data_items:
                if any(rule.filters and not rule.filters.isValid for rule in rule.dedupeRules):
                    logger.error(
                        f"Filtering and task(s) creation of {len(data_items)} {data_type}(s) "
                        " have failed because one or more field data types"
                        f" are incompatible for one of the Deduplication rules in rule {rule.name}.",
                        resolution=(
                            "Reconfigure Deduplication rule using the Edit button in the"
                            " CTO Module -> Business Rules page."
                        )
                    )
                    break
                result = create_tickets_or_requests(
                    plugin=plugin,
                    data_item=data_item,
                    rule=rule,
                    queues=queue,
                    configuration=configuration,
                    data_type=data_type,
                )
                failed_tasks += result["failed"]
            jobs.task_done()
            log = (
                f"Tickets creation task completed successfully for the configuration {configuration.name}."
            )
            if failed_tasks:
                if failed_tasks == total_data_items:
                    log += f" Failed to create all {failed_tasks} tickets."
                else:
                    log += (
                        f" Failed to create {failed_tasks} out of {total_data_items} ticket(s)."
                    )
            logger.info(log)
            connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
                {"name": configuration.name},
                {"$set": {"storage": plugin.storage}},
            )
        except NotImplementedError:
            logger.error(
                f"Could not create tasks for the given {data_type.value}s "
                f"with configuration {configuration.name}. "
                f"Plugin does not implement create_task method.",
                details=traceback.format_exc(),
                error_code="CTO_1012",
            )
        except Exception:
            logger.error(
                f"Error occurred while creating tasks with configuration {configuration.name}.",
                details=traceback.format_exc(),
                error_code="CTO_1013",
            )


def _create_tickets(
    data_items: Union[List[Alert], List[Event]],
    data_type: DataType
):
    """Create tickets for the list of alerts.

    Args:
        data_items (Union[List[Alert], List[Event]]): List of alerts/events.
        data_type: (DataType): Type of data (either 'alert' or 'event').
    """
    rules = [
        BusinessRuleDB(**r)
        for r in connector.collection(Collections.ITSM_BUSINESS_RULES).find(
            {"muted": False}
        )
    ]
    mappings = ()
    jobs = Queue()
    for rule in rules:
        mappings = _create_mappings_for_rules(rule, mappings)

    for mapping in mappings:
        mapping = (data_items,) + mapping
        jobs.put(mapping)

    threads = []
    for _ in range(MAX_NUM_OF_THREADS):
        threadProcess = threading.Thread(
            target=_create_tickets_async,
            args=(jobs, data_type),
        )
        threads.append(threadProcess)
        threadProcess.start()

    for thread in threads:
        thread.join()


def _create_mappings_for_rules(rule: BusinessRuleDB, mappings):
    """Create mappings for business rules."""
    if not rule.queues:  # ignore if no queues are linked
        return mappings
    for name, queues in rule.queues.items():
        mappings = mappings + ((rule, (name, queues)),)
    return mappings


def _store_data_items(
    data_items: Union[List[Alert], List[Event]],
    configuration: ConfigurationDB = None,
    data_type: DataType = DataType.ALERT,
):
    """Store the alerts/events into the database."""
    items = []
    storage_collection = (
        Collections.ITSM_ALERTS
        if data_type == DataType.ALERT
        else Collections.ITSM_EVENTS
    )
    for data_item in data_items:
        if configuration:
            data_item.configuration = configuration.name
        connector.collection(storage_collection).update_one(
            {"id": data_item.id}, {"$set": data_item.model_dump()}, upsert=True
        )
        items.append(data_item.model_dump())


def _sync_alerts_and_events(
    rule: str,
    configuration: str,
    start_time: datetime,
    end_time: datetime,
    data_type: DataType,
):
    """Sync alerts and events by data type."""
    # filtered alerts/events based on business rule
    query = {
        "$and": [
            json.loads(
                rule.filters.mongo,
                object_hook=lambda pair: parse_dates(pair, data_fields()),
            ),
            {"timestamp": {"$lte": end_time, "$gte": start_time}},
        ]
    }
    if not rule.filters.isValid:
        logger.error(
            f"Sync with business rule {rule.name} for configuration {configuration} "
            f" have failed because one or more field data types are incompatible for rule {rule.name}.",
            resolution=(
                "Reconfigure Business rule using the Edit button in the"
                " CTO Module -> Business Rules page."
            )
        )
        return []
    data_collection = Collections.ITSM_ALERTS if data_type == DataType.ALERT else Collections.ITSM_EVENTS
    Model = Alert if data_type == DataType.ALERT else Event
    total_count = 0
    skip = 0

    while True:
        results = []
        cursor = (
            connector.collection(data_collection)
            .find(query)
            .skip(skip)
            .limit(MAX_BATCH_SIZE)
        )
        for data in cursor:
            skip += 1
            results.append(Model(**data))
        total_count += len(results)
        _create_tickets_for_rule(results, rule, configuration, data_type=data_type)
        if skip % 10000 == 0 and len(results) != 0:
            continue
        else:
            break
    if not total_count:
        logger.info(f"No {data_type.value}s to be synced for configuration {configuration}.")
        return True
    return True


@APP.task(name="itsm.store_cre_alerts")
@integration("itsm")
@track()
def store_cre_alerts(alerts: List[Alert]):
    """Store alerts and trigger business rules.

    Args:
        alerts (List[Alert]): List of alerts.
    """
    if not alerts:
        return
    _store_data_items(alerts, data_type=DataType.ALERT)
    logger.info(f"Stored {len(alerts)} alerts successfully from CRE.")
    _create_tickets(alerts, data_type=DataType.ALERT)


@APP.task(name="itsm.sync_alerts_and_events")
@integration("itsm")
@track()
def sync_alerts_and_events(rule: str, configuration: str, days: int):
    """Sync all alerts and events with the queue."""
    logger.info(
        f"Sync with business rule {rule} for configuration {configuration} has started."
    )
    rule = connector.collection(Collections.ITSM_BUSINESS_RULES).find_one(
        {"name": rule}
    )
    if rule is None:
        logger.error(
            f"Business rule {rule} no longer exists. Skipping itsm.sync_alerts_and_events task.",
            error_code="CTO_1014",
        )
        return False
    rule = BusinessRuleDB(**rule)
    configuration_dict = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
        {"name": configuration}
    )
    if not configuration_dict.get("active"):
        logger.info(
            f"CTO configuration {configuration} is disabled. Skipping sync with business rule {rule.name}."
        )
        return False
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    _sync_alerts_and_events(rule, configuration, start_time, end_time, data_type=DataType.ALERT)
    _sync_alerts_and_events(rule, configuration, start_time, end_time, data_type=DataType.EVENT)


@APP.task(name="itsm.pull_data_items", acks_late=False)
@integration("itsm")
@track()
def pull_data_items(
    configuration_name: str,
    data: List = None,
    data_type: str = None,
    sub_type: str = None,
):
    """Pull alerts/events for the given configuration.

    Args:
        configuration_name (str): Name of the configuration.
        data (List|None): List of alerts/events.
        data_type (str|None): Type of the data.
        sub_type (str|None): Sub type of the data.
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
        logger.error(
            f"Plugin with ID {configuration.plugin} does not exist. Skipping itsm.pull_data_items task.",
            error_code="CTO_1015",
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
    data_type = DataType.EVENT if data_type == "events" else DataType.ALERT
    logger.info(f"Pulling {data_type.value}s for configuration {configuration.name}.")
    if isinstance(data, bytes):
        data = (
            parse_events(data, tenant_config_name=configuration.tenant, data_type=data_type.value, sub_type=sub_type)
            if configuration.tenant
            else parse_events(data, configuration=configuration, data_type=data_type.value, sub_type=sub_type)
        )
    try:
        plugin.data = data
        if sub_type is None:
            for i in plugin.data or []:
                if i.get("alert_type"):
                    sub_type = i["alert_type"]
                    break
        plugin.data_type = data_type
        plugin.sub_type = sub_type
        if plugin.data:
            try:
                tenant_obj = alerts_helper.get_tenant_itsm(configuration_name)
                provider_obj = plugin_provider_helper.get_provider(tenant_obj.name)
                provider_obj.extract_and_store_fields(
                    plugin.data,
                    (
                        NetskopeFieldType.ALERT
                        if data_type == DataType.ALERT
                        else NetskopeFieldType.EVENT
                    ),
                )
            except Exception as e:  # NOSONAR
                logger.warn(
                    f"Failed to extract and store fields for {sub_type} {data_type.value}s, {e}",
                    details=traceback.format_exc()
                )
        data = plugin.pull_alerts()
        data = apply_custom_mapping(data, configuration, direction=MappingDirection.FORWARD)
        connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
            {"name": configuration.name}, {"$set": {"checkpoint": checkpoint, "storage": plugin.storage}}
        )
        if not data:
            logger.info(
                f"No new {data_type.value}s received from configuration {configuration.name}."
            )
            return _end_life(configuration.name, True)
        logger.info(
            f"{len(data)} {data_type.value}(s) "
            f"pulled successfully from configuration {configuration.name}."
        )
    except NotImplementedError:
        logger.error(
            f"Could not pull {data_type.value}s. "
            "Plugin does not implement pull_alerts method.",
            details=traceback.format_exc(),
            error_code="CTO_1016",
        )
        return _end_life(configuration.name, False)
    except Exception:
        logger.error(
            "An exception occurred while executing itsm.pull_data_items task.",
            details=traceback.format_exc(),
            error_code="CTO_1017",
        )
        return _end_life(configuration.name, False)
    try:
        _store_data_items(
            data,
            configuration,
            data_type=data_type,
        )
        logger.info(
            f"Stored {len(data)} {data_type.value}s successfully"
            f" from configuration {configuration.name}."
        )
    except Exception:
        logger.error(
            f"An exception occurred while storing {data_type.value}s.",
            details=traceback.format_exc(),
            error_code="CTO_1018",
        )
        return _end_life(configuration.name, False)
    _create_tickets(data, data_type)
    return _end_life(configuration.name, True)


def pull_historical_data(
    data_type,
    tenant,
    sub_types,
    configuration,
    end_time
):
    """Pull historical data from netskope."""
    ingestion_count = 0
    total = 0
    try:
        iterator_name = f"{tenant.name}_{configuration.name}_%s_historical"
        if data_type == "alerts":
            start_time = end_time - timedelta(days=configuration.parameters.get("params", {})["days"])
        else:
            start_time = end_time - timedelta(hours=configuration.parameters.get("params", {})["hours"])
        if start_time == end_time:
            logger.info(f"Historical data pull for {data_type} has been skipped for '{configuration.name}' plugin,"
                        " because it is disabled from the configuration.")
            return {"success": True}
        if not sub_types:
            logger.info(
                f"Historical data pull for {data_type} has been skipped for '{configuration.name}' plugin,"
                f" because no {data_type} are selected in the configuration."
            )
            return {"success": False}

        ProviderClass = helper.find_by_id(tenant.plugin)
        provider = ProviderClass(
            tenant.name, tenant.parameters, tenant.storage, datetime.now(), logger
        )

        pulled_data = provider.pull(
            data_type,
            iterator_name,
            pull_type="historical_pulling",
            configuration_name=configuration.name,
            start_time=start_time,
            end_time=end_time,
            override_subtypes=sub_types,
            compress_historical_data=False,
        )

        for data, sub_type, _, _ in pulled_data:
            if data:
                for event in data:
                    event[f"{data_type.rstrip('s')}_type"] = sub_type
                provider.extract_and_store_fields(data, data_type, sub_type)
                execute_celery_task(
                    pull_data_items.apply_async,
                    "itsm.pull_data_items",
                    args=[
                        configuration.name,
                        gzip.compress(
                            json.dumps({"result": data}).encode("utf-8"),
                            compresslevel=3,
                        ),
                        data_type,
                        sub_type,
                    ],
                )
                counter = len(data)
                total += counter

        logger.info(
            f"Historical {data_type} pull has been completed for {configuration.name}."
            f"Total {data_type}: {total}, Ingestion Tasks Added: {ingestion_count}. "
        )
    except Exception:
        logger.error(
            f"Historical {data_type} pulling failed for {configuration.name}.",
            details=traceback.format_exc(),
            error_code="CTO_1041",
        )
        return {"success": False, "message": f"{data_type} pulling has been failed."}


@APP.task(name="itsm.pull_historical_events", acks_late=False)
@integration("itsm")
@track()
def pull_historical_events(
    configuration_name: str
):
    """Execute historical data pull.

    Args:
        source (str): Source configuration name.
        start_time (datetime): Start time.
        end_time (datetime): End time.
    """
    if not back_pressure_mechanism():
        return {"success": False}  # TODO: Do we need to add log?

    configuration = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
        {"name": configuration_name}
    )
    if configuration is None:
        logger.error(
            f"ITSM configuration {configuration} no longer exists. Skipping historical event data pull.",
            error_code="CTO_1037",
        )
        return {"success": False}
    configuration = ConfigurationDB(**configuration)
    success, content = validate_tenant(configuration.tenant, check_v2_token=False)
    if not success:
        return content

    tenant = TenantDB(**content)
    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
    plugin = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        configuration.storage,
        None,
        logger,
    )
    event_types = plugin.get_types_to_pull("events")
    pull_historical_data(
        "events",
        tenant,
        event_types,
        configuration,
        datetime.now()
    )
    try:
        connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
            {"name": configuration.name}, {"$set": {"storage": plugin.storage}}
        )
    except Exception:
        logger.error(
            f"Failed to update storage for configuration {configuration.name}.",
            details=traceback.format_exc(),
            error_code="CTO_1046",
        )
    return {"success": True}
