"""CLS Plugin Lifecycle."""

import gzip
import json
import os
import pickle
import time
import traceback
from functools import partial
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from itertools import repeat
from sys import getsizeof

from celery.exceptions import SoftTimeLimitExceeded
from mongoquery import Query

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import (
    NetskopeFieldType,
    TenantDB,
    CLSSIEMStatusType,
    CLSSIEMCountType,
    BatchDataType,
    BatchDataSourceType,
)
from netskope.common.models.settings import CLSRetryStrategy
from netskope.common.utils import (
    Collections,
    DBConnector,
    DataBatchManager,
    Logger,
    Notifier,
    SecretDict,
    integration,
    parse_dates,
    track,
    parse_events,
    deep_stringify,
)
from netskope.common.utils.alerts_helper import AlertsHelper
from netskope.common.utils.back_pressure import back_pressure_mechanism
from netskope.common.utils.const import SOFT_TIME_LIMIT, TASK_TIME_LIMIT
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.utils.validate_tenant import validate_tenant
from netskope.integrations.cls.models import BusinessRuleDB, ConfigurationDB, Filters
from netskope.integrations.cls.utils.webtx_parser import WebtxParser
from netskope.integrations.cls.tasks.pull import pull

connector = DBConnector()
batch_manager = DataBatchManager()
logger = Logger()
helper = PluginHelper()
notifier = Notifier()
plugin_provider_helper = PluginProviderHelper()
alerts_helper = AlertsHelper()

THREAD_COUNT = 3
MAX_RETRIES = 3
MAX_MAINTENANCE_WINDOW_MINUTES = int(os.getenv("MAX_MAINTENANCE_WINDOW_MINUTES", 15))
IDEAL_INGESTION_TIME_SEC = 10


def end_life(name: str, success: bool) -> bool:
    """Update the lastRunSuccess and lastRunAt and exit.

    Args:
        name (str): Name of the configuration.
        success (bool): lastRunSuccess value to be updated.

    Returns:
        bool: Value of `success`.
    """
    connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
        {"name": name},
        {
            "$set": {
                "lastRunAt": datetime.now(),
                "lastRunSuccess": success,
                # "lockedAt": None,
            }
        },
    )
    return success


def get_chunks(data, n_chunks):
    """Yield successive n_chunks sized chunks from list of data.

    Args:
        data: List to be divided in chunks
        n_chunks: Length of resultant list after division of the given list
    """
    for i in range(0, len(data), n_chunks):
        yield data[i:i + n_chunks]


def execute(func, data, n_chunks, data_type, subtype):
    """
    Add the given task to Queue, which will be eventually picked by one of the threads of the thread pool.

    :param func: Function to be executed in thread
    :param data: Data to be pushed to target SIEM. (Indirect argument of given function)
    :param data_type: Type of data being fetched (alert or event)
    :param subtype: Subtype of data being fetched (e.g. Subtypes of alert is policy, malware etc.)
    :param n_chunks: The size of data in which the given data will be divided and given to each thread
    """
    chunks = []
    for chunk in get_chunks(data, n_chunks):
        chunks.append(chunk)

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        for _ in executor.map(func, chunks, repeat(data_type), repeat(subtype)):
            # There's nothing to process in this loop as plugin.push() does not return anything.
            pass


@APP.task(name="cls.parse_and_ingest_webtx")
@integration("cls")
@track()
def parse_and_ingest_webtx(
    source: str, destination: str, rule: str, data: list, fields: list
):
    """Parse and ingest webtx."""
    allow_empty_values = False
    isJsonTransformation = False

    source_plugin_info = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
        {"name": source}
    )
    destination_plugin_info = connector.collection(
        Collections.CLS_CONFIGURATIONS
    ).find_one({"name": destination})

    if source_plugin_info:
        allow_empty_values = (
            True
            if source_plugin_info.get("parameters", {}).get("allow_empty_values", "no")
            == "yes"
            else False
        )
    if destination_plugin_info:
        isJsonTransformation = not destination_plugin_info.get("parameters", {}).get(
            "transformData", False
        )

    provider = None
    try:
        if source_plugin_info.get("tenant", ""):
            provider = plugin_provider_helper.get_provider(
                source_plugin_info.get("tenant", "")
            )
    except Exception:
        pass
    final_param_value = allow_empty_values and isJsonTransformation
    all_logs = []
    if provider and "transform" in dir(provider):
        all_logs = provider.transform(
            data, "webtx", "v2", fields=fields, allow_empty_values=final_param_value
        )
    else:
        # Backword compatibility for older version to new version migrations
        parser = WebtxParser(final_param_value)
        for message, message_fields in zip(data, fields):
            message = gzip.decompress(message).decode("utf-8")
            parser.fields = message_fields
            for log in message.split("\n"):
                all_logs.append(parser.parse(log))
    batch = batch_manager.create(
        BatchDataType.WEBTX,
        "v2",
        len(all_logs),
        source,
        BatchDataSourceType.CONFIGURATION,
    )
    batch_id = batch["_id"]
    logger.info(
        f"Parsed {len(all_logs)} webtx logs from {source} successfully for {destination}.",
        details=(f"Batch ID: {batch_id}" if batch_id else None),
    )
    batch_manager.add_cls_siem(
        batch_id,
        source,
        destination,
        CLSSIEMStatusType.PENDING,
    )
    if provider:
        provider.extract_and_store_fields(all_logs, NetskopeFieldType.WEBTX)
    batch_manager.update_cls_siem(
        batch_id,
        source,
        destination,
        CLSSIEMStatusType.FILTERED,
        count_type=CLSSIEMCountType.FILTERED,
        count=len(all_logs),
    )
    transform_and_ingest(
        source, destination, all_logs, "webtx", "v2", batch_id=batch_id
    )


@APP.task(name="cls.ingest")
@integration("cls")
@track()
def ingest(
    source: str,
    configuration: str,
    data: list,
    data_type: str,
    sub_type: str,
    retries_remaining: int = None,
    batch_id=None,
):
    """Ingest data into third party platform.

    Args:
        source (str): Name of the source configuration.
        configuration (str): Name of the destination configuration.
        data (List): Data to be ingested.
        data_type (str): Type of the data to be ingested (alerts, events)
        sub_type (str): Sub type of the data to be ingested.
        retries_remaining (int, optional): Number of remaining retries. Defaults to None.
        This should not be set from outside of `ingest`.

    Returns:
        dict: Result object.
    """
    if not batch_id and data_type == "webtx":
        batch_id = batch_manager.create(
            BatchDataType.WEBTX_BLOBS,
            sub_type,
            len(data),
            source,
            BatchDataSourceType.CONFIGURATION,
        ).get("_id", None)
        batch_manager.add_cls_siem(
            batch_id,
            source,
            configuration,
            CLSSIEMStatusType.PENDING,
        )
    if retries_remaining is None:
        settings = connector.collection(Collections.SETTINGS).find_one(
            {}, {"cls.retryStrategy": 1, "_id": 0}
        )
        if (
            settings.get("cls", {}).get("retryStrategy", CLSRetryStrategy.LIMITED)
            == CLSRetryStrategy.INFINITE
        ):
            retries_remaining = float("inf")
        else:
            retries_remaining = MAX_RETRIES
    if isinstance(data, bytes):
        data = pickle.loads(gzip.decompress(data))
    worker_start_time = int(time.time())
    logger.debug(
        f"Attempting to ingest {len(data)} [{data_type}][{sub_type}] log(s) to configuration {configuration}.",
        details=(f"Batch ID: {batch_id}" if batch_id else None),
    )
    configuration = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
        {"name": configuration}
    )
    if not configuration:
        return {
            "success": False,
            "message": f"Configuration {configuration} no longer exists.",
        }
    configuration = ConfigurationDB(**configuration)
    if not configuration.active:
        if batch_id:
            batch_manager.update_cls_siem(
                batch_id,
                source,
                configuration.name,
                CLSSIEMStatusType.ERROR,
                CLSSIEMCountType.INGESTED,
                count=0,
                error_on=CLSSIEMStatusType.INGESTING,
            )
        return {
            "success": True,
            "message": f"Configuration {configuration} is disabled. Not ingesting.",
        }
    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR
    if not PluginClass:
        return {
            "success": False,
            "message": f"Plugin with id {configuration.plugin} no longer exists.",
        }
    ingested_count = len(data)
    while retries_remaining > 0:
        try:
            if batch_id:
                batch_manager.update_cls_siem(
                    batch_id, source, configuration.name, CLSSIEMStatusType.INGESTING
                )
            plugin = PluginClass(
                configuration.name,
                SecretDict(configuration.parameters),
                configuration.storage or {},
                None,
                logger,
                mappings=None,  # mapping file not required for ingestion
                source=source,
            )
            result = plugin.push(data, data_type, sub_type)
            if result and hasattr(result, "failed_data") and result.failed_data:
                ingested_count -= len(result.failed_data)
            if data_type == "webtx" and sub_type == "2.0.0":
                size = 0
                for message in data:
                    size += getsizeof(message)
                if result and hasattr(result, "failed_data") and result.failed_data:
                    ingested_count -= len(result.failed_data)
                    for message in result.failed_data:
                        size -= getsizeof(message)
                connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
                    {"name": configuration.name},
                    {
                        "$inc": {
                            "bytesIngested": size,
                        },
                        "$set": {
                            "lastRunAt": datetime.now(),
                            "storage": plugin.storage,
                        },
                    },
                )
                worker_end_time = int(time.time())
                logger.info(
                    f"Ingested {ingested_count} [{data_type}][{sub_type}] log(s) of size {size} bytes into "
                    f"configuration {configuration.name} successfully. "
                    f"Time taken: {worker_end_time - worker_start_time} seconds.",
                    details=(f"Batch ID: {batch_id}" if batch_id else None),
                )
                if batch_id:
                    batch_manager.update_cls_siem(
                        batch_id,
                        source,
                        configuration.name,
                        CLSSIEMStatusType.FILTERED,
                        CLSSIEMCountType.FILTERED,
                        count=ingested_count,
                        size=size,
                    )
                    batch_manager.update_cls_siem(
                        batch_id,
                        source,
                        configuration.name,
                        CLSSIEMStatusType.TRANSFORMED,
                        CLSSIEMCountType.TRANSFORMED,
                        count=ingested_count,
                        size=size,
                    )
                    batch_manager.update_cls_siem(
                        batch_id,
                        source,
                        configuration.name,
                        CLSSIEMStatusType.INGESTED,
                        CLSSIEMCountType.INGESTED,
                        count=ingested_count,
                        size=size,
                    )
            else:
                if batch_id:
                    batch_manager.update_cls_siem(
                        batch_id,
                        source,
                        configuration.name,
                        CLSSIEMStatusType.INGESTED,
                        CLSSIEMCountType.INGESTED,
                        count=ingested_count,
                    )
                connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
                    {"name": configuration.name},
                    {
                        "$inc": {
                            "logsIngested": ingested_count,
                        },
                        "$set": {
                            "lastRunAt": datetime.now(),
                            "storage": plugin.storage,
                        },
                    },
                )
                worker_end_time = int(time.time())
                logger.info(
                    f"Ingested {ingested_count} [{data_type}][{sub_type}] log(s) into "
                    f"configuration {configuration.name} successfully. "
                    f"Time taken: {worker_end_time-worker_start_time} seconds.",
                    details=(f"Batch ID: {batch_id}" if batch_id else None),
                )
            return {"success": True}
        except SoftTimeLimitExceeded:
            raise
        except Exception:
            retries_remaining = retries_remaining - 1
            if retries_remaining > 0:
                # refresh configuration
                configuration = connector.collection(
                    Collections.CLS_CONFIGURATIONS
                ).find_one({"name": configuration.name})
                if not configuration:
                    logger.error(
                        (
                            f"Error occurred while ingesting [{data_type}][{sub_type}] data, "
                            f"discarding the data as destination configuration does not exist."
                        ),
                        error_code="CLS_1005",
                        details=(f"Batch ID: {batch_id}\n" if batch_id else "")
                        + f"{traceback.format_exc()}",
                    )
                    break
                configuration = ConfigurationDB(**configuration)
                if not configuration.active:
                    logger.error(
                        (
                            f"Error occurred while ingesting [{data_type}][{sub_type}] data for {configuration.name}, "
                            f"discarding the data as destination configuration is now disabled."
                        ),
                        error_code="CLS_1005",
                        details=(f"Batch ID: {batch_id}\n" if batch_id else "")
                        + f"{traceback.format_exc()}",
                    )
                    break
                message = (
                    f"Error occurred while ingesting [{data_type}][{sub_type}] data, "
                    f"re-attempting to ingest [{data_type}][{sub_type}] data for {configuration.name} configuration."
                )
                if retries_remaining != float("inf"):
                    message = (
                        f"{message} "
                        f"{retries_remaining} {'retry' if retries_remaining == 1 else 'retries'} remaining."
                    )
                logger.error(
                    message,
                    error_code="CLS_1005",
                    details=(f"Batch ID: {batch_id}\n" if batch_id else "")
                    + f"{traceback.format_exc()}",
                )
                time.sleep(30)
            else:
                logger.error(
                    f"Error occurred while ingesting [{data_type}][{sub_type}] data, "
                    f"discarding [{data_type}][{sub_type}] data for {configuration.name} configuration.",
                    error_code="CLS_1005",
                    details=(f"Batch ID: {batch_id}\n" if batch_id else "")
                    + f"{traceback.format_exc()}",
                )
                break


def _transform_and_ingest(
    source, destination, data, data_type, data_subtype, historical=False, batch_id=None
):
    """Transform data and schedule an ingestion.

    Args:
        destination (str): Destination configuration name.
        events (List): List of events.
        event_type (str): Event type.

    Returns:
        bool: Whether the transformation was successful or not.
    """
    destination = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
        {"name": destination}
    )
    if not destination:  # destination no longer exists
        return False
    destination = ConfigurationDB(**destination)
    if not destination.active:  # destination is disabled; ignore
        return True
    PluginClass = helper.find_by_id(destination.plugin)  # NOSONAR S117
    if PluginClass is None:
        logger.error(
            f"Could not find the plugin with id='{destination.plugin}'. Skipping transform and ingest for CLS.",
            error_code="CLS_1006",
            details=(f"Batch ID: {batch_id}" if batch_id else None),
        )
        return False
    logger.info(
        f"Transforming {len(data)} [{data_type}][{data_subtype}] log(s) for {destination.name} configuration.",
        details=(f"Batch ID: {batch_id}" if batch_id else None),
    )
    if batch_id:
        batch_manager.update_cls_siem(
            batch_id,
            source,
            destination.name,
            CLSSIEMStatusType.TRANSFORMING,
            CLSSIEMCountType.TRANSFORMED,
            count=0,
        )
    mapping = None
    if destination.attributeMapping:
        mapping = connector.collection(Collections.CLS_MAPPING_FILES).find_one(
            {
                "name": destination.attributeMapping,
                "repo": destination.attributeMappingRepo,
            }
        )
        if not mapping:
            attribute_mapping_repo_text = (
                f" ({destination.attributeMappingRepo})"
                if destination.attributeMappingRepo
                else ""
            )
            logger.error(
                f"Could not find the mapping file {destination.attributeMapping}{attribute_mapping_repo_text}"
                f" required for {destination.name}. Skipping transform and ingest for CLS.",
                error_code="CLS_1007",
                details=(f"Batch ID: {batch_id}" if batch_id else None),
            )
            if batch_id:
                batch_manager.update_cls_siem(
                    batch_id,
                    source,
                    destination.name,
                    CLSSIEMStatusType.ERROR,
                    CLSSIEMCountType.TRANSFORMED,
                    count=0,
                    error_on=CLSSIEMStatusType.TRANSFORMING,
                )
            return False
    if (
        PluginClass
        and PluginClass.metadata
        and not PluginClass.metadata.get("format_options", None)
    ):
        destination.parameters.update(
            {
                "transformData": destination.parameters.get("transformData", "cef")
                == "cef"
            }
        )
    mapping_data = None if mapping is None else json.loads(mapping.get("jsonData"))
    plugin = PluginClass(
        destination.name,
        SecretDict(destination.parameters),
        destination.storage,
        None,
        logger,
        mappings=mapping_data,
        source=source,
    )
    if mapping_data:
        if data_subtype.lower() not in get_supported_subtypes(mapping_data):
            logger.debug(
                f"Transformation of {len(data)} [{data_type}][{data_subtype}] "
                f"for {destination.name} has been skipped due to missing mapping.",
                details=(f"Batch ID: {batch_id}" if batch_id else None),
            )
            return True
    try:
        plugin.data = data
        plugin.data_type = data_type
        plugin.sub_type = data_subtype
        transformed = plugin.transform(data, data_type, data_subtype)
        logger.info(
            f"Transformation of {len(transformed)} [{data_type}][{data_subtype}] "
            f"for {destination.name} has been complete.",
            details=(f"Batch ID: {batch_id}" if batch_id else None),
        )
        if plugin.storage:
            connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
                {"name": destination.name},
                {"$set": {"storage": plugin.storage}},
            )
        if batch_id:
            batch_manager.update_cls_siem(
                batch_id,
                source,
                destination.name,
                CLSSIEMStatusType.TRANSFORMED,
                CLSSIEMCountType.TRANSFORMED,
                count=len(transformed),
            )
        if transformed:
            start_time = time.time()
            ingestion_status = ingest(
                source,
                destination.name,
                transformed,
                data_type,
                data_subtype,
                batch_id=batch_id,
            )
            if batch_id and (
                not isinstance(ingestion_status, dict)
                or not ingestion_status.get("success")
            ):
                batch_manager.update_cls_siem(
                    batch_id,
                    source,
                    destination.name,
                    CLSSIEMStatusType.ERROR,
                    CLSSIEMCountType.INGESTED,
                    count=0,
                    error_on=CLSSIEMStatusType.INGESTING,
                )
            ingestion_time_diff = round(time.time() - start_time, 2)
            if (
                isinstance(ingestion_status, dict)
                and ingestion_status.get("success")
                and ingestion_time_diff > IDEAL_INGESTION_TIME_SEC
            ):
                logger.warn(
                    f"Ingestion of {len(data)} [{data_type}][{data_subtype}] for "
                    f"{destination.name} has taken {ingestion_time_diff}"
                    " seconds to complete.",
                    details=(f"Batch ID: {batch_id}" if batch_id else None),
                )
            return (
                ingestion_status.get("success")
                if isinstance(ingestion_status, dict)
                else False
            )
        else:
            return False
    except NotImplementedError:
        logger.error(
            f"Plugin {destination.plugin} has not implemented transform method.",
            error_code="CLS_1008",
            details=(f"Batch ID: {batch_id}\n" if batch_id else "")
            + f"{traceback.format_exc()}",
        )
        if batch_id:
            batch_manager.update_cls_siem(
                batch_id,
                source,
                destination.name,
                CLSSIEMStatusType.ERROR,
                CLSSIEMCountType.TRANSFORMED,
                count=0,
                error_on=CLSSIEMStatusType.TRANSFORMING,
            )
    except SoftTimeLimitExceeded:
        raise
    except Exception:
        logger.error(
            f"Transformation of {len(data)} [{data_type}][{data_subtype}] for {destination.name} has "
            f"failed with an exception.",
            error_code="CLS_1009",
            details=(f"Batch ID: {batch_id}\n" if batch_id else "")
            + f"{traceback.format_exc()}",
        )
        if batch_id:
            batch_manager.update_cls_siem(
                batch_id,
                source,
                destination.name,
                CLSSIEMStatusType.ERROR,
                CLSSIEMCountType.TRANSFORMED,
                count=0,
                error_on=CLSSIEMStatusType.TRANSFORMING,
            )
        return False


def pull_third_party_historical_data(
    source: str,
    destination: str,
    start_time: datetime,
    end_time: datetime,
    query: Query,
    rule: BusinessRuleDB,
):
    """Pull third-party historical data."""

    def process_logs(
        source: str,
        destination: str,
        rule: BusinessRuleDB,
        data: list[dict],
        data_type: str,
        data_subtype: str,
    ):
        if isinstance(data, bytes):
            configuration_dict = connector.collection(
                Collections.CLS_CONFIGURATIONS
            ).find_one({"name": source})
            if configuration_dict:
                configuration = ConfigurationDB(**configuration_dict)
                data = (
                    parse_events(
                        data,
                        tenant_config_name=configuration.tenant,
                        data_type=data_type,
                        sub_type=data_subtype,
                    )
                    if configuration.tenant
                    else parse_events(
                        data,
                        configuration=configuration,
                        data_type=data_type,
                        sub_type=data_subtype,
                    )
                )
            else:
                data = parse_events(data)
        data = data.get("result", []) if isinstance(data, dict) else data
        rule_filters = Filters(**rule.filters.model_dump())
        if not rule_filters.isValid:
            logger.error(
                f"Filtering and transformation of {len(data)} historical {data_subtype} {data_type} "
                f"have failed because one or more field data types "
                f"are incompatible for rule {rule.name}."
                " Reconfigure Business rule using the Edit button in the CLS Module -> Business Rules page."
            )
            return
        filtered_data = list(filter(lambda d: query.match(deep_stringify(d)), data))

        if filtered_data:
            filtered_data = gzip.compress(
                json.dumps({"result": filtered_data}).encode("utf-8"),
                compresslevel=3,
            )
            execute_celery_task(
                transform_and_ingest.apply_async,
                "cls.transform_and_ingest",
                args=[
                    source,
                    destination,
                    filtered_data,
                    data_type,
                    data_subtype,
                    True,
                ],
            )

    try:
        return pull(
            source,
            start_time,
            end_time,
            destination,
            rule.name,
            lifecycle=partial(
                process_logs,
                source=source,
                destination=destination,
                rule=rule,
            ),
        )
    except Exception:
        logger.error(
            f"Historical pulling failed for the window {start_time} UTC to {end_time} UTC "
            f"for {source} to {destination}, rule {rule.name}.",
            details=traceback.format_exc(),
            error_code="CLS_1013",
        )


def pull_historical_data(
    type_,
    tenant,
    sub_types,
    source,
    destination,
    start_time,
    end_time,
    is_manual_sync,
    query,
    rule,
):
    """Pull historical data from netskope."""
    ingestion_count = 0
    total = 0
    try:
        iterator_name = f"{tenant.name}_{source.name}_{destination}"
        if rule.siemMappingIDs and rule.siemMappingIDs.get(
            f"{source.name}_{destination}"
        ):
            iterator_name += (
                f"_{rule.siemMappingIDs[f'{source.name}_{destination}']['id']}"
            )
        if not is_manual_sync:
            iterator_name += "_%s_historical"
            if type_ == "alerts":
                start_time = end_time - timedelta(days=source.parameters["days"])
        else:
            iterator_name += f"{start_time}_%s_historical"

        ProviderClass = helper.find_by_id(tenant.plugin)
        provider = ProviderClass(
            tenant.name, tenant.parameters, tenant.storage, datetime.now(), logger
        )

        pulled_data = provider.pull(
            type_,
            iterator_name,
            pull_type="historical_pulling",
            configuration_name=source.name,
            start_time=start_time,
            end_time=end_time,
            destination_configuration=destination,
            business_rule=rule.name,
            override_subtypes=sub_types,
            compress_historical_data=True,
        )

        for data, data_type, _, _ in pulled_data:
            if isinstance(data, bytes):
                data = (
                    parse_events(
                        data,
                        tenant_config_name=source.tenant,
                        data_type=type_,
                        sub_type=data_type,
                    )
                    if source.tenant
                    else parse_events(
                        data, configuration=source, data_type=type_, sub_type=data_type
                    )
                )
            data = data.get("result", []) if isinstance(data, dict) else data
            batch = batch_manager.create(
                type_,
                data_type,
                len(data),
                source.name,
                BatchDataSourceType.CONFIGURATION,
            )
            batch_id = batch["_id"]
            batch_manager.add_cls_siem(
                batch_id,
                source.name,
                destination,
                CLSSIEMStatusType.PENDING,
            )
            for event in data:
                event[f"{type_.rstrip('s')}_type"] = data_type
            if provider:
                provider.extract_and_store_fields(data, type_, data_type)
            rule_filters = Filters(**rule.filters.model_dump())
            if not rule_filters.isValid:
                logger.error(
                    f"Filtering and transformation of {len(data)} historical {type_} alerts "
                    f" have failed because one or more field data types"
                    f" are incompatible for rule {rule.name}.",
                    resolution=(
                        "Reconfigure Business rule using the Edit button in the "
                        "CLS Module -> Business Rules page."
                    ),
                    details=(f"Batch ID: {batch_id}" if batch_id else None),
                )
                continue
            filtered_data = list(filter(lambda d: query.match(deep_stringify(d)), data))

            if filtered_data:
                batch_manager.update_cls_siem(
                    batch_id,
                    source.name,
                    destination,
                    CLSSIEMStatusType.FILTERED,
                    CLSSIEMCountType.FILTERED,
                    len(filtered_data),
                )
                ingestion_count += 1
                filtered_data = gzip.compress(
                    json.dumps({"result": filtered_data}).encode("utf-8"),
                    compresslevel=3,
                )
                execute_celery_task(
                    transform_and_ingest.apply_async,
                    "cls.transform_and_ingest",
                    args=[
                        source.name,
                        destination,
                        filtered_data,
                        type_,
                        data_type,
                        True,
                        batch_id,
                    ],
                )
            counter = len(data)
            total += counter

        logger.info(
            f"Historical {type_} pull has been completed for {source.name} to {destination}, rule {rule.name}. "
            f"Total {type_}: {total}, Ingestion Tasks Added: {ingestion_count}. "
        )
    except Exception:
        if is_manual_sync:
            logger.error(
                f"Historical {type_} pulling failed for the window {start_time} UTC to {end_time} UTC "
                f"for {source.name} to {destination}, rule {rule.name}.",
                details=traceback.format_exc(),
                error_code="CLS_1013",
            )
        else:
            logger.error(
                f"Historical {type_} pulling failed for {source.name} to {destination}, rule {rule.name}.",
                details=traceback.format_exc(),
                error_code="CLS_1014",
            )
        return {"success": False, "message": f"{type_} pulling has been failed."}


def pull_logs(source, destination, start_time, end_time, is_manual_sync, rule):
    """Pull logs from netskope."""
    try:
        if PluginHelper.is_syslog_service_plugin(source.plugin):
            PluginClass = helper.find_by_id(source.plugin)  # NOSONAR
            if PluginClass is None:
                logger.error(
                    f"Could not find the plugin with id='{source.plugin}'",
                    error_code="CE_1115",
                )
                return {"success": False}

            plugin = PluginClass(
                source.name,
                SecretDict(source.parameters),
                source.storage,
                None,
                logger,
                mappings=None,  # mapping file not required for ingestion
                source=None,
            )
            if start_time is None:
                start_time = datetime.now() - timedelta(
                    source.parameters.get("days", 7)
                )
            logs_type = source.parameters.get("logs_type", ["info", "warning", "error"])
            query = {
                "$and": [
                    {"ce_log_type": {"$in": logs_type}},
                    {"createdAt": {"$gte": start_time, "$lt": end_time}},
                ]
            }
            filter_query = json.loads(
                rule.filters.mongo,
                object_hook=lambda pair: parse_dates(
                    pair, ignore_regex=True, add_legecy_prefix=False
                ),
            )
            filter_query = Query(filter_query)
            logs_cursor = connector.collection(Collections.LOGS).find(query)
            logs_batch = plugin.pull(logs_cursor, start_time, end_time)
            count = 0
            for logs in logs_batch:
                filtered_logs_dict = defaultdict(list)
                for log in logs:
                    count += 1
                    filtered_logs_dict[log.get("ce_log_type", log.get("type"))].append(log)
                for log_type, logs_data in filtered_logs_dict.items():
                    if logs_data:
                        batch = batch_manager.create(
                            BatchDataType.LOGS,
                            log_type,
                            len(logs_data),
                            source.name,
                            BatchDataSourceType.CONFIGURATION,
                        )
                        batch_id = batch["_id"]
                        batch_manager.add_cls_siem(
                            batch_id,
                            source.name,
                            destination,
                            CLSSIEMStatusType.PENDING,
                        )
                        rule_filters = Filters(**rule.filters.model_dump())
                        if not rule_filters.isValid:
                            logger.error(
                                f"Filtering and transformation of {len(logs_data)} {log_type} logs "
                                f" have failed because one or more field data types"
                                f" are incompatible for rule {rule.name}.",
                                resolution=(
                                    "Reconfigure Business rule using the Edit button in "
                                    "the CLS Module -> Business Rules page."
                                ),
                                details=(f"Batch ID: {batch_id}" if batch_id else None),
                            )
                            continue
                        filtered_logs = list(
                            filter(
                                lambda log_item: filter_query.match(
                                    deep_stringify(log_item)
                                ),
                                logs_data,
                            )
                        )
                        logger.debug(
                            f"{len(filtered_logs)} {log_type} logs remaining after filtering.",
                            details=(f"Batch ID: {batch_id}" if batch_id else None),
                        )
                        if filtered_logs:
                            for filtered_log in filtered_logs:
                                if "createdAt" in filtered_log and isinstance(
                                    filtered_log["createdAt"], datetime
                                ):
                                    filtered_log["createdAt"] = filtered_log[
                                        "createdAt"
                                    ].strftime("%m/%d/%Y %I:%M:%S %p")
                            batch_manager.update_cls_siem(
                                batch_id,
                                source.name,
                                destination,
                                CLSSIEMStatusType.FILTERED,
                                CLSSIEMCountType.FILTERED,
                                len(filtered_logs),
                            )
                            _ = _transform_and_ingest(
                                source.name,
                                destination,
                                filtered_logs,
                                "logs",
                                log_type,
                                historical=True,
                                batch_id=batch_id,
                            )
            connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
                {"name": source.name}, {"$set": {"storage": plugin.storage}}
            )
            if is_manual_sync:
                logger.info(
                    f"Historical log pull has been completed for the window "
                    f"{start_time} UTC to {end_time} UTC "
                    f"for {source.name} to {destination}, rule {rule.name}."
                    f"Total Logs: {count}."
                )
            else:
                logger.info(
                    f"Historical log pull has been completed "
                    f"for {source.name} to {destination}, rule {rule.name}."
                    f"Total Logs: {count}."
                )
            return {"success": True}
    except Exception as err:
        logger.error(f"Error occurred while fetching logs: {err}")
        return {
            "success": False,
            "message": f"Error occurred while fetching logs: {err}",
        }


@APP.task(name="cls.transform_and_ingest")
@track()
def transform_and_ingest(
    source, destination, data, data_type, data_subtype, historical=False, batch_id=None
):
    """Wrap task for private transform and ingest method."""
    if batch_id:
        batch_manager.update_cls_siem(
            batch_id, source, destination, CLSSIEMStatusType.TRANSFORMING
        )
    configuration = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
        ({"name": source})
    )
    if configuration is None:
        if batch_id:
            batch_manager.update_cls_siem(
                batch_id,
                source,
                destination,
                CLSSIEMStatusType.ERROR,
                CLSSIEMCountType.TRANSFORMED,
                0,
                error_on=CLSSIEMStatusType.TRANSFORMING,
            )
        return {
            "success": False,
            "message": f"Configuration {source} does not exist.",
        }
    if isinstance(data, bytes):
        if configuration:
            data = (
                parse_events(
                    data,
                    tenant_config_name=configuration.get("tenant"),
                    data_type=data_type,
                    sub_type=data_subtype,
                )
                if configuration.get("tenant")
                else parse_events(
                    data,
                    configuration=ConfigurationDB(**configuration),
                    data_type=data_type,
                    sub_type=data_subtype,
                )
            )
        else:
            data = parse_events(data)
    data = data.get("result", []) if isinstance(data, dict) else data
    _ = _transform_and_ingest(
        source,
        destination,
        data,
        data_type,
        data_subtype,
        historical,
        batch_id=batch_id,
    )


@APP.task(name="cls.execute_historical", acks_late=False)
@track()
def execute_historical(
    source: str,
    destination: str,
    rule: str,
    start_time: datetime,
    start_time_alert: datetime,
    end_time: datetime,
    subtype: str = ...,
    is_manual_sync: bool = False,
):
    """Execute historical data pull.

    Args:
        source (str): Source configuration name.
        destination (str): Destination configuration name.
        rule (str): Business rule name.
        start_time (datetime): Start time.
        end_time (datetime): End time.
        subtype (str, optional): Subtype of events. Defaults to ....
        retries_remaining (int, optional): Number of retries. Defaults to 3.
    """
    if not back_pressure_mechanism():
        return {"success": False}  # TODO: Do we need to add log?

    rule_dict = connector.collection(Collections.CLS_BUSINESS_RULES).find_one(
        {"name": rule}
    )

    if rule_dict is None:
        logger.info(
            f"Business rule {rule} no longer exists. Skipping historical data pull.",
            error_code="CLS_1010",
        )
        return {"success": False}

    rule = BusinessRuleDB(**rule_dict)
    source_dict = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
        {"name": source}
    )
    if source_dict is None:
        logger.info(
            f"CLS configuration {source} no longer exists. Skipping historical data pull.",
            error_code="CLS_1011",
        )
        return {"success": False}
    destination_dict = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
        {"name": destination}
    )
    if destination_dict is None:
        logger.info(
            f"CLS configuration {destination} no longer exists. Skipping historical data pull.",
            error_code="CLS_1012",
        )
        return {"success": False}
    if not destination_dict.get("active"):
        logger.debug(
            f"CLS configuration {destination} is disabled. Skipping historical data pull."
        )
        return {"success": True}
    source: ConfigurationDB = ConfigurationDB(**source_dict)

    query = json.loads(
        rule.filters.mongo,
        object_hook=lambda pair: parse_dates(
            pair, ignore_regex=True, add_legecy_prefix=False
        ),
    )
    query = Query(query)

    if PluginHelper.is_syslog_service_plugin(source.plugin):
        return pull_logs(
            source, destination, start_time, end_time, is_manual_sync, rule
        )

    if source.tenant is None:  # third party plugin that supports pull
        return pull_third_party_historical_data(
            source.name, destination, start_time, end_time, query, rule
        )

    success, content = validate_tenant(source.tenant, check_v2_token=False)
    if not success:
        return content

    tenant = TenantDB(**content)
    sub_types = source.parameters.get("event_type", []) if subtype is ... else [subtype]
    PluginClass = helper.find_by_id(source.plugin)  # NOSONAR S117
    if PluginClass is None:
        return {
            "success": False,
            "message": f"Could not find plugin with id {source.plugin}.",
        }
    supported_data_types = PluginClass.metadata.get("supported_subtypes", {}).keys()
    if "events" in supported_data_types:
        if end_time == start_time:
            logger.info(
                f"Historical data pull for events has been skipped for '{source.name}' plugin,"
                " because it is disabled from the configuration."
            )
        else:
            pull_historical_data(
                "events",
                tenant,
                sub_types,
                source,
                destination,
                start_time,
                end_time,
                is_manual_sync,
                query,
                rule,
            )
    if "alerts" in supported_data_types:
        if start_time_alert == end_time:
            logger.info(
                f"Historical data pull for alerts has been skipped for '{source.name}' plugin,"
                " because it is disabled from the configuration."
            )
        else:
            pull_historical_data(
                "alerts",
                tenant,
                source.parameters.get("alert_types", []),
                source,
                destination,
                start_time_alert,
                end_time,
                is_manual_sync,
                query,
                rule,
            )
    return {"success": True}


@APP.task(name="cls.execute_plugin")
@integration("cls")
@track()
def execute_plugin(
    configuration_name,
    historical=False,
    data: list = None,
    data_type: str = None,
    sub_type: str = None,
    logs: list = None,
    batch_id: str = None,
):
    """Execute CLS plugin lifecycle.

    Args:
        configuration_name (str): Name of the source configuration.
        events (List, optional): List of events.. Defaults to None.
        event_type (str, optional): Event type. Defaults to None.
        alerts (List, optional): List of alerts.. Defaults to None.
        Logs (List, optional): List of CE logs.. Defaults to None.
    """
    try:
        logger.update_level()
        success = True
        configuration = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
            ({"name": configuration_name})
        )
        if configuration is None:
            return {
                "success": False,
                "message": f"Configuration {configuration_name} does not exist.",
            }

        # Update the source configuration end_life
        end_life(configuration_name, True)
        prev_data = data
        data_len = None

        configuration = ConfigurationDB(**configuration)

        if isinstance(data, bytes):
            data = (
                parse_events(
                    data,
                    tenant_config_name=configuration.tenant,
                    data_type=data_type,
                    sub_type=sub_type,
                )
                if configuration.tenant
                else parse_events(
                    data,
                    configuration=configuration,
                    data_type=data_type,
                    sub_type=sub_type,
                )
            )

        PluginClass = helper.find_by_id(configuration.plugin)

        if not PluginClass:
            return {
                "success": False,
                "message": f"Could not find plugin with id {configuration.plugin}.",
            }

        plugin = PluginClass(
            configuration.name,
            SecretDict(configuration.parameters),
            configuration.storage,
            None,
            logger,
        )

        field_learning_data = []

        for rule in connector.collection(Collections.CLS_BUSINESS_RULES).find({}):
            rule = BusinessRuleDB(**rule)
            if destinations := rule.siemMappings.get(configuration.name):
                if batch_id:
                    for destination in destinations:
                        batch_manager.add_cls_siem(
                            batch_id,
                            configuration.name,
                            destination,
                            CLSSIEMStatusType.PENDING,
                        )
                query = json.loads(
                    rule.filters.mongo,
                    object_hook=lambda pair: parse_dates(
                        pair, ignore_regex=True, add_legecy_prefix=False
                    ),
                )
                query = Query(query)
                if data_type == "alerts" and data is not None:
                    filtered_alerts_dict = defaultdict(list)
                    alerts = data.get("result", []) if isinstance(data, dict) else data
                    data_len = len(alerts)
                    for alert in alerts:
                        filtered_alerts_dict[alert["alert_type"]].append(alert)
                    # Transform and push the filtered_alerts
                    for alert_type, alert_data in filtered_alerts_dict.items():
                        logger.info(
                            f"Found {len(alert_data)} {alert_type} alerts.",
                            details=(f"Batch ID: {batch_id}" if batch_id else None),
                        )
                        if alert_data:
                            field_learning_data.append(
                                (alert_data, NetskopeFieldType.ALERT, alert_type)
                            )
                        rule_filters = Filters(**rule.filters.model_dump())
                        if not rule_filters.isValid:
                            logger.error(
                                f"Filtering and transformation of {len(alert_data)} {alert_type} alerts "
                                f" have failed because one or more field data types"
                                f" are incompatible for rule {rule.name}.",
                                resolution=(
                                    "Reconfigure Business rule using the Edit button in "
                                    "the CLS Module -> Business Rules page."
                                ),
                                details=(f"Batch ID: {batch_id}" if batch_id else None),
                            )
                            continue
                        filtered_alerts = list(
                            filter(
                                lambda alert: query.match(deep_stringify(alert)),
                                alert_data,
                            )
                        )
                        logger.debug(
                            f"{len(filtered_alerts)} {alert_type} alerts remaining after filtering.",
                            details=(f"Batch ID: {batch_id}" if batch_id else None),
                        )
                        # add another task with the transformed data to the queue
                        for destination in destinations:
                            success = True
                            batch_manager.update_cls_siem(
                                batch_id,
                                configuration_name,
                                destination,
                                CLSSIEMStatusType.FILTERED,
                                count_type=CLSSIEMCountType.FILTERED,
                                count=len(filtered_alerts),
                            )
                            if filtered_alerts:
                                if alert_type in ["ctep", "ips", "c2"]:
                                    success = _transform_and_ingest(
                                        configuration_name,
                                        destination,
                                        filtered_alerts,
                                        "alerts",
                                        "ctep",
                                        historical=historical,
                                        batch_id=batch_id,
                                    )
                                else:
                                    success = _transform_and_ingest(
                                        configuration_name,
                                        destination,
                                        filtered_alerts,
                                        "alerts",
                                        alert_type,
                                        historical=historical,
                                        batch_id=batch_id,
                                    )
                            end_life(destination, success)
                elif data_type == "events" and data is not None:
                    event_data = (
                        data.get("result", []) if isinstance(data, dict) else data
                    )
                    data_len = len(event_data)
                    for event in event_data:
                        event["event_type"] = sub_type

                    logger.info(
                        f"Found {len(event_data)} {sub_type} events.",
                        details=(f"Batch ID: {batch_id}" if batch_id else None),
                    )
                    if event_data:
                        field_learning_data.append(
                            (event_data, NetskopeFieldType.EVENT, sub_type)
                        )
                    rule_filters = Filters(**rule.filters.model_dump())
                    if not rule_filters.isValid:
                        logger.error(
                            f"Filtering and transformation of {len(event_data)} {sub_type} events "
                            f" have failed because one or more field data types"
                            f" are incompatible for rule {rule.name}.",
                            resolution=(
                                "Reconfigure Business rule using the Edit button "
                                "in the CLS Module -> Business Rules page."
                            ),
                            details=(f"Batch ID: {batch_id}" if batch_id else None),
                        )
                        continue
                    filtered_events = list(
                        filter(
                            lambda event: query.match(deep_stringify(event)),
                            event_data,
                        )
                    )
                    logger.debug(
                        f"{len(filtered_events)} {sub_type} events remaining after filtering.",
                        details=(f"Batch ID: {batch_id}" if batch_id else None),
                    )
                    for destination in destinations:
                        success = True
                        if filtered_events:
                            batch_manager.update_cls_siem(
                                batch_id,
                                configuration_name,
                                destination,
                                CLSSIEMStatusType.FILTERED,
                                CLSSIEMCountType.FILTERED,
                                len(filtered_events),
                            )
                            success = _transform_and_ingest(
                                configuration_name,
                                destination,
                                filtered_events,
                                "events",
                                sub_type,
                                historical=historical,
                                batch_id=batch_id,
                            )
                        end_life(destination, success)
                elif data_type == "webtx" and data:
                    # assuming that webtx data will always be list of dicts
                    data_len = len(data)
                    logger.info(
                        f"Found {len(data)} webtx events.",
                        details=(f"Batch ID: {batch_id}" if batch_id else None),
                    )
                    field_learning_data.append(
                        (data, NetskopeFieldType.WEBTX, sub_type)
                    )
                    rule_filters = Filters(**rule.filters.model_dump())
                    if not rule_filters.isValid:
                        logger.error(
                            f"Filtering and transformation of {len(data)} webtx events "
                            f" have failed because one or more field data types"
                            f" are incompatible for rule {rule.name}.",
                            resolution=(
                                "Reconfigure Business rule using the Edit button in the CLS Module "
                                "-> Business Rules page."
                            ),
                            details=(
                                f"Batch ID: {batch_id}"
                                if batch_id
                                else None
                            )
                        )
                        continue
                    filtered_events = list(
                        filter(
                            lambda event: query.match(deep_stringify(event)),
                            data,
                        )
                    )
                    logger.debug(
                        f"{len(filtered_events)} webtx events remaining after filtering.",
                        details=(f"Batch ID: {batch_id}" if batch_id else None),
                    )
                    for destination in destinations:
                        success = True
                        if filtered_events:
                            success = _transform_and_ingest(
                                configuration_name,
                                destination,
                                filtered_events,
                                "webtx",
                                sub_type,
                                historical=historical,
                            )
                        end_life(destination, success)
                elif logs is not None:
                    data_len = len(logs)
                    filtered_logs_dict = defaultdict(list)
                    for log in logs:
                        filtered_logs_dict[log.get("ce_log_type", log.get("type"))].append(log)
                    logs_sent_to_destination_status = {}
                    for log_type, logs_data in filtered_logs_dict.items():
                        batch = batch_manager.create(
                            BatchDataType.LOGS,
                            log_type,
                            len(logs_data),
                            configuration_name,
                            BatchDataSourceType.CONFIGURATION,
                        )
                        batch_id = batch["_id"]
                        rule_filters = Filters(**rule.filters.model_dump())
                        if not rule_filters.isValid:
                            logger.error(
                                f"Filtering and transformation of {len(logs_data)} {log_type} logs "
                                f" have failed because one or more field data types"
                                f" are incompatible for rule {rule.name}.",
                                resolution=(
                                    "Reconfigure Business rule using the Edit button in "
                                    "the CLS Module -> Business Rules page."
                                ),
                                details=(f"Batch ID: {batch_id}" if batch_id else None),
                            )
                            continue
                        filtered_logs = list(
                            filter(
                                lambda log_item: query.match(deep_stringify(log_item)),
                                logs_data,
                            )
                        )
                        for filtered_log in filtered_logs:
                            if "createdAt" in filtered_log and isinstance(
                                filtered_log["createdAt"], datetime
                            ):
                                filtered_log["createdAt"] = filtered_log[
                                    "createdAt"
                                ].strftime("%m/%d/%Y %I:%M:%S %p")
                        logger.debug(
                            f"{len(filtered_logs)} {log_type} logs remaining after filtering.",
                            details=(f"Batch ID: {batch_id}" if batch_id else None),
                        )
                        for destination in destinations:
                            batch_manager.add_cls_siem(
                                batch_id,
                                configuration_name,
                                destination,
                                CLSSIEMStatusType.PENDING,
                            )
                            success = True
                            if filtered_logs:
                                batch_manager.update_cls_siem(
                                    batch_id,
                                    configuration_name,
                                    destination,
                                    CLSSIEMStatusType.FILTERED,
                                    CLSSIEMCountType.FILTERED,
                                    len(filtered_logs),
                                )
                                success = _transform_and_ingest(
                                    configuration.name,
                                    destination,
                                    filtered_logs,
                                    "logs",
                                    log_type,
                                    historical=historical,
                                    batch_id=batch_id,
                                )
                            if not logs_sent_to_destination_status.get(destination):
                                logs_sent_to_destination_status[destination] = []
                            logs_sent_to_destination_status[destination].append(success)
                    for destination, results in logs_sent_to_destination_status.items():
                        end_life(destination, all(results))
                elif logs is None:
                    for destination in destinations:
                        end_life(destination, True)

        for data, data_type, data_subtype in field_learning_data:
            try:
                # try source plugin's extract_and_store_fields method
                plugin.extract_and_store_fields(data, data_type, data_subtype)
            except NotImplementedError:
                # fallback to provider's extract_and_store_fields if
                # source plugin doesn't implement one
                try:
                    tenant_obj = alerts_helper.get_tenant_cls(configuration_name)
                    provider_obj = plugin_provider_helper.get_provider(tenant_obj.name)
                    provider_obj.extract_and_store_fields(
                        data,
                        data_type,
                        data_subtype,
                    )
                except SoftTimeLimitExceeded:
                    raise
                except Exception as e:  # NOSONAR
                    logger.warn(
                        f"Failed to extract and store fields for {data_subtype} {data_type}, {e}",
                        details=(f"Batch ID: {batch_id}\n" if batch_id else "")
                        + f"{traceback.format_exc()}",
                    )
            except SoftTimeLimitExceeded:
                raise
            except Exception as e:  # NOSONAR
                logger.warn(
                    f"Failed to extract and store fields for {data_subtype} {data_type}, {e}",
                    details=(f"Batch ID: {batch_id}\n" if batch_id else "")
                    + f"{traceback.format_exc()}",
                )
        try:
            connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
                {"name": configuration_name},
                {"$set": {"storage": plugin.storage}},
            )
        except SoftTimeLimitExceeded:
            raise
        except Exception as e:  # NOSONAR
            logger.warn(
                f"Failed to update storage for {configuration_name}, {e}",
                details=traceback.format_exc(),
            )
        return {"success": True}
    except SoftTimeLimitExceeded:
        logger.debug(
            f"cls.execute_plugin soft time limit exceeded for "
            f"{data_len} {sub_type} {data_type}; rescheduling task."
        )
        execute_celery_task(
            execute_plugin.apply_async,
            "cls.execute_plugin",
            args=[configuration_name],
            soft_time_limit=SOFT_TIME_LIMIT,
            time_limit=TASK_TIME_LIMIT,
            kwargs={
                "historical": historical,
                "data": prev_data,
                "data_type": data_type,
                "sub_type": sub_type,
                "logs": logs,
            },
        )


def get_supported_subtypes(mapping):
    """Return supported subtypes in mappings."""
    return set(
        map(
            str.lower,
            (
                list(
                    mapping.get("taxonomy", {}).get("json", {}).get("alerts", {}).keys()
                )
                + list(
                    mapping.get("taxonomy", {}).get("json", {}).get("events", {}).keys()
                )
                + list(
                    mapping.get("taxonomy", {}).get("json", {}).get("webtx", {}).keys()
                )
                + list(
                    mapping.get("taxonomy", {}).get("json", {}).get("logs", {}).keys()
                )
                + list(mapping.get("taxonomy", {}).get("alerts", {}).keys())
                + list(mapping.get("taxonomy", {}).get("events", {}).keys())
                + list(mapping.get("taxonomy", {}).get("webtx", {}).keys())
                + list(mapping.get("taxonomy", {}).get("logs", {}).keys())
            ),
        )
    )
