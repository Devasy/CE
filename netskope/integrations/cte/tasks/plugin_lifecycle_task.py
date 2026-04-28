"""Provides task to handle plugin lifecycle."""

from __future__ import absolute_import, unicode_literals
import json
from ipaddress import IPv4Address, IPv6Address, ip_address
import traceback
import re
from typing import List
from netskope.common.models.settings import CTECriterias, SettingsDB
from netskope.integrations.cte.models.indicator import (
    IndicatorDBWithSources,
    IndicatorSourceDB,
    SeverityType,
)
from netskope.integrations.cte.models.plugin import AggregateStrategy
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta

from netskope.common.celery.main import APP
from netskope.common.utils import (
    Logger,
    DBConnector,
    Collections,
    integration,
    parse_dates,
    track,
    SecretDict,
    parse_events,
)
from netskope.common.utils.plugin_helper import PluginHelper

from netskope.integrations.cte.models import (
    Indicator,
    ConfigurationDB,
    IndicatorType,
)
from netskope.integrations.cte.utils.schema import INDICATOR_STRING_FIELDS
from netskope.integrations.cte.utils.tag_utils import TagUtils


connector = DBConnector()
logger = Logger()
helper = PluginHelper()


def _load_mongo_filters(filters: str) -> dict:
    """Parse dict for mongo filter query."""
    return json.loads(
        filters,
        object_hook=lambda pair: parse_dates(pair, INDICATOR_STRING_FIELDS),
    )


def is_valid_indicator_list(indicators: List) -> bool:
    """Determine if the list is valid indicator list.

    Args:
        indicators (List): List of items to be verified.

    Returns:
        bool: Indicates if the list is valid or not.
    """
    return (
        indicators is not None
        and isinstance(indicators, list)
        and all(isinstance(indicator, Indicator) for indicator in indicators)
    )


def get_possible_destinations(source_config_name: str) -> List[str]:
    """Get all possible destinations apart from source configs.

    Args:
        source_config_name (str): source name from which indicators are pulled.

    Returns:
        List[str]: List of possible destinations configuration names.
    """
    destinations_dict = []
    destinations = set()
    for business_rule in connector.collection(Collections.CTE_BUSINESS_RULES).find({}):
        destinations.update(business_rule.get("sharedWith", {}).get(source_config_name, {}).keys())
    for x in destinations:
        destinations_dict.append({"name": x, "status": "pending"})
    return destinations_dict


def get_existing_indicator(value: str) -> dict:
    """Return indicator by value.

    Args:
        value (str): Value of the indicator.

    Returns:
        dict: Indicator from the database.
    """
    return connector.collection(Collections.INDICATORS).find_one(
        {"value": value}
    )


def _set_defaults(
    indicator: Indicator, configuration: ConfigurationDB
) -> Indicator:
    """Set default values for indicator."""
    if indicator.firstSeen is None:
        indicator.firstSeen = datetime.now().replace(tzinfo=None)
    if indicator.lastSeen is None:
        indicator.lastSeen = datetime.now().replace(tzinfo=None)
    if indicator.expiresAt is None and configuration.ageAfterDays != 0:
        indicator.expiresAt = datetime.now() + timedelta(
            days=configuration.ageAfterDays
        )
    return indicator


def compare_severity(existing, newer):
    """Compare the severity of 2 IOCs."""
    if existing == SeverityType.CRITICAL:
        return False
    elif existing == SeverityType.HIGH:
        return newer == SeverityType.CRITICAL
    elif existing == SeverityType.MEDIUM:
        return newer == SeverityType.CRITICAL or newer == SeverityType.HIGH
    elif existing == SeverityType.LOW:
        return (
            newer == SeverityType.CRITICAL
            or newer == SeverityType.HIGH
            or SeverityType.MEDIUM
        )
    else:
        return newer != SeverityType.UNKNOWN


def _update_existing_indicator(
    indicator: Indicator,
    existing_indicator: dict,
    configuration: ConfigurationDB,
    is_internal,
    from_api,
    destinations: List[dict],
):
    """Update values of the existing indicator from new indicator.

    This also sets latest values in the `indicator` so push method has latest values.
    """
    # Get the ioc source
    indicator_source = None
    indicator_source_idx = 0
    for idx, ioc in enumerate(existing_indicator.get("sources", [])):
        if ioc.get("source", "") == configuration.name:
            indicator_source = IndicatorSourceDB(**ioc)
            indicator_source_idx = idx
            break

    # set reputation from configuration; i.e. reputation override
    if (
        configuration.reputation is not None
        and 1 <= configuration.reputation <= 10
    ):
        indicator.reputation = configuration.reputation

    if indicator_source is None:
        indicator_source = IndicatorSourceDB(
            **{
                **indicator.model_dump(),
                **{
                    "internalHits": 1 if is_internal else 0,
                    "externalHits": 1 if not is_internal else 0,
                    "source": configuration.name,
                    "destinations": destinations,
                },
            },
        )
        existing_indicator.get("sources", []).append(indicator_source)
    else:
        settings = connector.collection(Collections.SETTINGS).find_one({})
        settings = SettingsDB(**settings)
        indicator_source.destinations = destinations
        reconciliation_creteria = settings.cte.criteria

        if is_internal:
            indicator_source.internalHits += 1
        else:
            indicator_source.externalHits += 1

        if reconciliation_creteria == CTECriterias.HIGHEST_SEVERITY:
            if compare_severity(indicator_source.severity, indicator.severity):
                # Replace the Indicator
                indicator_source.reputation = indicator.reputation
                indicator_source.severity = indicator.severity
                indicator_source.comments = indicator.comments
                indicator_source.extendedInformation = (
                    indicator.extendedInformation
                )
                indicator_source.tags = (
                    list(
                        set(indicator_source.tags + indicator.tags)
                    ) if configuration.tagsAggregateStrategy is AggregateStrategy.APPEND
                    else list(indicator.tags)
                )

            # update lastSeen to latest
            if indicator.firstSeen.replace(
                tzinfo=None
            ) < indicator_source.firstSeen.replace(tzinfo=None):
                indicator_source.firstSeen = indicator.firstSeen
            else:
                indicator.firstSeen = indicator_source.firstSeen

            # update firstSeen to earliest
            if indicator.lastSeen.replace(
                tzinfo=None
            ) > indicator_source.lastSeen.replace(tzinfo=None):
                indicator_source.lastSeen = indicator.lastSeen
            else:
                indicator.lastSeen = indicator_source.lastSeen

        elif reconciliation_creteria == CTECriterias.LAST_SEEN:
            if indicator.lastSeen.replace(
                tzinfo=None
            ) > indicator_source.lastSeen.replace(tzinfo=None):
                # Replace the Indicator
                indicator_source.lastSeen = indicator.lastSeen
                indicator_source.reputation = indicator.reputation
                indicator_source.severity = indicator.severity
                indicator_source.comments = indicator.comments
                indicator_source.extendedInformation = (
                    indicator.extendedInformation
                )
                indicator_source.tags = (
                    list(
                        set(indicator_source.tags + indicator.tags)
                    ) if configuration.tagsAggregateStrategy is AggregateStrategy.APPEND
                    else list(indicator.tags)
                )

            # update lastSeen to latest
            if indicator.firstSeen.replace(
                tzinfo=None
            ) < indicator_source.firstSeen.replace(tzinfo=None):
                indicator_source.firstSeen = indicator.firstSeen
            else:
                indicator.firstSeen = indicator_source.firstSeen
        else:
            if indicator.firstSeen.replace(
                tzinfo=None
            ) < indicator_source.firstSeen.replace(tzinfo=None):
                # Replace the Indicator
                indicator_source.firstSeen = indicator.firstSeen
                indicator_source.reputation = indicator.reputation
                indicator_source.severity = indicator.severity
                indicator_source.comments = indicator.comments
                indicator_source.extendedInformation = (
                    indicator.extendedInformation
                )
                indicator_source.tags = (
                    list(
                        set(indicator_source.tags + indicator.tags)
                    ) if configuration.tagsAggregateStrategy is AggregateStrategy.APPEND
                    else list(indicator.tags)
                )

            # update lastSeen to latest
            if indicator.lastSeen.replace(
                tzinfo=None
            ) > indicator_source.lastSeen.replace(tzinfo=None):
                indicator_source.lastSeen = indicator.lastSeen
            else:
                indicator.lastSeen = indicator_source.lastSeen

        # update comment if not already set or being set from API i.e. always
        # allow to update comments from API
        if from_api:
            if indicator.comments is not None:
                indicator_source.comments = indicator.comments
            indicator_source.tags = (
                list(
                    set(indicator_source.tags + indicator.tags)
                ) if configuration.tagsAggregateStrategy is AggregateStrategy.APPEND
                else list(indicator.tags)
            )
        indicator_source.retracted = False
        indicator_source.retractionDestinations = []
        existing_indicator.get("sources", [])[
            indicator_source_idx
        ] = indicator_source

    # update expiresAt to latest
    if (
        existing_indicator["expiresAt"] is None
        or indicator.expiresAt is None
        or indicator.expiresAt.replace(tzinfo=None)
        > existing_indicator["expiresAt"]
    ):
        existing_indicator["expiresAt"] = indicator.expiresAt
    else:
        indicator.expiresAt = existing_indicator["expiresAt"]
    existing_indicator["active"] = (
        indicator.active or existing_indicator["active"]
    )


def insert_or_update_indicator(
    configuration: ConfigurationDB,
    indicator: Indicator,
    is_internal: bool,
    from_api: bool = False,
) -> bool:
    """Insert or update the indicator in the database.

    Args:
        configuration (ConfigurationDB): Configuration assiciated with it.
        indicator (Indicator): Indicator to be inserted or updated.
        is_internal (bool): Whether the indicator was internal or not.
        from_api (bool): Whether this method is called directly from API or from plugin.

    Returns:
        bool: Whether the indicator was successfully inserted or not.
    """
    indicator = _set_defaults(indicator, configuration)
    existing_indicator = get_existing_indicator(indicator.value)
    destinations = get_possible_destinations(configuration.name)
    if existing_indicator is None:  # first time insertion
        # set reputation from configuration
        if (
            configuration.reputation is not None
            and 1 <= configuration.reputation <= 10
        ):
            indicator.reputation = configuration.reputation

        indicator_source = IndicatorSourceDB(
            **{
                **indicator.model_dump(),
                **{
                    "internalHits": 1 if is_internal else 0,
                    "externalHits": 1 if not is_internal else 0,
                    "source": configuration.name,
                    "destinations": destinations,
                },
            },
        )

        indicator_model = IndicatorDBWithSources(
            **{
                **indicator.model_dump(),
                **{
                    "internalHits": 1 if is_internal else 0,
                    "externalHits": 1 if not is_internal else 0,
                    "source": configuration.name,
                },
            },
            sources=[indicator_source],
        )
        connector.collection(Collections.INDICATORS).update_one(
            {"value": indicator_model.value},
            {"$set": indicator_model.model_dump()},
            upsert=True,
        )
    else:  # alrady in the database
        _update_existing_indicator(
            indicator, existing_indicator, configuration, is_internal, from_api, destinations
        )

        indicator_model = IndicatorDBWithSources(**existing_indicator)
        indicator_dict = indicator_model.model_dump()
        indicator_dict.pop("internalHits", None)
        indicator_dict.pop("externalHits", None)

        upsert_dict = {"$set": indicator_dict}
        if is_internal:
            upsert_dict["$inc"] = {"internalHits": 1}
        else:
            upsert_dict["$inc"] = {"externalHits": 1}

        update_result = connector.collection(
            Collections.INDICATORS
        ).update_one({"_id": existing_indicator["_id"]}, upsert_dict)
        if not update_result.modified_count > 0:
            logger.error(
                f"Could not store the indicator with "
                f"value='{indicator.value}'",
                error_code="CTE_1000",
            )


def end_life(name: str, success: bool) -> bool:
    """Update the lastRunSuccess and lastRunAt and exit.

    Args:
        name (str): Name of the configuration.
        success (bool): lastRunSuccess value to be updated.

    Returns:
        bool: Value of `success`.
    """
    connector.collection(Collections.CONFIGURATIONS).update_one(
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


def build_mongo_query(filters: List[dict]) -> dict:
    """Build a monogo query from custom filters.

    Args:
        filters (list[dict]): Filters to be applied.

    Returns:
        dict: Mongo query.
    """
    mongo_filters = {}
    for f in filters:
        if f["key"] == "age" and f["operator"] == "lte":
            mongo_filters["lastSeen"] = {
                "$gte": (datetime.now() - timedelta(days=int(f["value"])))
            }
    return mongo_filters


def _update_storage(name: str, storage: dict):
    connector.collection(Collections.CONFIGURATIONS).update_one(
        {"name": name}, {"$set": {"storage": storage}}
    )


def validate_iocs(type: IndicatorType, value: str):
    """Validate iocs values."""
    if type == IndicatorType.SHA256 and re.match(r"^[A-Fa-f0-9]{64}$", value):
        return True
    elif type == IndicatorType.MD5 and re.match(r"^[A-Fa-f0-9]{32}$", value):
        return True
    elif (
        type
        in [
            IndicatorType.URL,
            IndicatorType.HOSTNAME,
            IndicatorType.DOMAIN,
            IndicatorType.FQDN,
        ]
        and len(value) > 3
    ):
        return True
    elif type == IndicatorType.IPV4:
        try:
            addr = ip_address(value)
            return isinstance(addr, IPv4Address)
        except ValueError:
            return False
    elif type == IndicatorType.IPV6:
        try:
            addr = ip_address(value)
            return isinstance(addr, IPv6Address)
        except ValueError:
            return False
    else:
        return False


def _process_indicators_batch(
    source: ConfigurationDB,
    metadata,  # NOSONAR S117
    indicators: List[Indicator],
):
    """
    Process a batch of indicators.

    Args:
        source (ConfigurationDB): The source configuration database.
        PluginClass: The plugin class.
        indicators (List[Indicator]): A list of indicators to process.

    Returns:
        int: The number of indicators processed.
    """
    skipped_url_count = 0
    count = 0
    for indicator in indicators:
        if not isinstance(indicator, Indicator):
            continue
        if validate_iocs(indicator.type, indicator.value):
            count += 1
            insert_or_update_indicator(
                source,
                indicator,
                is_internal=metadata.get("netskope", False)
            )
        else:
            if indicator.type == IndicatorType.URL:
                skipped_url_count += 1
            else:
                logger.info(
                    f"Skipped indicator of type: {indicator.type} and value: {indicator.value} "
                    f"from the batch."
                )

    if skipped_url_count > 0:
        logger.info(
            f"Skipped storing {skipped_url_count} invalid URLs from the batch."
        )
    logger.info(
        f"Completed storing the batch of {count} indicator(s) for configuration "
        f"'{source.name}'."
    )
    return count


@APP.task(name="cte.execute_plugin", acks_late=False)
@integration("cte")
@track()
def execute_plugin(configuration_name, data: List = None, data_type: str = None, sub_type: str = None):
    """Execute the entire plugin lifecycle.

    Args:
        configuration_name (str): Name of the configuration with which to
        execute the plugin.

    Returns:
        bool: Whether lifecycle was executed successfully or not.
    """
    try:
        logger.update_level()
        configuration_db_dict = connector.collection(
            Collections.CONFIGURATIONS
        ).find_one(({"name": configuration_name}))
        if configuration_db_dict is None:
            return end_life(configuration_name, False)

        configuration_db = ConfigurationDB(**configuration_db_dict)

        PluginClass = helper.find_by_id(  # NOSONAR S117
            configuration_db.plugin
        )
        if PluginClass is None:
            logger.error(
                f"Could not find the plugin with id='{configuration_db.plugin}'. Skipping the CTE lifecycle execution.",
                error_code="CTE_1001",
            )
            return end_life(configuration_name, False)

        # if inactive; return
        if not configuration_db.active:
            return False

        logger.info(
            f"Executing pull cycle for CTE configuration '{configuration_name}'."
        )
        plugin = PluginClass(
            configuration_db.name,
            SecretDict(configuration_db.parameters),
            configuration_db.storage,
            configuration_db.checkpoint,
            logger,
            ssl_validation=configuration_db.sslValidation,
        )
        plugin.sub_checkpoint = configuration_db.subCheckpoint
        # If it is the first time the plugin is executed, set the
        # cycleStartedAt to now otherwise keep the same value
        configuration_db.cycleStartedAt = (
            datetime.now()
            if configuration_db.cycleStartedAt is None
            else configuration_db.cycleStartedAt
        )
        # update the cycleStartedAt in mongo
        connector.collection(Collections.CONFIGURATIONS).update_one(
            {"name": configuration_db.name},
            {"$set": {"cycleStartedAt": configuration_db.cycleStartedAt}},
        )

        TagUtils.source = configuration_db.name

        if isinstance(data, bytes):
            data = (
                parse_events(data, tenant_config_name=configuration_db.tenant, data_type=data_type, sub_type=sub_type)
                if configuration_db.tenant
                else parse_events(data, configuration=configuration_db, data_type=data_type, sub_type=sub_type)
            )
        plugin.data = data

        if data_type is None:
            data_type = "alerts"
        if sub_type is None:
            for i in plugin.data or []:
                if i.get("alert_type"):
                    sub_type = i["alert_type"]
                    break
        plugin.data_type = data_type
        plugin.sub_type = sub_type
        indicators = plugin.pull()
        TagUtils.source = None
        if isinstance(indicators, list):  # convert to generator
            indicators = (i for i in [(indicators, None)])

        total_indicators = 0
        success = True
        for batch, sub_checkpoint in indicators:
            if not isinstance(batch, list):
                logger.error(
                    f"Pull method returned data with invalid datatype for plugin "
                    f"with id='{configuration_db.plugin}'",
                    error_code="CTE_1002",
                )
                success = False
                break
            if not batch:
                logger.info(
                    f"No new indicators to be shared from configuration '{configuration_db.name}'."
                )
                success = True
                break
            total_indicators += _process_indicators_batch(
                configuration_db, plugin.metadata, batch
            )
            connector.collection(Collections.CONFIGURATIONS).update_one(
                {"name": configuration_db.name},
                {
                    "$set": {
                        "lockedAt.pull": datetime.now(),
                        "subCheckpoint": sub_checkpoint,
                    }
                },
            )
        if success:
            connector.collection(Collections.CONFIGURATIONS).update_one(
                {"_id": configuration_db_dict["_id"]},
                {
                    "$set": {
                        "checkpoint": configuration_db.cycleStartedAt,
                        "cycleStartedAt": None,
                        "subCheckpoint": None,
                    }
                },
            )
            _update_storage(configuration_db.name, plugin.storage)
            logger.info(
                f"Completed executing pull cycle for CTE configuration "
                f"'{configuration_name}'. Fetched {total_indicators} indicator(s)."
            )

        return end_life(configuration_name, success)
    except NotImplementedError:
        logger.error(
            f"Pull method not implemented by plugin for configuration '{configuration_name}'.",
            details=traceback.format_exc(),
            error_code="CTE_1003",
        )
    except PyMongoError:
        logger.error(
            "Error occurred while connecting to the database.",
            details=traceback.format_exc(),
            error_code="CTE_1004",
        )
    except Exception:
        logger.error(
            f"Error occurred while executing the plugin lifecycle for configuration '{configuration_name}'.",
            details=traceback.format_exc(),
            error_code="CTE_1005",
        )
    return end_life(configuration_name, False)
