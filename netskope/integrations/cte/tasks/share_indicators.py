"""Share indicators task."""

from __future__ import absolute_import, unicode_literals
import json
import re
import traceback
from typing import List, Dict, Optional
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
    has_source_info_args
)
from netskope.common.models import SettingsDB
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.integrations.cte.plugin_base import PushResult, ValidationResult
from netskope.integrations.cte.models import (
    ConfigurationDB,
    IndicatorGenerator,
)
from netskope.integrations.cte.models.business_rule import BusinessRuleDB
from netskope.integrations.cte.utils.schema import INDICATOR_STRING_FIELDS
from netskope.integrations.cte.utils import RETRACTION_IOC_BATCH_SIZE
from netskope.integrations.cte.tasks.plugin_lifecycle_task import get_possible_destinations

connector = DBConnector()
logger = Logger()
helper = PluginHelper()


def _load_mongo_filters(filters: str) -> dict:
    """Parse dict for mongo filter query."""
    return json.loads(
        filters,
        object_hook=lambda pair: parse_dates(pair, INDICATOR_STRING_FIELDS),
    )


def validate_result_and_update(
    shared_with: str,
    push_result: PushResult,
    filters: dict = None,
    source_config_name: str = None
) -> bool:
    """Validate push result and update all the indicators.

    Args:
        shared_with (str): Name of the configuration that is being shared with.
        push_result (PushResult): Result of the push method.
        total (int): total number of indicators
        total_inactive (int): total number of inactive indicators
        business_rule_qualified (int): number of indicators which qualifies the business rule.
        filters (dict): mongo filter dictionary
    """
    if not isinstance(push_result, PushResult):
        logger.error(
            f"Could not share indicators with configuration "
            f"'{shared_with}'. Invalid return type.",
            error_code="CTE_1006",
        )
        return False, False, []
    if push_result.success is True:
        # add shared_with config name in the sharedWith array for all
        # the indicators
        if not push_result.already_shared:
            if push_result.failed_iocs:
                connector.collection(Collections.INDICATORS).update_many(
                    {
                        "value": {"$in": push_result.failed_iocs},
                        "sources": {
                            "$elemMatch": {
                                "source": source_config_name,
                                "destinations": {
                                    "$elemMatch": {
                                        "name": shared_with,
                                        "status": "inprogress"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "$set": {
                            "sources.$[elem].destinations.$[dest].status": "failed"
                        }
                    },
                    array_filters=[
                        {"elem.source": source_config_name},
                        {"dest.name": shared_with}
                    ]
                )
                filters["$and"].append({"value": {"$nin": push_result.failed_iocs}})
            connector.collection(Collections.INDICATORS).update_many(
                filters, {"$addToSet": {"sharedWith": shared_with}}
            )
            logger.info(
                f"Completed Sharing of indicators from source "
                f"configuration '{source_config_name}' to configuration '{shared_with}'."
            )
            return True, push_result.should_run_cleanup, (push_result.failed_iocs if push_result.failed_iocs else [])
    else:
        logger.error(
            f"Could not share indicators with configuration "
            f"'{shared_with}'. {re.sub(r'token=([0-9a-zA-Z]*)', 'token=********&', push_result.message)}",
            details=re.sub(
                r"token=([0-9a-zA-Z]*)", "token=********&", push_result.message
            ),
            error_code="CTE_1007",
        )
        return False, push_result.should_run_cleanup, []


def _update_storage(name: str, storage: dict):
    connector.collection(Collections.CONFIGURATIONS).update_one(
        {"name": name}, {"$set": {"storage": storage}}
    )


def build_mongo_query(
    rule: BusinessRuleDB,
    source: Optional[str] = None,
    lastseen: Optional[datetime] = None,
) -> Dict:
    """Build a mongo query for the business rule.

    Args:
        rule (BusinessRuleDB): Business rule to build the query for.

    Returns:
        Dict: Mongo query.
    """
    query = {
        "$and": [
            json.loads(
                rule.filters.mongo,
                object_hook=lambda pair: parse_dates(pair),
            )
        ]
    }
    for mute in rule.exceptions:  # exclude iocs matching the mute rule
        if mute.filters:
            mute_query = json.loads(
                mute.filters.mongo, object_hook=lambda pair: parse_dates(pair)
            )
            query["$nor"] = query.get("$nor", []) + [mute_query]
        if mute.tags:
            query["$and"].append({"sources": {"$elemMatch": {"tags": {"$nin": mute.tags}}}})
    if source:
        query["$and"].append(
            {
                "sources": {
                    "$elemMatch": {
                        "source": source,
                        "$or": [
                            {"retracted": False},
                            {"retracted": {"$exists": False}},
                        ],
                    }
                }
            }
        )
    else:
        query["$and"].append(
            {
                "sources": {
                    "$elemMatch": {
                        "$or": [{"retracted": False}, {"retracted": {"$exists": False}}]
                    }
                }
            }
        )
    if lastseen:
        query["$and"].append(
            {"sources": {"$elemMatch": {"lastSeen": {"$gt": lastseen}}}}
        )
    return query


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
                "lastRunAt.share": datetime.now(),
                "lastRunSuccess.share": success,
                # "lockedAt": None,
            }
        },
    )
    return success


@APP.task(name="cte.share_indicators")
@integration("cte")
@track()
def share_indicators(
    source_config_name: Optional[str] = None,
    destination_config_name: Optional[str] = None,
    rule: Optional[str] = None,
    action: Optional[Dict] = {},
    lastseen: Optional[int] = None,
    indicators: Optional[List] = None,
    share_new_indicators: Optional[bool] = False,
):
    """Evaluate business rules and push iocs.

    Args:
        rule (Optional[str], optional): Specific business rule to evaluate.
        All rules if None. Defaults to None.
        source_config (Optional[str], optional): Name of the source configuration.
        Defaults to None.
        destination_config_name (Optional[str], optional): Name of the destination configuration.
        Defaults to None.
        action (Optional[Dict], optional): Name of a specific action to
        perform. Defaults to None.
        lastseen (Optional[datetime], optional): Evaluate only on the
        indicators appears lastseen after. Defaults to None.
    """
    try:
        if indicators:
            possible_destinations = get_possible_destinations(source_config_name)
            connector.collection(Collections.INDICATORS).update_many(
                {"value": {"$in": indicators}},
                {
                    "$set": {
                        "sources.$[elem].destinations": possible_destinations
                    }
                },
                array_filters=[
                    {"elem.source": source_config_name},
                ]
            )
            logger.info(
                f"{len(indicators)} indicators from the source configuration named '{source_config_name}' "
                "will be shared in the next sharing cycle with possible destinations."
            )
            return True
        destination_config = connector.collection(Collections.CONFIGURATIONS).find_one(
            {"name": destination_config_name}
        )
        is_run_action_cleanup = False
        if destination_config and share_new_indicators:
            # Run pending historical tasks
            destination_config_model = ConfigurationDB(**destination_config)
            for manual_sync_config in destination_config_model.manualSync:
                should_run_cleanup = share_iocs(
                    source_config_name=manual_sync_config.source,
                    destination_config_name=destination_config_name,
                    rule_provided=manual_sync_config.rule,
                    action=manual_sync_config.action,
                    lastseen=manual_sync_config.lastseen
                )
                connector.collection(Collections.CONFIGURATIONS).update_one(
                    {"name": destination_config_name},
                    {
                        "$set": {
                            "lockedAt.share": datetime.now(),
                        }
                    }
                )
                is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
            connector.collection(Collections.CONFIGURATIONS).update_one(
                {"name": destination_config_name}, {"$set": {"manualSync": []}}
            )
        # Run task either for maintenance window or queued manual sync task or any indicator insert/update through APIs.
        should_run_cleanup = share_iocs(
            source_config_name, destination_config_name, rule, action, lastseen, indicators, share_new_indicators
        )
        is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
        # create plugin class
        configuration = ConfigurationDB(**destination_config)
        Plugin = helper.find_by_id(configuration.plugin)  # NOSONAR S117
        if Plugin is None:
            logger.info(
                f"Could not share indicators with configuration "
                f"'{configuration.name}'; plugin with "
                f"id='{configuration.plugin}' does not exist.",
                error_code="CTE_1009",
            )
        plugin = Plugin(
            configuration.name,
            SecretDict(configuration.parameters),
            configuration.storage,
            configuration.checkpoint,
            logger,
            ssl_validation=configuration.sslValidation,
        )
        logger.info(
            f"Completed Sharing of indicators for the configuration '{destination_config_name}'."
        )
        settings = SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
        if share_new_indicators and settings.cte and settings.cte.iocRetraction:
            if not plugin.metadata.get("delete_supported", False):
                logger.info(
                    f"Destination configuration with name '{configuration.name}' "
                    f"doesn't support deletion of retracted indicators."
                )
            else:
                should_run_cleanup = cte_retract_indicators(
                    destination_config_name,
                    plugin.metadata.get("patch_supported", True)
                )
                is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
        if is_run_action_cleanup:
            plugin.run_action_cleanup()

    except Exception:
        logger.error(
            f"Error occurred while sharing indicators to configuration "
            f"'{destination_config_name}'.",
            details=traceback.format_exc(),
            error_code="CTE_1012",
        )


def share_iocs(
    source_config_name: Optional[str] = None,
    destination_config_name: Optional[str] = None,
    rule_provided: Optional[str] = None,
    action: Optional[Dict] = {},
    lastseen: Optional[int] = None,
    indicators: Optional[List] = None,
    share_new_indicators: Optional[bool] = False,
    is_run_action_cleanup: Optional[bool] = False,
    is_retraction_call: Optional[bool] = False
):
    """Evaluate business rules and push iocs.

    Args:
        rule (Optional[str], optional): Specific business rule to evaluate.
        All rules if None. Defaults to None.
        source_config (Optional[str], optional): Name of the source configuration.
        Defaults to None.
        destination_config_name (Optional[str], optional): Name of the destination configuration.
        Defaults to None.
        action (Optional[Dict], optional): Name of a specific action to
        perform. Defaults to None.
        lastseen (Optional[datetime], optional): Evaluate only on the
        indicators appears lastseen after. Defaults to None.
    """
    source_configs_list = (
        [source_config_name]
        if source_config_name
        else list(
            connector.collection(Collections.CONFIGURATIONS).distinct(
                "name",
                {"name": {"$nin": [destination_config_name]}}
            )
        )
    )
    destination_dict = {}
    actions = []

    for source_config_name in source_configs_list:
        # Update LockedAt field so that task will not exceed max lock wait time.
        connector.collection(Collections.CONFIGURATIONS).update_one(
            {"name": destination_config_name},
            {
                "$set": {
                    "lockedAt.share": datetime.now(),
                }
            }
        )
        # Only update status when maintenance sharing running.
        if share_new_indicators:
            ioc_update_result = connector.collection(Collections.INDICATORS).update_many(
                {
                    "sources": {
                        "$elemMatch": {
                            "source": source_config_name,
                            "destinations": {
                                "$elemMatch": {
                                    "name": destination_config_name,
                                    "status": "pending"
                                }
                            }
                        }
                    }
                },
                {
                    "$set": {
                        "sources.$[elem].destinations.$[dest].status": "inprogress"
                    }
                },
                array_filters=[
                    {"elem.source": source_config_name},
                    {"dest.name": destination_config_name}
                ]
            )
            if not ioc_update_result.modified_count:
                logger.info(
                    f"No indicators to share from source configuration '{source_config_name}' "
                    f"to destination configuration '{destination_config_name}'."
                )

        for rule in connector.collection(Collections.CTE_BUSINESS_RULES).find(
            {"name": rule_provided} if rule_provided else {"muted": False}
        ):
            rule = BusinessRuleDB(**rule)
            sharedWith = rule.sharedWith
            # if destination plugin is configured then only we will share the indicators.
            if not (sharedWith and sharedWith.get(source_config_name, {})):
                continue
            if source_config_name not in sharedWith.keys():
                # Skip if source configuration not found in business rule
                continue
            if destination_config_name is None:
                destination_dict = sharedWith.get(source_config_name, {})
            else:
                source_dict = sharedWith.get(source_config_name, {})
                destination_dict[destination_config_name] = source_dict.get(
                    destination_config_name, []
                )
            if (
                share_new_indicators
                and destination_config_name in destination_dict.keys()
                and not destination_dict[destination_config_name]
            ):
                continue
            if share_new_indicators and ioc_update_result and not ioc_update_result.modified_count:
                end_life(destination_config_name, True)
                continue
            logger.debug(
                f"Sharing indicators to all the configured destinations for the '{source_config_name}' configuration, "
                f"using the business rule '{rule.name}'."
            )
            query_for_total = {}
            query = {"$and": []}
            query_inactive = {"$and": []}
            for mute in rule.exceptions:  # exclude iocs matching the mute rule
                if mute.filters:
                    mute_query = json.loads(
                        mute.filters.mongo,
                        object_hook=lambda pair: parse_dates(pair),
                    )
                    query["$nor"] = query.get("$nor", []) + [mute_query]
                if mute.tags:
                    query['$and'].append({"sources": {"$elemMatch": {"tags": {"$nin": mute.tags}}}})
            for config, actions_list in destination_dict.items():
                # TODO for actions
                configuration_dict = connector.collection(
                    Collections.CONFIGURATIONS
                ).find_one({"name": config})
                if configuration_dict is None:
                    # configuration does not exist anymore
                    logger.info(
                        f"Could not share indicators with configuration "
                        f"'{config}'; it does not exist.",
                        error_code="CTE_1008",
                    )
                    continue
                configuration = ConfigurationDB(**configuration_dict)
                if not configuration.active:  # If plugin is disabled
                    logger.debug(f"Configuration '{config}' is disabled; sharing skipped.")
                    continue
                Plugin = helper.find_by_id(configuration.plugin)  # NOSONAR S117
                if Plugin is None:
                    logger.info(
                        f"Could not share indicators with configuration "
                        f"'{config}'; plugin with "
                        f"id='{configuration.plugin}' does not exist.",
                        error_code="CTE_1009",
                    )
                    continue
                try:
                    logger.info(
                        f"Indicator sharing has been initiated from '{source_config_name}' to '{config}' "
                        f"using the business rule '{rule.name}'."
                    )
                    plugin = Plugin(
                        configuration.name,
                        SecretDict(configuration.parameters),
                        configuration.storage,
                        configuration.checkpoint,
                        logger,
                        ssl_validation=configuration.sslValidation,
                    )
                    query["$and"] = [
                        # add if any previous query
                        *query["$and"],
                        # apply the usual filters
                        _load_mongo_filters(rule.filters.mongo),
                        # only from the configuration that is configured to share with us
                        (
                            {
                                "sources": {
                                    "$elemMatch": {
                                        "source": source_config_name,
                                        "$or": [{"retracted": False}, {"retracted": {"$exists": False}}],
                                        "destinations.name": destination_config_name,
                                        "destinations.status": "inprogress"
                                    }
                                }
                            }
                            if share_new_indicators and plugin.metadata["patch_supported"]
                            else {
                                "sources": {
                                    "$elemMatch": {
                                        "source": source_config_name,
                                        "$or": [{"retracted": False}, {"retracted": {"$exists": False}}],
                                    }
                                }
                            }
                        ),
                        # only active
                        {"active": True},
                    ]
                    query_for_total["$and"] = [
                        (
                            {
                                "sources": {
                                    "$elemMatch": {
                                        "source": source_config_name,
                                        "destinations.name": destination_config_name,
                                        "destinations.status": "inprogress",
                                        "$or": [{"retracted": False}, {"retracted": {"$exists": False}}],
                                    }
                                }
                            }
                            if share_new_indicators and plugin.metadata["patch_supported"]
                            else {
                                "sources": {
                                    "$elemMatch": {
                                        "source": source_config_name,
                                        "$or": [{"retracted": False}, {"retracted": {"$exists": False}}],
                                    }
                                }
                            }
                        ),
                        # only active
                        {"active": True},
                    ]
                    query_inactive["$and"] = [
                        # add if any previous query
                        *query_inactive["$and"],
                        # apply the usual filters
                        _load_mongo_filters(rule.filters.mongo),
                        # only from the configuration that is configured to share with us
                        (
                            {
                                "sources": {
                                    "$elemMatch": {
                                        "source": source_config_name,
                                        "destinations.name": destination_config_name,
                                        "destinations.status": "inprogress"
                                    }
                                }
                            }
                            if share_new_indicators and plugin.metadata["patch_supported"]
                            else {
                                "sources": {
                                    "$elemMatch": {
                                        "source": source_config_name
                                    }
                                }
                            }
                        ),
                        # only inactive
                        {"active": False},
                    ]
                    if lastseen:
                        query["$and"].append(
                            {
                                "sources": {
                                    "$elemMatch": {
                                        "lastSeen": {
                                            "$gt": datetime.now()
                                            - timedelta(days=lastseen)
                                        }
                                    }
                                }
                            }
                        )
                        query_for_total["$and"].append(
                            {
                                "sources": {
                                    "$elemMatch": {
                                        "lastSeen": {
                                            "$gt": datetime.now()
                                            - timedelta(days=lastseen)
                                        }
                                    }
                                }
                            }
                        )
                    total_inactive = list(
                        connector.collection(Collections.INDICATORS).aggregate(
                            [
                                {"$match": query_inactive},
                                {"$group": {"_id": None, "count": {"$sum": 1}}},
                            ],
                            allowDiskUse=True,
                        )
                    )
                    pipeline = [
                        {
                            "$facet": {
                                "totalResult": [
                                    {
                                        "$match": query_for_total  # Add any additional global match conditions here
                                    },
                                    {
                                        "$group": {
                                            "_id": None,
                                            "totalCount": {"$sum": 1},
                                        }
                                    },
                                ],
                                "filteredResult": [
                                    {
                                        "$match": query  # Add your filter criteria here
                                    },
                                    {
                                        "$group": {
                                            "_id": None,
                                            "filteredCount": {"$sum": 1},
                                        }
                                    },
                                ],
                            }
                        }
                    ]
                    result = list(connector.collection(Collections.INDICATORS).aggregate(pipeline, allowDiskUse=True))
                    count_matrix = result[0]
                    count_documents = (
                        result[0]["filteredResult"][0]["filteredCount"]
                        if count_matrix["filteredResult"]
                        else 0
                    )
                    action_run_success = True
                    failed_iocs = []
                    if count_documents > 0 or is_retraction_call:
                        if not action:
                            actions = actions_list.copy()
                        else:
                            actions.append(action)
                        for action_dict in actions:
                            cursor = connector.collection(Collections.INDICATORS).aggregate(
                                [
                                    {"$match": query},
                                    {"$sort": {"sources.lastSeen": -1}},
                                ],
                                allowDiskUse=True,
                            )
                            length_ = connector.collection(Collections.INDICATORS).count_documents(
                                query
                            )

                            inactive_count = total_inactive[0].get("count", 0) if len(total_inactive) > 0 else 0
                            logger.info(f"Sending total of {length_} qualified indicators based on the rule name"
                                        f" '{rule.name}' for sharing. {inactive_count} indicators are inactive.")
                            share_source_info = has_source_info_args(
                                plugin,
                                "push",
                                ["source", "business_rule", "plugin_name"]
                            )
                            plugin_name = None
                            if share_source_info:
                                source_config = connector.collection(Collections.CONFIGURATIONS).find_one(
                                    {"name": source_config_name}
                                )
                                PluginClass = helper.find_by_id(source_config.get("plugin"))
                                plugin_name = PluginClass.metadata.get("name", "")

                            validate_result, should_run_action_cleanup, action_failed_iocs = validate_result_and_update(
                                configuration.name,
                                plugin.push(
                                    IndicatorGenerator(cursor, source_config_name).all(),
                                    dict(action_dict),
                                    source_config_name,
                                    rule.name,
                                    plugin_name
                                ) if share_source_info else plugin.push(
                                    IndicatorGenerator(cursor, source_config_name).all(),
                                    dict(action_dict)
                                ),
                                filters=query,
                                source_config_name=source_config_name
                            )
                            failed_iocs.extend(action_failed_iocs)
                            #  Retrun True when IoCs are added to URLlist in Netskope CTE Plugin
                            if (
                                plugin.metadata.get("netskope", False)
                                and validate_result
                                and should_run_action_cleanup
                            ):
                                is_run_action_cleanup = True
                            # All action should performed successfully.
                            action_run_success = action_run_success and validate_result
                            # Update the storage
                            _update_storage(configuration.name, plugin.storage)
                        # mark all indicator as shared.
                        if destination_config_name and share_new_indicators and action_run_success:
                            connector.collection(Collections.INDICATORS).update_many(
                                {
                                    "value": {"$nin": list(set(failed_iocs))},
                                    "sources": {
                                        "$elemMatch": {
                                            "source": source_config_name,
                                            "destinations": {
                                                "$elemMatch": {
                                                    "name": destination_config_name,
                                                    "status": "inprogress"
                                                }
                                            }
                                        }
                                    }
                                },
                                {
                                    "$set": {
                                        "sources.$[elem].destinations.$[dest].status": "shared"
                                    }
                                },
                                array_filters=[
                                    {"elem.source": source_config_name},
                                    {"dest.name": destination_config_name}
                                ]
                            )
                    else:
                        logger.info(
                            f"No indicators from source {source_config_name} to share "
                            f"on destination {configuration.name}.",
                        )
                    end_life(destination_config_name, action_run_success)
                except NotImplementedError:
                    logger.error(
                        f"Could not share indicators with configuration "
                        f"'{configuration.name}'. Push method not implemented.",
                        details=traceback.format_exc(),
                        error_code="CTE_1010",
                    )
                except Exception:
                    logger.error(
                        f"Error occurred while sharing indicators with configuration "
                        f"'{configuration.name}'.",
                        details=traceback.format_exc(),
                        error_code="CTE_1011",
                    )
    return is_run_action_cleanup


def get_all_actions_from_rule(
    destination_config_name: str = ""
):
    """Get all action configured for any destination plugin.

    Args:
        destination_config_name (str, optional): _description_. Defaults to "".

    Returns:
        _type_: _description_
    """
    actions = {}
    rules = connector.collection(Collections.CTE_BUSINESS_RULES).find(
        {"muted": False}
    )
    for rule in rules:
        rule = BusinessRuleDB(**rule)
        sharedWith = rule.sharedWith
        for source, destinations in sharedWith.items():
            if destination_config_name in destinations.keys():
                if source in actions:
                    actions[source].extend(destinations.get(destination_config_name, []))
                else:
                    actions[source] = destinations.get(destination_config_name, [])
    return actions


def cte_retract_indicators(
    destination_config_name: str = None,
    patch_supported: bool = True,
):
    """Retract indicators for destination configuration.

    Args:
        destination_config_name (str): _description_.
    """
    try:
        configuration_dict = connector.collection(
            Collections.CONFIGURATIONS
        ).find_one({"name": destination_config_name})
        if configuration_dict is None:
            # configuration does not exist anymore
            logger.info(
                f"Could not share indicators with configuration "
                f"'{destination_config_name}'; it does not exist."
            )
            return
        configuration = ConfigurationDB(**configuration_dict)
        if not configuration.active:  # If plugin is disabled
            logger.debug(f"Configuration '{destination_config_name}' is disabled; IoC Retraction skipped.")
            return
        actions = get_all_actions_from_rule(configuration.name)
        if not actions:
            logger.info(
                f"Destination configuration with name '{configuration.name}' isn't added in sharing configurations. "
                f"Skipping IoC(s) Retraction."
            )
            return
        ioc_update_result = connector.collection(Collections.INDICATORS).update_many(
            {
                "sources": {
                    "$elemMatch": {
                        "retracted": True,
                        "retractionDestinations": {
                            "$elemMatch": {
                                "name": configuration.name,
                                "status": "pending"
                            }
                        }
                    }
                }
            },
            {
                "$set": {
                    "sources.$[elm].retractionDestinations.$[dest].status": "inprogress"
                }
            },
            array_filters=[
                {"elm.retracted": True},
                {"dest.name": configuration.name}
            ]
        )
        if not ioc_update_result.modified_count:
            logger.info(
                f"No indicators to be retracted from destination configuration '{configuration.name}'."
            )
            return
        Plugin = helper.find_by_id(configuration.plugin)  # NOSONAR S117
        if Plugin is None:
            logger.info(
                f"Could not retract indicators from configuration "
                f"'{configuration.name}'; plugin with "
                f"id='{configuration.plugin}' does not exist.",
                error_code="CTE_1009",
            )
            return
        plugin = Plugin(
            configuration.name,
            SecretDict(configuration.parameters),
            configuration.storage,
            configuration.checkpoint,
            logger,
            ssl_validation=configuration.sslValidation,
        )
        # For plugins which are accepting all indicators.
        if not patch_supported:
            should_run_action_cleanup = share_iocs(
                destination_config_name=configuration.name, is_retraction_call=True
            )
            connector.collection(Collections.INDICATORS).update_many(
                {
                    "sources": {
                        "$elemMatch": {
                            "retracted": True,
                            "retractionDestinations": {
                                "$elemMatch": {
                                    "name": configuration.name,
                                    "status": "inprogress"
                                }
                            }
                        }
                    }
                },
                {
                    "$set": {
                        "sources.$[elm].retractionDestinations.$[dest].status": "retracted"
                    }
                },
                array_filters=[
                    {"elm.retracted": True},
                    {"dest.name": configuration.name}
                ]
            )
            return should_run_action_cleanup

        # Get batch size from plugin if any else default
        retraction_batch = RETRACTION_IOC_BATCH_SIZE
        try:
            retraction_batch = plugin.retraction_batch
        except AttributeError:
            pass
        disabled_retraction = False
        for source_config_name, action_config_list in actions.items():
            query = {}
            # Get all indicators which are retracted from source and shared with destination.
            query["$and"] = [
                {
                    "sources": {
                        "$elemMatch": {
                            "source": source_config_name,
                            "retracted": True,
                            "retractionDestinations.name": configuration.name,
                            "retractionDestinations.status": "inprogress"
                        }
                    },
                    "sharedWith": {"$in": [configuration.name]}
                }
            ]
            pipeline = [
                {
                    "$facet": {
                        "filteredResult": [
                            {
                                "$match": query  # Add your filter criteria here
                            },
                            {
                                "$group": {
                                    "_id": None,
                                    "filteredCount": {"$sum": 1},
                                }
                            },
                        ],
                    }
                }
            ]
            result = list(connector.collection(Collections.INDICATORS).aggregate(pipeline, allowDiskUse=True))
            count_matrix = result[0]
            count_documents = (
                result[0]["filteredResult"][0]["filteredCount"]
                if count_matrix["filteredResult"]
                else 0
            )
            if count_documents > 0:
                # get all indicators which needs to be retracted.
                cursor = connector.collection(Collections.INDICATORS).aggregate(
                    [
                        {"$match": query},
                        {"$sort": {"sources.lastSeen": -1}},
                    ],
                    allowDiskUse=True,
                )
                batch_retraction_results = plugin.retract_indicators(
                    IndicatorGenerator(cursor, source_config_name).all(batch_size=retraction_batch),
                    action_config_list
                )
                success = True
                for batch_result in batch_retraction_results:
                    if not isinstance(batch_result, ValidationResult):
                        logger.error(
                            f"Could not Retract indicators in batch for configuration "
                            f"'{configuration.name}'. Invalid return type.",
                            error_code="CTE_1006",
                        )
                        success = False
                        break
                    if not batch_result.success:
                        success = False
                        if batch_result.disabled:
                            disabled_retraction = True
                            break
                        logger.error(
                            f"Could not retract indicators for configuration "
                            f"'{configuration.name}'. "
                            f"{re.sub(r'token=([0-9a-zA-Z]*)', 'token=********&', batch_result.message)}",
                            details=re.sub(
                                r"token=([0-9a-zA-Z]*)", "token=********&", batch_result.message
                            ),
                            error_code="CTE_1007",
                        )
                        break
                connector.collection(Collections.INDICATORS).update_many(
                    {
                        "sources": {
                            "$elemMatch": {
                                "retracted": True,
                                "source": source_config_name,
                                "retractionDestinations": {
                                    "$elemMatch": {
                                        "name": configuration.name,
                                        "status": "inprogress"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "$set": {
                            "sources.$[elm].retractionDestinations.$[dest].status": (
                                "failed"
                                if not success
                                else "retracted"
                            )
                        }
                    },
                    array_filters=[
                        {"elm.source": source_config_name},
                        {"dest.name": configuration.name}
                    ]
                )
                # Retraction failed.
                if not success:
                    continue
                logger.info(
                    f"Completed retraction of {count_documents} indicators for "
                    f"destination configuration '{configuration.name}', "
                    f"which are retracted from configuration '{source_config_name}'."
                )
            else:
                logger.info(f"No retracted indicators found for source configuration '{source_config_name}'.")
        if disabled_retraction:
            connector.collection(Collections.INDICATORS).update_many(
                {
                    "sources": {
                        "$elemMatch": {
                            "retracted": True,
                            "retractionDestinations": {
                                "$elemMatch": {
                                    "name": configuration.name,
                                    "status": "inprogress"
                                }
                            }
                        }
                    }
                },
                {
                    "$set": {
                        "sources.$[elm].retractionDestinations.$[dest].status": "N/A"
                    }
                },
                array_filters=[
                    {"elm.retracted": True},
                    {"dest.name": configuration.name}
                ]
            )
            logger.info(
                f"IoC(s) Retraction is disabled for destination configuration '{configuration.name}'. "
                "Added N/A as a retraction result."
            )
    except Exception:
        logger.error(
            f"Error occurred while retracting indicators for configuration "
            f"'{destination_config_name}'.",
            details=traceback.format_exc(),
            error_code="CTE_1011",
        )
    # Their will not be any action cleanup since netskope is not there as destination.
    return False
