"""Provides a task to handle ioc retraction feature."""

import traceback
from pymongo.errors import PyMongoError

from netskope.common.celery.main import APP
from netskope.common.utils import (
    Logger,
    DBConnector,
    integration,
    Collections,
    track,
    SecretDict,
)
from netskope.common.utils.plugin_helper import PluginHelper

from netskope.integrations.cte.utils import RETRACTION_IOC_BATCH_SIZE
from netskope.integrations.cte.utils.tag_utils import TagUtils
from netskope.integrations.cte.tasks.plugin_lifecycle_task import get_possible_destinations

from netskope.integrations.cte.models import (
    ConfigurationDB,
    IndicatorGenerator
)

connector = DBConnector()
logger = Logger()
helper = PluginHelper()


@APP.task(name="cte.ioc_retraction", acks_late=False)
@integration("cte")
@track()
def ioc_retraction():
    """Execute the IoCs Retraction Task.

    Returns:
        bool: Whether IoC retraction was executed successfully or not.
    """
    config_list = connector.collection(Collections.CONFIGURATIONS).distinct(
        "name", {}
    )
    for cte_plugin_name in config_list:
        try:
            logger.update_level()
            cte_plugin = connector.collection(Collections.CONFIGURATIONS).find_one({"name": cte_plugin_name})
            configuration_db = ConfigurationDB(**cte_plugin)
            if not configuration_db.active:
                logger.info(
                    "Skipping IoC Retraction for configuration "
                    f"'{configuration_db.name}' as it is currently disabled."
                )
                continue
            PluginClass = helper.find_by_id(  # NOSONAR S117
                configuration_db.plugin
            )
            if PluginClass is None:
                logger.info(
                    f"Could not find the plugin with id='{configuration_db.plugin}'. "
                    "Skipping the IoCs retraction execution.",
                    error_code="CTE_1001",
                )
                continue
            plugin = PluginClass(
                configuration_db.name,
                SecretDict(configuration_db.parameters),
                configuration_db.storage,
                configuration_db.checkpoint,
                logger,
                ssl_validation=configuration_db.sslValidation,
            )
            retraction_batch = RETRACTION_IOC_BATCH_SIZE
            try:
                retraction_batch = plugin.fetch_retraction_batch
            except AttributeError:
                pass
            if not plugin.metadata.get("fetch_retraction_info", False):
                logger.info(
                    f"Source configuration '{configuration_db.name}' doesn't support "
                    "fetching retraction information for indicators."
                )
                continue
            logger.info(
                "Fetching retraction information for indicators "
                f"with source configuration '{configuration_db.name}'."
            )

            TagUtils.source = configuration_db.name
            query = {}
            query["$and"] = [
                # only from the configuration that is configured to share with us
                {
                    "sources": {
                        "$elemMatch": {
                            "source": configuration_db.name,
                            "retracted": False
                        }
                    }
                },
                # only active
                {"active": True},
            ]
            cursor = connector.collection(Collections.INDICATORS).aggregate(
                [
                    {"$match": query},
                    {"$sort": {"sources.lastSeen": -1}},
                ],
                allowDiskUse=True,
            )
            indicators = plugin.get_modified_indicators(
                # generator object.
                IndicatorGenerator(cursor, configuration_db.name).all(batch_size=retraction_batch)
            )
            TagUtils.source = None

            total_indicators = 0
            success = True
            destinations = get_possible_destinations(configuration_db.name)
            for batch, is_retraction_disabled in indicators:
                if is_retraction_disabled:
                    logger.info(
                        f"IoC(s) Retraction is disabled for source configuration '{configuration_db.name}'. "
                        "Retracted indicators will not be fetched from configuration."
                    )
                    success = False
                    break
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
                total_indicators += len(batch)
                connector.collection(
                    Collections.INDICATORS
                ).update_many(
                    {"value": {"$in": batch}},
                    {
                        "$set": {
                            "sources.$[elem].retracted": True,
                            "sources.$[elem].retractionDestinations": destinations
                        }
                    },
                    array_filters=[
                        {"elem.source": configuration_db.name}
                    ]
                )
            if success:
                logger.info(
                    f"Completed executing cycle to get modified indicator(s) for CTE configuration "
                    f"'{configuration_db.name}'. Fetched {total_indicators} indicator(s)."
                )
        except NotImplementedError:
            logger.error(
                "Get modified indicators method not implemented by plugin for "
                f"configuration '{configuration_db.name}'.",
                details=traceback.format_exc(),
                error_code="CTE_1003",
            )
        except PyMongoError:
            logger.error(
                "Error occurred while connecting to the database.",
                details=traceback.format_exc(),
                error_code="CTE",
            )
        except Exception:
            logger.error(
                "Error occurred while executing the ioc retraction.",
                details=traceback.format_exc(),
                error_code="CTE_1005",
            )
    return {"sucess": True}
