"""EDM Plugin Lifecycle."""

import traceback
from datetime import datetime, UTC

from pymongo.errors import PyMongoError

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    SecretDict,
    integration,
    track,
)
from netskope.common.utils import PluginHelper
from netskope.integrations.edm.models import BusinessRuleDB, ConfigurationDB, StatusType
from netskope.integrations.edm.utils.task_listing import set_task_status

from .share_data import share_data

connector = DBConnector()
logger = Logger()
helper = PluginHelper()


def _update_storage(name: str, storage: dict):
    connector.collection(Collections.EDM_CONFIGURATIONS).update_one(
        {"name": name}, {"$set": {"storage": storage}}
    )


def end_life(name: str, success: bool) -> bool:
    """Update the lastRunSuccess and lastRunAt and exit.

    Args:
        name (str): Name of the configuration.
        success (bool): lastRunSuccess value to be updated.

    Returns:
        bool: Value of `success`.
    """
    connector.collection(Collections.EDM_CONFIGURATIONS).update_one(
        {"name": name},
        {
            "$set": {
                "lastRunAt": datetime.now(UTC),
                "lastRunSuccess": success,
            }
        },
    )
    if not success:
        # Only mark rules that were being processed (GENERATING_HASH) as FAILED.
        # Rules in other states (e.g., UPLOADING_HASH) should not be affected.
        all_source_rules = connector.collection(Collections.EDM_BUSINESS_RULES).find(
            {f"sharedWith.{name}": {"$exists": True}}
        )
        for rule in all_source_rules:
            rule_db = BusinessRuleDB(**rule)
            if rule_db.status == StatusType.GENERATING_HASH:
                set_task_status(rule_name=rule_db.name, status=StatusType.FAILED)
    return success


def _mark_eligible_destinations_generating(source_config_name: str):
    """Re-check and mark eligible destinations as GENERATING_HASH.

    This is called at the start of execute_plugin to dynamically pick up
    any destinations that are in ready states (COMPLETED, FAILED, SCHEDULED).

    Args:
        source_config_name (str): Name of the source configuration.
    """
    all_source_rules = connector.collection(Collections.EDM_BUSINESS_RULES).find(
        {f"sharedWith.{source_config_name}": {"$exists": True}}
    )
    ready_statuses = {StatusType.COMPLETED, StatusType.FAILED, StatusType.SCHEDULED}
    for rule in all_source_rules:
        rule_db = BusinessRuleDB(**rule)
        if rule_db.status in ready_statuses:
            set_task_status(rule_name=rule_db.name, status=StatusType.GENERATING_HASH)


@APP.task(name="edm.execute_plugin", acks_late=False)
@integration("edm")
@track()
def execute_plugin(configuration_name):
    """Execute EDM plugin lifecycle.

    Runs the full plugin lifecycle for the source configuration:
    - Pull data from source
    - Generate EDM hashes
    - Upload hashes to all eligible destinations

    Re-checks destination statuses at the start to dynamically pick up any
    destinations that have become ready since the sync was triggered.

    Args:
        configuration_name (str): Name of the source configuration.
    """
    logger.info(
        f"Executing pulling lifecylce for source configuration with name {configuration_name}."
    )

    # Re-check and mark eligible destinations as GENERATING_HASH
    _mark_eligible_destinations_generating(configuration_name)

    try:
        configuration_db_dict = connector.collection(
            Collections.EDM_CONFIGURATIONS
        ).find_one(({"name": configuration_name}))
        if configuration_db_dict is None:
            return end_life(configuration_name, False)
        configuration_db = ConfigurationDB(**configuration_db_dict)
        PluginClass = helper.find_by_id(configuration_db.plugin)  # NOSONAR S117

        if PluginClass is None:
            logger.error(
                f"Could not find the plugin with id='{configuration_db.plugin}'. Skipping the EDM lifecycle execution.",
                error_code="EDM_1001",
            )
            return end_life(configuration_name, False)

        if not configuration_db.active:
            return False

        plugin = PluginClass(
            configuration_db.name,
            SecretDict(configuration_db.parameters),
            configuration_db.storage,
            configuration_db.checkpoint,
            logger,
            plugin_type=configuration_db.pluginType,
        )

        logger.info(
            f"Executing pull method for EDM configuration '{configuration_name}'."
        )
        start_time = datetime.now(UTC)
        # Pulling data in plugin
        plugin.pull()
        connector.collection(Collections.EDM_CONFIGURATIONS).update_one(
            {"_id": configuration_db_dict["_id"]},
            {"$set": {"checkpoint": start_time}},
        )
        # update storage
        _update_storage(configuration_db.name, plugin.storage)
        logger.info(
            f"Completed executing pull method EDM for configuration: {configuration_name}"
        )
        execute_celery_task(
            share_data.apply_async,
            "edm.share_data",
            args=[configuration_db.name],
        )
        return end_life(configuration_name, True)
    except NotImplementedError:
        logger.error(
            f"Pull method not implemented by plugin for configuration '{configuration_name}'.",
            details=traceback.format_exc(),
            error_code="EDM_1002",
        )
    except PyMongoError:
        logger.error(
            "Error occurred while connecting to the database.",
            details=traceback.format_exc(),
            error_code="EDM_1003",
        )
    except Exception:
        logger.error(
            f"Error occurred while executing the plugin lifecycle for configuration '{configuration_name}'. ",
            details=traceback.format_exc(),
            error_code="EDM_1004",
        )
    return end_life(configuration_name, False)
