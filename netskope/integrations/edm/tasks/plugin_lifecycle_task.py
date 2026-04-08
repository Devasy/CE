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
from netskope.integrations.edm.models import ConfigurationDB, StatusType
from netskope.integrations.edm.utils.task_listing import set_task_status_by_config_name

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
    set_task_status_by_config_name(
        name, StatusType.GENERATING_HASH if success else StatusType.FAILED
    )
    return success


@APP.task(name="edm.execute_plugin", acks_late=False)
@integration("edm")
@track()
def execute_plugin(configuration_name):
    """Execute EDM plugin lifecycle.

    Args:
        configuration_name (str): Name of the source configuration.
    """
    set_task_status_by_config_name(configuration_name, StatusType.GENERATING_HASH)
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
