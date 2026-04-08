"""CFC Plugin Lifecycle."""
import os
import shutil
import traceback
from datetime import datetime, UTC

from pymongo.errors import PyMongoError

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.utils import (Collections, DBConnector, Logger, Notifier,
                                   PluginHelper, SecretDict, integration,
                                   track)
from netskope.integrations.cfc.models import ConfigurationDB
from netskope.integrations.cfc.utils import FILE_PATH
from netskope.integrations.cfc.plugin_base import PullResult

from .share_data import share_data

connector = DBConnector()
logger = Logger()
helper = PluginHelper()
notifier = Notifier()


def _is_sharing_available_for_plugin(source_configuration_name):
    """Fetch details about sharing for given configuration name."""
    result = False
    sharings = connector.collection(Collections.CFC_SHARING).find(
        {"sourceConfiguration": source_configuration_name}, projection=["destinationConfiguration"]
    )
    for sharing in sharings:
        destination_config_name = sharing["destinationConfiguration"]
        config = connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
            {"name": destination_config_name}
        )
        if config and config["active"] and helper.find_by_id(config["plugin"]):
            result = True
            break
    return result


def _update_storage(name: str, storage: dict):
    connector.collection(Collections.CFC_CONFIGURATIONS).update_one(
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
    connector.collection(Collections.CFC_CONFIGURATIONS).update_one(
        {"name": name},
        {
            "$set": {
                "lastRunAt": datetime.now(UTC),
                "lastRunSuccess": success,
            }
        },
    )

    return success


def _clean_files_on_error(name: str):
    """Clean all the files for a plugin configuration."""
    if name:
        files_base_path = f"{FILE_PATH}/{name}"
        if os.path.exists(files_base_path):
            shutil.rmtree(files_base_path)


@APP.task(name="cfc.execute_plugin", acks_late=False)
@integration("cfc")
@track()
def execute_plugin(configuration_name: str):
    """Execute CFC plugin lifecycle.

    Args:
        configuration_name (str): Name of the source configuration.

    Returns:
        bool: Whether lifecycle was executed successfully or not.
    """
    try:
        configuration_db_dict = connector.collection(
            Collections.CFC_CONFIGURATIONS
        ).find_one(({"name": configuration_name}))
        if configuration_db_dict is None:
            return end_life(configuration_name, False)
        configuration_db = ConfigurationDB(**configuration_db_dict)
        configuration_db_id = str(configuration_db_dict["_id"])
        PluginClass = helper.find_by_id(configuration_db.plugin)  # NOSONAR S117

        if PluginClass is None:
            logger.error(
                f"Could not find the plugin with id='{configuration_db.plugin}'. "
                "Skipping the CFC lifecycle execution.",
                error_code="CFC_1016",
            )
            return end_life(configuration_name, False)

        if not configuration_db.active:
            logger.info(
                "Skipping the CFC lifecycle execution. "
                f"Configuration '{configuration_name}' is disabled"
            )
            return False

        plugin = PluginClass(
            configuration_db.name,
            SecretDict(configuration_db.parameters),
            configuration_db.storage,
            configuration_db.checkpoint,
            logger
        )
        logger.info(
            f"Executing pull method for CFC configuration '{configuration_name}'."
        )
        start_time = datetime.now(UTC)
        # Pulling metadata and files in plugin
        is_sharing_available = _is_sharing_available_for_plugin(configuration_name)

        result: PullResult = plugin.pull(pull_files=is_sharing_available)
        metadata_list = result.metadata

        connector.collection(Collections.CFC_CONFIGURATIONS).update_one(
            {"_id": configuration_db_dict["_id"]},
            {"$set": {"checkpoint": start_time}},
        )

        # update storage
        _update_storage(configuration_db.name, plugin.storage)
        logger.info(
            f"Completed executing pull method for CFC configuration: '{configuration_name}'." +
            (
                f" Fetched {len(metadata_list)} images and their metadata."
                if is_sharing_available
                else f" Fetched metadata of {len(metadata_list)} image."
            )
        )

        # Marking all image data as outdated initially
        connector.collection(Collections.CFC_IMAGES_METADATA).update_many(
            {"sourcePluginID": configuration_db_id},
            {"$set": {"outdated": True}}
        )
        # Storing metadata in DB
        if metadata_list and isinstance(metadata_list, list):
            for metadata in metadata_list:
                connector.collection(Collections.CFC_IMAGES_METADATA).update_one(
                    {
                        "sourcePlugin": configuration_db.name,
                        "sourcePluginID": configuration_db_id,
                        "sourceType": "plugin",
                        "path": metadata["path"],
                        "dirUuid": metadata["dirUuid"],
                    },
                    {"$set": {**metadata, "outdated": False}},
                    upsert=True,
                )
        elif not metadata_list:
            logger.error(
                f"No metadata is fetched for CFC plugin "
                f"with id '{configuration_db.plugin}'.",
                error_code="CFC_1011",
            )
            return end_life(configuration_name, result.success)
        else:
            logger.error(
                f"Pull method returned data with invalid datatype for CFC plugin "
                f"with id '{configuration_db.plugin}'.",
                error_code="CFC_1012",
            )
            _clean_files_on_error(configuration_name)
            return end_life(configuration_name, False)
        logger.info(
            f"Metadata of {len(metadata_list)} images are stored for CFC configuration "
            f"'{configuration_name}'."
        )

        # Sharing task
        execute_celery_task(
            share_data.apply_async,
            "cfc.share_data",
            args=[configuration_db.name, configuration_db_id],
            kwargs={"pull_success": result.success},
        )

        return end_life(configuration_name, result.success)
    except NotImplementedError:
        notifier.error(
            (
                "Pull method is not implemented by plugin for "
                f"CFC configuration '{configuration_name}'."
            )
        )
        logger.error(
            f"Pull method not implemented by plugin for CFC configuration '{configuration_name}'.",
            details=traceback.format_exc(),
            error_code="CFC_1013",
        )
    except PyMongoError:
        notifier.error(
            (
                "Unable to connect to the database while executing the plugin lifecycle for "
                f"CFC configuration '{configuration_name}'."
            )
        )
        logger.error(
            "Error occurred while connecting to the database.",
            details=traceback.format_exc(),
            error_code="CFC_1014",
        )
    except Exception:
        notifier.error(
            (
                "An error occurred while executing the plugin lifecycle for "
                f"CFC configuration '{configuration_name}'."
            )
        )
        logger.error(
            f"Error occurred while executing the plugin lifecycle for CFC configuration '{configuration_name}'. ",
            details=traceback.format_exc(),
            error_code="CFC_1015",
        )
    _clean_files_on_error(configuration_name)
    return end_life(configuration_name, False)
