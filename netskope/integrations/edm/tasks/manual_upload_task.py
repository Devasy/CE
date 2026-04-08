"""Manual CSV Task."""

import os
import shutil
import traceback
from pathlib import Path
from datetime import datetime, UTC

from pymongo.errors import PyMongoError

from netskope.common.celery.main import APP
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    SecretDict,
    integration,
    track,
)
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.integrations.edm.models import (
    ConfigurationDB,
    ManualUploadConfigurationDB,
    StatusType,
    EDMStatistics,
    EDMTaskType,
)
from netskope.integrations.edm.utils import (
    MANUAL_UPLOAD_PATH,
    MANUAL_UPLOAD_PREFIX,
    ManualUploadManager,
    increment_count,
)
from netskope.integrations.edm.utils.task_listing import change_manual_task_status

from .poll_edm_apply_status import sent_hashes_for_polling

connector = DBConnector()
logger = Logger()
helper = PluginHelper()


def manual_task_end_life(name: str, success: bool) -> bool:
    """Update the lastRunSuccess and lastRunAt and exit.

    Args:
        name (str): Name of the configuration.
        success (bool): lastRunSuccess value to be updated.

    Returns:
        bool: Value of `success`.
    """
    connector.collection(Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS).update_one(
        {"name": name},
        {
            "$set": {
                "lastRunAt": datetime.now(UTC),
                "lastRunSuccess": success,
            }
        },
    )
    change_manual_task_status(
        config_name=name,
        status=StatusType.UPLOAD_COMPLETED if success else StatusType.FAILED,
    )
    return success


def _update_manual_upload_storage(name: str, storage: dict):
    """Update manual upload storage.

    Args:
        name (str): name of manual upload configuration
        storage (dict): storage of manual upload configuration
    """
    connector.collection(Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS).update_one(
        {"name": name}, {"$set": {"storage": storage}}
    )


def _update_storage(name: str, storage: dict):
    """Update storage of plugin configuration.

    Args:
        name (str): name of configuration
        storage (dict): storage of configuration
    """
    connector.collection(Collections.EDM_CONFIGURATIONS).update_one(
        {"name": name}, {"$set": {"storage": storage}}
    )


@APP.task(name="edm.execute_manual_upload_task", acks_late=False)
@integration("edm")
@track()
def execute_manual_upload_task(configuration_name, shared_with, file_name):
    """Execute manual task.

    Args:
        configuration_name (str): manual upload configuration name
        sharedWith (dict): dict containing destination config with actions
        file_name (str): csv name of configuration
    """
    change_manual_task_status(
        config_name=configuration_name, status=StatusType.GENERATING_HASH
    )
    try:
        csv_configuration_db_dict = connector.collection(
            Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS
        ).find_one(({"name": configuration_name}))

        if csv_configuration_db_dict is None:
            return manual_task_end_life(configuration_name, False)

        configuration_db = ManualUploadConfigurationDB(
            **csv_configuration_db_dict
        )

        configuration_db.storage["file_name"] = configuration_db.fileName
        configuration_db.storage["edm_hash_folder"] = (
            f"{MANUAL_UPLOAD_PATH}/{configuration_db.name}"
        )

        manual_upload_object = ManualUploadManager(
            name=configuration_db.name,
            file_name=configuration_db.fileName,
            logger=logger,
            configuration=configuration_db.parameters,
            storage=configuration_db.storage,
        )

        csv_file_path = (
            f"{MANUAL_UPLOAD_PATH}/{configuration_db.name}/{configuration_db.fileName}"
        )
        is_unsanitized_data = configuration_db.parameters.get("sanity_inputs", {}).get(
            "is_unsanitized_data", True
        )
        if is_unsanitized_data:
            file_path = Path(configuration_db.fileName)
            if "csv" in file_path.suffix.lower():
                os.rename(
                    csv_file_path,
                    f"{MANUAL_UPLOAD_PATH}/{configuration_db.name}/{file_path.stem}.good",
                )
                logger.debug(
                    f"{MANUAL_UPLOAD_PREFIX} {csv_file_path} file Rename successfully"
                )
            else:
                error_message = f"{MANUAL_UPLOAD_PREFIX} Data file doesn't exist at {csv_file_path}."
                logger.error(message=error_message)
                return manual_task_end_life(configuration_db.name, False)

        else:
            manual_upload_object.csv_sanitize(csv_file_path)

        try:
            manual_upload_object.generate_csv_edm_hash()

            _update_manual_upload_storage(
                configuration_db.name, configuration_db.storage
            )
            # _change_emd_hash_status(name=configuration_db.name, emd_hash_status=True)

            change_manual_task_status(
                config_name=configuration_db.name, status=StatusType.UPLOADING_HASH
            )

            dest_config_name = list(shared_with.keys())[0]

            actions = shared_with.get(dest_config_name, [])

            dest_configuration_dict = connector.collection(
                Collections.EDM_CONFIGURATIONS
            ).find_one(({"name": dest_config_name}))

            if dest_configuration_dict is None:
                # configuration does not exist anymore
                logger.error(
                    f"{MANUAL_UPLOAD_PREFIX} Could not share hashes with configuration "
                    f"'{dest_config_name}'; Configuration does not exist.",
                    error_code="EDM_1008",
                )
                return manual_task_end_life(configuration_db.name, False)

            dest_configuration = ConfigurationDB(**dest_configuration_dict)
            if not dest_configuration.active:  # If plugin is disabled
                logger.debug(
                    f"{MANUAL_UPLOAD_PREFIX} Configuration '{dest_configuration.name}' is disabled; sharing skipped."
                )
                return manual_task_end_life(configuration_db.name, False)

            Plugin = helper.find_by_id(dest_configuration.plugin)  # NOSONAR S117
            if Plugin is None:
                logger.error(
                    f"{MANUAL_UPLOAD_PREFIX} Could not share hashes with configuration "
                    f"'{dest_configuration.name}'; plugin with "
                    f"id='{dest_configuration.plugin}' does not exist.",
                    error_code="EDM_1009",
                )
                return manual_task_end_life(configuration_db.name, False)

            hashes = {configuration_db.name: configuration_db.storage}

            plugin = Plugin(
                dest_configuration.name,
                SecretDict(dest_configuration.parameters),
                {**dest_configuration.storage, **hashes},
                dest_configuration.checkpoint,
                logger,
            )
            plugin.ssl_validation = dest_configuration.sslValidation

            for action_dict in actions:
                result = plugin.push(configuration_db.name, action_dict)

            if result.success:
                _update_storage(dest_configuration.name, plugin.storage)

                _update_manual_upload_storage(
                    configuration_db.name, configuration_db.storage
                )

                change_manual_task_status(
                    config_name=configuration_db.name,
                    status=StatusType.UPLOAD_COMPLETED,
                    update_shared_at=True,
                )
                # Upodate EDM Statistics
                increment_count(EDMStatistics.SENT_HASHES_COUNT.value, 1)
                if result.apply_success:
                    sent_hashes_for_polling(
                        fileSourceType=EDMTaskType.MANUAL,
                        fileSourceID=str(csv_configuration_db_dict["_id"]),
                        tenant=dest_configuration.tenant,
                        file_id=result.file_id,
                        upload_id=result.upload_id,
                    )
                else:
                    logger.error(
                        f"{MANUAL_UPLOAD_PREFIX} Error occurred while applying EDM hashes with configuration "
                        f"'{dest_config_name}'.",
                        error_code="EDM_1038",
                        details=result.message,
                    )
                    return manual_task_end_life(configuration_db.name, False)
            else:
                logger.error(
                    f"{MANUAL_UPLOAD_PREFIX} Error occurred while executing the manual upload task for "
                    f"configuration '{file_name}' and destination '{dest_config_name}'.",
                    error_code="EDM_1037",
                    details=result.message,
                )
                return manual_task_end_life(configuration_db.name, False)
        finally:
            if (
                manual_upload_object.storage
                and "edm_hash_folder" in manual_upload_object.storage
                and os.path.exists(manual_upload_object.storage["edm_hash_folder"])
            ):
                shutil.rmtree(manual_upload_object.storage["edm_hash_folder"])

        return manual_task_end_life(configuration_db.name, True)
    except PyMongoError:
        logger.error(
            f"{MANUAL_UPLOAD_PREFIX} Error occurred while connecting to the database.",
            details=traceback.format_exc(),
            error_code="EDM_1033",
        )
        return manual_task_end_life(configuration_db.name, False)
    except Exception:
        dest_config_name = list(shared_with.keys())[0]
        logger.error(
            f"{MANUAL_UPLOAD_PREFIX} Error occurred while executing the manual upload task for "
            f"configuration '{file_name}' and destination '{dest_config_name}'.",
            details=traceback.format_exc(),
            error_code="EDM_1034",
        )
    return manual_task_end_life(configuration_name, False)
