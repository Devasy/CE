"""CFC Manual Upload Task."""

import os
import traceback
from bson.objectid import ObjectId
from datetime import datetime, UTC
from shutil import rmtree


from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    Notifier,
    SecretDict,
    integration,
    track,
    Scheduler,
)
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.integrations.cfc.models import (
    ConfigurationDB,
    StatusType,
    CFCStatistics,
    CFCTaskType,
)
from netskope.integrations.cfc.plugin_base import PushResult
from netskope.integrations.cfc.utils import (
    MANUAL_UPLOAD_PATH,
    increment_count,
    MANUAL_UPLOAD_PREFIX,
    NetskopeClientCFC,
)
from netskope.integrations.cfc.utils.cfc_file_utils import CFCFileUtils
from ..utils import (
    MAX_PENDING_STATUS_TIME,
    MANUAL_UPLOAD_TASK_DELAY_TIME,
    create_hashes,
)

connector = DBConnector()
logger = Logger()
helper = PluginHelper()
notifier = Notifier()
scheduler = Scheduler()


def _update_error_state(name: str, error: bool, error_message: str):
    """Update error state in sharing collection."""
    connector.collection(Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS).update_one(
        {"name": name},
        {"$set": {"errorState": {"error": error, "errorMessage": error_message}}},
    )


def _update_config_status(config_id, status):
    connector.collection(Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS).update_one(
        {"_id": config_id}, {"$set": {"status": status, "updatedAt": datetime.now(UTC)}}
    )


def _clean_files(source_config_name: str):
    if source_config_name:
        files_base_path = f"{MANUAL_UPLOAD_PATH}/{source_config_name}"
        files = connector.collection(Collections.CFC_IMAGES_METADATA).find(
            {"sourcePlugin": source_config_name, "sourceType": CFCTaskType.MANUAL.value}
        )
        for image in files:
            if image["path"]:
                file_path = f"{files_base_path}/{os.path.dirname(image['path']).replace('/', '_')}/{image['file']}"
            else:
                file_path = f"{files_base_path}/{image['file']}"
            if image["file"] and os.path.exists(file_path):
                os.remove(file_path)


@APP.task(name="cfc.manual_upload_task", acks_late=True)
@integration("cfc")
@track()
def manual_upload_task(config_id: str, configuration_name: str):
    """Manual upload task.

    Args:
        file_name (str): The name of the file to be uploaded.
    """
    try:
        manual_upload_config = connector.collection(
            Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
        ).find_one({"name": configuration_name})

        destination_config = connector.collection(
            Collections.CFC_CONFIGURATIONS
        ).find_one({"name": manual_upload_config.get("destinationConfiguration")})

        if not manual_upload_config:
            error_message = (
                f"{MANUAL_UPLOAD_PREFIX} configuration with name "
                f"'{manual_upload_config.get('destinationConfiguration')}' doesn't exists."
            )
            logger.error(error_message)
            _update_error_state(configuration_name, True, error_message)
            raise Exception(error_message)

        if not destination_config:
            error_message = (
                f"Could not share CFC hashes with configuration "
                f"'{manual_upload_config.get('destinationConfiguration')}'; it does not exist."
            )
            logger.error(error_message)
            _update_error_state(configuration_name, True, error_message)
            raise Exception(error_message)

        last_upload_time_diff = None
        pending_upload = None
        for file in manual_upload_config.get("files", []):
            if file["status"] == StatusType.PENDING:
                pending_upload = True
                last_upload_time_diff = datetime.now(UTC) - manual_upload_config.get(
                    "lastUploadTime", manual_upload_config.get("createdAt")
                )
                break

        if pending_upload and last_upload_time_diff.seconds < MAX_PENDING_STATUS_TIME:
            logger.debug(
                f"Some file uploads pending for manual task {configuration_name}. Rescheduling current task"
            )
            execute_celery_task(
                manual_upload_task.apply_async,
                "cfc.manual_upload_task",
                args=[str(config_id), configuration_name],
                countdown=MANUAL_UPLOAD_TASK_DELAY_TIME,
            )
            return

        if pending_upload:
            connector.collection(
                Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
            ).update_many(
                {"name": configuration_name, "files.status": StatusType.PENDING.value},
                {
                    "$set": {
                        "files.$[ele].status": StatusType.FILE_UPLOAD_FAILED.value,
                        "files.$[ele].updatedAt": datetime.now(UTC),
                    }
                },
                array_filters=[{"ele.status": StatusType.PENDING.value}],
            )

        if not destination_config.get("active"):
            error_message = (
                f"Configuration '{manual_upload_config.get('destinationConfiguration')}' "
                "is disabled; sharing skipped."
            )
            logger.error(error_message)
            notifier.error(
                f"Configuration '{destination_config.get('name')}' is disabled."
                " Therefore, sharing of CFC Hashes is skipped."
                " Please enable the plugin configuration to successfully share the CFC Hashes."
            )
            _update_error_state(configuration_name, True, error_message)
            raise Exception(error_message)

        configuration = ConfigurationDB(**destination_config)

        destination = destination_config.get("name")
        classifierID = manual_upload_config.get("classifierID")
        classifierName = manual_upload_config.get("classifierName")
        trainingType = manual_upload_config.get("trainingType")
        files = list(
            connector.collection(Collections.CFC_IMAGES_METADATA).find(
                {"sourcePlugin": configuration_name, "sourceType": CFCTaskType.MANUAL.value}
            )
        )
        if not files:
            error_message = f"No files metadata found for manual upload task '{configuration_name}'."
            logger.error(error_message)
            _update_error_state(configuration_name, True, error_message)
            raise Exception(error_message)

        _update_config_status(
            config_id=ObjectId(config_id), status=StatusType.GENERATING_HASH
        )
        CFCFileUtils.update_files_status(
            configuration_name=configuration_name,
            files=files,
            destination_plugin_name=destination,
            classifier_id=classifierID,
            classifier_name=classifierName,
            training_type=trainingType,
            status=StatusType.GENERATING_HASH,
        )

        # Hashing script called here
        hash_file_path, new_file_path_mappings = create_hashes(
            source_config_name=configuration_name,
            destination_config_name=destination,
            classifier_name=classifierName,
            classifier_id=classifierID,
            files=files,
            manual_upload=True,
        )

        _update_config_status(
            config_id=ObjectId(config_id), status=StatusType.UPLOADING_HASH
        )
        CFCFileUtils.update_files_status(
            configuration_name=configuration_name,
            files=files,
            destination_plugin_name=destination,
            classifier_id=classifierID,
            classifier_name=classifierName,
            training_type=trainingType,
            status=StatusType.UPLOADING_HASH,
        )

        Plugin = helper.find_by_id(configuration.plugin, validate=True)  # NOSONAR S117
        if Plugin is None:
            error_message = (
                f"Could not share CFC hashes with configuration "
                f"'{configuration.name}'; plugin with "
                f"id='{configuration.plugin}' does not exist."
            )
            logger.error(
                error_message,
                error_code="CFC_1046",
            )
            _update_error_state(configuration_name, True, error_message)
            raise Exception(error_message)

        try:
            plugin = Plugin(
                configuration.name,
                SecretDict(configuration.parameters),
                configuration.storage,
                configuration.checkpoint,
                logger,
            )
            plugin.ssl_validation = configuration.sslValidation

            action_dict = {"value": "share_cfc_hash"}
            manual_upload_config["isManual"] = True
            result: PushResult = plugin.push(
                hash_file_path=hash_file_path,
                action_dict=action_dict,
                mapping=manual_upload_config,
            )
            connector.collection(Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS).update_one(
                {"name": configuration.name},
                {"$set": {"storage": plugin.storage}}
            )
            invalid_files = result.invalid_files
            (
                valid_file_metadata,
                invalid_file_metadata,
                invalid_same_files,
                invalid_files_stats,
                any_sub_file_available,
            ) = CFCFileUtils.categorize_files(
                files=files,
                invalid_files=invalid_files,
                new_file_path_mappings=new_file_path_mappings,
            )
            error_state = {"error": False, "errorMessage": ""}

            if not result.success:
                error_state = {"error": True, "errorMessage": result.message}
                total_errored_files = sum(
                    invalid_files_stats.values()
                )
                error_msg = (
                    "Sharing for "
                    f"Classifier: '{classifierName}'"
                    f" as a {trainingType} is not completed successfully.\n"
                    f"Cause: {result.message}"
                )
                if total_errored_files > 0:
                    error_msg += (
                        f"\nFailed for {total_errored_files} out of {len(files)} files. "
                        f"Out of which '{invalid_files_stats[NetskopeClientCFC.MAX_STATUS]}' files failed "
                        "as the maximum number of files per classifier is reached. "
                        f"{invalid_files_stats[NetskopeClientCFC.INVALID_FILE_STATUS]} "
                        "files were not valid.\n"
                        f"{invalid_files_stats[NetskopeClientCFC.SAME_HASH_STATUS]} files "
                        "had same hash as the ones already shared."
                    )
                if any_sub_file_available:
                    error_msg += (
                        f"\n{invalid_files_stats[NetskopeClientCFC.INVALID_FILE_STATUS]} "
                        "files had some sub directories/files and were not shared."
                    )
                logger.error(
                    "Error occurred while sharing the CFC hashes "
                    f"with the destination configuration: '{destination}'.",
                    error_code="CFC_1035",
                    details=error_msg
                )
            # Update CFC Statistics
            if len(valid_file_metadata):
                increment_count(
                    CFCStatistics.SENT_IMAGES_COUNT.value,
                    len(valid_file_metadata),
                )

            CFCFileUtils.update_files_status(
                configuration_name=configuration_name,
                files=invalid_file_metadata,
                destination_plugin_name=destination,
                classifier_id=classifierID,
                classifier_name=classifierName,
                training_type=trainingType,
                status=StatusType.FAILED,
                update_last_shared=False,
            )
            CFCFileUtils.update_files_status(
                configuration_name=configuration_name,
                files=list(valid_file_metadata.values()),
                destination_plugin_name=destination,
                classifier_id=classifierID,
                classifier_name=classifierName,
                training_type=trainingType,
                status=StatusType.SUCCESS,
                update_last_shared=True,
            )
            if invalid_same_files:
                CFCFileUtils.update_files_status(
                    files=invalid_same_files,
                    classifier_id=classifierID,
                    classifier_name=classifierName,
                    training_type=trainingType,
                    destination_plugin_name=destination,
                    status=StatusType.SUCCESS,
                    update_last_shared=False,
                )

            # Refresh config to get updated file statuses after all updates
            updated_config = connector.collection(
                Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
            ).find_one({"_id": manual_upload_config.get("_id")})
            updated_files = updated_config.get("files", [])

            # Calculate final status based on individual file outcomes
            final_status_value = CFCFileUtils.calculate_status_from_files(updated_files)
            final_status = StatusType(final_status_value)

            _update_config_status(
                config_id=manual_upload_config.get("_id"),
                status=final_status,
            )
            _update_error_state(
                configuration_name, error_state["error"], error_state["errorMessage"]
            )

        except NotImplementedError:
            error_message = (
                f"Could not share CFC hashes with configuration "
                f"'{configuration_name}'. Push method not implemented."
            )
            logger.error(
                error_message,
                details=traceback.format_exc(),
                error_code="CFC_1047",
            )
            CFCFileUtils.update_files_status(
                configuration_name=configuration_name,
                files=files,
                destination_plugin_name=destination,
                classifier_id=classifierID,
                classifier_name=classifierName,
                training_type=trainingType,
                status=StatusType.FAILED,
            )
            _update_config_status(
                config_id=manual_upload_config.get("_id"), status=StatusType.FAILED
            )
            _update_error_state(configuration_name, True, error_message)
        except Exception:
            error_message = (
                f"Error occurred while sharing CFC hashes with configuration "
                f"'{configuration_name}'."
            )
            logger.error(
                error_message,
                details=traceback.format_exc(),
                error_code="CFC_1048",
            )
            CFCFileUtils.update_files_status(
                configuration_name=configuration_name,
                files=files,
                destination_plugin_name=destination,
                classifier_id=classifierID,
                classifier_name=classifierName,
                training_type=trainingType,
                status=StatusType.FAILED,
            )
            _update_config_status(
                config_id=manual_upload_config.get("_id"), status=StatusType.FAILED
            )
            _update_error_state(configuration_name, True, error_message)
        finally:
            if os.path.exists(hash_file_path):
                rmtree(os.path.dirname(hash_file_path))
        _clean_files(configuration_name)
    except Exception as error:
        _update_config_status(config_id=ObjectId(config_id), status=StatusType.FAILED)
        _clean_files(configuration_name)
        logger.error(
            f"Error occurred while sharing data for manual configuration "
            f"'{configuration_name}'. Error: {error}",
            details=traceback.format_exc(),
            error_code="CFC_1049",
        )
