"""CFC Sharing Tasks."""

import copy
import os
import traceback
from datetime import datetime, UTC
from shutil import rmtree
from typing import Dict, List

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
from netskope.common.utils.exceptions import ForbiddenError
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.integrations.cfc.models import (
    BusinessRuleDB,
    ConfigurationDB,
    StatusType,
    TrainingType,
    CFCStatistics,
)
from netskope.integrations.cfc.plugin_base import PushResult
from netskope.integrations.cfc.utils import (
    create_hashes,
    FILE_PATH,
    increment_count,
    NetskopeClientCFC,
)
from netskope.integrations.cfc.utils.query import build_mongo_query
from netskope.integrations.cfc.utils.cfc_file_utils import CFCFileUtils

connector = DBConnector()
logger = Logger()
helper = PluginHelper()


def _get_storage(name: str):
    """Get storage of CFC plugin configuration."""
    return connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
        {"name": name}, {"storage": True}
    )


def _update_storage(name: str, storage: dict):
    """Update storage of CFC plugin configuration."""
    latest_storage = _get_storage(name).get("storage", {}) or {}
    latest_storage.update(storage)
    connector.collection(Collections.CFC_CONFIGURATIONS).update_one(
        {"name": name}, {"$set": {"storage": latest_storage}}
    )


def _filter_metadata(source_config_id: str, mappings: List[Dict]):
    """Filter metadata and group them by business rules selected in mappings."""
    error_state = {"error": False, "errorMessage": ""}
    filtered_results = {}
    new_mappings = copy.deepcopy(mappings)
    mappings_changed = False
    for idx in range(len(new_mappings)):
        try:
            rule = BusinessRuleDB(
                **connector.collection(Collections.CFC_BUSINESS_RULES).find_one(
                    {"name": new_mappings[idx]["businessRule"]}
                )
            )
            if not rule:
                error_message = f"No business rule with name '{new_mappings[idx]['businessRule']}' exists."
                new_mappings[idx]["errorState"] = {
                    "error": True,
                    "errorMessage": error_message,
                }
                error_state["error"] = True
                error_state["errorMessage"] = error_message
                mappings_changed = True
                continue
            if rule.muted:
                error_message = (
                    f"Business rule '{new_mappings[idx]['businessRule']}' "
                    "is muted so skipping it for sharing."
                )
                logger.warn(error_message)
                continue
            query = build_mongo_query(rule)
            query["$and"] = query.get("$and", []) + [
                {"sourcePluginID": source_config_id, "sourceType": "plugin", "outdated": False}
            ]
            files = connector.collection(Collections.CFC_IMAGES_METADATA).find(query)
            filtered_results[rule.name] = list(files)
            new_mappings[idx]["errorState"] = {"error": False, "errorMessage": ""}
        except PyMongoError:
            error_message = "Unable to connect to the database."
            new_mappings[idx]["errorState"] = {
                "error": True,
                "errorMessage": error_message,
            }
            error_state["error"] = True
            error_state["errorMessage"] = error_message
            mappings_changed = True

    return error_state, filtered_results, new_mappings, mappings_changed


def _update_mapping(id: str, old_mapping, new_mapping):
    """Update a mapping in sharing collection."""
    connector.collection(Collections.CFC_SHARING).update_one(
        {
            "_id": id,
            "mappings": {
                "$elemMatch": {
                    "businessRule": old_mapping["businessRule"],
                    "classifierID": old_mapping["classifierID"],
                    "trainingType": old_mapping["trainingType"],
                }
            },
        },
        {"$set": {"mappings.$": new_mapping}},
    )


def _update_mappings(id: str, new_mappings):
    """Update mappings in sharing collection."""
    connector.collection(Collections.CFC_SHARING).update_one(
        {"_id": id},
        {"$set": {"mappings": new_mappings}},
    )


def _update_error_state(id: str, error: bool, error_message: str):
    """Update error state in sharing collection."""
    connector.collection(Collections.CFC_SHARING).update_one(
        {"_id": id},
        {"$set": {"errorState": {"error": error, "errorMessage": error_message}}},
    )


def _update_task_status(id: str, status: StatusType):
    """Update error state in sharing collection."""
    connector.collection(Collections.CFC_SHARING).update_one(
        {"_id": id},
        {"$set": {"status": status}},
    )


def _upsert_destinations_of_image_data(
    files: list,
    destination_plugin_name: str,
    classifier_id: str,
    classifier_name: str,
    training_type: TrainingType = TrainingType.POSITIVE,
    status: StatusType = StatusType.UPLOADING_HASH,
    update_last_shared: bool = True,
):
    """Update or insert details about destinations in image data list."""
    for file in files:
        CFCFileUtils.upsert_image_destination(
            id=file["_id"],
            classifier_id=classifier_id,
            classifier_name=classifier_name,
            training_type=training_type,
            destination_plugin_name=destination_plugin_name,
            status=status,
            update_last_shared=update_last_shared,
        )


def _clean_files(source_config_id: str, source_config_name: str):
    if source_config_name:
        files_base_path = f"{FILE_PATH}/{source_config_name}"
        images = connector.collection(Collections.CFC_IMAGES_METADATA).find(
            {
                "sourcePlugin": source_config_name,
                "sourcePluginID": source_config_id,
                "sourceType": "plugin",
            }
        )
        for image in images:
            file_path = f"{files_base_path}/{image['dirUuid']}"
            if image["dirUuid"] and os.path.exists(file_path):
                rmtree(file_path)


@APP.task(name="cfc.share_data")
@integration("cfc")
@track()
def share_data(
    source_config_name: str,
    source_config_id: str,
    pull_success: bool = True
):
    """Evaluate sharings and push cfc hashes.

    Args:
        source_config (Optional[str], optional): Name of the source configuration.
        Defaults to None.
        destination_config_name (Optional[str], optional): Name of the destination configuration.
        Defaults to None.
        action (Optional[Dict], optional): Name of a specific action to
        perform. Defaults to None.
    """
    try:
        # Validating configurations
        configuration_db_dict = connector.collection(
            Collections.CFC_CONFIGURATIONS
        ).find_one(({"name": source_config_name}))
        configuration_db = ConfigurationDB(**configuration_db_dict)
        sharings = connector.collection(Collections.CFC_SHARING).find(
            {"sourceConfiguration": source_config_name}
        )

        # Iterating sharings
        for sharing in sharings:
            source_storage = _get_storage(source_config_name)
            source_storage = source_storage["storage"] if source_storage else {}
            destination_configuration = sharing["destinationConfiguration"]

            # Validating destination configuration
            configuration_dict = connector.collection(
                Collections.CFC_CONFIGURATIONS
            ).find_one({"name": destination_configuration})
            if configuration_dict is None:
                # configuration does not exist anymore
                error_message = (
                    f"Could not share CFC hashes with configuration "
                    f"'{destination_configuration}'; it does not exist."
                )
                logger.error(
                    error_message,
                    error_code="CFC_1020",
                )
                _update_error_state(sharing["_id"], True, error_message)
                continue
            configuration = ConfigurationDB(**configuration_dict)
            if not configuration.active:  # If plugin is disabled
                error_message = f"Configuration '{configuration.name}' is disabled; sharing skipped."
                logger.error(
                    error_message,
                    error_code="CFC_1021",
                )
                _update_error_state(sharing["_id"], True, error_message)
                continue
            Plugin = helper.find_by_id(
                configuration.plugin, validate=True
            )  # NOSONAR S117
            if Plugin is None:
                error_message = (
                    f"Could not share CFC hashes with configuration "
                    f"'{configuration.name}'; plugin with "
                    f"id='{configuration.plugin}' does not exist."
                )
                logger.error(
                    error_message,
                    error_code="CFC_1037",
                )
                _update_error_state(sharing["_id"], True, error_message)
                continue
            try:
                mappings = sharing["mappings"]

                # Filtering metadata based on mappings of the sharing configurations
                error_state, filtered_meta, new_mappings, mappings_changed = (
                    _filter_metadata(source_config_id, mappings)
                )
                if mappings_changed:
                    # Changing mappings if there found any error in a mapping
                    _update_mappings(sharing["_id"], new_mappings)
                # Setting error for sharing configuration
                _update_error_state(
                    sharing["_id"], error_state["error"], error_state["errorMessage"]
                )
                if error_state["error"]:
                    continue
                plugin = Plugin(
                    configuration.name,
                    SecretDict(configuration.parameters),
                    configuration.storage,
                    configuration.checkpoint,
                    logger,
                )
                plugin.ssl_validation = configuration.sslValidation
                actions = sharing["actions"]
                # Iterating the actions
                error_state = {"error": False, "errorMessage": ""}
                for action_dict in actions:
                    _update_task_status(sharing["_id"], StatusType.GENERATING_HASH)
                    # Iterating mappings
                    for mapping in new_mappings:
                        if not mapping["classifierID"]:
                            error_msg = (
                                f"'{mapping['classifierName']}' classifier is deleted"
                                f" from The Netskope tenant '{configuration.tenant}'."
                                f" Skipping sharing of CFC hashes for this classifier."
                            )
                            error_state = {
                                "error": True,
                                "errorMessage": error_msg,
                            }
                            logger.error(
                                error_msg
                            )
                            continue
                        files = filtered_meta.get(mapping["businessRule"], [])
                        if not files:
                            continue
                        hash_file_path = None
                        try:
                            # Generating CFC hashes for the files
                            _upsert_destinations_of_image_data(
                                files=files,
                                classifier_id=mapping["classifierID"],
                                classifier_name=mapping["classifierName"],
                                training_type=mapping["trainingType"],
                                destination_plugin_name=configuration.name,
                                status=StatusType.GENERATING_HASH,
                                update_last_shared=False,
                            )
                            logger.info(
                                f"Generating CFC hashes of {len(files)} files"
                                f" for the mapping Business Rule: '{mapping['businessRule']}' "
                                f"and Classifier: '{mapping['classifierName']}'"
                                f" as a {mapping['trainingType']}"
                            )
                            hash_file_path, new_file_path_mappings = create_hashes(
                                source_config_name=source_config_name,
                                destination_config_name=configuration.name,
                                classifier_name=mapping["classifierName"],
                                classifier_id=mapping["classifierID"],
                                files=files,
                            )
                            logger.info(
                                f"CFC hashes generated successfully for {len(files)} files"
                                f" for the mapping Business Rule: '{mapping['businessRule']}' "
                                f"and Classifier: '{mapping['classifierName']}'"
                                f" as a {mapping['trainingType']}"
                            )
                            # Adding entries to the destinations
                            _upsert_destinations_of_image_data(
                                files=files,
                                classifier_id=mapping["classifierID"],
                                classifier_name=mapping["classifierName"],
                                training_type=mapping["trainingType"],
                                destination_plugin_name=configuration.name,
                                status=StatusType.UPLOADING_HASH,
                                update_last_shared=False,
                            )
                            #  Pushing hashes via destination configuration
                            result: PushResult = plugin.push(
                                action_dict=action_dict,
                                hash_file_path=hash_file_path,
                                mapping=mapping,
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
                            new_mapping = result.mapping
                            new_mapping["errorState"] = {
                                "error": not result.success,
                                "errorMessage": result.message,
                            }
                            if not result.success:
                                total_errored_files = sum(
                                    [
                                        invalid_files_stats[NetskopeClientCFC.INVALID_FILE_STATUS],
                                        invalid_files_stats[NetskopeClientCFC.MAX_STATUS]
                                    ]
                                )
                                error_msg = (
                                    "Sharing for the mapping "
                                    f"Business Rule: '{mapping['businessRule']}' "
                                    f"and Classifier: '{mapping['classifierName']}'"
                                    f" as a {mapping['trainingType']} is not completed successfully.\n"
                                    f"Cause: {result.message}"
                                )
                                if total_errored_files > 0:
                                    error_msg += (
                                        f"\nFailed for {total_errored_files} out of {len(files)} files. "
                                        f"Out of which '{invalid_files_stats[NetskopeClientCFC.MAX_STATUS]}' files "
                                        "failed as the maximum number of files per classifier is reached. "
                                        f"{invalid_files_stats[NetskopeClientCFC.INVALID_FILE_STATUS]} "
                                        "files were not valid.\n"
                                        f"{invalid_files_stats[NetskopeClientCFC.SAME_HASH_STATUS]} files "
                                        "had same hash as the ones already shared."
                                    )
                                if any_sub_file_available:
                                    error_msg += (
                                        f"\n{invalid_files_stats[NetskopeClientCFC.INVALID_FILE_STATUS]} "
                                        "files had sub files and were not shared."
                                    )
                                logger.error(
                                    "Error occurred in one of the mappings while sharing the CFC hashes "
                                    f"with the destination configuration: '{destination_configuration}'.",
                                    error_code="CFC_1022",
                                    details=error_msg
                                )
                            elif invalid_files_stats[NetskopeClientCFC.MAX_STATUS] > 0:
                                logger.warn(
                                    "For one of the mappings, sharing of CFC hashes "
                                    f"with the destination configuration: '{destination_configuration}' "
                                    f"{invalid_files_stats[NetskopeClientCFC.SAME_HASH_STATUS]} files "
                                    "had same hash as the ones already shared.",
                                )
                            # Update CFC Statistics
                            if len(valid_file_metadata):
                                increment_count(
                                    CFCStatistics.SENT_IMAGES_COUNT.value,
                                    len(valid_file_metadata),
                                )

                            # Update status of the destination list with success or
                            # failed based on result.failed_hash_uploads
                            _upsert_destinations_of_image_data(
                                files=invalid_file_metadata,
                                classifier_id=mapping["classifierID"],
                                classifier_name=mapping["classifierName"],
                                training_type=mapping["trainingType"],
                                destination_plugin_name=configuration.name,
                                status=StatusType.FAILED,
                                update_last_shared=False,
                            )
                            _upsert_destinations_of_image_data(
                                files=list(valid_file_metadata.values()),
                                classifier_id=mapping["classifierID"],
                                classifier_name=mapping["classifierName"],
                                training_type=mapping["trainingType"],
                                destination_plugin_name=configuration.name,
                                status=StatusType.SUCCESS,
                                update_last_shared=True,
                            )
                            if invalid_same_files:
                                _upsert_destinations_of_image_data(
                                    files=invalid_same_files,
                                    classifier_id=mapping["classifierID"],
                                    classifier_name=mapping["classifierName"],
                                    training_type=mapping["trainingType"],
                                    destination_plugin_name=configuration.name,
                                    status=StatusType.SUCCESS,
                                    update_last_shared=False,
                                )
                            if mapping != new_mapping:
                                _update_mapping(
                                    sharing["_id"],
                                    mapping,
                                    new_mapping
                                )
                        except ForbiddenError:
                            message = (
                                f"Skipping the sharing for Source configuration: '{source_config_name}' "
                                f"and Destination configuration: '{destination_configuration}' due to "
                                "insufficient access of the endpoints."
                            )
                            logger.error(message=message, error_code="CFC_1023")
                            error_state = {"error": True, "errorMessage": message}
                            break
                        except Exception as error:
                            _upsert_destinations_of_image_data(
                                files=files,
                                classifier_id=mapping["classifierID"],
                                classifier_name=mapping["classifierName"],
                                training_type=mapping["trainingType"],
                                destination_plugin_name=configuration.name,
                                status=StatusType.FAILED,
                                update_last_shared=False,
                            )
                            raise error
                        finally:
                            if os.path.exists(hash_file_path):
                                rmtree(os.path.dirname(hash_file_path))
                        # Update the storage which will be set in push
                        _update_storage(configuration.name, plugin.storage)
                        if not result.success and not error_state["error"]:
                            error_state = {
                                "error": True,
                                "errorMessage": result.message,
                            }
                    if error_state["error"]:
                        _update_error_state(
                            sharing["_id"],
                            error_state["error"],
                            error_state["errorMessage"],
                        )
                    else:
                        connector.collection(Collections.CFC_SHARING).update_one(
                            {"_id": sharing["_id"]},
                            {"$set": {"sharedAt": datetime.now(UTC)}},
                        )
                    final_status = StatusType(
                        CFCFileUtils.get_sharing_status_for_destination(source_config_id, configuration.name)
                    )
                    _update_task_status(sharing["_id"], final_status)
            except NotImplementedError:
                error_message = (
                    f"Could not share CFC hashes with configuration "
                    f"'{configuration.name}'. Push method not implemented."
                )
                logger.error(
                    error_message,
                    details=traceback.format_exc(),
                    error_code="CFC_1024",
                )
                _update_task_status(sharing["_id"], StatusType.FAILED)
                _update_error_state(sharing["_id"], True, error_message)
            except Exception:
                error_message = (
                    f"Error occurred while sharing CFC hashes with configuration "
                    f"'{configuration.name}'."
                )
                logger.error(
                    error_message,
                    details=traceback.format_exc(),
                    error_code="CFC_1025",
                )
                _update_task_status(sharing["_id"], StatusType.FAILED)
                _update_error_state(sharing["_id"], True, error_message)
    except Exception:
        logger.error(
            f"Error occurred while sharing data for configuration "
            f"'{configuration_db.name}'.",
            details=traceback.format_exc(),
            error_code="CFC_1026",
        )
    finally:
        _clean_files(
            source_config_id=source_config_id, source_config_name=source_config_name
        )
