"""EDM Sharing Tasks."""
import os
import traceback
import shutil
from typing import Dict, Optional

from netskope.common.celery.main import APP
from netskope.common.utils.exceptions import ForbiddenError
from netskope.common.utils import (Collections, DBConnector, Logger, Notifier,
                                   SecretDict, integration, track)
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.integrations.edm.models import (BusinessRuleDB, EDMStatistics,
                                              ConfigurationDB, ShareDataType,
                                              StatusType, EDMTaskType)
from netskope.integrations.edm.utils import increment_count
from netskope.integrations.edm.utils.task_listing import (
    complete_task,
    set_task_status,
    set_task_status_by_config_name,
)

from .poll_edm_apply_status import sent_hashes_for_polling

connector = DBConnector()
logger = Logger()
helper = PluginHelper()
notifier = Notifier()


def _update_storage(name: str, storage: dict):
    latest_storage = _get_storage(name).get("storage", {}) or {}
    latest_storage.update(storage)
    connector.collection(Collections.EDM_CONFIGURATIONS).update_one(
        {"name": name}, {"$set": {"storage": latest_storage}}
    )


def _get_storage(name: str):
    return connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
        {"name": name}, {"storage": True}
    )


@APP.task(name="edm.share_data")
@integration("edm")
@track()
def share_data(
    source_config_name: str,
    destination_config_name: Optional[str] = None,
    rule: Optional[str] = None,
    action: Optional[Dict] = {},
    hash_dict: Optional[Dict] = None,
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
    """
    try:
        configuration_db_dict = connector.collection(
            Collections.EDM_CONFIGURATIONS
        ).find_one(({"name": source_config_name}))
        configuration_db = ConfigurationDB(**configuration_db_dict)
        set_task_status_by_config_name(
            configuration_db.name,
            StatusType.UPLOADING_HASH,
            destination_config_name=destination_config_name,
        )
        PluginClass = helper.find_by_id(configuration_db.plugin)  # NOSONAR S117
        if not PluginClass:
            logger.error(
                f"Could not share EDM hashes with configuration "
                f"'{configuration_db.name}'; plugin with "
                f"id='{configuration_db.plugin}' does not exist.",
                error_code="EDM_1007",
            )
            return
        supported_edm_method = [
            ShareDataType(share_method)
            for share_method in PluginClass.metadata.get("supported_edm_method")
        ]
        if ShareDataType.EDM_FILE_HASHES.value in supported_edm_method:
            share_file_hashes(
                source_config_name=source_config_name,
                destination_config_name=destination_config_name,
                rule=rule,
                action=action,
                hash_dict=hash_dict,
            )
    except NotImplementedError:
        logger.error(
            f"Could not share data with configuration "
            f"'{configuration_db.name}'. Push method not implemented.",
            details=traceback.format_exc(),
            error_code="EDM_1010",
        )
    except Exception:
        logger.error(
            f"Error occurred while sharing data with configuration "
            f"'{configuration_db.name}'.",
            details=traceback.format_exc(),
            error_code="EDM_1011",
        )


def share_file_hashes(
    source_config_name: str,
    destination_config_name: Optional[str] = None,
    rule: Optional[str] = None,
    action: Optional[Dict] = {},
    hash_dict: Optional[Dict] = None,
):
    """Push EDM Hashes.

    Args:
        source_config (Optional[str], optional): Name of the source configuration.
        Defaults to None.
        destination_config_name (Optional[str], optional): Name of the destination configuration.
        Defaults to None.
        rule (Optional[str], optional): Specific business rule to evaluate.
        All rules if None. Defaults to None.
        action (Optional[Dict], optional): Name of a specific action to
        perform. Defaults to None.
    """
    rules = connector.collection(Collections.EDM_BUSINESS_RULES).find(
        {"name": rule} if rule else {}
    )
    destination_dict = {}
    if not hash_dict:
        hash_dict = _get_storage(source_config_name)
        hash_dict = hash_dict["storage"] if hash_dict else {}
    try:
        for rule in rules:
            rule_dict = rule
            rule = BusinessRuleDB(**rule)
            sharedWith = rule.sharedWith
            if source_config_name not in sharedWith.keys():
                # Skip if source configuration not found in business rule
                continue
            if destination_config_name is None:
                destination_dict = sharedWith.get(source_config_name, {})
                set_task_status(rule_name=rule.name, status=StatusType.UPLOADING_HASH)
            else:
                source_dict = sharedWith.get(source_config_name, {})
                if destination_config_name in source_dict:
                    destination_dict[destination_config_name] = source_dict.get(
                        destination_config_name, []
                    )
                    set_task_status(rule_name=rule.name, status=StatusType.UPLOADING_HASH)
            for config, actions_list in destination_dict.items():
                actions = []
                configuration_dict = connector.collection(
                    Collections.EDM_CONFIGURATIONS
                ).find_one({"name": config})
                if configuration_dict is None:
                    # configuration does not exist anymore
                    logger.error(
                        f"Could not share EDM hashes with configuration "
                        f"'{config}'; it does not exist.",
                        error_code="EDM_1006",
                    )
                    set_task_status(rule_name=rule.name, status=StatusType.FAILED)
                    continue
                configuration = ConfigurationDB(**configuration_dict)
                if not configuration.active:  # If plugin is disabled
                    logger.debug(f"Configuration '{config}' is disabled; sharing skipped.")
                    notifier.error(
                        f"Configuration '{config}' is disabled."
                        " Therefore, sharing of EDM Hashes is skipped."
                        " Please enable the plugin configuration to successfully share the EDM Hash."
                    )
                    set_task_status(rule_name=rule.name, status=StatusType.FAILED)
                    continue
                Plugin = helper.find_by_id(configuration.plugin)  # NOSONAR S117
                if Plugin is None:
                    logger.error(
                        f"Could not share EDM hashes with configuration "
                        f"'{config}'; plugin with "
                        f"id='{configuration.plugin}' does not exist.",
                        error_code="EDM_1007",
                    )
                    set_task_status(rule_name=rule.name, status=StatusType.FAILED)
                    continue
                try:
                    storage = configuration.storage
                    storage.setdefault(source_config_name, {})
                    storage[source_config_name].update(hash_dict)
                    plugin = Plugin(
                        configuration.name,
                        SecretDict(configuration.parameters),
                        storage,
                        configuration.checkpoint,
                        logger,
                    )
                    plugin.ssl_validation = configuration.sslValidation
                    if not action:
                        actions = actions_list.copy()
                    else:
                        actions.append(action)
                    for action_dict in actions:
                        #  pushing hashes from destination_config_name
                        result = plugin.push(
                            source_config_name=source_config_name, action_dict=action_dict
                        )
                        if result.success:
                            # Update the storage which will be set in push
                            _update_storage(configuration.name, plugin.storage)
                            complete_task(rule_name=rule.name)

                            # Update EDM Statistics
                            increment_count(EDMStatistics.SENT_HASHES_COUNT.value, 1)
                            if configuration.tenant:
                                if result.apply_success:
                                    sent_hashes_for_polling(
                                        fileSourceType=EDMTaskType.PLUGIN,
                                        fileSourceID=str(rule_dict["_id"]),
                                        tenant=configuration.tenant,
                                        file_id=result.file_id,
                                        upload_id=result.upload_id,
                                    )
                                else:
                                    logger.error(
                                        "Error occurred while applying EDM hashes with configuration "
                                        f"'{configuration.name}'.",
                                        error_code="EDM_1036",
                                        details=result.message
                                    )
                                    set_task_status(rule_name=rule.name, status=StatusType.FAILED)
                                    continue
                            else:
                                set_task_status(rule_name=rule.name, status=StatusType.COMPLETED)
                        else:
                            logger.error(
                                f"Error occurred while sharing EDM hashes with configuration "
                                f"'{configuration.name}'.",
                                error_code="EDM_1035",
                                details=result.message
                            )
                            set_task_status(rule_name=rule.name, status=StatusType.FAILED)
                            continue
                except ForbiddenError:
                    logger.error(
                        "Received exit code 403, Forbidden Error. "
                        "Could not share EDM hashes with configuration "
                        f"'{configuration.name}'.",
                        error_code="EDM_1045"
                    )
                    set_task_status(rule_name=rule.name, status=StatusType.FAILED)
                except NotImplementedError:
                    logger.error(
                        f"Could not share EDM hashes with configuration "
                        f"'{configuration.name}'. Push method not implemented.",
                        details=traceback.format_exc(),
                        error_code="EDM_1010",
                    )
                    set_task_status(rule_name=rule.name, status=StatusType.FAILED)
                except Exception:
                    logger.error(
                        f"Error occurred while sharing EDM hashes with configuration "
                        f"'{configuration.name}'.",
                        details=traceback.format_exc(),
                        error_code="EDM_1011",
                    )
                    set_task_status(rule_name=rule.name, status=StatusType.FAILED)
    finally:
        if (
            hash_dict
            and "edm_hash_folder" in hash_dict
            and os.path.exists(hash_dict["edm_hash_folder"])
        ):
            shutil.rmtree(hash_dict["edm_hash_folder"])
