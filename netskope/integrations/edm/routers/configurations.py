"""Provides configuration related endpoints."""
import os
import shutil
import traceback
from datetime import datetime, UTC
from shutil import rmtree
from typing import Any, List, Union

from fastapi import APIRouter, HTTPException, Security, Path, Body

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    Scheduler,
    SecretDict,
    get_dynamic_fields_from_plugin,
)
from netskope.common.utils.plugin_helper import PluginHelper

from ..models import (
    ActionWithoutParams,
    CleanSampleFilesIn,
    CleanSampleFilesOut,
    ConfigurationDB,
    ConfigurationDelete,
    ConfigurationIn,
    ConfigurationOut,
    ConfigurationUpdate,
    ConfigurationNameValidationIn,
)
from ..models.plugin import (
    find_active_zip_name_conflict,
    get_zip_name_from_configuration,
)
from ..plugin_base import PluginBase, ValidationResult
from ..utils import FILE_PATH, MANUAL_UPLOAD_PATH, UPLOAD_PATH
from netskope.integrations import trim_space_parameters_fields

router = APIRouter()
scheduler = Scheduler()
plugin_helper = PluginHelper()
logger = Logger()
db_connector = DBConnector()


def log_changes(configuration, updated_configuration):
    """Log changes based on the incoming request."""
    if configuration.active is not None:
        logger.debug(
            f"Configuration '{configuration.name}' is {'enabled' if updated_configuration.active else 'disabled'}."
        )
    if configuration.pollInterval or configuration.pollIntervalUnit:
        logger.debug(
            f"Configuration '{configuration.name}' poll interval set to "
            f"{updated_configuration.pollInterval} {updated_configuration.pollIntervalUnit}."
        )
    if configuration.parameters:
        logger.debug(
            f"Plugin configuration updated for configuration '{configuration.name}'."
        )


def filter_out_none_values(data: dict) -> dict:
    """Filter out keys with None values from a dict.

    Args:
        data (dict): Dictionary to be filtered.

    Returns:
        dict: Filtered dictionary.
    """
    return {k: v for k, v in data.items() if v is not None}


def _delete_business_rule_for_configuration(config_name: str):
    """Delete business rule when source or destination configuration is deleted.

    Args:
        config_name (str): configuration name which is being deleted
    """
    rules = db_connector.collection(Collections.EDM_BUSINESS_RULES).find({})
    rules_to_delete = []
    for rule in rules:
        shared_with = rule.get("sharedWith", {})
        if config_name in shared_with:
            rules_to_delete.append(rule["name"])
            continue
        for dest_dict in shared_with.values():
            if config_name in dest_dict:
                rules_to_delete.append(rule["name"])
    db_connector.collection(Collections.EDM_BUSINESS_RULES).delete_many(
        {"name": {"$in": rules_to_delete}}
    )


def get_plugin_instance(
    plugin_id: str,
    configuration: Union[ConfigurationIn, ConfigurationUpdate],
    configuration_db: ConfigurationDB = None,
) -> PluginBase:
    """
    Retrieve an instance of a plugin based on the provided plugin ID and configuration.

    Args:
        plugin_id (str): The ID of the plugin to retrieve.
        configuration (Union[ConfigurationIn, ConfigurationUpdate]): The configuration for the plugin.

    Returns:
        PluginBase: An instance of the plugin.

    Raises:
        HTTPException: If the plugin with the given ID does not exist.
    """
    PluginClass = plugin_helper.find_by_id(plugin_id)  # NOSONAR S117
    if PluginClass is None:
        raise HTTPException(400, f"Plugin with id '{plugin_id}' does not exist.")
    trim_space_parameters_fields(configuration.parameters)
    plugin: PluginBase = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        configuration_db.storage if configuration_db else {},
        None,
        logger,
    )
    return plugin


@router.get(
    "/plugins/configurations",
    response_model=List[ConfigurationOut],
    tags=["EDM Configurations"],
    description="Get list of all the configurations.",
)
async def read_all_configurations_list(
    active: bool = None,
    user: User = Security(get_current_user, scopes=["edm_read"]),
):
    """List out all the configurations.

    Args:
        active (bool, optional): Get only (in)active configurations. Defaults to None.

    Returns:
        List[ConfigurationOut]: List of configurations.
    """
    find_dict = {}
    if active is not None:
        find_dict["active"] = active
    results = db_connector.collection(Collections.EDM_CONFIGURATIONS).find(find_dict)
    out = []
    netskope_configs = []
    for config_db in results:
        if "edm_write" not in user.scopes:
            config_db["parameters"] = {}
        PluginClass = plugin_helper.find_by_id(config_db["plugin"])
        if not PluginClass:
            raise HTTPException(
                400,
                f"Plugin with id='{config_db.plugin}' does not exist.",
            )
        metadata = PluginClass.metadata
        config = ConfigurationOut(
            **config_db,
            pluginName=metadata.get("name"),
            pluginVersion=metadata.get("version"),
            netskope=metadata.get("netskope", False),
            pushSupported=metadata.get("push_supported", False),
            pullSupported=metadata.get("pull_supported", False),
        )
        if metadata.get("netskope", False):
            netskope_configs.append(config)
        else:
            out.append(config)
    return netskope_configs + sorted(out, key=lambda c: c.pluginName.lower())


def _clean_sample_files(
    configuration_name: str, source_type: str = "plugin"
) -> CleanSampleFilesOut:
    """Clean sample files related to provided Plugin configuration.

    Args:
        configuration_name (str): The name of the Plugin configuration
        source_type (str): The type of the configuration. Defaults to "plugin".

    Raises:
        HTTPException: If an error occurs during the cleaning process, an HTTPException
        is raised with a 400 status code and an error message.

    Returns:
        CleanSampleFilesOut: A dictionary containing the result of the cleaning operation,
        including the configuration name, status, and a message.
    """
    try:
        if source_type == "plugin":
            directory_path = os.path.join(FILE_PATH, configuration_name)
        elif source_type == "manual_upload":
            directory_path = os.path.join(MANUAL_UPLOAD_PATH, configuration_name)

        if (
            configuration_name
            and os.path.exists(directory_path)
            and os.path.isdir(directory_path)
        ):
            if source_type == "plugin":
                configuration = db_connector.collection(
                    Collections.EDM_CONFIGURATIONS
                ).find_one({"name": configuration_name})
            elif source_type == "manual_upload":
                configuration = db_connector.collection(
                    Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS
                ).find_one({"name": configuration_name})

            if configuration:
                if source_type == "plugin":
                    netskope_plugin_result = [
                        PluginHelper.check_plugin_name_with_regex(
                            "netskope_edm",
                            configuration["plugin"]
                        ),
                        PluginHelper.check_plugin_name_with_regex(
                            "netskope_edm_forwarder_receiver",
                            configuration["plugin"]
                        )
                    ]
                    if any(netskope_plugin_result):
                        return {
                            "name": configuration_name,
                            "sampleFileCleanupStatus": True,
                            "message": (
                                f"No sample files to cleanup for configuration '{configuration_name}'."
                            ),
                        }
                    logger.debug(
                        f"Deleting sample file(s) for plugin configuration: {configuration_name}."
                    )
                    files_list = ["sample.csv", "sample.good", "sample.bad"]
                elif source_type == "manual_upload":
                    csv_name = configuration["fileName"]
                    csv_name_prefix = os.path.splitext(configuration["fileName"])[0]
                    logger.debug(
                        f"Deleting sample file(s) for manual uploaded file: {csv_name}."
                    )
                    files_list = [
                        f"sample_{csv_name}",
                        f"sample_{csv_name_prefix}.good",
                        f"sample_{csv_name_prefix}.bad",
                    ]
                deleted_files_list = []
                for filename in files_list:
                    filepath = os.path.join(directory_path, filename)
                    if os.path.exists(filepath):
                        os.remove(filepath)
                        deleted_files_list.append(filename)
                if deleted_files_list:
                    if source_type == "plugin":
                        logger.info(
                            f"Deleted {deleted_files_list} files for "
                            f"Plugin configuration: {configuration_name}"
                        )
                    elif source_type == "manual_upload":
                        csv_name = configuration["fileName"]
                        logger.info(
                            f"Deleted {deleted_files_list} files for "
                            f"manual uploaded file: {csv_name}"
                        )
            else:
                shutil.rmtree(directory_path)
        return {
            "name": configuration_name,
            "sampleFileCleanupStatus": True,
            "message": (
                "Sample files deleted successfully for "
                f"'{configuration_name}' Plugin configuration."
            ),
        }
    except Exception as error:
        logger.error(
            (
                "Error occurred while cleaning up sample files "
                f"for '{configuration_name}' Plugin configuration."
            ),
            details=traceback.format_exc(),
        )
        raise HTTPException(
            500, "Error occurred while cleaning sample files. Check logs."
        ) from error


@router.post(
    "/plugins/configurations/cleanSampleFiles",
    response_model=CleanSampleFilesOut,
    tags=["EDM Configurations"],
)
async def clean_sample_files(
    configuration: CleanSampleFilesIn,
    _: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Clean sample files.

    Args:
        configuration_name (str): Plugin configuration name.
        _ (User, optional): An authenticated user with 'edm_write' scope.
    """
    return _clean_sample_files(
        configuration.name.strip(), source_type=configuration.sourceType.value
    )


@router.post(
    "/plugins/configurations/{plugin_id}",
    response_model=ConfigurationOut,
    tags=["EDM Configurations"],
    status_code=201,
    description="Create a new configuration.",
)
async def create_configuration(
    configuration: ConfigurationIn,
    plugin_id: str,
    user: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Create a new configuration.

    # response_model=ConfigurationOut,
    Args:
        plugin_id (str): ID of the plugin.
        configuration (ConfigurationIn): Configuration to be created.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        ConfigurationOut: Newly created configuration.
    """
    plugin_type_required_plugin = "netskope_edm_forwarder_receiver"
    # make sure plugin exists
    PluginClass = plugin_helper.find_by_id(plugin_id)  # NOSONAR S117
    if PluginClass is None:
        raise HTTPException(400, f"Plugin with id='{plugin_id}' does not exist.")
    # to trim extra spaces for parameters fields.
    trim_space_parameters_fields(configuration.parameters)
    # validate configuration
    plugin = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        {},
        None,
        logger,
        plugin_type=configuration.pluginType
    )
    plugin.ssl_validation = configuration.sslValidation
    if plugin_id and PluginHelper.check_plugin_name_with_regex(
        plugin_type_required_plugin,
        plugin_id
    ):
        if configuration.pluginType:
            if configuration.pluginType not in ["forwarder", "receiver"]:
                raise HTTPException(
                    400,
                    "Plugin type must be either 'forwarder' or 'receiver'.",
                )
            elif configuration.pluginType == "receiver":
                configuration.parameters = {}
        else:
            raise HTTPException(
                400, "Plugin type is required for plugin id '{plugin_id}'."
            )
    else:
        configuration.pluginType = None

    if not PluginClass.metadata.get("netskope", False) or (
        configuration.pluginType
        and configuration.pluginType == "forwarder"
    ):
        _validate_entire_configuration(plugin, configuration)

    # insert new configuration
    config_db = None
    try:
        config_db = ConfigurationDB(
            **(configuration.model_dump()),
            createdBy=user.username,
            createdAt=datetime.now(UTC),
        )
        config_db.storage = plugin.storage
        db_connector.collection(Collections.EDM_CONFIGURATIONS).insert_one(
            config_db.model_dump()
        )
    except Exception as error:
        logger.debug(
            "Error occurred while creating a new configuration.",
            details=traceback.format_exc(),
            error_code="EDM_1012",
        )
        raise HTTPException(
            500, "Error occurred while creating a new configuration."
        ) from error

    # Delete sample files of this configuration
    _clean_sample_files(configuration.name)

    # schedule on celery
    try:
        if not PluginClass.metadata.get("netskope", False):
            scheduler.schedule(
                name=configuration.name,
                task_name="edm.execute_plugin",
                poll_interval=configuration.pollInterval,
                poll_interval_unit=configuration.pollIntervalUnit,
                args=[configuration.name],
            )
        # No else ad no need to pull from tenant in EDM currently.
    except Exception as error:
        logger.debug(
            "Error occurred while scheduling the configuration.",
            details=traceback.format_exc(),
            error_code="EDM_1013",
        )
        raise HTTPException(
            500, "Error occurred while scheduling the configuration."
        ) from error
    logger.debug(
        f"Configuration '{configuration.name}' created for plugin '{plugin_id}'"
    )
    return {
        **(config_db.model_dump()),
        "pluginName": PluginClass.metadata.get("name"),
        "pluginVersion": PluginClass.metadata.get("version"),
        "pullSupported": PluginClass.metadata.get("pull_supported", False),
        "pushSupported": PluginClass.metadata.get("push_supported", False),
        "netskope": PluginClass.metadata.get("netskope", False),
    }


def _get_dynamic_step_fields(
    plugin_id: str, step_name: str,
    configuration: Union[ConfigurationIn, ConfigurationUpdate]
) -> List:
    """Fetch dynamic step fields from a plugin based on the provided plugin ID, step name, and configuration.

    Args:
        plugin_id (str): The ID of the plugin to retrieve dynamic step fields from.
        step_name (str): The name of the step for which dynamic fields are requested.
        configuration (Union[ConfigurationIn, ConfigurationUpdate]): Plugin configuration.

    Raises:
        HTTPException: If the plugin does not implement dynamic steps or if an error occurs during the retrieval.

    Returns:
        List: A list of dynamic step fields.
    """
    PluginClass = plugin_helper.find_by_id(plugin_id)  # NOSONAR S117
    if not PluginClass:
        raise HTTPException(400, f"Plugin with id='{plugin_id}' does not exist.")
    plugin = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        {},
        None,
        logger,
    )

    try:
        return plugin.get_fields(
            step_name,
            SecretDict(configuration.parameters)
        )

    except NotImplementedError:
        raise HTTPException(400, "Plugin does not implement dynamic steps.")

    except Exception:
        logger.error(
            "Error occurred while getting fields.",
            details=traceback.format_exc(),
            error_code="EDM_1005",
        )
        raise HTTPException(400, "Error occurred while getting fields. Check logs.")


def _clean_file_on_configuration_delete(name: str):
    """Clean EDM configuration related stored files.

    Args:
        name (str): EDM Configuration name
    """
    try:
        config_directories = [f"{FILE_PATH}/{name}", f"{UPLOAD_PATH}/{name}"]
        for file in config_directories:
            if os.path.exists(file):
                rmtree(file)
        logger.debug(
            f"Files for EDM configuration: '{name}' are cleaned as configuration is being deleted."
        )
    except Exception:
        logger.error(
            message=f"Error occured while cleaning files for EDM configuration: '{name}' on delete action.",
            error_code="EDM_1020",
            details=traceback.format_exc(),
        )


@router.post(
    "/plugins/configurations/validate/name",
    response_model=ValidationResult,
    tags=["EDM Configurations"],
    description="Validates configuration name.",
)
async def validate_configuration_name(
    _: ConfigurationNameValidationIn,
) -> ValidationResult:
    """Validate configuration for unique configuration name.

    Returns:
        ValidationResult: validation result
    """
    return ValidationResult(success=True, message="Validation successful.")


def _validate_configuration_step(step, configuration):
    """Validate individual steps of a configuration."""
    plugin = get_plugin_instance(configuration.plugin, configuration)
    try:
        return plugin.validate_step(step)
    except Exception as e:
        logger.error(
            f"Exception occurred while executing validate for step {step}",
            details=traceback.format_exc(),
            error_code="EDM_1043",
        )
        raise HTTPException(400, str(e))


def _validate_entire_configuration(plugin, configuration):
    """Validate all the steps of a configuration.

    Args:
        plugin (PluginClass): Plugin instance.
        configuration (dict): Plguin configuration.

    Raises:
        HTTPException: If validation fails.
    """
    if not configuration.parameters:
        raise HTTPException(400, "Please provide valid configuration parameters.")
    for step in configuration.parameters:
        try:
            result = plugin.validate_step(step)
        except Exception as e:
            logger.error(
                f"Exception occurred while executing validate for step {step}",
                details=traceback.format_exc(),
                error_code="EDM_1044",
            )
            raise HTTPException(400, str(e))
        if not result.success:
            raise HTTPException(400, result.message)


@router.post("/configuration/validate/{step}", tags=["EDM Configurations"])
async def validate_configuration_step(
    step: str = Path(...),
    configuration: ConfigurationIn = Body(...),
    user: User = Security(get_current_user, scopes=["edm_write"]),
) -> Any:
    """Validate a configuration step."""
    return _validate_configuration_step(step, configuration)


@router.patch("/configuration/validate/{step}", tags=["EDM Configurations"],)
async def validate_patch_configuration_step(
    step: str = Path(...),
    configuration: ConfigurationUpdate = Body(...),
    user: User = Security(get_current_user, scopes=["edm_write"]),
) -> Any:
    """Validate a configuration step."""
    return _validate_configuration_step(step, configuration)


@router.post(
    "/configuration/{plugin_id}/step/{name}",
    tags=["EDM Configurations"]
)
async def get_configuration_step_post(
    configuration: ConfigurationIn,
    plugin_id: str = Path(...),
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["edm_write"]),
) -> Any:
    """Get fields for the specified dynamic step.

    Args:
        configuration (ConfigurationIn): The configuration input for the dynamic step.
        plugin_id (str): The ID of the plugin for which to retrieve dynamic step fields.
        name (str): The name of the dynamic step for which to fetch fields.
        user (User): The authenticated user.

    Returns:
        Any: The dynamic step fields.
    """
    return _get_dynamic_step_fields(plugin_id, name, configuration)


@router.patch(
    "/configuration/{plugin_id}/step/{name}",
    tags=["EDM Configurations"],
)
async def get_configuration_step_patch(
    configuration: ConfigurationUpdate,
    plugin_id: str = Path(...),
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["edm_write"]),
) -> Any:
    """Get fields for the specified dynamic step.

    Args:
        configuration (ConfigurationIn): The configuration input for the dynamic step.
        plugin_id (str): The ID of the plugin for which to retrieve dynamic step fields.
        name (str): The name of the dynamic step for which to fetch fields.
        user (User): The authenticated user.

    Returns:
        Any: The dynamic step fields.
    """
    return _get_dynamic_step_fields(plugin_id, name, configuration)


@router.patch(
    "/plugins/configurations",
    response_model=ConfigurationOut,
    tags=["EDM Configurations"],
    description="Update an existing configuration.",
)
async def update_configuration(
    configuration: ConfigurationUpdate,
    user: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Update existing configuration.

    Args:
        configuration (ConfigurationUpdate): Updated configuration.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        ConfigurationOut: The newly updated configuration.
    """
    # new added
    updated_configuration_dict = filter_out_none_values(configuration.model_dump())
    configuration_db_dict = db_connector.collection(
        Collections.EDM_CONFIGURATIONS
    ).find_one({"name": configuration.name})

    # Clean sample files if confguration parameters are updated
    if configuration.active != configuration_db_dict["active"]:
        _clean_sample_files(configuration.name)

    # merge existing and updated fields
    updated_configuration_dict = {
        **configuration_db_dict,
        **updated_configuration_dict,
        "lastUpdatedBy": user.username,
        "lastUpdatedAt": datetime.now(UTC),
    }

    updated_configuration = ConfigurationDB(**updated_configuration_dict)

    # make sure plugin exists
    PluginClass = plugin_helper.find_by_id(updated_configuration.plugin)  # NOSONAR S117
    if PluginClass is None:
        raise HTTPException(
            400,
            f"Plugin with id='{updated_configuration.plugin}' does not exist.",
        )

    # to trim extra spaces for parameters fields.
    trim_space_parameters_fields(configuration.parameters)

    # validate configuration if active
    if updated_configuration.active is True:
        if not configuration_db_dict["active"]:
            conflict_name = find_active_zip_name_conflict(
                updated_configuration.name,
                exclude_config=updated_configuration.name,
            )
            if conflict_name:
                zip_name = get_zip_name_from_configuration(updated_configuration.name)
                raise HTTPException(
                    400,
                    (
                        f"Cannot enable configuration '{updated_configuration.name}' because "
                        f"active configuration '{conflict_name}' already uses EDM zip '{zip_name}'."
                    ),
                )
        plugin = PluginClass(
            configuration.name,
            SecretDict(updated_configuration.parameters),
            updated_configuration.storage,
            updated_configuration.checkpoint,
            logger,
            plugin_type=updated_configuration.pluginType
        )
        # below lines with sslValidation and use proxy is not needed here
        plugin.ssl_validation = updated_configuration.sslValidation
        if not PluginClass.metadata.get("netskope", False) or (
            updated_configuration.pluginType
            and updated_configuration.pluginType == "forwarder"
        ):
            _validate_entire_configuration(plugin, updated_configuration)
        updated_configuration.storage = plugin.storage

    # if marked inactive, remember the inactivation time
    if updated_configuration.active is False:
        updated_configuration.disabledAt = datetime.now(UTC)

    update_result = db_connector.collection(Collections.EDM_CONFIGURATIONS).update_one(
        {"_id": configuration_db_dict["_id"]},
        {"$set": updated_configuration.model_dump()}
    )
    if not update_result.modified_count > 0:
        raise HTTPException(500, "Error occurred while updating the configuration.")

    if updated_configuration.active is False:
        # remove schedule if marked inactive
        scheduler.delete(configuration.name)
    else:
        # if not upsert the schedule in case it was inactive or pollInterval
        # has been changed
        if not PluginClass.metadata.get("netskope", False):
            scheduler.upsert(
                name=updated_configuration.name,
                task_name="edm.execute_plugin",
                poll_interval=updated_configuration.pollInterval,
                poll_interval_unit=updated_configuration.pollIntervalUnit,
                args=[updated_configuration.name],
            )
    log_changes(configuration, updated_configuration)
    metadata = plugin_helper.find_by_id(updated_configuration.plugin).metadata

    # Delete sample files of this configuration
    _clean_sample_files(configuration.name)
    return {
        **updated_configuration.model_dump(),
        "pluginName": metadata.get("name"),
        "pluginVersion": metadata.get("version"),
        "netskope": metadata.get("netskope", False),
        "pushSupported": metadata.get("push_supported", False),
        "pullSupported": metadata.get("pull_supported", False),
    }


@router.delete(
    "/configuration",
    description="Delete an existing configuration.",
    tags=["EDM Configurations"],
)
async def delete_configuration(
    configuration: ConfigurationDelete,
    user: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Delete a configuration."""
    db_connector.collection(Collections.EDM_CONFIGURATIONS).delete_one(
        {"name": configuration.name}
    )
    _clean_file_on_configuration_delete(configuration.name)
    _delete_business_rule_for_configuration(configuration.name)
    scheduler.delete(configuration.name)
    logger.debug(
        f"Configuration with name '{configuration.name}' deleted by {user.username}."
    )
    return {}


@router.get("/configurations/{name}/actions", tags=["EDM Configurations"])
async def list_actions(
    name: str,
    user: User = Security(get_current_user, scopes=["edm_read"]),
) -> Any:
    """List all actions."""
    configuration = db_connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        raise HTTPException(400, f"EDM configuration with name {name} does not exist.")
    configuration = ConfigurationDB(**configuration)
    PluginClass = plugin_helper.find_by_id(configuration.plugin)  # NOSONAR
    if PluginClass is None:
        raise HTTPException(
            400, f"Plugin with id {configuration.plugin} does not exist."
        )
    plugin: PluginBase = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        configuration.storage,
        configuration.checkpoint,
        logger,
    )
    plugin.ssl_validation = configuration.sslValidation
    try:
        return plugin.get_actions()
    except Exception:
        logger.debug(
            "Error occurred while getting list of actions.",
            details=traceback.format_exc(),
            error_code="EDM_1014",
        )
        raise HTTPException(400, "Could not get action list. Check logs.")


@router.post("/configurations/{name}/fields", tags=["EDM Configurations"])
async def get_action_fields(
    action: ActionWithoutParams,
    name: str,
    user: User = Security(get_current_user, scopes=["edm_read"]),
) -> Any:
    """List all actions."""
    configuration = db_connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        raise HTTPException(400, f"EDM configuration with name {name} does not exist.")
    configuration = ConfigurationDB(**configuration)
    PluginClass = plugin_helper.find_by_id(configuration.plugin)  # NOSONAR
    if PluginClass is None:
        raise HTTPException(
            400, f"Plugin with id {configuration.plugin} does not exist."
        )
    plugin: PluginBase = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        configuration.storage,
        configuration.checkpoint,
        logger,
    )
    plugin.ssl_validation = configuration.sslValidation
    try:
        return plugin.get_action_fields(action)
    except Exception:
        logger.debug(
            "Error occurred while getting list of actions.",
            details=traceback.format_exc(),
            error_code="EDM_1015",
        )
        raise HTTPException(400, "Could not get action fields. Check logs.")


@router.post(
    "/get_dynamic_fields/{plugin_id}",
    tags=["Plugins Dynamic fields"],
    description="Get the dynamic fields from EDM plugin based on other fields.",
)
async def get_dynamic_fields(
    plugin_id: str,
    config_details: dict,
    user: User = Security(get_current_user, scopes=["edm_write"])
):
    """Get the dynamic fields from plugin."""
    return get_dynamic_fields_from_plugin(
        plugin_id, config_details
    )
