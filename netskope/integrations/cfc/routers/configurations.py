"""Provides configuration related endpoints."""

import traceback
from datetime import datetime, UTC
from typing import Any, List, Union

from fastapi import APIRouter, HTTPException, Path, Security, Body

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User, TenantDB
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    Scheduler,
    SecretDict,
    get_dynamic_fields_from_plugin,
)
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.integrations import trim_space_parameters_fields

from ..models import (
    ActionWithoutParams,
    ConfigurationDB,
    ConfigurationDelete,
    ConfigurationIn,
    ConfigurationNameValidationIn,
    ConfigurationOut,
    ConfigurationUpdate,
    ConfigurationValidationIn,
)
from ..plugin_base import PluginBase, ValidationResult
from pymongo import ReturnDocument

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


@router.get(
    "/configurations",
    response_model=List[ConfigurationOut],
    tags=["CFC Configurations"],
    description="Get list of all the configurations.",
)
async def read_all_configurations_list(
    active: bool = None,
    user: User = Security(get_current_user, scopes=["cfc_read"]),
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
    results = db_connector.collection(Collections.CFC_CONFIGURATIONS).find(find_dict)
    out = []
    netskope_configs = []
    for config_db in results:
        if "cfc_write" not in user.scopes:
            config_db["parameters"] = {}
        metadata = plugin_helper.find_by_id(config_db["plugin"]).metadata
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


def get_plugin_instance(
    plugin_id: str,
    configuration: ConfigurationValidationIn,
    configuration_db: ConfigurationDB = None,
) -> PluginBase:
    """
    Retrieve an instance of a plugin based on the provided plugin ID and configuration.

    Args:
        plugin_id (str): The ID of the plugin to retrieve.
        configuration (ConfigurationValidationIn): The configuration for the plugin.

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


def _validate_configuration_step(step, configuration, configuration_db=None):
    """Validate individual steps of a configuration."""
    plugin = get_plugin_instance(
        configuration.plugin, configuration, configuration_db=configuration_db
    )
    try:
        return plugin.validate_step(step)
    except Exception as e:
        logger.error(
            f"Exception occurred while executing validate for step {step}",
            details=traceback.format_exc(),
            error_code="CFC_1058",
        )
        raise HTTPException(400, str(e))


@router.post("/configuration/validate/{step}", tags=["CFC Configurations"])
async def validate_configuration_step(
    step: str = Path(...),
    configuration: ConfigurationIn = Body(...),
    user: User = Security(get_current_user, scopes=["cfc_write"]),
) -> Any:
    """Validate a configuration step."""
    return _validate_configuration_step(step, configuration)


@router.patch("/configuration/validate/{step}", tags=["CFC Configurations"])
async def validate_patch_configuration_step(
    step: str = Path(...),
    configuration: ConfigurationUpdate = Body(...),
    user: User = Security(get_current_user, scopes=["cfc_write"]),
) -> Any:
    """Validate a configuration step."""
    config_db = db_connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
        {"name": configuration.name}
    )
    if config_db is None:
        raise HTTPException(404, f"Configuration '{configuration.name}' not found.")
    configuration_db = ConfigurationDB(**config_db)
    return _validate_configuration_step(step, configuration, configuration_db)


def _get_dynamic_step_fields(
    plugin_id: str,
    step_name: str,
    configuration: Union[ConfigurationIn, ConfigurationUpdate],
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
    plugin = PluginClass(
        configuration.name, SecretDict(configuration.parameters), {}, None, logger
    )
    try:
        return plugin.get_fields(step_name, SecretDict(configuration.parameters))
    except NotImplementedError as error:
        logger.error(
            message=str(error), details=traceback.format_exc(), error_code="CFC_1003"
        )
        raise HTTPException(400, str(error))
    except Exception as error:
        logger.error(
            message=str(error), details=traceback.format_exc(), error_code="CFC_1004"
        )
        raise HTTPException(
            500,
            f"Error occurred while retrieving dynamic step fields for '{step_name}'.",
        )


@router.post(
    "/configuration/{plugin_id}/step/{name}",
    response_model=List,
    status_code=200,
    tags=["CFC Configurations"],
    description="Retrieve configuration fields",
)
async def get_configuration_step_post(
    configuration: ConfigurationIn,
    plugin_id: str = Path(...),
    name: str = Path(...),
    _: User = Security(get_current_user, scopes=["cfc_write"]),
) -> List:
    """
    Retrieve the configuration fields for specified step.

    Args:
        configuration (ConfigurationIn): The configuration input.
        plugin_id (str): The ID of the plugin.
        name (str): The name of the step.
        _: User: The user security object.

    Returns:
        List: The list of dynamic step fields.
    """
    return _get_dynamic_step_fields(plugin_id, name, configuration)


@router.patch(
    "/configuration/{plugin_id}/step/{name}",
    response_model=List,
    status_code=200,
    tags=["CFC Configurations"],
    description="Retrieve configuration fields",
)
async def get_configuration_step_patch(
    configuration: ConfigurationUpdate,
    plugin_id: str = Path(...),
    name: str = Path(...),
    _: User = Security(get_current_user, scopes=["cfc_write"]),
) -> List:
    """
    Retrieve the configuration fields for specified step.

    Args:
        - configuration (ConfigurationUpdate): The updated configuration data.
        - plugin_id (str): The ID of the plugin.
        - name (str): The name of the configuration step.
        - _: User: The user security object.

    Returns:
        - List: A list of dynamic step fields for the given plugin and step.
    """
    return _get_dynamic_step_fields(plugin_id, name, configuration)


@router.post(
    "/configuration/validate_configuration_name",
    response_model=ValidationResult,
    status_code=200,
    tags=["CFC Configurations"],
    description="Validate plugin configuration name",
)
async def validate_plugin_configuration_name(
    _: ConfigurationNameValidationIn,
    user: User = Security(get_current_user, scopes=["cfc_write"]),
) -> ValidationResult:
    """
    Validate plugin configuration name.

    Args:
        _: ConfigurationNameValidationIn: The configuration name validation input.
        user (User): The user making the request.

    Returns:
        ValidationResult: A ValidationResult object representing the result of the validation.
    """
    return ValidationResult(success=True, message="Validation successful.")


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
                error_code="CFC_1059",
            )
            raise HTTPException(400, str(e))
        if not result.success:
            raise HTTPException(400, result.message)


@router.post(
    "/configuration/{plugin_id}",
    response_model=ConfigurationOut,
    tags=["CFC Configurations"],
    status_code=201,
    description="Create a new configuration.",
)
async def create_configuration(
    configuration: ConfigurationIn,
    plugin_id: str,
    user: User = Security(get_current_user, scopes=["cfc_write"]),
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
    PluginClass = plugin_helper.find_by_id(plugin_id)  # NOSONAR S117
    if PluginClass is None:
        raise HTTPException(400, f"Plugin with id '{plugin_id}' does not exist.")
    trim_space_parameters_fields(configuration.parameters)
    plugin: PluginBase = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        {},
        None,
        logger,
    )

    plugin.ssl_validation = configuration.sslValidation
    if not PluginClass.metadata.get("netskope", False):
        _validate_entire_configuration(plugin, configuration)
    else:
        if not configuration.tenant:
            raise HTTPException(400, "Tenant can not be empty.")
        tenant = TenantDB(
            **db_connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                {"name": configuration.tenant}
            )
        )
        result = plugin.validate_configuration_parameters(tenant)
        if not result.success:
            raise HTTPException(400, result.message)

    # insert new configuration
    config_db = None
    try:
        config_db = ConfigurationDB(
            **(configuration.model_dump()),
            createdBy=user.username,
            createdAt=datetime.now(UTC),
            storage=plugin.storage,
        )
        db_connector.collection(Collections.CFC_CONFIGURATIONS).insert_one(
            config_db.model_dump()
        )
    except Exception as error:
        logger.debug(
            "Error occurred while creating a new configuration.",
            details=traceback.format_exc(),
            error_code="CFC_1001",
        )
        raise HTTPException(
            500, "Error occurred while creating a new configuration."
        ) from error

    # schedule on celery
    try:
        if not PluginClass.metadata.get("netskope", False):
            scheduler.schedule(
                name=configuration.name,
                task_name="cfc.execute_plugin",
                poll_interval=configuration.pollInterval,
                poll_interval_unit=configuration.pollIntervalUnit,
                args=[configuration.name],
            )
    except Exception as error:
        logger.debug(
            "Error occurred while scheduling the configuration.",
            details=traceback.format_exc(),
            error_code="CFC_1002",
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


@router.patch(
    "/configuration",
    response_model=ConfigurationOut,
    tags=["CFC Configurations"],
    description="Update an existing configuration.",
)
async def update_configuration(
    configuration: ConfigurationUpdate,
    user: User = Security(get_current_user, scopes=["cfc_write"]),
):
    """Update existing configuration.

    Args:
        configuration (ConfigurationUpdate): Updated configuration.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        ConfigurationOut: The newly updated configuration.
    """
    request_configuration = configuration
    # to trim extra spaces for parameters fields.
    trim_space_parameters_fields(configuration.parameters)
    update_payload = filter_out_none_values(configuration.model_dump())
    existing_configuration_dict = db_connector.collection(
        Collections.CFC_CONFIGURATIONS
    ).find_one({"name": configuration.name})

    merged_dict = {**existing_configuration_dict, **update_payload}
    configuration_to_validate = ConfigurationDB(**merged_dict)
    existing_configuration = ConfigurationDB(**existing_configuration_dict)

    # make sure plugin exists
    PluginClass = plugin_helper.find_by_id(configuration_to_validate.plugin)
    if PluginClass is None:
        raise HTTPException(
            400,
            f"Plugin with id '{configuration_to_validate.plugin}' does not exist.",
        )

    # validate configuration if active
    plugin = PluginClass(
        configuration_to_validate.name,
        SecretDict(configuration_to_validate.parameters),
        configuration_to_validate.storage,
        configuration_to_validate.checkpoint,
        logger,
    )
    plugin.ssl_validation = configuration_to_validate.sslValidation
    if configuration_to_validate.active is True and not PluginClass.metadata.get(
        "netskope"
    ):
        _validate_entire_configuration(plugin, configuration_to_validate)
    elif configuration_to_validate.active is True and PluginClass.metadata.get(
        "netskope"
    ):
        if not configuration_to_validate.tenant:
            raise HTTPException(400, "Tenant can not be empty.")
        tenant = TenantDB(
            **db_connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                {"name": configuration_to_validate.tenant}
            )
        )
        result = plugin.validate_configuration_parameters(tenant)
        if not result.success:
            raise HTTPException(400, result.message)

    update_payload["lastUpdatedBy"] = user.username
    update_payload["lastUpdatedAt"] = datetime.now(UTC)
    update_payload["storage"] = plugin.storage

    if (
        existing_configuration.active is True
        and configuration_to_validate.active is False
    ):
        update_payload["disabledAt"] = datetime.now(UTC)

    updated_config_from_db = db_connector.collection(
        Collections.CFC_CONFIGURATIONS
    ).find_one_and_update(
        {"name": configuration.name},
        {"$set": update_payload},
        return_document=ReturnDocument.AFTER,
    )
    if not updated_config_from_db:
        raise HTTPException(500, "Error occurred while updating the configuration.")

    configuration = ConfigurationDB(**updated_config_from_db)
    if configuration.active is False:
        # remove schedule if marked inactive
        scheduler.delete(configuration.name)
    else:
        # if not upsert the schedule in case it was inactive or pollInterval
        # has been changed
        if not PluginClass.metadata.get("netskope", False):
            scheduler.upsert(
                name=configuration.name,
                task_name="cfc.execute_plugin",
                poll_interval=configuration.pollInterval,
                poll_interval_unit=configuration.pollIntervalUnit,
                args=[configuration.name],
            )
    log_changes(request_configuration, configuration)
    metadata = plugin_helper.find_by_id(configuration.plugin).metadata

    return {
        **configuration.model_dump(),
        "pluginName": metadata.get("name"),
        "pluginVersion": metadata.get("version"),
        "netskope": metadata.get("netskope", False),
        "pushSupported": metadata.get("push_supported", False),
        "pullSupported": metadata.get("pull_supported", False),
    }


@router.delete(
    "/configuration",
    description="Delete an existing configuration.",
    tags=["CFC Configurations"],
)
async def delete_configuration(
    configuration: ConfigurationDelete,
    user: User = Security(get_current_user, scopes=["cfc_write"]),
):
    """Delete configuration.

    Args:
        configuration (ConfigurationDelete): Delete configuration.

    Returns:
        dict: indicated configuration has been deleted
    """
    """Delete a configuration."""
    configuration_db = db_connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
        {"name": configuration.name}
    )
    db_connector.collection(Collections.CFC_CONFIGURATIONS).delete_one(
        {"name": configuration.name}
    )
    db_connector.collection(Collections.CFC_SHARING).delete_many(
        {
            "$or": [
                {"sourceConfiguration": configuration.name},
                {"destinationConfiguration": configuration.name},
            ]
        }
    )
    # Add logic to make image metadata outdated if requested
    scheduler.delete(configuration.name)
    if not configuration.keepData:
        db_connector.collection(Collections.CFC_IMAGES_METADATA).delete_many(
            {"sourcePluginID": str(configuration_db["_id"])}
        )
        logger.debug(
            "All the image data entries for the plugin configuration: "
            f" '{configuration.name}' are deleted."
        )
    else:
        db_connector.collection(Collections.CFC_IMAGES_METADATA).update_many(
            {"sourcePluginID": str(configuration_db["_id"])},
            {"$set": {"outdated": True}},
        )
        logger.debug(
            "All the image data entries for the plugin configuration: "
            f" '{configuration.name}' are marked as outdated."
        )
    logger.debug(
        f"Configuration with name '{configuration.name}' deleted by {user.username}."
    )
    return {}


@router.get("/configurations/{name}/actions", tags=["CFC Configurations"])
async def list_actions(
    name: str,
    _: User = Security(get_current_user, scopes=["cfc_read"]),
) -> Any:
    """List all actions.

    Args:
        name (str): Configuration name.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        Any: List of supported actions for a plugin.
    """
    configuration = db_connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        raise HTTPException(400, f"CFC configuration with name {name} does not exist.")
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
            error_code="CFC_1009",
        )
        raise HTTPException(400, "Could not get action list. Check logs.")


@router.post("/configurations/{name}/fields", tags=["CFC Configurations"])
async def get_action_fields(
    action: ActionWithoutParams,
    name: str,
    _: User = Security(get_current_user, scopes=["cfc_read"]),
) -> Any:
    """List all action fields.

    Args:
        action (ActionWithoutParams): Action for which fields are requested.
        name (str): Configuration name

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        Any: List of supported action fields for a plugin action.
    """
    configuration = db_connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        raise HTTPException(400, f"CFC configuration with name {name} does not exist.")
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
        fields = plugin.get_action_fields(action)
        db_connector.collection(Collections.CFC_CONFIGURATIONS).update_one(
            {"name": name}, {"$set": {"storage": plugin.storage}}
        )
        return fields
    except Exception:
        logger.debug(
            "Error occurred while getting list of actions fields.",
            details=traceback.format_exc(),
            error_code="CFC_1010",
        )
        raise HTTPException(400, "Could not get action fields. Check logs.")


@router.post(
    "/get_dynamic_fields/{plugin_id}",
    tags=["Plugins Dynamic fields"],
    description="Get the dynamic fields from CFC plugin based on other fields.",
)
async def get_dynamic_fields(
    plugin_id: str,
    config_details: dict,
    user: User = Security(get_current_user, scopes=["cfc_write"]),
):
    """Get the dynamic fields from plugin."""
    return get_dynamic_fields_from_plugin(plugin_id, config_details)
