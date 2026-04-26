"""Provides configuration related endpoints."""

import traceback
from typing import List, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Security

from netskope.common.api.routers.auth import get_current_user
from netskope.common.celery.historical_alerts import historical_alerts
from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
    Scheduler,
    SecretDict,
    get_dynamic_fields_from_plugin,
)
from netskope.common.models import User, TenantDB, ActionType
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.utils.common_pull_scheduler import (
    schedule_or_delete_common_pull_tasks,
)
from netskope.integrations import trim_space_parameters_fields
from netskope.common.celery.scheduler import execute_celery_task

from ..models import (
    ConfigurationIn,
    ConfigurationUpdate,
    ConfigurationOut,
    ConfigurationDB,
    ConfigurationDelete,
)
from netskope.integrations.cte.models.business_rule import ActionWithoutParams
from ..plugin_base import PluginBase
from pymongo import ReturnDocument


router = APIRouter()
scheduler = Scheduler()
plugin_helper = PluginHelper()
logger = Logger()
db_connector = DBConnector()


def is_lastrun_checkpoint_valid(configuration: ConfigurationDB) -> bool:
    """Determine if the provided configuration has valid checkpoint set.

    Args:
        configuration (ConfigurationIn): Configuration to be validated.

    Returns:
        bool: Whether the checkpoint is valid or not.
    """
    current_time = datetime.now()
    if configuration.checkpoint is not None and current_time < configuration.checkpoint:
        return False
    else:
        return True


def is_reputation_valid(configuration: ConfigurationIn) -> bool:
    """Determine if the provided configuration has valid reputation set.

    Args:
        configuration (ConfigurationIn): Configuration to be validated.

    Returns:
        bool: Whether the reputation is valid or not.
    """
    if 0 <= configuration.reputation <= 10:
        return True
    else:
        return False


@router.get(
    "/plugins/configurations",
    response_model=List[ConfigurationOut],
    tags=["Plugins"],
    description="Get list of all the configurations.",
)
async def read_all_configurations_list(
    active: bool = None,
    user: User = Security(get_current_user, scopes=["cte_read"]),
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
    results = db_connector.collection(Collections.CONFIGURATIONS).find(find_dict)
    out = []
    netskope_configs = []
    for config_db in results:
        if "cte_write" not in user.scopes:
            config_db["parameters"] = {}
        metadata = plugin_helper.find_by_id(config_db["plugin"]).metadata
        config = ConfigurationOut(
            **config_db,
            pluginName=metadata.get("name"),
            pluginVersion=metadata.get("version"),
            pushSupported=metadata.get("push_supported", True),
        )
        if metadata.get("netskope", False):
            netskope_configs.append(config)
        else:
            out.append(config)
    return netskope_configs + sorted(out, key=lambda c: c.pluginName.lower())


@router.post(
    "/plugins/configurations/{plugin_id}",
    response_model=ConfigurationOut,
    tags=["Plugins"],
    status_code=201,
    description="Create a new configuration.",
)
async def create_configuration(
    plugin_id: str,
    configuration: ConfigurationIn,
    user: User = Security(get_current_user, scopes=["cte_write"]),
):
    """Create a new configuration.

    Args:
        plugin_id (str): ID of the plugin.
        configuration (ConfigurationIn): Configuration to be created.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        ConfigurationOut: Newly created configuration.
    """
    # make sure plugin exists
    PluginClass = plugin_helper.find_by_id(plugin_id)  # NOSONAR S117
    if PluginClass.metadata.get("netskope", False) and configuration.tenant is None:
        raise HTTPException(400, "Invalid tenant provided.")
    if PluginClass is None:
        raise HTTPException(400, f"Plugin with id='{plugin_id}' does not exist.")
    if not is_reputation_valid(configuration):
        raise HTTPException(400, "reputation must be between 0 and 10.")

    # to trim extra spaces for parameters fields.
    trim_space_parameters_fields(configuration.parameters)
    # initialize the storage to be passed.
    configuration_storage = {}

    # validate configuration
    plugin = PluginClass(
        configuration.name,
        configuration.parameters,
        configuration_storage,
        None,
        logger,
        ssl_validation=configuration.sslValidation,
    )
    try:
        if configuration.tenant:
            validation_result = plugin.validate(
                SecretDict(configuration.parameters), configuration.tenant
            )
        else:
            validation_result = plugin.validate(SecretDict(configuration.parameters))
    except Exception as e:
        raise HTTPException(400, str(e))
    if validation_result.success is False:
        raise HTTPException(
            400,
            f"One of the configuration parameter is invalid. "
            f"{validation_result.message}",
        )
    # insert new configuration
    config_db = None
    try:
        if configuration.tenant is not None:
            tenant = db_connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                {"name": configuration.tenant}
            )
            tenant = TenantDB(**tenant)

        provider_id = PluginClass.metadata.get("provider_id")
        config_db = ConfigurationDB(
            **(configuration.model_dump()),
            plugin=plugin_id,
            providerID=provider_id,
            storage=plugin.storage,
        )
        db_connector.collection(Collections.CONFIGURATIONS).insert_one(
            config_db.model_dump()
        )
    except Exception:
        logger.debug(
            "Error occurred while creating a new configuration.",
            details=traceback.format_exc(),
            error_code="CTE_1012",
        )
        raise HTTPException(500, "Error occurred while creating a new configuration.")

    # schedule on celery
    try:
        if PluginClass.metadata.get("push_supported", True):
            scheduler.schedule(
                name=configuration.name + "_share",
                task_name="cte.share_indicators",
                poll_interval=configuration.pollInterval,
                poll_interval_unit=configuration.pollIntervalUnit,
                args=[None, configuration.name],
                kwargs={"share_new_indicators": True},
            )

        if (
            configuration.tenant is not None
            and config_db.parameters.get("is_pull_required", "").lower() == "yes"
        ):
            schedule_or_delete_common_pull_tasks(configuration.tenant)

        if not PluginClass.metadata.get("netskope", False):
            scheduler.schedule(
                name=configuration.name,
                task_name="cte.execute_plugin",
                poll_interval=configuration.pollInterval,
                poll_interval_unit=configuration.pollIntervalUnit,
                args=[configuration.name],
            )
        else:
            name_tenant = tenant.name
            name_tenant = name_tenant.replace(" ", "_")
            execute_celery_task(
                historical_alerts.apply_async,
                "common.historical_alerts",
                args=[
                    configuration.tenant,
                    Collections.CONFIGURATIONS.value,
                    configuration.name,
                    datetime.now(),
                    plugin.get_types_to_pull("alerts"),
                ],
            )
    except Exception:
        logger.debug(
            "Error occurred while scheduling the configuration.",
            details=traceback.format_exc(),
            error_code="CTE_1013",
        )
        raise HTTPException(500, "Error occurred while scheduling the configuration.")
    logger.debug(
        f"Configuration '{configuration.name}' created for plugin '{plugin_id}'"
    )
    return {
        **config_db.model_dump(),
        "pluginName": PluginClass.metadata.get("name"),
        "pluginVersion": PluginClass.metadata.get("version"),
        "pushSupported": PluginClass.metadata.get("push_supported", True),
    }


def log_changes(configuration, updated_configuration):
    """Log changes based on the incoming request."""
    if configuration.active is not None:
        logger.debug(
            f"Configuration '{configuration.name}' is {'enabled' if configuration.active else 'disabled'}."
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
    if configuration.filters:
        logger.debug(
            f"Sharing filters updated for configuration '{configuration.name}'."
        )


def filter_out_none_values(data: dict) -> dict:
    """Filter out keys with None values from a dict.

    Args:
        data (dict): Dictionary to be filtered.

    Returns:
        dict: Filtered dictionary.
    """
    return {k: v for k, v in data.items() if v is not None}


@router.patch(
    "/plugins/configurations",
    response_model=ConfigurationOut,
    tags=["Plugins"],
    description="Update an existing configuration.",
)
async def update_configuration(
    configuration: ConfigurationUpdate,
    user: User = Security(get_current_user, scopes=["cte_write"]),
):
    """Update existing configuration.

    Args:
        configuration (ConfigurationUpdate): Updated configuration.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        ConfigurationOut: The newly updated configuration.
    """
    # to trim extra spaces for parameters fields.
    trim_space_parameters_fields(configuration.parameters)
    update_payload = filter_out_none_values(configuration.model_dump())
    configuration_db_dict = db_connector.collection(
        Collections.CONFIGURATIONS
    ).find_one({"name": configuration.name})

    # merge existing and updated fields
    updated_configuration_dict = {
        **configuration_db_dict,
        **update_payload,
    }

    updated_configuration = ConfigurationDB(**updated_configuration_dict)

    if not is_lastrun_checkpoint_valid(updated_configuration):
        raise HTTPException(400, "Invalid last run checkpoint.")
    if not is_reputation_valid(updated_configuration):
        raise HTTPException(400, "reputation must be between 0 and 10.")
    # make sure plugin exists
    PluginClass = plugin_helper.find_by_id(updated_configuration.plugin)  # NOSONAR S117
    if PluginClass is None:
        raise HTTPException(
            400,
            f"Plugin with id='{updated_configuration.plugin}' does not exist.",
        )
    plugin = PluginClass(
        updated_configuration.name,
        SecretDict(updated_configuration.parameters),
        updated_configuration.storage,
        None,
        logger,
        ssl_validation=updated_configuration.sslValidation,
    )

    # validate configuration if active
    if updated_configuration.active is True:
        try:
            if updated_configuration.tenant:
                validation_result = plugin.validate(
                    SecretDict(updated_configuration.parameters),
                    updated_configuration.tenant,
                )
            else:
                validation_result = plugin.validate(
                    SecretDict(updated_configuration.parameters)
                )
        except Exception as e:
            raise HTTPException(400, str(e))
        if validation_result.success is False:
            raise HTTPException(
                400,
                f"One of the configuration parameter is invalid. "
                f"{validation_result.message}",
            )

    # if marked inactive, remember the inactivation time
    if (
        updated_configuration.active is False
        and configuration_db_dict.get("active") is True
    ):
        update_payload["disabledAt"] = datetime.now()
        #  Call plugin cleanup method with disable action type.
        try:
            plugin.cleanup(action_type=ActionType.DISABLE.value)
        except NotImplementedError:
            logger.debug(
                f"Plugin {configuration.name} does not implement cleanup method, "
                "skipping clean up while disabling configuration."
            )
    update_payload["storage"] = plugin.storage
    updated_config_from_db = db_connector.collection(
        Collections.CONFIGURATIONS
    ).find_one_and_update(
        {"_id": configuration_db_dict["_id"]},
        {"$set": update_payload},
        return_document=ReturnDocument.AFTER,
    )
    if not updated_config_from_db:
        raise HTTPException(500, "Error occurred while updating the configuration.")

    updated_configuration = ConfigurationDB(**updated_config_from_db)
    if updated_configuration.active is False:
        # remove schedule if marked inactive
        scheduler.delete(configuration.name)
        scheduler.delete(configuration.name + "_share")
    else:
        # if not upsert the schedule in case it was inactive or pollInterval
        # has been changed
        if PluginClass.metadata.get("push_supported", True):
            scheduler.upsert(
                name=updated_configuration.name + "_share",
                task_name="cte.share_indicators",
                poll_interval=updated_configuration.pollInterval,
                poll_interval_unit=updated_configuration.pollIntervalUnit,
                args=[None, configuration.name],
                kwargs={"share_new_indicators": True},
            )
        if not PluginClass.metadata.get("netskope", False):
            scheduler.upsert(
                name=updated_configuration.name,
                task_name="cte.execute_plugin",
                poll_interval=updated_configuration.pollInterval,
                poll_interval_unit=updated_configuration.pollIntervalUnit,
                args=[updated_configuration.name],
            )
    if updated_configuration.tenant is not None:
        schedule_or_delete_common_pull_tasks(updated_configuration.tenant)
    existing_configuration = ConfigurationDB(**configuration_db_dict)
    log_changes(existing_configuration, updated_configuration)

    return {
        **updated_configuration.model_dump(),
        "pluginName": plugin_helper.find_by_id(
            updated_configuration.plugin
        ).metadata.get("name"),
    }


@router.delete(
    "/configuration",
    description="Delete an existing configuration.",
    tags=["Plugins"],
)
async def delete_configuration(
    configuration: ConfigurationDelete,
    user: User = Security(get_current_user, scopes=["cte_write"]),
):
    """Delete a configuration."""
    configuration_db = db_connector.collection(Collections.CONFIGURATIONS).find_one(
        {"name": configuration.name}
    )
    configuration_db = ConfigurationDB(**configuration_db)
    if not configuration.keepData:
        db_connector.collection(Collections.INDICATORS).update_many(
            {"sources": {"$elemMatch": {"source": configuration.name}}},
            {"$pull": {"sources": {"source": configuration.name}}},
        )
        db_connector.collection(Collections.INDICATORS).delete_many(
            {"sources": {"$exists": True, "$eq": []}},
        )
        logger.debug(
            f"Indicators with source '{configuration.name}' deleted by {user.username}."
        )
    PluginClass = plugin_helper.find_by_id(configuration_db.plugin)
    #  Call plugin cleanup method with disable action type.
    if PluginClass is not None:
        plugin = PluginClass(
            configuration_db.name,
            SecretDict(configuration_db.parameters),
            configuration_db.storage,
            None,
            logger,
            ssl_validation=configuration_db.sslValidation,
        )
        try:
            plugin.cleanup(action_type=ActionType.DELETE.value)
        except NotImplementedError:
            logger.debug(
                f"Plugin {configuration.name} does not implement cleanup method, "
                "skipping clean up while deleting configuration."
            )
    if PluginClass.metadata.get("netskope", False):
        plugin_provider_helper = PluginProviderHelper()
        plugin_provider_helper.check_and_update_forbidden_endpoints(
            configuration_db.tenant
        )
    db_connector.collection(Collections.CONFIGURATIONS).delete_one(
        {"name": configuration.name}
    )
    db_connector.collection(Collections.CTE_BUSINESS_RULES).update_many(
        {}, {"$unset": {f"sharedWith.{configuration.name}": ""}}
    )
    for brule in db_connector.collection(Collections.CTE_BUSINESS_RULES).find({}):
        sharedWith = brule.get("sharedWith", {})
        for destination in sharedWith.values():
            if configuration.name in destination.keys():
                del destination[configuration.name]
        db_connector.collection(Collections.CTE_BUSINESS_RULES).update_one(
            {"name": brule["name"]}, {"$set": {"sharedWith": sharedWith}}
        )
    scheduler.delete(configuration.name)
    scheduler.delete(configuration.name + "_share")
    if configuration_db.tenant is not None:
        schedule_or_delete_common_pull_tasks(configuration_db.tenant)
    logger.debug(
        f"Configuration with name '{configuration.name}' deleted by {user.username}."
    )
    return {}


@router.get("/configurations/{name}/actions", tags=["Plugins"])
async def list_actions(
    name: str,
    user: User = Security(get_current_user, scopes=["cte_read"]),
) -> Any:
    """List all actions."""
    configuration = db_connector.collection(Collections.CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        raise HTTPException(400, f"CTE configuration with name {name} does not exist.")
    configuration = ConfigurationDB(**configuration)
    PluginClass = plugin_helper.find_by_id(configuration.plugin)  # NOSONAR
    if PluginClass is None:
        raise HTTPException(
            400, f"Plugin with id {configuration.plugin} does not exist."
        )
    plugin: PluginBase = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        {},
        None,
        logger,
        ssl_validation=configuration.sslValidation,
    )
    try:
        return plugin.get_actions()
    except Exception:
        logger.info(
            "Error occurred while getting list of actions.",
            details=traceback.format_exc(),
            error_code="CTE_1014",
        )
        raise HTTPException(400, "Could not get action list. Check logs.")


@router.post("/configurations/{name}/fields", tags=["Plugins"])
async def get_action_fields(
    action: ActionWithoutParams,
    name: str,
    user: User = Security(get_current_user, scopes=["cte_read"]),
) -> Any:
    """List all actions."""
    configuration = db_connector.collection(Collections.CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        raise HTTPException(400, f"CTE configuration with name {name} does not exist.")
    configuration = ConfigurationDB(**configuration)
    PluginClass = plugin_helper.find_by_id(configuration.plugin)  # NOSONAR
    if PluginClass is None:
        raise HTTPException(
            400, f"Plugin with id {configuration.plugin} does not exist."
        )
    plugin: PluginBase = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        {},
        configuration.checkpoint,
        logger,
        ssl_validation=configuration.sslValidation,
    )
    try:
        return plugin.get_action_fields(action)
    except Exception:
        logger.info(
            "Error occurred while getting list of actions.",
            details=traceback.format_exc(),
            error_code="CTE_1025",
        )
        raise HTTPException(400, "Could not get action fields. Check logs.")


@router.post(
    "/get_dynamic_fields/{plugin_id}",
    tags=["Plugins Dynamic fields"],
    description="Get the dynamic fields from plugin based on other fields.",
)
async def get_dynamic_fields(
    plugin_id: str,
    config_details: dict,
    user: User = Security(get_current_user, scopes=["cte_write"]),
):
    """Get the dynamic fields from plugin."""
    return get_dynamic_fields_from_plugin(plugin_id, config_details)
