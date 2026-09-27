"""Configuration related endpoints."""
import json
from datetime import datetime, timedelta
from typing import List, Any
from fastapi import APIRouter, HTTPException, Path, Body, Security
import traceback
from netskope.common.utils import (
    PluginHelper,
    Logger,
    DBConnector,
    Collections,
    Scheduler,
    SecretDict,
    get_dynamic_fields_from_plugin,
)
from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User, TenantDB, ActionType
from netskope.common.celery.historical_alerts import historical_alerts
from netskope.integrations import trim_space_parameters_fields
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.utils.common_pull_scheduler import (
    schedule_or_delete_common_pull_tasks,
)

from ..models import (
    ConfigurationIn,
    ConfigurationOut,
    ConfigurationDelete,
    ConfigurationUpdate,
    ConfigurationDB,
    Filters,
)
from ..plugin_base import ValidationResult
from ..tasks.pull_data_items import pull_historical_events
from .custom_fields import sync_plugin_defaults_to_custom_fields
from pymongo import ReturnDocument


router = APIRouter()
logger = Logger()
connector = DBConnector()
scheduler = Scheduler()
helper = PluginHelper()


def _filter_out_none_values(data: dict) -> dict:
    """Filter out keys with None values from a dict.

    Args:
        data (dict): Dictionary to be filtered.

    Returns:
        dict: Filtered dictionary.
    """
    return {k: v for k, v in data.items() if v is not None}


def _get_task_name(name: str) -> str:
    """Get task name from any string.

    Args:
        name (str): String.

    Returns:
        str: A string that can be used as task name.
    """
    return f"itsm.{name}"


def _get_sharing_task_name(name: str) -> str:
    """Get sharing task name from any string.

    Args:
        name (str): String.

    Returns:
        str: A string that can be used as sharing task name.
    """
    return _get_task_name(name) + "_sharing"


def _validate_duplicate_mappings(parameters: dict):
    """Validate that there are no duplicate mappings in the configuration."""
    mapping_config = parameters.get("mapping_config", {})
    if not mapping_config:
        return

    for section_name, section_data in mapping_config.items():
        mappings_array = section_data.get("mappings", [])
        if isinstance(mappings_array, list):
            seen_mappings = set()
            for mapping_dict in mappings_array:
                entry = frozenset(mapping_dict.items())
                if entry in seen_mappings:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Duplicate mapping found in the '{section_name}'"
                            " section. Please remove it before saving."
                        ),
                    )
                seen_mappings.add(entry)


@router.get("/configuration", tags=["CTO Configurations"])
async def get_configuration_list(
    user: User = Security(get_current_user, scopes=["cto_read"]),
) -> List[ConfigurationOut]:
    """Get list of configurations."""
    out = []
    netskope_configs = []
    for configuration in connector.collection(Collections.ITSM_CONFIGURATIONS).find({}):
        if "cto_write" not in user.scopes:
            configuration["parameters"] = {}

        metadata = helper.find_by_id(configuration["plugin"]).metadata
        config = ConfigurationOut(
            **configuration,
            receivingSupported=metadata.get("receiving_supported", False),
            sharingSupported=metadata.get("sharing_supported", False),
            pluginName=metadata.get("name"),
            netskope=metadata.get("netskope", False),
        )
        if metadata.get("netskope", False):
            if config_filters := config.parameters.get("params", {}).get("filters", {}):
                if config_filters:
                    try:
                        config.parameters["params"]["filters"]["isValid"] = Filters(
                            query=config_filters.get("query", ""),
                            mongo=json.dumps(config_filters.get("mongo", {}) or {}),
                        ).isValid
                    except Exception as ex:
                        logger.info(
                            f"Failed to validate filters for configuration {config.name}: {ex}"
                        )
                        config.parameters["params"]["filters"] = {
                            "query": "",
                            "mongo": {},
                            "isValid": False,
                        }
            netskope_configs.append(config)
        else:
            out.append(config)
    return netskope_configs + sorted(out, key=lambda c: c.pluginName.lower())


@router.get("/configuration/queues", tags=["CTO Configurations"])
async def get_configuration_queues(
    name: str,
    user: User = Security(get_current_user, scopes=["cto_read"]),
) -> Any:
    """Get list of queues for a configuration."""
    configuration = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        logger.error(
            f"Could not find a configuration with name {name}. Skipping get_queues task.",
            error_code="CTO_1001",
        )
        return []
    configuration = ConfigurationDB(**configuration)
    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
    plugin = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        configuration.storage or {},  # No need to update storage as it get call
        None,
        logger,
    )
    try:
        return plugin.get_queues()
    except NotImplementedError:
        logger.error(
            f"Plugin {configuration.plugin} does not implement the get_queues method.",
            details=traceback.format_exc(),
            error_code="CTO_1002",
        )
        return []
    except Exception:
        logger.error(
            f"Error occurred while fetching queues for configuration {configuration.name}.",
            details=traceback.format_exc(),
            error_code="CTO_1003",
        )
        raise HTTPException(400, "Error occurred. Check logs.")


def _get_available_mapping_fields(name: str):
    """Get list of available mapping fields."""
    configuration = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        raise HTTPException(404, "Configuration not found.")
    configuration = ConfigurationDB(**configuration)
    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
    plugin = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        configuration.storage or {},  # No need to update storage as it get call
        None,
        logger,
    )
    try:
        return plugin.get_available_fields(SecretDict(configuration.parameters))
    except NotImplementedError:
        return []
    except Exception:
        logger.error(
            "Error occurred while getting available fields.",
            details=traceback.format_exc(),
            error_code="CTO_1004",
        )
        raise HTTPException(400, "Error occurred. Check logs.")


@router.post("/configuration/{name}/fields", tags=["CTO Configurations"])
async def get_available_mapping_fields(
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["cto_read"]),
) -> Any:
    """Get list of all the available configuration fields."""
    return _get_available_mapping_fields(name)


@router.patch("/configuration/{name}/fields", tags=["CTO Configurations"])
async def get_available_mapping_fields_patch(
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["cto_read"]),
) -> Any:
    """Get list of all the available configuration fields."""
    return _get_available_mapping_fields(name)


# Default mappings


@router.post("/configuration/{name}/mappings", tags=["CTO Configurations"])
async def get_default_mappings(
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["cto_read"]),
) -> Any:
    """Get list of all the available default mappings."""
    configuration = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        raise HTTPException(404, "Configuration not found.")
    configuration = ConfigurationDB(**configuration)
    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
    plugin = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        configuration.storage or {},  # No need to update storage as it get call
        None,
        logger,
    )
    try:
        return plugin.get_default_mappings(SecretDict(configuration.parameters))
    except NotImplementedError:
        return []
    except Exception:
        logger.error(
            "Error occurred while getting default mapping.",
            details=traceback.format_exc(),
            error_code="CTO_1005",
        )
        raise HTTPException(400, "Error occurred. Check logs.")


@router.post("/configuration/validate/", tags=["CTO Configurations"])
async def validate_configuration(
    configuration: ConfigurationIn = Body(...),
    user: User = Security(get_current_user, scopes=["cto_write"]),
) -> ValidationResult:
    """Validate a configuration without saving."""
    return ValidationResult(success=True, message="Validation successful.")


@router.patch("/configuration/validate/", tags=["CTO Configurations"])
async def validate_patch_configuration(
    configuration: ConfigurationUpdate = Body(...),
    user: User = Security(get_current_user, scopes=["cto_write"]),
) -> ValidationResult:
    """Validate a configuration without saving."""
    return ValidationResult(success=True, message="Validation successful.")


def _validate_configuration_step(step, configuration):
    """Validate individual steps of a configuration."""
    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
    plugin = PluginClass(
        configuration.name,
        None,
        {},
        None,
        logger,
    )
    try:
        configuration_dict = {
            **configuration.parameters,
            "tenant": configuration.tenant,
        }
        return plugin.validate_step(
            step,
            SecretDict(configuration_dict),
        )
    except Exception as e:
        logger.error(
            f"Exception occurred while executing validate for step {step}",
            details=traceback.format_exc(),
            error_code="CTO_1006",
        )
        raise HTTPException(400, str(e))


@router.post("/configuration/validate/{step}", tags=["CTO Configurations"])
async def validate_configuration_step(
    step: str = Path(...),
    configuration: ConfigurationIn = Body(...),
    user: User = Security(get_current_user, scopes=["cto_write"]),
) -> Any:
    """Validate a configuration step."""
    return _validate_configuration_step(step, configuration)


@router.patch("/configuration/validate/{step}", tags=["CTO Configurations"])
async def validate_patch_configuration_step(
    step: str = Path(...),
    configuration: ConfigurationUpdate = Body(...),
    user: User = Security(get_current_user, scopes=["cto_write"]),
) -> Any:
    """Validate a configuration step."""
    return _validate_configuration_step(step, configuration)


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
    configuration_dict = {
        **configuration.parameters,
        "tenant": configuration.tenant,
    }
    for step in configuration.parameters:
        try:
            result = plugin.validate_step(
                step,
                SecretDict(configuration_dict),
            )
        except Exception as e:
            logger.error(
                f"Exception occurred while executing validate for step {step}",
                details=traceback.format_exc(),
                error_code="CTO_1022",
            )
            raise HTTPException(400, str(e))
        if not result.success:
            raise HTTPException(400, result.message)


@router.post("/configuration", tags=["CTO Configurations"])
async def create_configuration(
    configuration: ConfigurationIn,
    user: User = Security(get_current_user, scopes=["cto_write"]),
) -> Any:
    """Create a new configuration."""
    try:
        _validate_duplicate_mappings(configuration.parameters)
        PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
        plugin = PluginClass(
            configuration.name,
            SecretDict(configuration.parameters),
            {},
            None,
            logger,
        )

        trim_space_parameters_fields(configuration.parameters)
        _validate_entire_configuration(plugin, configuration)
    except HTTPException as error:
        raise error
    except Exception:
        logger.error(
            f"Exception occurred while validating configuration {configuration.name}",
            details=traceback.format_exc(),
            error_code="CTO_1039",
        )
        raise HTTPException(
            400, f"Error occurred. validating configuration {configuration.name}."
        )
    try:
        # all steps were successfully valiadted
        tenant_poll_interval = {}
        configuration.storage = plugin.storage
        if configuration.tenant is not None:
            tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                {"name": configuration.tenant}
            )
            tenant = TenantDB(**tenant)
            tenant_poll_interval["interval"] = tenant.pollInterval
            tenant_poll_interval["unit"] = tenant.pollIntervalUnit
        connector.collection(Collections.ITSM_CONFIGURATIONS).insert_one(
            ConfigurationDB(**configuration.model_dump()).model_dump()
        )
        sync_plugin_defaults_to_custom_fields(configuration.plugin)
    except Exception:
        logger.error(
            f"Exception occurred while creating configuration {configuration.name}",
            details=traceback.format_exc(),
            error_code="CTO_1040",
        )
        raise HTTPException(
            400,
            f"Error occurred. While creating the configuration {configuration.name}.",
        )
    try:
        if configuration.tenant is not None:
            schedule_or_delete_common_pull_tasks(configuration.tenant)

        tasks = []
        if plugin.metadata.get("pulling_supported") and not PluginClass.metadata.get("netskope", False):
            tasks.append("itsm.pull_data_items")
        if plugin.metadata.get("receiving_supported"):
            tasks.append("itsm.sync_states")
        for task in tasks:
            scheduler.schedule(
                name=_get_task_name(configuration.name),
                task_name=task,
                poll_interval=configuration.pollInterval,
                poll_interval_unit=configuration.pollIntervalUnit,
                args=[configuration.name],
            )
        if PluginClass.metadata.get("netskope", False):
            name_tenant = tenant.name
            name_tenant = name_tenant.replace(" ", "_")
            if not plugin.get_types_to_pull("alerts"):
                logger.info(
                    f"Historical data pull for alerts has been skipped for '{configuration.name}' plugin,"
                    f" because no alerts are selected in the configuration."
                )
            else:
                end_time = datetime.now()
                start_time = end_time - timedelta(
                    days=configuration.parameters.get("params", {})["days"]
                )
                if start_time == end_time:
                    logger.info(
                        f"Historical data pull for alerts has been skipped for '{configuration.name}' plugin,"
                        " because it is disabled from the configuration."
                    )
                else:
                    execute_celery_task(
                        historical_alerts.apply_async,
                        "common.historical_alerts",
                        args=[
                            configuration.tenant,
                            Collections.ITSM_CONFIGURATIONS.value,
                            configuration.name,
                            datetime.now(),
                            plugin.get_types_to_pull("alerts"),
                        ],
                    )
            if not plugin.get_types_to_pull("events"):
                logger.info(
                    f"Historical data pull for events has been skipped for '{configuration.name}' plugin,"
                    f" because no alerts are selected in the configuration."
                )
            else:
                end_time = datetime.now()
                start_time = end_time - timedelta(
                    hours=configuration.parameters.get("params", {})["hours"]
                )
                if start_time == end_time:
                    logger.info(
                        f"Historical data pull for events has been skipped for '{configuration.name}' plugin,"
                        " because it is disabled from the configuration."
                    )
                else:
                    execute_celery_task(
                        pull_historical_events.apply_async,
                        "itsm.pull_historical_events",
                        args=[configuration.name],
                    )
        if (
            PluginClass.metadata.get("sharing_supported", False)
            and configuration.updateIncidents
            and "incident" in plugin.get_types_to_pull("events")
        ):
            scheduler.schedule(
                name=_get_sharing_task_name(configuration.name),
                task_name="itsm.update_incidents",
                poll_interval=configuration.pollInterval,
                poll_interval_unit=configuration.pollIntervalUnit,
                args=[configuration.name],
            )
    except Exception:
        logger.error(
            "Error occurred while scheduling the tasks for the configuration.",
            details=traceback.format_exc(),
            error_code="CTO_1042",
        )
        raise HTTPException(400, "Error occurred. While scheduling the tasks.")
    logger.debug(
        f"Ticket Orchestrator configuration {configuration.name} created for plugin with id {configuration.plugin}."
    )
    return configuration


def _get_dynamic_step_fields(step_name: str, configuration: dict) -> List:
    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
    plugin = PluginClass(
        configuration.name,
        None,
        None,
        None,
        logger,
    )
    try:
        return plugin.get_fields(step_name, SecretDict(configuration.parameters))
    except NotImplementedError:
        raise HTTPException(400, "Plugin does not implement dynamic steps.")
    except Exception:
        logger.error(
            "Error occurred while getting fields.",
            details=traceback.format_exc(),
            error_code="CTO_1007",
        )
        raise HTTPException(400, "Error occurred while getting fields. Check logs.")


@router.post("/configuration/step/{name}", tags=["CTO Configurations"])
async def get_configuration_step_post(
    configuration: ConfigurationIn,
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["cto_write"]),
) -> Any:
    """Get fields for the specified dynamic step."""
    return _get_dynamic_step_fields(name, configuration)


@router.patch("/configuration/step/{name}", tags=["CTO Configurations"])
async def get_configuration_step_patch(
    configuration: ConfigurationUpdate,
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["cto_write"]),
) -> Any:
    """Get fields for the specified dynamic step."""
    return _get_dynamic_step_fields(name, configuration)


@router.patch("/configuration", tags=["CTO Configurations"])
async def update_configuration(
    configuration: ConfigurationUpdate,
    user: User = Security(get_current_user, scopes=["cto_write"]),
) -> Any:
    """Update an existing configuration."""
    try:
        if configuration.parameters:
            _validate_duplicate_mappings(configuration.parameters)

        # to trim extra spaces for parameters fields.
        trim_space_parameters_fields(configuration.parameters)

        update_payload = _filter_out_none_values(configuration.model_dump())
        existing_configuration_dict = connector.collection(
            Collections.ITSM_CONFIGURATIONS
        ).find_one({"name": configuration.name})
        update_dict = {
            **existing_configuration_dict,
            **update_payload,
        }
        existing_configuration = ConfigurationDB(**existing_configuration_dict)
        configuration = ConfigurationDB(**update_dict)
        PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
        plugin = PluginClass(
            configuration.name,
            SecretDict(configuration.parameters),
            existing_configuration.storage or {},
            None,
            logger,
        )
        if configuration.active:
            _validate_entire_configuration(plugin, configuration)
        tenant_poll_interval = {}
        if configuration.tenant is not None:
            tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                {"name": configuration.tenant}
            )
            tenant = TenantDB(**tenant)
            tenant_poll_interval["interval"] = tenant.pollInterval
            tenant_poll_interval["unit"] = tenant.pollIntervalUnit
        configuration.storage = plugin.storage
        update_payload["storage"] = configuration.storage
        if existing_configuration.active is True and configuration.active is False:
            update_payload["disabledAt"] = datetime.now()
        updated_config_from_db = connector.collection(
            Collections.ITSM_CONFIGURATIONS
        ).find_one_and_update(
            {"name": configuration.name},
            {"$set": update_payload},
            return_document=ReturnDocument.AFTER,
        )
        if not updated_config_from_db:
            raise HTTPException(500, "Error occurred while updating the configuration.")
        configuration = ConfigurationDB(**updated_config_from_db)
        sync_plugin_defaults_to_custom_fields(configuration.plugin)

        if configuration.tenant is not None:
            schedule_or_delete_common_pull_tasks(configuration.tenant)

        tasks = []
        if plugin.metadata.get("pulling_supported") and not PluginClass.metadata.get("netskope", False):
            tasks.append("itsm.pull_data_items")
        if plugin.metadata.get("receiving_supported"):
            tasks.append("itsm.sync_states")
        for task in tasks:
            if configuration.active is False:
                scheduler.delete(_get_task_name(configuration.name))
            else:
                scheduler.upsert(
                    name=_get_task_name(configuration.name),
                    task_name=task,
                    poll_interval=(
                        configuration.pollInterval
                    ),
                    poll_interval_unit=(
                        configuration.pollIntervalUnit
                    ),
                    args=[configuration.name],
                )
        if PluginClass.metadata.get("sharing_supported", False):
            if (
                configuration.active is False
                or "incident" not in plugin.get_types_to_pull("events")
                or not configuration.updateIncidents
            ):
                scheduler.delete(_get_sharing_task_name(configuration.name))
            else:
                scheduler.upsert(
                    name=_get_sharing_task_name(configuration.name),
                    task_name="itsm.update_incidents",
                    poll_interval=configuration.pollInterval,
                    poll_interval_unit=configuration.pollIntervalUnit,
                    args=[configuration.name],
                )
        if existing_configuration.active is False and configuration.active is True:
            logger.debug(
                f"Ticket Orchestrator configuration {configuration.name} successfully enabled."
            )
        elif existing_configuration.active is True and configuration.active is False:
            # Call plugin cleanup method with disable action type.
            PluginClass = helper.find_by_id(configuration.plugin)
            if PluginClass is not None:
                plugin = PluginClass(
                    configuration.name,
                    SecretDict(configuration.parameters),
                    configuration.storage or {},
                    configuration.checkpoint,
                    logger,
                )
                try:
                    plugin.cleanup(action_type=ActionType.DISABLE.value)
                    updated_config_from_db = connector.collection(
                        Collections.ITSM_CONFIGURATIONS
                    ).find_one_and_update(
                        {"name": configuration.name},
                        {"$set": {"storage": plugin.storage}},
                        return_document=ReturnDocument.AFTER,
                    )
                except NotImplementedError:
                    logger.debug(
                        f"Plugin {configuration.name} does not implement cleanup method, "
                        "skipping clean up while disabling configuration."
                    )
            logger.debug(
                f"Ticket Orchestrator configuration {configuration.name} successfully disabled."
            )
        else:
            logger.debug(
                f"Ticket Orchestrator configuration {configuration.name} successfully updated."
            )
        return ConfigurationOut(
            **configuration.model_dump(),
            receivingSupported=helper.find_by_id(configuration.plugin).metadata.get(
                "receiving_supported", False
            ),
            sharingSupported=helper.find_by_id(configuration.plugin).metadata.get(
                "sharing_supported", False
            ),
            pluginName=helper.find_by_id(configuration.plugin).metadata.get("name"),
        )
    except HTTPException as error:
        raise error
    except Exception:
        logger.error(
            "Error occurred while updating configuration.",
            details=traceback.format_exc(),
            error_code="CTO_1038",
        )
        raise HTTPException(status_code=500, detail="Could not update configuration.")


@router.delete("/configurations", tags=["CTO Configurations"])
async def delete_configuration(
    configuration: ConfigurationDelete,
    user: User = Security(get_current_user, scopes=["cto_write"]),
):
    """Delete a configuration."""
    configuration_db = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
        {"name": configuration.name}
    )
    configuration_db = ConfigurationDB(**configuration_db)

    PluginClass = helper.find_by_id(configuration_db.plugin)
    if PluginClass is not None:
        plugin = PluginClass(
            configuration_db.name,
            SecretDict(configuration_db.parameters),
            configuration_db.storage or {},
            configuration_db.checkpoint,
            logger,
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

    connector.collection(Collections.ITSM_CONFIGURATIONS).delete_one(
        {"name": configuration.name}
    )

    if configuration_db.tenant is not None:
        schedule_or_delete_common_pull_tasks(configuration_db.tenant)

    scheduler.delete(_get_task_name(configuration.name))
    scheduler.delete(_get_sharing_task_name(configuration.name))
    # remove queues if they are configured
    connector.collection(Collections.ITSM_BUSINESS_RULES).update_many(
        {}, {"$unset": {f"queues.{configuration.name}": ""}}
    )
    logger.debug(
        f"Ticket Orchestrator configuration {configuration.name} successfully delete."
    )
    return {}


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
