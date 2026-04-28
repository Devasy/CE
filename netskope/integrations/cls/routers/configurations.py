"""Provides configuration related endpoints."""

import traceback
from typing import List
from fastapi import APIRouter, HTTPException, Security

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from netskope.common.celery.main import APP
from netskope.common.api.routers.auth import get_current_user
from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
    Scheduler,
    get_dynamic_fields_from_plugin,
    has_source_info_args,
    SecretDict,
)
from netskope.common.models import User, TenantDB, ActionType
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.utils.webtx_notifier import add_or_acknowledge_webtx_disabled_banner
from netskope.common.utils.common_pull_scheduler import schedule_or_delete_common_pull_tasks
from netskope.integrations import trim_space_parameters_fields
from ..models import (
    ConfigurationIn,
    ConfigurationOut,
    ConfigurationUpdate,
    ConfigurationDelete,
    ConfigurationDB,
)
from ..utils import schedule_or_delete_third_party_pull_task

router = APIRouter()
plugin_helper = PluginHelper()
logger = Logger()
db_connector = DBConnector()
scheduler = Scheduler()


@router.get(
    "/configurations",
    response_model=List[ConfigurationOut],
    tags=["CLS Configurations"],
    description="Get list of all the configurations.",
)
async def read_all_configurations_list(
    user: User = Security(get_current_user, scopes=["cls_read"]),
):
    """List out all the configurations.

    Args:
        active (bool, optional): Get only (in)active configurations. Defaults to None.

    Returns:
        List[ConfigurationOut]: List of configurations.
    """
    out = []
    netskope_configs = []
    for configuration in db_connector.collection(
        Collections.CLS_CONFIGURATIONS
    ).find({}):
        if "cls_write" not in user.scopes:
            configuration["parameters"] = {}
        metadata = plugin_helper.find_by_id(configuration["plugin"]).metadata
        config = ConfigurationOut(
            **configuration,
            pushSupported=metadata.get("push_supported", True),
            pluginName=metadata.get("name"),
            netskope=metadata.get("netskope", False),
            types=metadata.get("types", []),
            pullSupported=metadata.get("pull_supported", False),
        )
        if metadata.get("netskope", False):
            netskope_configs.append(config)
        else:
            out.append(config)
    return netskope_configs + sorted(out, key=lambda c: c.pluginName.lower())


@router.post(
    "/configurations",
    response_model=ConfigurationOut,
    tags=["CLS Configurations"],
    status_code=201,
    description="Create a new configuration.",
)
async def create_configuration(
    configuration: ConfigurationIn,
    user: User = Security(get_current_user, scopes=["cls_write"]),
) -> ConfigurationOut:
    """Create a new configuration.

    Args:
        configuration (ConfigurationIn): Configuration to be created.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        ConfigurationOut: Newly created configuration.
    """
    # make sure plugin exists
    PluginClass = plugin_helper.find_by_id(configuration.plugin)  # NOSONAR S117
    if PluginClass is None:
        raise HTTPException(
            400, f"Plugin with id='{configuration.plugin}' does not exist."
        )

    # to trim extra spaces for parameters fields.
    trim_space_parameters_fields(configuration.parameters)

    # insert new configuration
    try:
        if configuration.tenant is not None:
            tenant = db_connector.collection(
                Collections.NETSKOPE_TENANTS
            ).find_one({"name": configuration.tenant})
            tenant = TenantDB(**tenant)
            configuration.pollInterval = tenant.pollInterval
            configuration.pollIntervalUnit = tenant.pollIntervalUnit

        db_connector.collection(Collections.CLS_CONFIGURATIONS).insert_one(
            ConfigurationDB(**configuration.model_dump()).model_dump()
        )
    except Exception:
        logger.debug(
            "Error occurred while creating a new configuration.",
            details=traceback.format_exc(),
            error_code="CLS_1003",
        )
        raise HTTPException(
            500, "Error occurred while creating a new configuration."
        )

    logger.debug(
        f"Configuration '{configuration.name}' created for plugin '{configuration.plugin}'"
    )

    return {
        **configuration.model_dump(),
        "pluginName": PluginClass.metadata.get("name"),
        "pluginVersion": PluginClass.metadata.get("version"),
        "pushSupported": PluginClass.metadata.get("push_supported", True),
        "pullSupported": PluginClass.metadata.get("pull_supported", False),
        "netskope": PluginClass.metadata.get("netskope", False),
        "types": PluginClass.metadata.get("types", []),
    }


def filter_out_none_values(data: dict) -> dict:
    """Filter out keys with None values from a dict.

    Args:
        data (dict): Dictionary to be filtered.

    Returns:
        dict: Filtered dictionary.
    """
    return {k: v for k, v in data.items() if v is not None}


@router.patch(
    "/configurations",
    response_model=ConfigurationOut,
    tags=["CLS Configurations"],
    description="Update an existing configuration.",
)
async def update_configuration(
    configuration: ConfigurationUpdate,
    user: User = Security(get_current_user, scopes=["cls_write"]),
):
    """Update existing configuration.

    Args:
        configuration (ConfigurationUpdate): Updated configuration.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        ConfigurationOut: The newly updated configuration.
    """
    is_webtx = False
    if configuration.tenant is not None:
        tenant = TenantDB(
            **db_connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                {"name": configuration.tenant}
            )
        )
        configuration.pollInterval = tenant.pollInterval
        configuration.pollIntervalUnit = tenant.pollIntervalUnit
    # to trim extra spaces for parameters fields.
    trim_space_parameters_fields(configuration.parameters)
    configuration_dict = configuration.model_dump(exclude_none=True, exclude={"oldConfig"})
    configuration_dict["attributeMappingRepo"] = configuration.attributeMappingRepo
    after = db_connector.collection(
        Collections.CLS_CONFIGURATIONS
    ).find_one_and_update(
        {"name": configuration.name},
        {"$set": configuration_dict},
        return_document=ReturnDocument.AFTER,
    )
    after = ConfigurationDB(**after)
    # TODO: update schedule
    if (
        configuration.oldConfig.active is False
        and configuration.active is True
    ):

        if "netskope_webtx.main" in configuration.plugin:
            try:
                is_webtx = True
                add_or_acknowledge_webtx_disabled_banner()
                scheduler.schedule(
                    name=f"tenant.{configuration.tenant}.{configuration.name}.webtx",
                    task_name="common.pull",
                    poll_interval=30,
                    poll_interval_unit="seconds",
                    args=["webtx", configuration.tenant],
                    kwargs={"configuration_name": configuration.name}
                )
            except DuplicateKeyError:
                pass
        logger.debug(f"CLS configuration {configuration.name} enabled.")
    elif (
        configuration.oldConfig.active is True
        and configuration.active is False
    ):
        try:
            if after.task.get("task_id") is not None:
                logger.debug(f"Terminating webtransaction process for {after.task.get('task_id')}")
                APP.control.revoke(after.task.get("task_id"), terminate=True, signal='SIGINT')
        except Exception:
            logger.debug("Error occurred while terminating the WebTx task.",
                         details=traceback.format_exc())
        scheduler.delete(f"tenant.{configuration.tenant}.{configuration.name}.webtx")
        #  Call plugin cleanup method with disable action type.
        PluginClass = plugin_helper.find_by_id(configuration.plugin)
        if PluginClass is not None:
            plugin = PluginClass(
                configuration.name,
                SecretDict(configuration.parameters),
                configuration.storage,
                None,
                logger
            )
            try:
                has_action_type = has_source_info_args(plugin, "cleanup", ["action_type"])
                if "cleanup" in dir(plugin) and has_action_type:
                    plugin.cleanup(action_type=ActionType.DISABLE.value)
                elif "cleanup" in dir(plugin):
                    plugin.cleanup()
                db_connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
                    {"name": configuration.name},
                    {"$set": {"storage": plugin.storage}},
                )
            except NotImplementedError:
                logger.debug(
                    f"Plugin {configuration.name} does not implement cleanup method, "
                    "skipping clean up while disabling configuration."
                )
        logger.debug(f"CLS configuration {configuration.name} disabled.")
    else:
        if after.task.get("task_id") is not None:
            logger.debug(f"Terminating webtransaction process for {after.task.get('task_id')}")
            APP.control.revoke(after.task.get("task_id"), terminate=True, signal='SIGINT')
        logger.debug(f"CLS configuration {configuration.name} updated.")
    if configuration.tenant is not None and not is_webtx:
        schedule_or_delete_common_pull_tasks(configuration.tenant)

    schedule_or_delete_third_party_pull_task()
    return configuration


@router.delete(
    "/configuration",
    description="Delete an existing configuration.",
    tags=["CLS Configurations"],
)
async def delete_configuration(
    configuration: ConfigurationDelete,
    user: User = Security(get_current_user, scopes=["cls_write"]),
):
    """Delete a configuration."""
    is_webtx = False
    configuration = db_connector.collection(
        Collections.CLS_CONFIGURATIONS
    ).find_one({"name": configuration.name})
    configuration = ConfigurationDB(**configuration)
    try:
        if configuration.task.get("task_id") is not None:
            logger.debug(f"Terminating webtransaction process for {configuration.task.get('task_id')}")
            APP.control.revoke(configuration.task.get("task_id"), terminate=True, signal='SIGINT')
    except Exception:
        logger.debug("Error occurred while terminating the WebTx task.",
                     details=traceback.format_exc())
    PluginClass = plugin_helper.find_by_id(configuration.plugin)
    if "netskope_webtx.main" in configuration.plugin:
        is_webtx = True
        scheduler.delete(f"tenant.{configuration.tenant}.{configuration.name}.webtx")
        add_or_acknowledge_webtx_disabled_banner()
    PluginClass = plugin_helper.find_by_id(configuration.plugin)
    if PluginClass is not None:
        plugin = PluginClass(configuration.name, SecretDict(
            configuration.parameters), configuration.storage, None, logger)
        try:
            has_action_type = has_source_info_args(plugin, "cleanup", ["action_type"])
            if "cleanup" in dir(plugin) and has_action_type:
                plugin.cleanup(action_type=ActionType.DELETE.value)
            elif "cleanup" in dir(plugin):
                plugin.cleanup()
        except NotImplementedError:
            logger.debug(
                f"Plugin {configuration.name} does not implement cleanup method, "
                "skipping clean up while deleting configuration."
            )
    db_connector.collection(Collections.CLS_CONFIGURATIONS).delete_one(
        {"name": configuration.name}
    )

    if configuration.tenant is not None and not is_webtx:
        schedule_or_delete_common_pull_tasks(configuration.tenant)

    schedule_or_delete_third_party_pull_task()

    if PluginHelper.is_syslog_service_plugin(configuration.plugin):
        db_connector.collection(Collections.SCHEDULES).delete_one(
            {"name": f"cls.{configuration.name}"}
        )
    db_connector.collection(Collections.CLS_BUSINESS_RULES).update_many(
        {}, {"$unset": {f"siemMappings.{configuration.name}": ""}}
    )
    for rule in db_connector.collection(Collections.CLS_BUSINESS_RULES).find(
        {}
    ):
        for key, values in rule.get("siemMappings", {}).items():
            if configuration.name in values:
                db_connector.collection(
                    Collections.CLS_BUSINESS_RULES
                ).update_one(
                    {"name": rule["name"]},
                    {"$pull": {f"siemMappings.{key}": configuration.name}},
                )
    if PluginClass.metadata.get("netskope", False):
        plugin_provider_helper = PluginProviderHelper()
        plugin_provider_helper.check_and_update_forbidden_endpoints(configuration.tenant)
    logger.debug(f"CLS configuration with name '{configuration.name}' deleted.")
    return {"success": True}


@router.post(
    "/get_dynamic_fields/{plugin_id}",
    tags=["Plugins Dynamic fields"],
    description="Get the dynamic fields from CLS plugin based on other fields.",
)
async def get_dynamic_fields(
    plugin_id: str,
    config_details: dict,
    user: User = Security(get_current_user, scopes=["cls_write"])
):
    """Get the dynamic fields from plugin."""
    return get_dynamic_fields_from_plugin(plugin_id, config_details)
