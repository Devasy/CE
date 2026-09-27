"""Configuration related endpoints."""

import traceback
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Security
from pymongo import ReturnDocument

from netskope.common.celery.historical_alerts import historical_alerts
from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User, PollIntervalUnit, ActionType
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    Scheduler,
    SecretDict,
    get_dynamic_fields_from_plugin,
)
from netskope.common.utils.common_pull_scheduler import (
    schedule_or_delete_common_pull_tasks,
)
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.celery.scheduler import execute_celery_task
from netskope.integrations.crev2.plugin_base import Entity

from ..tasks.historical_pull import historical_appdata
from ..models import (
    ActionWithoutParams,
    ConfigurationUpdate,
    ConfigurationDB,
    ConfigurationIn,
    ConfigurationOut,
)
from ..utils import NETSKOPE_POLL_INTERVAL, NETSKOPE_POLL_INTERVAL_UNIT

connector = DBConnector()
helper = PluginHelper()
logger = Logger()
router = APIRouter()
scheduler = Scheduler()


def _add_task_prefix(name: str) -> str:
    """Add CREv2 prefix to task."""
    return f"cre.{name}"


@router.get("/configurations", tags=["CREv2 Configurations"])
async def list_configurations(
    user: User = Security(get_current_user, scopes=["cre_read"]),
) -> list[ConfigurationOut]:
    """List configurations."""
    out = []
    for config in connector.collection(Collections.CREV2_CONFIGURATIONS).find({}):
        if "cre_write" not in user.scopes:
            config["parameters"] = {}
        out.append(
            ConfigurationOut(
                **config,
                pluginName=helper.find_by_id(config["plugin"]).metadata.get("name"),
            )
        )
    return out


@router.post("/plugins/{name}/entities", tags=["CREv2 Configurations"])
async def list_entities(
    name: str,
    parameters: dict,
    _: User = Security(get_current_user, scopes=["cre_read"]),
) -> list[Entity]:
    """List entities."""
    PluginClass = helper.find_by_id(name)  # NOSONAR S117
    if not PluginClass:
        raise HTTPException(status_code=404, detail="Plugin not found.")
    plugin = PluginClass(None, parameters, {}, None, logger)
    entities = plugin.get_entities()
    for entity in entities:
        for field in entity.fields:
            if not field.label:
                field.label = field.name
        if not entity.label:
            entity.label = entity.name
    return entities


@router.get("/configurations/{name}/actions", tags=["CREv2 Configurations"])
async def list_actions(
    name: str,
    _: User = Security(get_current_user, scopes=["cre_read"]),
) -> list[ActionWithoutParams]:
    """List actions."""
    configuration = connector.collection(Collections.CREV2_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if not configuration:
        raise HTTPException(
            404,
            f"CRE configuration with name {name} does not exist.",
        )
    configuration = ConfigurationDB(**configuration)
    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
    if not PluginClass:
        raise HTTPException(
            404, f"Plugin with id {configuration.plugin} does not exist."
        )
    plugin = PluginClass(
        None,
        SecretDict(configuration.parameters),
        {},
        None,
        logger,
    )
    try:
        return plugin.get_actions()
    except Exception as ex:
        logger.error(
            "Error occurred while getting action list.",
            details=traceback.format_exc(),
        )
        raise HTTPException(500, "Could not get action list.") from ex


@router.post("/configurations/{name}/fields", tags=["CREv2 Configurations"])
async def get_action_fields(
    action: ActionWithoutParams,
    name: str,
    user: User = Security(get_current_user, scopes=["cre_read"]),
):
    """List all actions."""
    configuration = connector.collection(Collections.CREV2_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if configuration is None:
        raise HTTPException(400, f"CRE configuration with name {name} does not exist.")
    configuration = ConfigurationDB(**configuration)
    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR
    if PluginClass is None:
        raise HTTPException(
            400, f"Plugin with id {configuration.plugin} does not exist."
        )
    plugin = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        {},
        None,
        logger,
    )
    try:
        return plugin.get_action_params(action)
    except Exception as ex:
        logger.error(
            "Error occurred while getting list of actions.",
            details=traceback.format_exc(),
        )
        raise HTTPException(400, "Could not get action parameters. Check logs.") from ex


@router.post("/configurations", tags=["CREv2 Configurations"])
async def create_configuration(
    configuration: ConfigurationIn,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> ConfigurationOut:
    """Create a configuration."""
    if configuration._plugin_class.metadata.get("netskope", False):
        configuration.pollInterval = 30
        configuration.pollIntervalUnit = PollIntervalUnit.SECONDS
    connector.collection(Collections.CREV2_CONFIGURATIONS).insert_one(
        ConfigurationDB(**configuration.model_dump()).model_dump()
    )
    if configuration.active and configuration.mappedEntities:
        if (
            # is not a netskope plugin
            not configuration._plugin_class.metadata.get("netskope", False)
        ):
            # is not a netskope plugin
            scheduler.schedule(
                f"Fetch {_add_task_prefix(configuration.name)}",
                "cre.fetch_records",
                configuration.pollInterval,
                configuration.pollIntervalUnit,
                [configuration.name],
            )
        else:
            scheduler.schedule(
                f"Update {_add_task_prefix(configuration.name)}",
                "cre.update_records",
                NETSKOPE_POLL_INTERVAL,
                NETSKOPE_POLL_INTERVAL_UNIT,
                [configuration.name],
            )
            schedule_or_delete_common_pull_tasks(configuration.tenant)
            end_time = datetime.now()
            start_time = end_time - timedelta(
                hours=configuration.parameters.get("initial_range", 0)
            )
            if start_time == end_time:
                logger.info(
                    f"Historical data pull for events has been skipped for '{configuration.name}' plugin,"
                    " because it is disabled from the configuration."
                )
            else:
                execute_celery_task(
                    historical_appdata.apply_async,
                    "cre.historical_appdata",
                    args=[
                        configuration.tenant,
                        configuration.name,
                        start_time,
                        end_time,
                    ],
                )
            end_time = datetime.now()
            start_time = end_time - timedelta(
                days=configuration.parameters.get("days", 0)
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
                        Collections.CREV2_CONFIGURATIONS.value,
                        configuration.name,
                        datetime.now(),
                        ["uba"],
                    ],
                )

    return configuration


@router.patch("/configurations", tags=["CREv2 Configurations"])
async def update_configuration(
    configuration: ConfigurationUpdate,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> ConfigurationOut:
    """Update a configuration."""
    updated_configuration = connector.collection(
        Collections.CREV2_CONFIGURATIONS
    ).find_one_and_update(
        {"name": configuration.name},
        {"$set": configuration.model_dump(exclude_unset=True)},
        return_document=ReturnDocument.AFTER,
    )
    # update poll interval if it has changed
    if (
        (
            configuration.pollInterval is not None
            or configuration.pollIntervalUnit is not None
        )
        and not configuration._plugin_class.metadata.get("netskope", False)
        and configuration.mappedEntities
    ):
        poll_interval = (
            configuration.pollInterval
            or configuration._existing_configuration.pollInterval
        )
        poll_interval_unit = (
            configuration.pollIntervalUnit
            or configuration._existing_configuration.pollIntervalUnit
        )
        scheduler.upsert(
            f"Fetch {_add_task_prefix(configuration.name)}",
            "cre.fetch_records",
            poll_interval,
            poll_interval_unit,
            [configuration.name],
        )
    # Handle enable/disable
    if (not configuration.active and configuration._existing_configuration.active) or (
        configuration.mappedEntities == []
        and configuration._existing_configuration.mappedEntities
    ):
        scheduler.delete(f"Fetch {_add_task_prefix(configuration.name)}")
        scheduler.delete(f"Update {_add_task_prefix(configuration.name)}")
    elif (
        configuration.active
        and not configuration._existing_configuration.active
        and (
            configuration.mappedEntities
            or (
                configuration.mappedEntities is None
                and configuration._existing_configuration.mappedEntities
            )
        )
    ) or (
        configuration.active
        and configuration.mappedEntities
        and not configuration._existing_configuration.mappedEntities
    ):
        if not configuration._plugin_class.metadata.get("netskope", False):
            scheduler.upsert(
                f"Fetch {_add_task_prefix(configuration.name)}",
                "cre.fetch_records",
                configuration.pollInterval
                or configuration._existing_configuration.pollInterval,
                configuration.pollIntervalUnit
                or configuration._existing_configuration.pollIntervalUnit,
                [configuration.name],
            )
        else:
            scheduler.schedule(
                f"Update {_add_task_prefix(configuration.name)}",
                "cre.update_records",
                NETSKOPE_POLL_INTERVAL,
                NETSKOPE_POLL_INTERVAL_UNIT,
                [configuration.name],
            )
    if configuration._existing_configuration.active and not configuration.active:
        #  Call plugin cleanup method with disable action type.
        PluginClass = helper.find_by_id(configuration.plugin)
        if PluginClass is not None:
            plugin = PluginClass(
                configuration.name,
                SecretDict(configuration.parameters),
                configuration.storage,
                None,
                logger,
            )
            try:
                plugin.cleanup(action_type=ActionType.DISABLE.value)
                connector.collection(Collections.CREV2_CONFIGURATIONS).update_one(
                    {"name": configuration.name},
                    {"$set": {"storage": plugin.storage}},
                )
            except NotImplementedError:
                logger.debug(
                    f"Plugin {configuration.name} does not implement cleanup method, "
                    "skipping clean up while disabling configuration."
                )
    if configuration.tenant:
        schedule_or_delete_common_pull_tasks(configuration.tenant)
    return updated_configuration


@router.delete("/configurations/{name}", tags=["CREv2 Configurations"])
async def delete_configuration(
    name: str, user: User = Security(get_current_user, scopes=["cre_write"])
):
    """Delete a configuration."""
    configuration = connector.collection(
        Collections.CREV2_CONFIGURATIONS
    ).find_one({"name": name})

    configuration = ConfigurationDB(**configuration)

    PluginClass = helper.find_by_id(configuration.plugin)
    if PluginClass.metadata.get("netskope", False):
        plugin_provider_helper = PluginProviderHelper()
        plugin_provider_helper.check_and_update_forbidden_endpoints(
            configuration.tenant
        )
    if PluginClass is not None:
        plugin = PluginClass(
            configuration.name,
            SecretDict(configuration.parameters),
            configuration.storage,
            None,
            logger,
        )
        try:
            plugin.cleanup(action_type=ActionType.DELETE.value)
        except NotImplementedError:
            logger.debug(
                f"Plugin {configuration.name} does not implement cleanup method, "
                "skipping clean up while deleting configuration."
            )
    result = connector.collection(Collections.CREV2_CONFIGURATIONS).delete_one(
        {"name": name}
    )
    if result.deleted_count == 0:
        raise HTTPException(404, f"Could not find configuration with name {name}.")
    if configuration.tenant:
        schedule_or_delete_common_pull_tasks(configuration.tenant)

    scheduler.delete(f"Fetch {_add_task_prefix(name)}")
    scheduler.delete(f"Update {_add_task_prefix(name)}")
    connector.collection(Collections.CREV2_BUSINESS_RULES).update_many(
        {}, {"$unset": {f"actions.{name}": ""}}
    )
    logger.debug(f"Configuration with name '{name}' deleted by {user.username}.")
    return {"success": True}


@router.post(
    "/get_dynamic_fields/{plugin_id}",
    tags=["Plugins Dynamic fields"],
    description="Get the dynamic fields from plugin based on other fields.",
)
async def get_dynamic_fields(
    plugin_id: str,
    config_details: dict,
    user: User = Security(get_current_user, scopes=["cre_write"]),
):
    """Get the dynamic fields from plugin."""
    return get_dynamic_fields_from_plugin(plugin_id, config_details)
