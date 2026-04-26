"""Tenant pull logss task."""
from datetime import datetime

from .main import APP
from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
    RepoManager,
    PluginHelper,
    track,
    SecretDict
)
from netskope.integrations.cls.models import ConfigurationDB
from netskope.common.celery.scheduler import execute_celery_task

from netskope.integrations.cls.tasks.plugin_lifecycle_task import (
    execute_plugin as cls_execute_plugin,
)

manager = RepoManager()
helper = PluginHelper()
connector = DBConnector()
logger = Logger()


@APP.task(name="common.pull_logs")
@track()
def pull_logs(plugin_name: str):
    """Pull logs and execute all the Netskope configurations."""
    logger.debug(f"Pulling logs for {plugin_name} configuration.")
    plugin = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
        {"name": plugin_name}
    )
    if plugin is None:
        logger.debug(f"Plugin with name {plugin_name} no longer exists.")
        return {
            "success": False,
            "message": f"Plugin {plugin_name} does not exist",
        }
    configuration = ConfigurationDB(**plugin)
    module = {
        "collection": Collections.CLS_CONFIGURATIONS,
        "lifecycle": cls_execute_plugin,
        "task": "cls.execute_plugin"
    }
    PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR
    if PluginClass is None:
        logger.error(
            f"Could not find the plugin with id='{configuration.plugin}'",
            error_code="CE_1115"
        )
        return False

    # if inactive; return
    if not configuration.active:
        return False

    plugin = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        configuration.storage,
        None,
        logger,
        mappings=None,  # mapping file not required for ingestion
        source=None,
    )

    start_time = datetime.now() if configuration.lastRunAt is None else configuration.lastRunAt
    end_time = datetime.now()

    start_time = start_time.replace(microsecond=0)
    end_time = end_time.replace(microsecond=0)
    logs_type = configuration.parameters.get(
        "logs_type", ["info", "warning", "error"]
    )
    query = {
        "$and": [
            {"ce_log_type": {"$in": logs_type}},
            {"createdAt": {"$gte": start_time, "$lt": end_time}},
        ]
    }
    logs_cursor = connector.collection(Collections.LOGS).find(query)

    logs_batch = plugin.pull(logs_cursor, start_time, end_time)
    for logs in logs_batch:
        kwargs = {
            "logs": logs
        }
        execute_celery_task(module["lifecycle"].apply_async, module["task"], args=[configuration.name], kwargs=kwargs)

    connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
        {"name": configuration.name},
        {
            "$set": {
                "lastRunAt": end_time,
                "lastRunSuccess": True,
                "storage": plugin.storage
            }
        },
    )

    return {"success": True}
