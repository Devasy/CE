"""CLS specific pull task."""

import traceback
from datetime import datetime

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.utils import (
    Collections,
    DBConnector,
    DataBatchManager,
    Logger,
    SecretDict,
    integration,
    track,
    parse_events
)
from netskope.common.models import BatchDataSourceType
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.integrations.cls.models import (
    ConfigurationDB,
)

connector = DBConnector()
batch_manager = DataBatchManager()
logger = Logger()
helper = PluginHelper()


@APP.task(name="cls.pull")
@integration("cls")
@track()
def pull(
    configuration: str,
    start_time: datetime = None,
    end_time: datetime = None,
    destination: str = None,
    rule: str = None,
    lifecycle: callable = None,
) -> dict:
    """Pull data from CLS."""
    from .plugin_lifecycle_task import execute_plugin

    is_historical = start_time and end_time
    logger.info(f"Executing pull cycle for configuration {configuration}.")
    configuration_db = connector.collection(
        Collections.CLS_CONFIGURATIONS
    ).find_one({"name": configuration})
    if not configuration_db:
        return {
            "success": False,
            "message": f"Configuration {configuration} does not exist.",
        }

    configuration: ConfigurationDB = ConfigurationDB(**configuration_db)

    if not configuration.active:
        return {
            "success": False,
            "message": f"Configuration {configuration.name} is not active.",
        }

    PluginClass = helper.find_by_id(configuration.plugin)
    if not PluginClass:
        return {
            "success": False,
            "message": f"Plugin {configuration.plugin} does not exist.",
        }
    plugin = PluginClass(
        configuration.name,
        SecretDict(configuration.parameters),
        configuration.storage,
        configuration.checkpoint,
        logger,
    )
    plugin.sub_checkpoint = configuration.subCheckpoint
    end_time = datetime.now()
    batch_counter = 0
    try:
        for batch, batch_checkpoint in plugin.pull(
            **(
                {"start_time": start_time, "end_time": end_time}
                if is_historical
                else {}
            )
        ):
            batch_counter += 1
            events, batch_type, batch_subtype = batch

            temp_events = events
            if isinstance(events, bytes):
                temp_events = parse_events(events)
            data_count = len(
                temp_events.get("result", [])
                if isinstance(temp_events, dict)
                else temp_events
            )
            batch_obj = batch_manager.create(
                batch_type,
                batch_subtype,
                data_count,
                configuration.name,
                BatchDataSourceType.CONFIGURATION,
            )
            if lifecycle:
                lifecycle(
                    data=events,
                    data_type=batch_type,
                    data_subtype=batch_subtype,
                )
            else:
                execute_celery_task(
                    execute_plugin.apply_async,
                    "cls.execute_plugin",
                    args=[],
                    kwargs={
                        "configuration_name": configuration.name,
                        "data": events,
                        "data_type": batch_type,
                        "sub_type": batch_subtype,
                        "batch_id": batch_obj["_id"],
                    },
                )
            if not is_historical:
                connector.collection(
                    Collections.CLS_CONFIGURATIONS
                ).update_one(
                    {"name": configuration.name},
                    {"$set": {"subCheckpoint": batch_checkpoint}},
                )
    except Exception:
        if not is_historical:
            logger.error(
                f"Error occurred while executing pull cycle for CLS configuration "
                f"{configuration.name}.",
                details=traceback.format_exc(),
            )
        else:
            logger.error(
                f"Historical pulling failed for the window {start_time} UTC to {end_time} UTC "
                f"for {configuration.name} to {destination}, rule {rule}.",
                details=traceback.format_exc(),
                error_code="CLS_1013",
            )
        return {"success": False}

    if not is_historical:
        connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
            {"name": configuration.name},
            {
                "$set": (
                    {
                        "checkpoint": end_time,
                        "storage": configuration.storage,
                        # get rid of subCheckpoint since all batches were successfully pulled
                        "subCheckpoint": None,
                    }
                ),
            },
        )
        logger.info(
            f"Completed executing pull cycle for CLS configuration "
            f"{configuration.name}. Pulled {batch_counter} "
            f"batch{'' if batch_counter == 1 else 'es'}."
        )
    else:
        logger.info(
            f"Historical pull has been completed for {configuration.name} to {destination}, rule {rule}. "
            f"Ingestion Tasks Added: {batch_counter}. "
        )
    return {"success": True}
