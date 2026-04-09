"""Provides task for delete system logs."""

from datetime import datetime, timedelta
import traceback
from netskope.common.celery.main import APP
from netskope.common.models import SettingsDB
from netskope.common.utils import DBConnector, Collections, Logger, track


connector = DBConnector()
logger = Logger()


@APP.task(name="common.delete_tasks")
@track()
def delete_tasks():
    """Delete all the tasks that have exceeded the cleanup time duration."""
    try:
        settings = SettingsDB(
            **connector.collection(Collections.SETTINGS).find_one({})
        )
        delete_lesser_than = datetime.now() - timedelta(
            hours=settings.tasksCleanup
        )
        result = connector.collection(Collections.TASK_STATUS).delete_many(
            {"startedAt": {"$lte": delete_lesser_than}}
        )
        if result.deleted_count > 0:
            logger.info(
                f"Removed {result.deleted_count} task{'s' if result.deleted_count != 1 else ''}"
                f" as part of the automatic cleanup."
            )
    except Exception:
        logger.error(
            "Error occurred while cleaning up tasks.",
            details=traceback.format_exc(),
            error_code="CE_1008"
        )
