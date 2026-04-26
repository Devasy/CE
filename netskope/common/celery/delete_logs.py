"""Provides task for delete system logs."""

from datetime import datetime, timedelta
import traceback
from netskope.common.celery.main import APP
from netskope.common.models import SettingsDB
from netskope.common.utils import DBConnector, Collections, Logger, track, DataBatchManager


connector = DBConnector()
logger = Logger()
batch_manager = DataBatchManager()


@APP.task(name="common.delete_logs")
@track()
def delete_logs():
    """Delete all the system logs that have exceeded the cleanup time duration."""
    try:
        settings = SettingsDB(
            **connector.collection(Collections.SETTINGS).find_one({})
        )
        delete_lesser_than = datetime.now() - timedelta(
            days=settings.logsCleanup
        )
        result = connector.collection(Collections.LOGS).delete_many(
            {"createdAt": {"$lte": delete_lesser_than}}
        )
        if result.deleted_count > 0:
            logger.info(
                f"Removed {result.deleted_count} system log{'s' if result.deleted_count != 1 else ''}"
                f" as part of the automatic cleanup."
            )

        # Delete the container health check record which last updated older than 12 hrs ago.
        delete_lesser_than = datetime.now() - timedelta(
            days=1
        )
        result = connector.collection(Collections.NODE_HEALTH).delete_many(
            {"check_time": {"$lte": delete_lesser_than}}
        )
        if result.deleted_count > 0:
            logger.info(
                f"Removed {result.deleted_count} core container"
                f" health check record{'s' if result.deleted_count != 1 else ''}"
                f" as part of the automatic cleanup."
            )

        # Delete the data batch logs
        delete_lesser_than = datetime.now() - timedelta(
            days=settings.dataBatchCleanup
        )
        result = batch_manager.delete_by_filters(
            {"createdAt": {"$lte": delete_lesser_than}}
        )
        if result.deleted_count > 0:
            logger.info(
                f"Removed {result.deleted_count} alert/event/log/WebTx "
                f"statistic{'s' if result.deleted_count != 1 else ''}"
                f" as part of the automatic cleanup."
            )

        result = connector.collection(Collections.CLUSTER_HEALTH).delete_many(
            {"check_time": {"$lte": delete_lesser_than}}
        )
        if result.deleted_count > 0:
            logger.info(
                f"Removed {result.deleted_count} cluster health check record{'s' if result.deleted_count != 1 else ''}"
                f" as part of the automatic cleanup."
            )
    except Exception:
        logger.error(
            "Error occurred while cleaning up system logs.",
            details=traceback.format_exc(),
            error_code="CE_1007",
        )
