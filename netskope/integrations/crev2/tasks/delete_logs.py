"""Provides task for delete action logs."""

from datetime import datetime, timedelta
import traceback
from netskope.common.celery.main import APP
from netskope.common.models import SettingsDB
from netskope.common.utils import (
    DBConnector,
    Collections,
    integration,
    Logger,
    track,
)
from netskope.integrations.crev2.models import ActionLogStatus


connector = DBConnector()
logger = Logger()


@APP.task(name="cre.delete_logs")
@integration("cre")
@track()
def delete_logs():
    """Delete all the logs that have exceeded the cleanup time duration."""
    try:
        settings = SettingsDB(
            **connector.collection(Collections.SETTINGS).find_one({})
        )
        delete_lesser_than = datetime.now() - timedelta(
            days=settings.cre.logsCleanup
        )
        result = connector.collection(
            Collections.CREV2_ACTION_LOGS
        ).delete_many(
            {
                "performedAt": {"$lte": delete_lesser_than},
                "status": {
                    "$nin": [
                        ActionLogStatus.PENDING_APPROVAL,
                        ActionLogStatus.SCHEDULED,
                    ]
                },
            }
        )
        if result.deleted_count > 0:
            logger.info(
                f"Removed {result.deleted_count} action "
                f"log{'s' if result.deleted_count != 1 else ''}  as part of "
                f"the automatic cleanup."
            )
    except Exception:
        logger.error(
            "Error occurred while cleaning up logs.",
            details=traceback.format_exc(),
            error_code="CRE_1006",
        )
