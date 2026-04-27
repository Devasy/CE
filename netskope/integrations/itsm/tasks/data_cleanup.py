"""Provides task for deleting alerts/events/tasks/notifications."""

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta
import json
import traceback
from netskope.common.celery.main import APP
from netskope.common.models import SettingsDB, PollIntervalUnit
from netskope.common.utils import (
    DBConnector,
    Collections,
    integration,
    Logger,
    parse_dates,
    track,
)
from ..utils import task_query_schema

connector = DBConnector()
logger = Logger()


@APP.task(name="itsm.data_cleanup")
@integration("itsm")
@track()
def data_cleanup():
    """Delete all the alerts/events/tasks/notifications that have exceeded the cleanup time duration."""
    try:
        settings = SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
        delete_alerts_lesser_than = datetime.now() - timedelta(days=settings.alertCleanup)
        result = connector.collection(Collections.ITSM_ALERTS).delete_many(
            {"timestamp": {"$lte": delete_alerts_lesser_than}}
        )
        if result.deleted_count > 0:
            logger.info(
                f"Removed {result.deleted_count} alert{'s' if result.deleted_count != 1 else ''}"
                f" as part of the automatic cleanup."
            )
        delete_events_lesser_than = datetime.now() - timedelta(days=settings.eventCleanup)
        result = connector.collection(Collections.ITSM_EVENTS).delete_many(
            {"timestamp": {"$lte": delete_events_lesser_than}}
        )
        if result.deleted_count > 0:
            logger.info(
                f"Removed {result.deleted_count} event{'s' if result.deleted_count != 1 else ''}"
                f" as part of the automatic cleanup."
            )
        delete_lesser_than = datetime.now() - timedelta(days=settings.ticketsCleanup)
        STATIC_DICT, DATAITEM_DICT, _ = task_query_schema()
        TASK_STRING_FIELDS = list(STATIC_DICT.keys()) + list(DATAITEM_DICT.keys())
        if settings.ticketsCleanupMongo is not None:
            query = json.loads(
                settings.ticketsCleanupMongo,
                object_hook=lambda pair: parse_dates(pair, TASK_STRING_FIELDS),
            )
        else:
            query = {}
        if query:
            result = connector.collection(Collections.ITSM_TASKS).delete_many(
                {
                    "$and": [
                        {"lastUpdatedAt": {"$lte": delete_lesser_than}},
                        query,
                    ]
                }
            )
            if result.deleted_count > 0:
                logger.info(
                    f"Removed {result.deleted_count} ticket{'s' if result.deleted_count != 1 else ''}"
                    f" as part of the automatic cleanup."
                )
        else:
            logger.info("The ticket cleanup query is empty, therefore no tickets will be deleted.")
        # notification cleanUp
        if settings.notificationsCleanupUnit == PollIntervalUnit.HOURS:
            delete_lesser_than = datetime.now() - timedelta(
                hours=settings.notificationsCleanup
            )
        else:
            delete_lesser_than = datetime.now() - timedelta(
                days=settings.notificationsCleanup
            )
        result = connector.collection(Collections.ITSM_TASKS).delete_many(
            {
                "$and": [
                    {"createdAt": {"$lte": delete_lesser_than}},
                    {"status": "notification"},
                ]
            }
        )
        if result.deleted_count > 0:
            logger.info(
                f"Removed {result.deleted_count} notification{'s' if result.deleted_count != 1 else ''}"
                f" as part of the automatic cleanup."
            )
    except Exception:
        logger.error(
            "Error occurred while cleaning up alerts/events/tasks/notifications.",
            details=traceback.format_exc(),
            error_code="CTO_1009",
        )
