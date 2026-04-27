"""Provides task for unmuting business rules."""

from __future__ import absolute_import, unicode_literals
from datetime import datetime

from netskope.common.celery.main import APP
from netskope.common.utils import (
    DBConnector,
    Collections,
    integration,
    Logger,
    track,
)


connector = DBConnector()
logger = Logger()


@APP.task(name="itsm.unmute")
@integration("itsm")
@track()
def unmute_business_rule():
    """Unmute all the due business rules."""
    current_time = datetime.now()
    update_result = connector.collection(
        Collections.ITSM_BUSINESS_RULES
    ).update_many(
        {"unmuteAt": {"$ne": None, "$lte": current_time}, "muted": True},
        {"$set": {"muted": False, "unmuteAt": None}},
    )
    if update_result.modified_count > 0:
        logger.debug(
            f"Unmuted {update_result.modified_count} business rule(s)."
        )
    return update_result.modified_count
