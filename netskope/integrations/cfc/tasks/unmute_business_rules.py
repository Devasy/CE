"""Unmute business rule task."""
from __future__ import absolute_import, unicode_literals

from datetime import datetime, UTC

from netskope.common.celery.main import APP
from netskope.common.utils import Collections, DBConnector, Logger, integration, track

connector = DBConnector()
logger = Logger()


@APP.task(name="cfc.unmute_business_rules")
@integration("cfc")
@track()
def unmute_business_rules():
    """Unmute all the due cfc business rules."""
    current_time = datetime.now(UTC)
    update_result = connector.collection(Collections.CFC_BUSINESS_RULES).update_many(
        {"unmuteAt": {"$ne": None, "$lte": current_time}, "muted": True},
        {"$set": {"muted": False, "unmuteAt": None}},
    )
    if update_result.modified_count > 0:
        logger.debug(f"Unmuted {update_result.modified_count} CFC business rule(s).")
    return update_result.modified_count
