"""Provides task to handle deletoin of records."""

import traceback
from datetime import datetime, timedelta
from netskope.common.celery.main import APP
from netskope.common.utils import (
    DBConnector,
    Collections,
    integration,
    Logger,
    track,
)

db_connector = DBConnector()
logger = Logger()


@APP.task(name="cre.delete_records")
@integration("cre")
@track()
def delete_records() -> int:
    """Delete records from the database that have exceeded the purge time duration.

    Returns:
        int: Number of deleted records.
    """
    try:
        settings = db_connector.collection(Collections.SETTINGS).find_one({})
        delete_lesser_than = datetime.now() - timedelta(
            days=settings.get("cre", []).get("purgeDays")
        )
        total = 0
        for entity in db_connector.collection(Collections.CREV2_ENTITIES).find(
            {}
        ):
            total += (
                db_connector.collection(
                    f"{Collections.CREV2_ENTITY_PREFIX.value}{entity['name']}"
                )
                .delete_many({"lastUpdated": {"$lte": delete_lesser_than}})
                .deleted_count
            )
        logger.info(
            f"Deleted {total} record{'' if total == 1 else 's'} "
            f"as part of the CRE Purge data process."
        )
        return total
    except Exception:
        logger.error(
            "Error occured while deleting records.",
            details=traceback.format_exc(),
            error_code="CRE_1035",
        )
