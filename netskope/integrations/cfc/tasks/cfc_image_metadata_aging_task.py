"""Task to age out CFC image metadata."""

import traceback
from datetime import datetime, timedelta, UTC

from netskope.common.celery.main import APP
from netskope.common.models import SettingsDB
from netskope.common.utils import DBConnector, Collections, integration, Logger, track

db_connector = DBConnector()
logger = Logger()


@APP.task(name="cfc.age_cfc_image_metadata")
@integration("cfc")
@track()
def age_cfc_image_metadata():
    """Age out expired CFC image metadata that are collected as part of the plugin lifecycle."""
    try:
        settings = SettingsDB(
            **db_connector.collection(Collections.SETTINGS).find_one({})
        )
        expiry_time = datetime.now(UTC) - timedelta(days=settings.cfc.cfcImageMetadataCleanup)
        deletion_result = db_connector.collection(Collections.CFC_IMAGES_METADATA).delete_many(
            {"lastFetched": {"$lt": expiry_time}}
        )
        logger.info(
            message=f"Deleted {deletion_result.deleted_count} CFC image metadata as part of the automatic cleanup.",
        )
    except Exception as error:
        logger.error(
            message=f"Error: '{error}' occurred while aging out CFC image metadata.",
            details=traceback.format_exc(),
            error_code="CFC_1033",
        )
