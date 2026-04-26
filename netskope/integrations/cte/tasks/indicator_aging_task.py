"""Provides task to handle plugin lifecycle."""

from __future__ import absolute_import, unicode_literals
from datetime import datetime

from netskope.common.celery.main import APP
from netskope.common.models.settings import SettingsDB
from netskope.common.utils import DBConnector, Collections, integration, track, Logger


db_connector = DBConnector()
logger = Logger()


@APP.task(name="cte.age_indicators")
@integration("cte")
@track()
def age_indicators() -> tuple[int, int]:
    """Age out expired indicators from the database.

    Returns:
        int: Number of aged out indicators.
        int: Number of deleted indicators.
    """
    current_time = datetime.now()
    update_result = db_connector.collection(Collections.INDICATORS).update_many(
        {
            "$or": [
                {"expiresAt": {"$eq": None}, "active": True},
                {
                    "expiresAt": {"$ne": None, "$lte": current_time},
                    "active": True,
                },
            ]
        },
        {"$set": {"active": False}},
    )
    settings = SettingsDB(**db_connector.collection(Collections.SETTINGS).find_one({}))
    if settings.cte.deleteInactiveIndicators is True:
        indicators = db_connector.collection(Collections.INDICATORS).delete_many(
            {
                "$and": [
                    {"active": False},
                    {
                        "$or": [
                            {"sources": {"$not": {"$elemMatch": {"retracted": True}}}},
                            {
                                "sources": {
                                    "$elemMatch": {
                                        "retracted": True,
                                        "retractionDestinations": {
                                            "$ne": [],
                                            "$not": {
                                                "$elemMatch": {
                                                    "status": {"$ne": "retracted"}
                                                }
                                            },
                                        },
                                    }
                                }
                            },
                        ]
                    },
                ]
            }
        )
        logger.info(
            f"The count of Deleted Inactive Indicators are {indicators.deleted_count} indicators."
        )
    return update_result.modified_count, (
        indicators.deleted_count if settings.cte.deleteInactiveIndicators is True else 0
    )
