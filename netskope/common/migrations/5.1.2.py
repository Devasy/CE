"""Migrations for 5.1.2 release."""

import traceback
from netskope.common.utils import DBConnector, Logger, Collections
from netskope.common.models.settings import CREWeekDay

connector = DBConnector()
logger = Logger()


def cte_reputation_default():
    """Assign the default reputation to cte configurations."""
    print("Assigning default reputation to cte configurations...")
    try:
        connector.collection(Collections.CONFIGURATIONS).update_many(
            {"reputation": None},
            {"$set": {"reputation": 0}}
        )
    except Exception as e:
        logger.error("Error occurred while assigning default reputation to cte configurations.", traceback.format_exc())
        raise e


def remove_ns_fields_with_dot():
    """Remove ns fields with dot."""
    try:
        connector.collection(Collections.NETSKOPE_FIELDS).delete_many(
            {
                "name": {
                    "$regex": r"\."
                },
            }
        )
    except Exception as error:
        logger.error(
            f"Error occurred while removing fields with dot. Error: {error}",
            details=traceback.format_exc(),
            error_code="CE_1035"
        )
        raise error


def update_maintenance_days():
    """Update maintenance days."""
    print("Updating maintenance days to all days by default...")
    try:
        connector.collection(Collections.SETTINGS).update_one(
            {"cre.maintenanceDays": {"$exists": False}},
            {
                "$set": {
                    "cre.maintenanceDays": [
                        CREWeekDay.MONDAY.value,
                        CREWeekDay.TUESDAY.value,
                        CREWeekDay.WEDNESDAY.value,
                        CREWeekDay.THURSDAY.value,
                        CREWeekDay.FRIDAY.value,
                        CREWeekDay.SATURDAY.value,
                        CREWeekDay.SUNDAY.value,
                    ]
                }
            },
        )
    except Exception as error:
        logger.error(
            f"Error occurred while updating maintenance days. Error: {error}",
            details=traceback.format_exc(),
            error_code="CE_1057",
        )
        raise error


if __name__ == "__main__":
    remove_ns_fields_with_dot()
    cte_reputation_default()
    update_maintenance_days()
