"""Contain Methods for setting status related to task status."""
from datetime import datetime, UTC

from netskope.common.utils import Collections, DBConnector
connector = DBConnector()


def complete_task(id: str):
    """To be called on successfully run whole plugin life cycle on succesfull sharing.

    Cycle will be marked as completed and sharedAt will be set as current date & time.

    Args:
        id (str): cfc_sharing._id generated while creating business rule.
    """
    connector.collection(Collections.CFC_SHARING).update_one(
        {"_id": id},
        {
            "$set": {
                "sharedAt": datetime.now(UTC),
                "updatedAt": datetime.now(UTC),
                "status": "",
            }
        },
    )


def set_task_status(id: str, status):
    """Set status for task being run for CFC.

    Args:
        id (str): cfc_sharing._id generated while creating sharing.
        status (StatusType): Status to be set
    """
    connector.collection(Collections.CFC_SHARING).update_one(
        {"_id": id}, {"$set": {"status": status, "updatedAt": datetime.now(UTC)}}
    )
