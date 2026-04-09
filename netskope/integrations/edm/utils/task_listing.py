"""Contain Methods for setting status related to task status."""

from datetime import datetime, UTC

from netskope.common.utils import Collections, DBConnector
from netskope.integrations.edm.models.task_status import StatusType

connector = DBConnector()


def complete_task(rule_name: str):
    """To be called on successfully run whole plugin life cycle on succesfull sharing.

    Cycle will be marked as completed and sharedAt will be set as current date & time.

    Args:
        rule_name (str): business_rule.name generated while creating business rule.
    """
    connector.collection(Collections.EDM_BUSINESS_RULES).update_one(
        {"name": rule_name},
        {
            "$set": {
                "sharedAt": datetime.now(UTC),
                "updatedAt": datetime.now(UTC),
                "status": StatusType.UPLOAD_COMPLETED,
            }
        },
    )


def set_task_status(rule_name: str, status: StatusType):
    """Set status for task being run for EDM.

    Args:
        rule_name (str): business_rule.name generated while creating business rule.
        status (StatusType): Status to be set
    """
    connector.collection(Collections.EDM_BUSINESS_RULES).update_one(
        {"name": rule_name}, {"$set": {"status": status, "updatedAt": datetime.now(UTC)}}
    )


def set_task_status_by_config_name(
    config_name: str, status: StatusType, destination_config_name: str = None
):
    """Set status of all sharings by searching with config_name in sources.

    Args:
        config_name (str): EDM configuration name
        status (StatusType): Status to be set
    """
    rules = connector.collection(Collections.EDM_BUSINESS_RULES).find({})
    for rule in rules:
        if config_name in rule["sharedWith"]:
            change = True
            if (
                destination_config_name
                and destination_config_name not in rule["sharedWith"][config_name]
            ):
                change = False
            if change:
                set_task_status(rule["name"], status)


def change_manual_task_status(
    config_name: str,
    status: StatusType = StatusType.COMPLETED,
    update_shared_at: bool = False,
):
    """Set status for manual upload configuration.

    Args:
        config_name (str): name of manual upload configuration
        status (StatusType, optional): status of task. Defaults to StatusType.COMPLETED.
        update_shared_at (bool, optional): to update sharedAt time or not. Defaults to False.
    """
    if update_shared_at and status == StatusType.UPLOAD_COMPLETED:
        connector.collection(Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS).update_one(
            {"name": config_name},
            {
                "$set": {
                    "sharedAt": datetime.now(UTC),
                    "updatedAt": datetime.now(UTC),
                    "status": status,
                }
            },
        )
    else:
        connector.collection(Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS).update_one(
            {"name": config_name},
            {
                "$set": {
                    "updatedAt": datetime.now(UTC),
                    "status": status,
                }
            },
        )
