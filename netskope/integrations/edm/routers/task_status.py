"""Provides task status related endpoints."""

from typing import List

from fastapi import APIRouter, Security

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger

from ..models import EDMTask, EDMTaskType

router = APIRouter(prefix="/task_status", tags=["EDM Tasks"])
logger = Logger()
connector = DBConnector()


def _format_rule_as_task(rule: dict) -> EDMTask:
    """Format rule as task needed in response.

    Args:
        rule (dict): business_rule

    Returns:
        EDMTask: EDM Task formatted from business rule
    """
    sharedWith = rule["sharedWith"]
    source_config_name = list(sharedWith.keys())[0]
    destination_config_name = list(sharedWith[source_config_name].keys())[0]
    rule.update(
        {
            "id": rule["name"],
            "name": source_config_name,
            "target": destination_config_name,
            "taskType": EDMTaskType.PLUGIN,
        }
    )
    return EDMTask(**rule)


def _format_csv_config_as_task(config: dict) -> EDMTask:
    """Format Manual upload configuration as task needed in response.

    Args:
        config (dict): edm_manual_upload_configurations table

    Returns:
        EDMTask: EDM Task formatted from manual upload configuration
    """
    # edm_hash_available = False
    sharedWith = config.get("sharedWith", {})
    destination_config_name = list(sharedWith.keys())[0]
    config.update(
        {
            "id": str(config["name"]),
            "name": config["fileName"],
            "target": destination_config_name,
            "taskType": EDMTaskType.MANUAL,
        }
    )
    return EDMTask(**config)


def read_hasing_file(file_path):
    """Read hashing file."""
    with open(file_path, "rb") as f:
        yield from f  # Directly yielding the file object


@router.get("/edm", response_model=List[EDMTask])
async def get_edm_tasks_status(
    _: User = Security(get_current_user, scopes=["edm_read"]),
) -> List[EDMTask]:
    """List all EDM Tasks.

    Returns:
        List[EDMTask]: List of EDM Tasks.
    """
    tasks = []
    rules = connector.collection(Collections.EDM_BUSINESS_RULES).find({})
    for rule in rules:
        tasks.append(_format_rule_as_task(rule))

    manual_tasks = connector.collection(
        Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS
    ).find({})

    for manual_task in manual_tasks:
        tasks.append(_format_csv_config_as_task(manual_task))

    tasks = sorted(tasks, key=lambda task: task.updatedAt, reverse=True)
    return tasks
