"""Provides task status related endpoints."""

import traceback
from typing import List, Union

from fastapi import APIRouter, HTTPException, Security
from bson.objectid import ObjectId

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger

from ..models import (
    CFCTaskType,
    CFCPluginTask,
    CFCManualTask,
    FileStatus,
    StatusType,
)


router = APIRouter(prefix="/task_status", tags=["CFC Tasks"])
logger = Logger()
db_connector = DBConnector()


def _format_sharing(sharing) -> CFCPluginTask:
    """Get formatted sharing configuration as a task."""
    return CFCPluginTask(
        **{
            "id": str(sharing["_id"]),
            "source": sharing["sourceConfiguration"],
            "target": sharing["destinationConfiguration"],
            "status": sharing["status"],
            "updatedAt": sharing["updatedAt"],
            "sharedAt": sharing.get("sharedAt", None),
            "createdAt": sharing["createdAt"]
        }
    )


@router.get("/{taskType}", response_model=List[Union[CFCManualTask, CFCPluginTask]])
async def get_tasks_status(
    taskType: CFCTaskType = CFCTaskType.PLUGIN,
    _: User = Security(get_current_user, scopes=["cfc_read"]),
) -> List[Union[CFCManualTask, CFCPluginTask]]:
    """List all CFC Tasks.

    Returns:
        Union[List[CFCPluginTask], List[CFCManualTask]]: List of CFC Plugin or Manual Tasks.
    """
    try:
        if taskType == CFCTaskType.PLUGIN:
            sharings = db_connector.collection(Collections.CFC_SHARING).find({})
            return list(map(_format_sharing, sharings))
        else:
            configs = db_connector.collection(
                Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
            ).find({})
            tasks = []
            for config in configs:
                status = config.get("status", StatusType.PENDING)
                task = {
                    "id": str(config.get("_id")),
                    "source": config.get("name"),
                    "target": config.get("destinationConfiguration"),
                    "status": status,
                    "files": config.get("files", []),
                    "classifierName": config.get("classifierName"),
                    "trainingType": config.get("trainingType"),
                    "sharedAt": (
                        config.get("updatedAt")
                        if status
                        in (
                            StatusType.COMPLETED,
                            StatusType.PARTIALLY_COMPLETED,
                            StatusType.SUCCESS,
                            StatusType.PARTIAL_SUCCESS,
                        )
                        else None
                    ),
                    "has_failed_uploads": (
                        True
                        if status
                        in (
                            StatusType.FAILED,
                            StatusType.PARTIALLY_COMPLETED,
                            StatusType.PARTIAL_SUCCESS,
                        )
                        else False
                    ),
                    "createdAt": config.get("createdAt")
                }
                tasks.append(task)
            return tasks

    except Exception as error:
        error_message = (
            f"Error occurred while retrieving the list of tasks of type '{taskType.value}'."
        )
        logger.error(
            error_message,
            details=traceback.format_exc(),
            error_code="CFC_1036",
        )
        raise HTTPException(500, error_message) from error


@router.get("/uploads/{id}", response_model=List[FileStatus])
async def get_uploads(
    id: str,
    _: User = Security(get_current_user, scopes=["cfc_read"]),
) -> List[FileStatus]:
    """List all CFC Tasks.

    Returns:
        List[FileStatus]: List of files uploaded with its status and last updated time.
    """
    try:
        config = db_connector.collection(
            Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
        ).find_one({"_id": ObjectId(id)})
        if not config:
            raise HTTPException(404, "Manual upload configuration not found.")
        return config.get("files", [])
    except Exception as error:
        error_message = "Error occurred while retrieving the list of uploaded files."
        logger.error(
            error_message,
            details=traceback.format_exc(),
            error_code="CFC_1031",
        )
        raise HTTPException(500, error_message) from error
