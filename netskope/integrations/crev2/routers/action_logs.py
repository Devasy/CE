"""Action logs related APIs."""

import json
from typing import Union
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Security
from pymongo import ASCENDING, DESCENDING
from starlette.responses import JSONResponse
from datetime import datetime

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger, parse_dates

from ..models import (
    ActionBulkAction,
    ActionLogStatus,
    ActionQueryLocator,
    ActionValueLocator,
    RevertActionStatus,
)

connector = DBConnector()
logger = Logger()
router = APIRouter()


@router.get("/logs", tags=["CREv2 Action Logs"])
async def get_logs(
    skip: int = 0,
    limit: int = 10,
    sort: str = None,
    ascending: bool = True,
    aggregate: bool = False,
    filters: str = "{}",
    _: User = Security(get_current_user, scopes=["cre_read"]),
):
    """Get action logs."""
    # TODO: validate the JSON query.
    try:
        filters = json.loads(filters, object_hook=parse_dates)
    except json.decoder.JSONDecodeError as ex:
        raise HTTPException(400, "Invalid JSON query provided.") from ex
    result = list(
        connector.collection(Collections.CREV2_ACTION_LOGS).aggregate(
            (
                (
                    [
                        {
                            "$sort": {
                                sort: (ASCENDING if ascending else DESCENDING)
                            }
                        }
                    ]
                    if sort and not aggregate
                    else []
                )
                + [{"$match": filters}]
                + (
                    [{"$group": {"_id": None, "count": {"$sum": 1}}}]
                    if aggregate
                    else ([{"$skip": skip}] + [{"$limit": limit}])
                )
            ),
            allowDiskUse=True,
        )
    )
    if aggregate:
        if len(result) == 0:
            return JSONResponse(status_code=200, content={"count": 0})
        else:
            return JSONResponse(
                status_code=200, content={"count": result.pop()["count"]}
            )
    else:
        for record in result:
            record["id"] = str(record.pop("_id"))
            record["record"]["_id"] = str(record["record"]["_id"])
        return result


@router.patch("/logs/bulk", tags=["CREv2 Action Logs"])
async def bulk_action_on_logs(
    action: ActionBulkAction,
    logs: Union[ActionQueryLocator, ActionValueLocator],
    _: User = Security(get_current_user, scopes=["cre_write"]),
):
    """Bulk action on logs."""
    find = None
    if isinstance(logs, ActionQueryLocator):
        find = json.loads(logs.query, object_hook=parse_dates)
    elif isinstance(logs, ActionValueLocator):
        find = {"_id": {"$in": list(map(ObjectId, logs.values))}}
    status_filter = (
        {"status": ActionLogStatus.PENDING_APPROVAL}
        if action == ActionBulkAction.APPROVE
        else {
            "status": {
                "$in": [
                    ActionLogStatus.PENDING_APPROVAL,
                    ActionLogStatus.SCHEDULED,
                ]
            }
        }
    )
    find = {
        "$and": [
            find,
            status_filter,
        ]
    }
    result = connector.collection(Collections.CREV2_ACTION_LOGS).update_many(
        find,
        {
            "$set": {
                "status": (
                    ActionLogStatus.SCHEDULED
                    if action == ActionBulkAction.APPROVE
                    else ActionLogStatus.DECLINED
                ),
                "performedAt": datetime.now(),
            }
        },
    )
    logger.info(f"{result.modified_count} action(s) {action.value}d.")
    return {"success": True}


@router.patch("/logs/bulk_revert", tags=["CREv2 Action Logs"])
async def bulk_set_revert_status_on_logs(
    logs: Union[ActionQueryLocator, ActionValueLocator],
    _: User = Security(get_current_user, scopes=["cre_write"]),
):
    """Bulk set revert status on logs."""
    find = None
    if isinstance(logs, ActionQueryLocator):
        find = json.loads(logs.query, object_hook=parse_dates)
    elif isinstance(logs, ActionValueLocator):
        find = {"_id": {"$in": list(map(ObjectId, logs.values))}}
    else:
        return {"success": True}

    find_success_logs = {
        "$and": [
            find,
            {"status": ActionLogStatus.SUCCESS},
            {"revertActionParameters.revertActionStatus": {"$ne": RevertActionStatus.SUCCESS}},
        ]
    }
    success_logs_result = connector.collection(
        Collections.CREV2_ACTION_LOGS
    ).update_many(find_success_logs, {"$set": {"action.performRevert": True}})
    success_count = success_logs_result.modified_count

    decline_status = [
        ActionLogStatus.PENDING_APPROVAL,
        ActionLogStatus.SCHEDULED,
    ]
    find_decline = {"$and": [find, {"status": {"$in": decline_status}}]}
    decline_logs_result = connector.collection(
        Collections.CREV2_ACTION_LOGS
    ).update_many(
        find_decline,
        {
            "$set": {
                "status": ActionLogStatus.DECLINED,
                "action.performRevert": False,
                "revertActionParameters": {
                    "revertActionStatus": RevertActionStatus.SUCCESS,
                    "revertPerformedAt": datetime.now(),
                }
            }
        },
    )
    declined_count = decline_logs_result.modified_count

    if not success_count and not declined_count:
        log_msg = (
            "No action logs will be reverted. "
            "Either the action logs are already reverted or "
            "none of the action logs are in 'Success' state."
        )
    else:
        log_parts = []
        if success_count > 0:
            log_parts.append(f"{success_count} action log(s) will be reverted.")
        if declined_count > 0:
            log_parts.append(
                f"{declined_count} action log(s) will be marked as "
                "Declined as they are in 'Pending Approval' or 'Scheduled' state."
            )

        log_msg = " ".join(log_parts)

    logger.info(log_msg)

    return {"success": True}
