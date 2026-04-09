"""Plugin related endpoints."""

import traceback
import json
from typing import Any, Union
from datetime import datetime

from bson import SON, ObjectId
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Security
from jsonschema import ValidationError, validate
from netskope.common.api.routers.auth import get_current_user
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import User
from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
    PluginHelper,
    parse_dates,
    validate_limit,
)
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import OperationFailure
from starlette.responses import JSONResponse

from ..models import (
    DedupeRule,
    Filters,
    Task,
    TaskBulkAction,
    TaskQueryLocator,
    TaskRequestStatus,
    TaskStatus,
    TaskValueLocator,
)
from ..models.task import validate_query
from ..tasks.retry import retry_tickets
from ..utils import task_query_schema

router = APIRouter()
logger = Logger()
connector = DBConnector()
helper = PluginHelper()


def get_tasks(filters, skip, limit, sort, ascending):
    """Get tasks based on various filters."""
    pipeline = [
        {"$match": filters},
    ]
    if sort is not None and sort in [
        "id",
        "status",
        "dedupeCount",
        "dataItem",
        "link",
        "businessRule",
        "configuration",
        "dataSubType",
        "severity",
        "approvalStatus",
        "lastUpdatedAt",
    ]:
        pipeline.append(
            {"$sort": SON([(sort, ASCENDING if ascending else DESCENDING)])}
        )
    pipeline.append({"$skip": skip})
    pipeline.append({"$limit": limit})
    return connector.collection(Collections.ITSM_TASKS).aggregate(
        pipeline,
        allowDiskUse=True,
    )


@router.get("/tasks", tags=["CTO Tasks"], dependencies=[Depends(validate_limit)])
async def list_tasks(
    user: User = Security(get_current_user, scopes=["cto_read"]),
    skip: int = 0,
    limit: int = 10,
    sort: str = None,
    ascending: bool = True,
    aggregate: bool = False,
    filters: str = "{}",
) -> Any:
    """Get list of tasks."""
    try:
        out = []
        STATIC_DICT, DATAITEM_DICT, TASK_QUERY_SCHEMA = task_query_schema()
        validate(json.loads(filters), schema=TASK_QUERY_SCHEMA)
        TASK_STRING_FIELDS = list(STATIC_DICT.keys()) + list(DATAITEM_DICT.keys())
        filters = json.loads(
            filters,
            object_hook=lambda pair: parse_dates(pair, TASK_STRING_FIELDS),
        )
        if aggregate is False:
            task_dicts = get_tasks(filters, skip, limit, sort, ascending)
            for task_dict in task_dicts:
                out.append(Task(**task_dict))
            return out
        else:
            result = connector.collection(Collections.ITSM_TASKS).aggregate(
                [
                    {"$match": filters},
                    {"$group": {"_id": None, "count": {"$sum": 1}}},
                ],
                allowDiskUse=True,
            )
            result = list(result)
            if len(result) == 0:
                return JSONResponse(status_code=200, content={"count": 0})
            else:
                return JSONResponse(
                    status_code=200, content={"count": result.pop()["count"]}
                )
    except json.decoder.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON query provided.")
    except ValidationError as ex:
        raise HTTPException(400, f"Invalid query provided. {ex.message}.")
    except OperationFailure as ex:
        raise HTTPException(400, f"{ex}")
    except Exception:
        logger.debug(
            "Error occurred while processing the query.",
            details=traceback.format_exc(),
            error_code="CTO_1008",
        )
        raise HTTPException(400, "Error occurred while processing the query.")


@router.get("/task/{id}", tags=["CTO Tasks"])
async def get_task_by_id(
    id: str = Path(...),
    user: User = Security(get_current_user, scopes=["cto_read"]),
) -> Task:
    """Get task by ID."""
    task = connector.collection(Collections.ITSM_TASKS).find_one({"id": id})
    if task is None:
        raise HTTPException(404, f"Could not find task with id {id}.")
    return Task(**task)


@router.patch("/task/{id}/mute")
async def create_mute_rule(
    id: str = Path(...),
    fields: str = Query(...),
    user: User = Security(get_current_user, scopes=["cto_write"]),
):
    """Mute business rule by task."""
    task = connector.collection(Collections.ITSM_TASKS).find_one({"id": id})
    if task is None:
        raise HTTPException(400, "Task no longer exists on CTO.")
    task = Task(**task)
    data_item = task.dataItem
    fields = fields.split(",")
    values = {}
    for field in fields:
        if getattr(data_item, field, None) is not None:
            values[field] = getattr(data_item, field, None)
        elif data_item.rawData.get(field, None) is not None:
            values[f"rawData_{field}"] = data_item.rawData.get(field, None)
    # alertName Is equal "test" && alertType Is equal "another"
    query = []
    mongo = {"$and": []}
    for key, value in values.items():
        query.append(f'{key} Is equal "{value}"')
        mongo["$and"].append({f"{key}": {"$eq": f"{value}"}})
    query = " && ".join(query)
    rule = DedupeRule(
        name=f"Created from ServiceNow on {', '.join(fields)}",
        filters=Filters(query=query, mongo=json.dumps(mongo)),
    )
    result = connector.collection(Collections.ITSM_BUSINESS_RULES).update_one(
        {"name": task.businessRule}, {"$push": {"muteRules": rule.model_dump()}}
    )
    if result.matched_count < 1:
        raise HTTPException(400, "Business rule no longer exists on CTO.")
    else:
        return {"success": True}


@router.delete(
    "/tasks/bulk",
    tags=["CTO Tasks"],
    status_code=200,
    description="Bulk delete tasks.",
)
async def bulk_delete_tasks(
    delete: Union[TaskQueryLocator, TaskValueLocator],
    user: User = Security(get_current_user, scopes=["cto_write"]),
):
    """Bulk delete tasks."""
    final_query = None
    if isinstance(delete, TaskQueryLocator):
        final_query = validate_query(
            None, json.loads(delete.query)
        )
    elif isinstance(delete, TaskValueLocator):
        final_query = {"_id": {"$in": list(map(ObjectId, delete.ids))}}

    result = connector.collection(Collections.ITSM_TASKS).delete_many(final_query)
    logger.debug(
        f"{result.deleted_count} ticket(s) are deleted by {user.username} user."
    )
    if result.deleted_count == 0:
        raise HTTPException(400, "There are no tickets to delete.")
    return {"deleted": result.deleted_count}


@router.post(
    "/tasks/create",
    tags=["CTO Tasks"],
    status_code=200,
    description="Bulk retry tasks.",
)
async def retry_tasks(
    tasks: Union[TaskQueryLocator, TaskValueLocator],
    user: User = Security(get_current_user, scopes=["cto_write"]),
):
    """Create tasks for failed tasks."""
    try:
        ids = []
        id_counter = 0
        if isinstance(tasks, TaskQueryLocator):
            tasks.query = validate_query(None, json.loads(tasks.query))
            total_count = connector.collection(Collections.ITSM_TASKS).count_documents(tasks.query)
            tasks.query["approvalStatus"] = {
                "$in": [
                    TaskRequestStatus.APPROVED.value,
                    TaskRequestStatus.NOT_REQUIRED.value,
                ]
            }
            tasks.query["status"] = TaskStatus.FAILED.value
            for task in connector.collection(Collections.ITSM_TASKS).find(tasks.query):
                id_counter += 1
                ids.append(task.get("_id"))
        elif isinstance(tasks, TaskValueLocator):
            total_count = len(tasks.ids)
            for task in connector.collection(Collections.ITSM_TASKS).find(
                {
                    "_id": {"$in": [ObjectId(id) for id in tasks.ids]},
                    "approvalStatus": {
                        "$in": [
                            TaskRequestStatus.APPROVED.value,
                            TaskRequestStatus.NOT_REQUIRED.value,
                        ]
                    },
                    "status": TaskStatus.FAILED.value,
                }
            ):
                id_counter += 1
                ids.append(task.get("_id"))

        logger.info(
            message=f"{total_count - id_counter} out of {total_count} tickets will be "
            "skipped while retrying the ticket creation."
        )
        if not ids:
            return {"success": True}
        execute_celery_task(
            retry_tickets.apply_async,
            "itsm.retry_tickets",  # add task to queue.
            args=[ids],
        )
        return {"success": True}
    except Exception:
        logger.error(
            "Error occurred while retrying the ticket creation process.",
            error_code="CTO_1008",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            400, "Error occurred while retrying the ticket creation process."
        )


@router.patch("/tasks/bulk", description="Bulk action on tasks.", tags=["CTO Tasks"])
async def bulk_action_on_tasks(
    action: TaskBulkAction,
    tasks: Union[TaskQueryLocator, TaskValueLocator],
    _: User = Security(get_current_user, scopes=["cre_write"]),
):
    """Bulk action on tasks."""
    find = None
    if isinstance(tasks, TaskQueryLocator):
        find = validate_query(None, json.loads(tasks.query))
    elif isinstance(tasks, TaskValueLocator):
        find = {"_id": {"$in": list(map(ObjectId, tasks.ids))}}
    additional_status = (
        [TaskRequestStatus.SCHEDULED.value] if action == TaskBulkAction.DECLINE else []
    )
    find = {
        "$and": [
            find,
            {
                "approvalStatus": {
                    "$in": [
                        TaskRequestStatus.PENDING.value,
                    ]
                    + additional_status
                }
            },
        ]
    }
    result = connector.collection(Collections.ITSM_TASKS).update_many(
        find,
        {
            "$set": {
                "approvalStatus": (
                    TaskRequestStatus.SCHEDULED.value
                    if action == TaskBulkAction.APPROVE
                    else TaskRequestStatus.DECLINED.value
                ),
                "lastUpdatedAt": datetime.now(),
            }
        },
    )
    logger.info(f"{result.modified_count} ticket(s) {action.value}d.")
    return {"success": True}
