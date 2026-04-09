"""Provides Task Status related endpoints."""

import traceback
from typing import List
from fastapi import APIRouter, HTTPException, Security, Depends
from bson import SON
import json
from pymongo import ASCENDING, DESCENDING
from jsonschema import validate, ValidationError
from pymongo.errors import OperationFailure
from starlette.responses import JSONResponse
from ...utils import DBConnector, Collections, Logger, parse_dates, validate_limit
from .auth import get_current_user
from ...models import User, TaskStatus
from . import DATE_STRING_FILTERS

logger = Logger()
router = APIRouter()
db_connector = DBConnector()

STRING_FIELDS = ["message"]


QUERY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "definitions": {
        **DATE_STRING_FILTERS,
        "searchRoot": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"$ref": "#/definitions/stringFilters"},
                "status": {"$ref": "#/definitions/stringFilters"},
                "startedAt": {"$ref": "#/definitions/dateFilters"},
                "completedAt": {"$ref": "#/definitions/dateFilters"},
                "args": {"$ref": "#/definitions/stringFilters"},
                "$and": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/searchRoot"},
                },
                "$or": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/searchRoot"},
                },
                "$nor": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/searchRoot"},
                },
            },
        },
    },
    "type": "object",
    "$ref": "#/definitions/searchRoot",
    "additionalProperties": False,
}


def aggregate_taskstatus(filters: dict) -> JSONResponse:
    """Get aggregated Task Status count JSONResponse.

    Args:
        filters (dict): Filters to be applied.

    Returns:
        JSONResponse: JSON response.
    """
    result = db_connector.collection(Collections.TASK_STATUS).aggregate(
        [{"$match": filters}, {"$group": {"_id": None, "count": {"$sum": 1}}}]
    )
    result = list(result)
    if len(result) == 0:
        return JSONResponse(status_code=200, content={"count": 0})
    else:
        return JSONResponse(
            status_code=200, content={"count": result.pop()["count"]}
        )


def get_status(filters, skip, limit, sort, ascending):
    """Get task status based on the filters."""
    pipeline = [
        {"$match": filters},
    ]
    if sort is not None and sort in [
        "name",
        "status",
        "startedAt",
        "completedAt",
    ]:
        pipeline.append(
            {"$sort": SON([(sort, ASCENDING if ascending else DESCENDING)])}
        )
    pipeline.append({"$skip": skip})
    pipeline.append({"$limit": limit})
    return db_connector.collection(Collections.TASK_STATUS).aggregate(
        pipeline,
        allowDiskUse=True,
    )


@router.get(
    "/taskstatus/",
    response_model=List[TaskStatus],
    tags=["TaskStatus"],
    description="Read Task Status.",
    dependencies=[Depends(validate_limit)]
)
async def read_taskstatus(
    user: User = Security(get_current_user, scopes=["logs"]),
    skip: int = 0,
    limit: int = 10,
    sort: str = None,
    ascending: bool = True,
    aggregate: bool = False,
    filters: str = "{}",
):
    """Get list of task status.

    Args:
        skip (int, optional): Number of logs to skip. Defaults to 0.
        limit (int, optional): Number of logs to limit. Defaults to 10.
        aggregate (bool, optional): Set true if only count is required. Defaults to False.
        filters (str, optional): JSON string of filters to be applied. Defaults to "{}".

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        List[TaskStatus]: List of status matching the given query.
    """
    try:
        out = []
        validate(json.loads(filters), schema=QUERY_SCHEMA)
        filters = json.loads(
            filters, object_hook=lambda pair: parse_dates(pair, STRING_FIELDS)
        )
        if aggregate is False:
            tasks_dict = get_status(filters, skip, limit, sort, ascending)
            for task_dict in tasks_dict:
                out.append(TaskStatus(**task_dict))
            return out
        else:
            return aggregate_taskstatus(filters)

    except json.decoder.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON query provided.")
    except ValidationError as ex:
        raise HTTPException(400, f"Invalid query provided. {ex.message}.")
    except OperationFailure as ex:
        raise HTTPException(400, f"{ex}")
    except Exception:
        logger.error(
            "Error occurred while processing the query.",
            details=traceback.format_exc(),
            error_code="CE_1047",
        )
        raise HTTPException(400, "Error occurred while processing the query.")
