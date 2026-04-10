"""Provides logging related endpoints."""

from typing import List
import tempfile
from jsonschema import validate, ValidationError
import json
import traceback
from bson import SON
from fastapi import APIRouter, HTTPException, Security, Depends
from starlette.responses import JSONResponse, FileResponse
from pymongo.errors import OperationFailure
from pymongo import ASCENDING, DESCENDING

from ...utils import DBConnector, Collections, Logger, parse_dates, validate_limit
from . import DATE_STRING_FILTERS
from .auth import get_current_user
from ...models import Log, User
from netskope.common.utils.const import MAX_LOG_COUNT

router = APIRouter()
logger = Logger()
db_connector = DBConnector()

# for case-insensitive search
STRING_FIELDS = ["message"]

QUERY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "definitions": {
        **DATE_STRING_FILTERS,
        "searchRoot": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "message": {"$ref": "#/definitions/stringFilters"},
                "type": {"$ref": "#/definitions/stringFilters"},
                "ce_log_type": {"$ref": "#/definitions/stringFilters"},
                "errorCode": {"$ref": "#/definitions/stringFilters"},
                "details": {"$ref": "#/definitions/stringFilters"},
                "resolution": {"$ref": "#/definitions/stringFilters"},
                "createdAt": {"$ref": "#/definitions/dateFilters"},
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


def aggregate_logs(filters: dict) -> JSONResponse:
    """Get aggregated logs count JSONResponse.

    Args:
        filters (dict): Filters to be applied.

    Returns:
        JSONResponse: JSON response.
    """
    result = db_connector.collection(Collections.LOGS).aggregate(
        [{"$match": filters}, {"$group": {"_id": None, "count": {"$sum": 1}}}]
    )
    result = list(result)
    if len(result) == 0:
        return JSONResponse(status_code=200, content={"count": 0})
    else:
        return JSONResponse(
            status_code=200, content={"count": result.pop()["count"]}
        )


def get_logs(filters, skip, limit, sort, ascending, download):
    """Get logs based on the filters."""
    pipeline = [
        {"$match": filters},
    ]
    if sort is not None and sort in [
        "message",
        "ce_log_type",
        "createdAt",
        "errorCode",
        "resolution",
        "details",
    ]:
        pipeline.append(
            {"$sort": SON([(sort, ASCENDING if ascending else DESCENDING)])}
        )
    pipeline.append({"$skip": skip if download is False else 0})
    pipeline.append({"$limit": limit if download is False else MAX_LOG_COUNT})
    return db_connector.collection(Collections.LOGS).aggregate(
        pipeline,
        allowDiskUse=True,
    )


@router.get(
    "/logs/",
    response_model=List[Log],
    tags=["Logs"],
    description="Read logs.",
    dependencies=[Depends(validate_limit)]
)
async def read_logs(
    user: User = Security(  # NOSONAR: S107 Ignoring as these are query params in GET request
        get_current_user, scopes=["logs"]
    ),
    skip: int = 0,
    limit: int = 10,
    sort: str = None,
    ascending: bool = True,
    aggregate: bool = False,
    filters: str = "{}",
    download: bool = False,
):
    """Get list of logs.

    Args:
        skip (int, optional): Number of logs to skip. Defaults to 0.
        limit (int, optional): Number of logs to limit. Defaults to 10.
        aggregate (bool, optional): Set true if only count is required. Defaults to False.
        filters (str, optional): JSON string of filters to be applied. Defaults to "{}".

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        List[Log]: List of logs matching the given query.
    """
    try:
        out = []
        validate(json.loads(filters), schema=QUERY_SCHEMA)
        filters = json.loads(
            filters, object_hook=lambda pair: parse_dates(pair, STRING_FIELDS)
        )
        if aggregate is False:
            logs_dict = get_logs(
                filters, skip, limit, sort, ascending, download
            )
            if download:
                (mode, temp) = tempfile.mkstemp(".txt", "ncte_")
                nl = "\n"
                with open(temp, "w") as temp_file:
                    for logs_dict in logs_dict:
                        log_type = logs_dict.get('ce_log_type', logs_dict.get('type', ''))
                        temp_file.write(
                            f"[{logs_dict['createdAt']}Z] - [{logs_dict.get('errorCode', None)}] - "
                            f"[{log_type}] "
                            f"{logs_dict['message']}{nl}{'Resolution: ' + logs_dict.get('resolution', None) if logs_dict.get('resolution', None) else ''}{nl + 'Details: ' + nl + logs_dict.get('details', None) if logs_dict.get('details', None) else ''}\n"  # noqa
                        )
                return FileResponse(
                    temp,
                    headers={
                        "Content-Disposition": "attachment; filename=cte.log"
                    },
                )
            else:
                for log_dict in logs_dict:
                    out.append(
                        Log(
                            id=str(log_dict["_id"]),
                            message=log_dict["message"],
                            createdAt=log_dict["createdAt"],
                            ce_log_type=log_dict.get("ce_log_type", log_dict.get("type")),
                            errorCode=log_dict.get("errorCode", None),
                            details=log_dict.get("details", None),
                            resolution=log_dict.get("resolution", None),
                        )
                    )
                return out
        else:
            return aggregate_logs(filters)
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
            error_code="CE_1001",
        )
        raise HTTPException(400, "Error occurred while processing the query.")
