"""Plugin related endpoints."""

import json
import traceback
from typing import Union, Any
from fastapi import APIRouter, Security, HTTPException, Depends
from starlette.responses import JSONResponse
from bson import SON
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import OperationFailure
from jsonschema import validate, ValidationError

from netskope.common.utils import (
    DBConnector,
    Collections,
    parse_dates,
    Logger,
    validate_limit,
)
from netskope.common.models import User
from netskope.common.api.routers.auth import get_current_user
from netskope.integrations.itsm.utils import alert_event_query_schema

from ..models import Event, QueryLocator, ValueLocator, validate_query


router = APIRouter()
logger = Logger()
connector = DBConnector()


def get_events(filters, skip, limit, sort, ascending):
    """Get events based on various filters."""
    pipeline = [
        {"$match": filters},
    ]
    if sort is not None and sort in [
        "id",
        "eventType",
        "configuration",
        "user",
        "timestamp"
    ]:
        pipeline.append(
            {"$sort": SON([(sort, ASCENDING if ascending else DESCENDING)])}
        )
    pipeline.append({"$skip": skip})
    pipeline.append({"$limit": limit})
    return connector.collection(Collections.ITSM_EVENTS).aggregate(
        pipeline,
        allowDiskUse=True,
    )


@router.get("/events", tags=["ITSM Events"], dependencies=[Depends(validate_limit)])
async def list_events(
    user: User = Security(get_current_user, scopes=["cto_read"]),
    skip: int = 0,
    limit: int = 10,
    sort: str = None,
    ascending: bool = True,
    aggregate: bool = False,
    filters: str = "{}",
) -> Any:
    """Get list of events."""
    try:
        STATIC_DICT, RAWEVENT_DICT, EVENT_QUERY_SCHEMA = alert_event_query_schema()
        EVENT_STRING_FIELDS = list(STATIC_DICT.keys()) + list(RAWEVENT_DICT.keys())
        out = []
        validate(json.loads(filters), schema=EVENT_QUERY_SCHEMA)
        filters = json.loads(
            filters,
            object_hook=lambda pair: parse_dates(pair, EVENT_STRING_FIELDS),
        )
        if aggregate is False:
            indicator_dicts = get_events(filters, skip, limit, sort, ascending)
            for indicator_dict in indicator_dicts:
                out.append(Event(**indicator_dict))

            return out
        else:
            result = connector.collection(Collections.ITSM_EVENTS).aggregate(
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
            "Error occurred while processing the query for events.",
            details=traceback.format_exc(),
            error_code="CTO_1029",
        )
        raise HTTPException(
            400, "Error occurred while processing the query for events."
        )


@router.delete(
    "/events/bulk",
    tags=["ITSM Events"],
    status_code=201,
    description="Bulk delete events.",
)
async def bulk_delete_events(
    delete: Union[QueryLocator, ValueLocator],
    user: User = Security(get_current_user, scopes=["cto_write"]),
):
    """Bulk delete events."""
    if isinstance(delete, QueryLocator):
        delete_query = validate_query(None, json.loads(delete.query))
        result = connector.collection(Collections.ITSM_EVENTS).delete_many(delete_query)
    elif isinstance(delete, ValueLocator):
        result = connector.collection(Collections.ITSM_EVENTS).delete_many(
            {"id": {"$in": delete.ids}}
        )
    logger.debug(
        f"{result.deleted_count} events(s) are deleted by {user.username} user."
    )
    return {"deleted": result.deleted_count}
