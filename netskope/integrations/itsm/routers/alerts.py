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

from netskope.common.utils import DBConnector, Collections, parse_dates, Logger, validate_limit
from netskope.common.models import User
from netskope.common.api.routers.auth import get_current_user
from netskope.integrations.itsm.utils import alert_event_query_schema

from ..models import Alert, QueryLocator, ValueLocator, validate_query


router = APIRouter()
logger = Logger()
connector = DBConnector()


def get_alerts(filters, skip, limit, sort, ascending):
    """Get alerts based on various filters."""
    pipeline = [
        {"$match": filters},
    ]
    if sort is not None and sort in [
        "id",
        "alertName",
        "alertType",
        "configuration",
        "app",
        "appCategory",
        "user",
        "type",
        "timestamp",
    ]:
        pipeline.append({"$sort": SON([(sort, ASCENDING if ascending else DESCENDING)])})
    pipeline.append({"$skip": skip})
    pipeline.append({"$limit": limit})
    return connector.collection(Collections.ITSM_ALERTS).aggregate(
        pipeline,
        allowDiskUse=True,
    )


@router.get(
    "/alerts",
    tags=["ITSM Alerts"],
    dependencies=[Depends(validate_limit)]
)
async def list_alerts(
    user: User = Security(get_current_user, scopes=["cto_read"]),
    skip: int = 0,
    limit: int = 10,
    sort: str = None,
    ascending: bool = True,
    aggregate: bool = False,
    filters: str = "{}",
) -> Any:
    """Get list of alerts."""
    try:
        STATIC_DICT, RAWALERT_DICT, ALERT_QUERY_SCHEMA = alert_event_query_schema()
        ALERT_STRING_FIELDS = list(STATIC_DICT.keys()) + list(RAWALERT_DICT.keys())
        out = []
        validate(json.loads(filters), schema=ALERT_QUERY_SCHEMA)
        filters = json.loads(
            filters,
            object_hook=lambda pair: parse_dates(pair, ALERT_STRING_FIELDS),
        )
        if aggregate is False:
            indicator_dicts = get_alerts(filters, skip, limit, sort, ascending)
            for indicator_dict in indicator_dicts:
                out.append(Alert(**indicator_dict))

            return out
        else:
            result = connector.collection(Collections.ITSM_ALERTS).aggregate(
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
                return JSONResponse(status_code=200, content={"count": result.pop()["count"]})
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
            error_code="CTO_1000",
        )
        raise HTTPException(400, "Error occurred while processing the query.")


@router.delete(
    "/alerts/bulk",
    tags=["ITSM Alerts"],
    status_code=201,
    description="Bulk delete alerts.",
)
async def bulk_delete_alerts(
    delete: Union[QueryLocator, ValueLocator],
    user: User = Security(get_current_user, scopes=["cto_write"]),
):
    """Bulk delete alerts."""
    if isinstance(delete, QueryLocator):
        delete_query = validate_query(None, json.loads(delete.query))
        result = connector.collection(Collections.ITSM_ALERTS).delete_many(delete_query)
    elif isinstance(delete, ValueLocator):
        result = connector.collection(Collections.ITSM_ALERTS).delete_many({"id": {"$in": delete.ids}})
    logger.debug(f"{result.deleted_count} alert(s) are deleted by {user.username} user.")
    return {"deleted": result.deleted_count}
