"""Provides indicator related endpoints."""

from typing import List, Union
from jsonschema import validate, ValidationError
import json
import traceback
from bson import SON
from fastapi import APIRouter, HTTPException, Security, Query, Depends
from starlette.responses import JSONResponse
from pymongo.errors import OperationFailure
from pymongo import ASCENDING, DESCENDING

from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
    parse_dates,
    validate_limit,
)
from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.celery.scheduler import execute_celery_task

from ..tasks.plugin_lifecycle_task import (
    insert_or_update_indicator,
    validate_iocs,
    get_possible_destinations,
)
from ..tasks.share_indicators import share_indicators
from ..utils.schema import (
    INDICATOR_QUERY_SCHEMA as QUERY_SCHEMA,
    INDICATOR_STRING_FIELDS as STRING_FIELDS,
)
from ..models import IndicatorIn, ConfigurationDB
from ..models.indicator import (
    IndicatorBulkEdit,
    IndicatorOutWithSources,
    IndicatorQueryLocator,
    IndicatorValueLocator,
    RetractionUpdate,
)
from ..models.tags import TagIn
from .tags import create_tag

router = APIRouter()
logger = Logger()
db_connector = DBConnector()

read_indicators_description = """
Use this endpoint to list out indicators.

The `filter` query parameter supports a subset of Mongo type queries on all the
available attributes in the response. Supported operators are dependent on the
attribute type.

Following is the list of available attributes:
`value`, `type`, `test`, `active`, `internalHits`, `externalHits`, `expiresAt`,
`source`, `sharedWith`, `firstSeen`, `lastSeen`, `reputation`, `comments`


These are some of the example queries:

**Find all the indicators with reputation greater than 5 and externalHits more
than 10.**

```
{
    "$and": [
        "reputation": {
            "$gt": 5
        },
        "externalHits": {
            "$gt": 10
        }
    ]
}
```

**Find all indicators that have been shared with "Netskope Partners" configuration.**

```
{
    "sharedWith": {
        "$in": ["Netskope Partners"]
    }
}
```

**Not operator is only supported for regex matches.**

```
{
    "value": {
        "$not": "^(db.tt)"
    }
}
```

In the example showen above, the string `"^(db.tt)"` will always be converted to a regex.

*Note: The value passed to the filter query parameter must be a valid JSON object.
As JSON does not have support for datetime objects, any strings matching the 1970-01-01T00:00:00+0000 format
will automatically be converted to datetime objects.*

Refer to [Mongo's query documentation](https://docs.mongodb.com/manual/tutorial/query-documents/)
to read more about building Mongo queries.
"""


def get_indicators(filters, skip, limit, sort, ascending):
    """Get indicators based on various filters."""
    pipeline = [
        {"$match": filters},
    ]
    if sort is not None and sort in [
        "value",
        "type",
        "sources.source",
        "sources.comments",
        "test",
        "active",
        "internalHits",
        "externalHits",
        "sources.reputation",
        "sources.firstSeen",
        "sources.severity",
        "sources.lastSeen",
        "expiresAt",
    ]:
        pipeline.append(
            {"$sort": SON([(sort, ASCENDING if ascending else DESCENDING)])}
        )
    pipeline.append({"$skip": skip})
    pipeline.append({"$limit": limit})
    return db_connector.collection(Collections.INDICATORS).aggregate(
        pipeline,
        allowDiskUse=True,
    )


async def create_tag_helper(indicator):
    """Create a tag if it doesn't already exist.

    Args:
        indicator (IndicatorIn): IndicatorIn Model.

    Raises:
        HTTPException: If tag name has more than 50 characters.

    Returns:
        List[str]: Return list of tags which can associated with the indicator.
    """
    list_of_available_tags = []
    indicator.tags = list(set(indicator.tags))  # unique tags
    created_tags = []
    for tag_info in db_connector.collection(Collections.TAGS).find({}, {"name": 1}):
        list_of_available_tags.append(tag_info["name"])
    for tag_name in indicator.tags:
        tag_name = tag_name.strip()
        if len(tag_name) == 0:
            continue
        elif tag_name and (tag_name not in list_of_available_tags):
            tagin = TagIn(name=tag_name, color="#969696")
            list_of_available_tags.append(tag_name)
            created_tags.append(tag_name)
            await create_tag(tagin)
        else:
            created_tags.append(tag_name)
    return created_tags


@router.get(
    "/migration_status/",
    description="To check the status of migration of CTE Indicators",
)
async def migration_status(
    user: User = Security(get_current_user, scopes=["cte_read"]),
):
    """Get the migration status."""
    result = db_connector.collection(Collections.SETTINGS).find_one({})
    migration_status = result.get("migration_status", True)
    return {"migration_status": migration_status}


@router.get(
    "/indicators/",
    response_model=List[IndicatorOutWithSources],
    tags=["Indicators"],
    description=read_indicators_description,
    dependencies=[Depends(validate_limit)],
)
async def read_indicators(
    user: User = Security(get_current_user, scopes=["cte_read"]),
    skip: int = 0,
    limit: int = 10,
    sort: str = None,
    ascending: bool = True,
    aggregate: bool = False,
    filters: str = "{}",
):
    """Get list of indicators.

    Args:
        skip (int, optional): Number of indicators to skip. Defaults to 0.
        limit (int, optional): Number of indicators to limit. Defaults to 10.
        aggregate (bool, optional): Set true if only count is required. Defaults to False.
        filters (str, optional): JSON string of filters to be applied. Defaults to "{}".

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        List[IndicatorOutWithSources]: List of indicators matching the given query.
    """
    try:
        out = []
        validate(json.loads(filters), schema=QUERY_SCHEMA)
        filters = json.loads(
            filters, object_hook=lambda pair: parse_dates(pair, STRING_FIELDS)
        )
        if aggregate is False:
            indicator_dicts = get_indicators(filters, skip, limit, sort, ascending)
            for indicator_dict in indicator_dicts:
                out.append(IndicatorOutWithSources(**indicator_dict))

            return out
        else:
            result = db_connector.collection(Collections.INDICATORS).aggregate(
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
            error_code="CTE_1015",
        )
        raise HTTPException(400, "Error occurred while processing the query.")


@router.get(
    "/dashboards/",
    tags=["Indicators"],
)
async def aggregate_indicators(
    user: User = Security(get_current_user, scopes=["cte_read"]),
    limit: int = 10,
    count_by: str = "",
    group_by: List[str] = Query(None),
    ascending: bool = False,
    filters: str = "{}",
):
    """Get aggregated results from indicators.

    Args:
        limit (int, optional): Number of aggregation results to limit.
        Defaults to 10.
        count_by (str, optional): Field name to be used from IndicatorDB as
        count for aggregation. Defaults to 1.
        group_by (list, optional): List of field names to be used for grouping
        the records for aggregation. Defaults to [].
        ascending (bool, optional): Flag to denote sorting order.
        Defaults to False.
        filters (str, optional): JSON string of filters to be applied.
        Defaults to "{}".

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        JSONResponse: JSONResponse object containing aggregate results.
    """
    try:
        validate(json.loads(filters), schema=QUERY_SCHEMA)
        filters = json.loads(
            filters, object_hook=lambda pair: parse_dates(pair, STRING_FIELDS)
        )
        valid_group_fields = [
            "value",
            "type",
            "sources.source",
            "sources.comments",
            "test",
            "active",
            "internalHits",
            "externalHits",
            "sources.reputation",
            "sources.firstSeen",
            "sources.lastSeen",
            "expiresAt",
        ]

        valid_count_fields = [
            "internalHits",
            "externalHits",
        ]

        group_id = {}
        if group_by:
            if all(fld in valid_group_fields for fld in group_by):
                for fld in group_by:
                    group_id[fld.split(".")[-1]] = f"${fld}"
            else:
                raise HTTPException(
                    400, f"Invalid value provided for group_by {group_by}"
                )

        count = 1
        if count_by:
            if count_by in valid_count_fields:
                count = f"${count_by}"
            else:
                raise HTTPException(
                    400, f"Invalid value provided for count_by {count_by}"
                )
        else:
            if group_by and "value" in group_by:
                count = "$externalHits"

        result = db_connector.collection(Collections.INDICATORS).aggregate(
            [
                {"$match": filters},
                {"$group": {"_id": group_id, "count": {"$sum": count}}},
                {"$sort": {"count": 1 if ascending is True else -1}},
                {"$limit": limit},
            ],
            allowDiskUse=True,
        )
        result = list(result)
        return JSONResponse(status_code=200, content=result)
    except json.decoder.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON filter-query provided.")
    except ValidationError as ex:
        raise HTTPException(400, f"Invalid filter-query provided. {ex.message}.")
    except TypeError:
        raise HTTPException(400, "Unable to serialize MongoDB results")
    except OperationFailure as ex:
        raise HTTPException(400, f"{ex}")


@router.post(
    "/indicators/",
    response_model=List[IndicatorOutWithSources],
    tags=["Indicators"],
    status_code=201,
    description="Insert/update multiple indicators.",
)
async def create_indicators(
    indicators: List[IndicatorIn],
    user: User = Security(get_current_user, scopes=["cte_write"]),
):
    """Create new indicators.

    Args:
        indicators (List[IndicatorIn]): List of indicators to be created.
    """
    sources = {
        r["name"]: r
        for r in list(db_connector.collection(Collections.CONFIGURATIONS).find())
    }
    out = []
    for indicator in indicators:
        if not validate_iocs(indicator.type, indicator.value):
            raise HTTPException(422, "Invalid indicator value provided.")
        if indicator.tags:
            indicator.tags = await create_tag_helper(indicator)
        insert_or_update_indicator(
            ConfigurationDB(**(sources[indicator.source])),
            indicator,
            sources[indicator.source]["plugin"] == "netskope",
            True,
        )
        indicator = db_connector.collection(Collections.INDICATORS).find_one(
            {"value": indicator.value}
        )
        if indicator is None:
            continue
        indicator = IndicatorOutWithSources(**indicator)
        out.append(indicator)
        execute_celery_task(
            share_indicators.apply_async,
            "cte.share_indicators",
            args=[indicator.source],
            kwargs={"indicators": [indicator.value]},
        )
    return out


@router.patch(
    "/indicators/",
    response_model=Union[IndicatorOutWithSources, bool],
    tags=["Indicators"],
    status_code=201,
    description="Update a single indicator.",
)
async def update_indicator(
    indicator: Union[IndicatorIn, RetractionUpdate],
    user: User = Security(get_current_user, scopes=["cte_write"]),
):
    """Update existing indicator.

    Args:
        indicator (IndicatorIn): Indicator to be updated.
    """
    if isinstance(indicator, RetractionUpdate):
        destinations = get_possible_destinations(indicator.source)
        destinations_query = (
            {"sources.$[elem].destinations": destinations}
            if not indicator.retracted
            else {}
        )
        db_connector.collection(Collections.INDICATORS).update_one(
            {
                "value": indicator.value,
                "sources": {
                    "$elemMatch": {
                        "source": indicator.source,
                        "retracted": not indicator.retracted,
                    }
                },
            },
            {
                "$set": {
                    "sources.$[elem].retracted": indicator.retracted,
                    "sources.$[elem].retractionDestinations": (
                        destinations if indicator.retracted else []
                    ),
                    **destinations_query,
                }
            },
            array_filters=[{"elem.source": indicator.source}],
        )
        return True
    if indicator.tags:
        indicator.tags = await create_tag_helper(indicator)
    indicator_dict = db_connector.collection(Collections.INDICATORS).find_one(
        {"value": indicator.value}
    )
    if indicator_dict is None:
        raise HTTPException(
            404, f"Indicator with value='{indicator.value}' does not exist."
        )
    source_dict = db_connector.collection(Collections.CONFIGURATIONS).find_one(
        {"name": indicator.source}
    )
    if source_dict is None:
        raise HTTPException(
            400,
            f"Configuration with name='{indicator.source}' does not exist.",
        )
    source = ConfigurationDB(**source_dict)
    insert_or_update_indicator(source, indicator, source.plugin == "netskope", True)
    execute_celery_task(
        share_indicators.apply_async,
        "cte.share_indicators",
        args=[indicator.source],
        kwargs={"indicators": [indicator.value]},
    )
    indicator = db_connector.collection(Collections.INDICATORS).find_one(
        {"value": indicator.value}
    )
    return IndicatorOutWithSources(**indicator)


def update_indicators_by_locator(
    locator: Union[IndicatorQueryLocator, IndicatorValueLocator],
    update_query: dict,
    array_filters: list = [],
) -> int:
    """Update indicators.

    Args:
        locator: Either an IndicatorQueryLocator or IndicatorValueLocator
        update_query: The update operations to apply
        array_filters: Additional filters for array updates

    Returns:
        int: Number of documents matched
    """
    if isinstance(locator, IndicatorQueryLocator):
        locator_query = json.loads(locator.query, object_hook=parse_dates)
        update_response = db_connector.collection(Collections.INDICATORS).update_many(
            locator_query, update_query
        )
    elif isinstance(locator, IndicatorValueLocator):
        update_response = db_connector.collection(Collections.INDICATORS).update_many(
            {"value": {"$in": locator.values}, "sources.source": {"$exists": True}},
            update_query,
            array_filters=array_filters,
        )
    return update_response.matched_count


# TODO: Update the current tagging system
@router.patch(
    "/indicators/bulk",
    tags=["Indicators"],
    status_code=201,
    description="Bulk update indicators.",
)
async def bulk_update_indicators(
    edit: IndicatorBulkEdit,
    user: User = Security(get_current_user, scopes=["cte_write"]),
):
    """Bulk edit indicators.

    Args:
        edit: IndicatorBulkEdit containing the update operation details
        user: The authenticated user making the request

    Returns:
        dict: Dictionary with count of updated indicators
    """
    update_query = {}
    update_count = 0

    if isinstance(edit.locator, IndicatorQueryLocator):
        if edit.tags:
            if edit.tags.remove:
                update_query["$pull"] = update_query.get("$pull", {})
                update_query["$pull"]["sources.$[].tags"] = {"$in": edit.tags.remove}
                update_count = update_indicators_by_locator(edit.locator, update_query)
                update_query = {}

            if edit.tags.add:
                update_query["$addToSet"] = update_query.get("$addToSet", {})
                update_query["$addToSet"]["sources.$[].tags"] = {"$each": edit.tags.add}
                update_count = update_indicators_by_locator(edit.locator, update_query)
                update_query = {}

        return {"updated": update_count}

    elif isinstance(edit.locator, IndicatorValueLocator):
        if edit.locator.source is not None:
            if edit.tags:
                if edit.tags.remove:
                    update_query["$pull"] = update_query.get("$pull", {})
                    update_query["$pull"]["sources.$[i].tags"] = {
                        "$in": edit.tags.remove
                    }
                    update_count = update_indicators_by_locator(
                        edit.locator, update_query, [{"i.source": edit.locator.source}]
                    )
                    update_query = {}

                if edit.tags.add:
                    update_query["$addToSet"] = update_query.get("$addToSet", {})
                    update_query["$addToSet"]["sources.$[i].tags"] = {
                        "$each": edit.tags.add
                    }
                    update_count = update_indicators_by_locator(
                        edit.locator, update_query, [{"i.source": edit.locator.source}]
                    )
                    update_query = {}
        else:
            if edit.tags:
                if edit.tags.remove:
                    update_query["$pull"] = update_query.get("$pull", {})
                    update_query["$pull"]["sources.$[].tags"] = {
                        "$in": edit.tags.remove
                    }
                    update_count = update_indicators_by_locator(
                        edit.locator, update_query
                    )
                    update_query = {}

                if edit.tags.add:
                    update_query["$addToSet"] = update_query.get("$addToSet", {})
                    update_query["$addToSet"]["sources.$[].tags"] = {
                        "$each": edit.tags.add
                    }
                    update_count = update_indicators_by_locator(
                        edit.locator, update_query
                    )
                    update_query = {}
        return {"updated": update_count}


@router.delete(
    "/indicators/bulk",
    tags=["Indicators"],
    status_code=201,
    description="Bulk delete indicators.",
)
async def bulk_delete_indicators(
    delete: Union[IndicatorQueryLocator, IndicatorValueLocator],
    user: User = Security(get_current_user, scopes=["cte_write"]),
):
    """Bulk delete indicators.

    Args:
        delete: Either an IndicatorQueryLocator or IndicatorValueLocator to identify
            which indicators to delete
        user: The authenticated user making the request

    Returns:
        dict: Dictionary with count of deleted indicators
    """
    if isinstance(delete, IndicatorQueryLocator):
        parsed_query = json.loads(delete.query, object_hook=parse_dates)
        try:
            result = db_connector.collection(Collections.INDICATORS).delete_many(
                parsed_query
            )
        except Exception as e:
            logger.error(f"Failed to delete indicators: {e}")
    elif isinstance(delete, IndicatorValueLocator):
        result = db_connector.collection(Collections.INDICATORS).delete_many(
            {"value": {"$in": delete.values}}
        )

    logger.debug(
        f"{result.deleted_count or 0} indicator(s) deleted by {user.username}."
    )
    return {"deleted": result.deleted_count or 0}
