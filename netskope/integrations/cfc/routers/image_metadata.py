"""Provides image data related endpoints."""
import json
import traceback
from typing import List

from bson import SON
from fastapi import APIRouter, HTTPException, Security
from jsonschema import ValidationError, validate
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import OperationFailure

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger, Scheduler
from netskope.common.utils.plugin_helper import PluginHelper

from ..models import ImageMetadataDB, ImageMetadataOut
from ..utils import IMAGE_METADATA_QUERY_SCHEMA as QUERY_SCHEMA
from ..utils import parse_dates

router = APIRouter()
scheduler = Scheduler()
plugin_helper = PluginHelper()
logger = Logger()
db_connector = DBConnector()


def get_images_data(filters, skip, limit, sort, ascending) -> List[ImageMetadataDB]:
    """Get Images based on various filters.

    Args:
        filters (string): filters to apply on database.
        skip (int): Number of image metadata to skip. Defaults to 0.
        limit (int): Number of image metadata to limit. Defaults to 10.
        sort (str): Sorts result for given parameter. Defaults to None.
        ascending (bool): Sort order True ascending and False descending. Defaults to True.

    Returns:
        list: List of filters from provided query.
    """
    data = []
    pipeline = [
        {"$match": filters},
    ]
    if sort in [
        "sourcePlugin",
        "file",
        "path",
        "extension",
        "sourceType",
        "lastFetched",
        "fileSize"
    ]:
        pipeline.append(
            {"$sort": SON([(sort, ASCENDING if ascending else DESCENDING)])}
        )
    pipeline.append({"$skip": skip})
    pipeline.append({"$limit": limit})
    data_dicts = db_connector.collection(Collections.CFC_IMAGES_METADATA).aggregate(
        pipeline,
        allowDiskUse=True,
    )
    for data_dict in data_dicts:
        data.append(ImageMetadataDB(**data_dict))

    return data


@router.get(
    "/image_metadata/",
    response_model=ImageMetadataOut,
    tags=["CFC Image Metadata"],
    description="Get image metadata",
)
async def read_image_metadata(
    _: User = Security(get_current_user, scopes=["cfc_read"]),
    skip: int = 0,
    limit: int = 10,
    sort: str = None,
    ascending: bool = True,
    filters: str = "{}",
):
    """Get list of Image Metadata.

    Returns:
        List[ImageMetadataDB]: List of image metadata matching the given query.

    Args:
        skip (int, optional): Number of image metadata to skip. Defaults to 0.
        limit (int, optional): Number of image metadata to limit. Defaults to 10.
        filters (str, optional): JSON string of filters to be applied. Defaults to "{}".
        sort (str, optional): Sorts result for given parameter. Defaults to None.
        ascending (bool, optional): Sort order True ascending and False descending. Defaults to True.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        ImageMetadataOut: Count of results and list of result for provided query..
    """
    try:
        data = []
        validate(json.loads(filters), schema=QUERY_SCHEMA)
        filters = json.loads(
            filters, object_hook=lambda pair: parse_dates(pair)
        )
        data = get_images_data(filters, skip, limit, sort, ascending)

        count_result = db_connector.collection(Collections.CFC_IMAGES_METADATA).aggregate(
            [
                {"$match": filters},
                {"$group": {"_id": None, "count": {"$sum": 1}}},
            ],
            allowDiskUse=True,
        )
        count_result = list(count_result)
        count = count_result.pop()["count"] if count_result else 0

        result = ImageMetadataOut(
            count=count,
            data=data
        )

        return result
    except json.decoder.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON query provided.")
    except ValidationError as error:
        raise HTTPException(400, f"Invalid query provided. {error.message}.")
    except OperationFailure as error:
        raise HTTPException(400, f"{error}")
    except Exception:
        logger.debug(
            "Error occurred while processing the query.",
            details=traceback.format_exc(),
            error_code="CFC_1019",
        )
        raise HTTPException(400, "Error occurred while processing the query.")
