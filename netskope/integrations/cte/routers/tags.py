"""Provides tagging related endpoints."""

from typing import List
import json
from jsonschema import validate, ValidationError
from fastapi import APIRouter, HTTPException, Security

from netskope.common.utils import DBConnector, Collections, Logger, parse_dates
from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User

from ..utils.schema import INDICATOR_QUERY_SCHEMA, INDICATOR_STRING_FIELDS
from ..models import TagIn, TagOut, TagDelete, TagAppliedOn

router = APIRouter()
logger = Logger()
connector = DBConnector()


def get_applied_on(tag: str, filters: dict) -> TagAppliedOn:
    """Get appliedOn value for a tag.

    Args:
        tag (str): Tag to be searched.
        filters (dict): Filters to be applied on indicators.

    Returns:
        TagAppliedOn: Value indicating the indicators that this tag is applied on.
    """
    one_has_query = {"$and": [filters, {"sources": {"$elemMatch": {"tags": {"$in": [tag]}}}}]}
    one_has = connector.collection(Collections.INDICATORS).find_one(one_has_query) is not None
    if not one_has:
        return TagAppliedOn.NONE
    else:
        not_has_query = {"$and": [filters, {"sources": {"$elemMatch": {"tags": {"$nin": [tag]}}}}]}
        not_has = connector.collection(Collections.INDICATORS).find_one(not_has_query) is not None
        if not_has:
            return TagAppliedOn.SOME
        else:
            return TagAppliedOn.ALL


@router.get(
    "/tags",
    response_model=List[TagOut],
    tags=["Tags"],
    description="List tags.",
)
async def list_tags(
    filters: str = "{}",
    user: User = Security(get_current_user, scopes=["cte_read"]),
):
    """Get list of all the tags."""
    try:
        validate(json.loads(filters), schema=INDICATOR_QUERY_SCHEMA)
        filters = json.loads(
            filters,
            object_hook=lambda pair: parse_dates(pair, INDICATOR_STRING_FIELDS),
        )
    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON query provided.")
    except ValidationError as ex:
        raise HTTPException(400, f"Invalid query provided. {ex.message}.")
    tags = []
    for tag in connector.collection(Collections.TAGS).find({}):
        tags.append(TagOut(**tag, appliedOn=get_applied_on(tag["name"], filters)))
    return tags


@router.post("/tags", tags=["Tags"], description="Create new tag.")
async def create_tag(tag: TagIn, user: User = Security(get_current_user, scopes=["cte_write"])):
    """Create a new tag."""
    connector.collection(Collections.TAGS).insert_one(tag.model_dump())
    logger.debug(f"Tag '{tag.name}' successfully created.")
    return {}


def parse_mongo_query(tag_dict, tag_name):
    """Parse mongo query."""
    for key, val in tag_dict.items():
        if key in ["$and", "$or"]:
            for sub_query in val:
                if parse_mongo_query(sub_query, tag_name):
                    return True
        if key in ["sources", "$elemMatch"]:
            if parse_mongo_query(val, tag_name):
                return True
        elif key in ["tags"]:
            for k, v in val.items():
                if tag_name in v:
                    return True
    return False


@router.delete("/tags", tags=["Tags"], description="Delete a tag.")
async def delete_tag(tag: TagDelete, user: User = Security(get_current_user, scopes=["cte_write"])):
    """Delete a tag."""
    used = False

    for business_rule in list(connector.collection(Collections.CTE_BUSINESS_RULES).find({})):
        # simple rules
        tag_dict = json.loads(business_rule.get("filters", {}).get("mongo", {}))
        if parse_mongo_query(tag_dict, tag.name):
            used = True
            break

        # exception rule
        for exception_rule in business_rule.get("exceptions", []):
            if exception_rule.get("filters", None) is not None:
                exception_dict = exception_rule.get("filters", {}).get("mongo", {})
                if not isinstance(exception_dict, dict):
                    exception_dict = json.loads(exception_dict)
                if parse_mongo_query(exception_dict, tag.name):
                    used = True
                    break

            if exception_rule.get("tags", None) is not None:
                if tag.name in exception_rule.get("tags", []):
                    used = True
                    break
    if used:
        logger.debug(f"{tag.name} is in use by one of the CTE business rules.")
        raise HTTPException(400, f"{tag.name} is in use by one of the CTE business rules.")
    else:
        connector.collection(Collections.INDICATORS).update_many(
            {},
            {"$pull": {"sources.$[].tags": tag.name, "tags": tag.name}},
        )
        connector.collection(Collections.TAGS).delete_one({"name": tag.name})
        logger.debug(f"Tag '{tag.name}' has been deleted.")
    return {}
