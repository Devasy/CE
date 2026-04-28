"""Record related routes."""

import json
from typing import Union

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Security
from pymongo import ASCENDING, DESCENDING
from starlette.responses import JSONResponse

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger, parse_dates

from ..models import (
    Entity,
    EntityFieldType,
    RecordQueryLocator,
    RecordValueLocator,
    get_entity_by_name,
)
from ..utils import build_pipeline_from_entity

connector = DBConnector()
logger = Logger()
router = APIRouter()


def build_schema_from_entity(entity: Entity) -> dict:
    """Build schema from entity."""

    def _map_type(field_type: EntityFieldType) -> str:
        if field_type == EntityFieldType.REFERENCE:
            return "!struct"
        elif field_type in [
            EntityFieldType.CALCULATED,
            EntityFieldType.NUMBER,
            EntityFieldType.VALUE_MAP_NUMBER,
        ]:
            return "number"
        elif field_type == EntityFieldType.LIST:
            return "list"
        elif field_type == EntityFieldType.DATETIME:
            return "datetime"
        elif field_type == EntityFieldType.BOOLEAN:
            return "boolean"
        else:
            return "text"

    schema = {}
    for field in entity.fields:
        f = {
            "label": field.label,
            "type": _map_type(field.type),
        }

        if field.type == EntityFieldType.REFERENCE:
            f["subfields"] = {}
            ref_entity = get_entity_by_name(field.params.entity)
            for ref_field in ref_entity.fields:
                # TODO: subfield should not have `!struct`
                sub_field = {
                    "label": ref_field.label,
                    "type": _map_type(ref_field.type),
                }
                f["subfields"][ref_field.name] = sub_field
            f["subfields"]["lastUpdated"] = {
                "label": "Last Updated",
                "type": "datetime",
            }

        schema[field.name] = f
    schema["lastUpdated"] = {
        "label": "Last Updated",
        "type": "datetime",
    }
    return schema


@router.get("/entities/{name}/records", tags=["CREv2 Records"])
async def get_records(
    name: str,
    skip: int = 0,
    limit: int = 10,
    sort: str = None,
    ascending: bool = True,
    aggregate: bool = False,
    filters: str = "{}",
    _: User = Security(get_current_user, scopes=["cre_read"]),
):
    """Get all records."""
    entity = connector.collection(Collections.CREV2_ENTITIES).find_one(
        {"name": name}, {"_id": False}
    )
    if not entity:
        raise HTTPException(404, f"Could not find entity with name {name}.")
    filters = json.loads(filters, object_hook=parse_dates)
    entity = Entity(**entity)
    result = list(
        connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{name}"
        ).aggregate(
            (
                build_pipeline_from_entity(entity)
                + ([{"$match": filters}] if filters else [])
                + (
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
            if "lastEvals" in record:
                del record["lastEvals"]
            record["_id"] = str(record["_id"])
        return {
            "schema": build_schema_from_entity(entity),
            "records": result,
        }


@router.get("/entities/{name}/records/field/{field}", tags=["CREv2 Records"])
async def get_records_field_value(
    name: str,
    field: str,
    _: User = Security(get_current_user, scopes=["cre_read"]),
):
    """Get list of field values."""
    result = [
        {"label": elem, "value": None}
        for elem in connector.collection(f"{Collections.CREV2_ENTITY_PREFIX.value}{name}").distinct(field, {})
    ]
    return JSONResponse(
        status_code=200, content={"field_values": result}
    )


@router.delete("/entities/{name}/records/delete", tags=["CREv2 Records"])
async def delete_records(
    name: str,
    delete: Union[RecordQueryLocator, RecordValueLocator],
    user: User = Security(get_current_user, scopes=["cre_write"]),
):
    """Delete records."""
    if isinstance(delete, RecordQueryLocator):
        query = json.loads(delete.query, object_hook=parse_dates)
        result = connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{name}"
        ).delete_many(query)
    elif isinstance(delete, RecordValueLocator):
        result = connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{name}"
        ).delete_many({"_id": {"$in": list(map(ObjectId, delete.ids))}})
    logger.debug(
        f"{result.deleted_count} records(s) from entity {name} deleted "
        f"by {user.username}."
    )
    return {"deleted": result.deleted_count}
