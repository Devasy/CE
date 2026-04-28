"""Schemas related endpoints."""

import csv
import json
import traceback
from bson import BSON
from typing import Annotated

from dateutil import parser
from fastapi import APIRouter, File, HTTPException, Query, Security
from pydantic import ValidationError
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure

from netskope.common.api.routers.auth import get_current_user
from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger

from ..models import (
    Entity,
    EntityField,
    EntityFieldIn,
    EntityFieldType,
    EntityIn,
    EntityTypeCoalesceStrategy,
    EntityUpdate,
    get_entity_by_name,
)
from ..tasks.fetch_records import (
    import_records,
    update_calculated_fields,
    update_mapped_fields,
    MAX_ENTITY_SIZE_WITH_VALUE_MAP
)

router = APIRouter()
connector = DBConnector()
logger = Logger()
UNIQUE_INDEX_NAME = "unique_index"


def _get_next_index_name(index: str) -> str:
    """Get next index name for unique index."""
    if index == UNIQUE_INDEX_NAME:
        return f"{index}_1"
    current_count = int(index[len(f"{UNIQUE_INDEX_NAME}_"):])
    return f"{UNIQUE_INDEX_NAME}_{current_count + 1}"


def _update_indices(entity: Entity, field: str):
    """Update indices for an entity."""
    indices = connector.collection(
        f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.name}"
    ).index_information()
    current_index = [
        index
        for index in indices.keys()
        if index.startswith(UNIQUE_INDEX_NAME)
    ]
    new_index = (
        _get_next_index_name(current_index[0])
        if current_index
        else UNIQUE_INDEX_NAME
    )
    if unique_fields := [
        (f.name, ASCENDING) for f in entity.fields if f.unique
    ]:
        try:
            connector.collection(
                f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.name}"
            ).create_index(
                unique_fields,
                unique=True,
                name=new_index,
            )
        except DuplicateKeyError as ex:
            logger.debug(
                f"Could not create index {new_index}.",
                details=traceback.format_exc(),
            )
            raise HTTPException(
                400,
                f"Field {field} can not be set as unique as there are duplicate records.",
            ) from ex
        except OperationFailure:
            logger.debug(
                f"Could not create index {new_index} as it may already exist.",
                details=traceback.format_exc(),
            )
            return
    if current_index:
        connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.name}"
        ).drop_index(current_index[0])


@router.get("/entities", tags=["CREv2 Entities"])
async def get_entities(
    _: User = Security(get_current_user, scopes=["cre_read"])
) -> list[Entity]:
    """Get all entities."""
    return [
        Entity(**i)
        for i in connector.collection(Collections.CREV2_ENTITIES).find()
    ]


@router.post("/entities", tags=["CREv2 Entities"])
async def create_entity(
    entity: EntityIn,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> Entity:
    """Create an entity."""
    connector.collection(Collections.CREV2_ENTITIES).insert_one(
        entity.model_dump()
    )
    return entity


@router.patch("/entities/{name}", tags=["CREv2 Entities"])
async def update_entity(
    name: str,
    entity: EntityIn,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> Entity:
    """Update an entity."""
    connector.collection(Collections.CREV2_ENTITIES).update_one(
        {"name": name},
        {"$set": entity.model_dump()},
    )
    return entity


def schedule_update_calculated_fields_task(entity: str):
    """Schedule update_calculated_fields_task.

    Args:
        entity (str): Entity name.
    """
    logger.debug(
        f"Scheduling update calculated fields task for entity {entity}."
    )
    entity: Entity = get_entity_by_name(entity)
    if entity.ongoingCalculationUpdateTaskId:
        try:
            APP.control.revoke(
                entity.ongoingCalculationUpdateTaskId, terminate=True
            )
        except Exception:
            logger.error(
                f"Error revoking task {entity.ongoingCalculationUpdateTaskId} for "
                f"entity {entity.name}. Task may have completed.",
                details=traceback.format_exc(),
            )
    task = execute_celery_task(
        update_calculated_fields.apply_async,
        "cre.update_calculated_fields",
        args=[entity.name],
    )
    connector.collection(Collections.CREV2_ENTITIES).update_one(
        {"name": entity.name},
        {"$set": {"ongoingCalculationUpdateTaskId": task.task_id}},
    )


def schedule_update_mapping_fields_task(entity: str):
    """Schedule update_calculated_fields_task.

    Args:
        entity (str): Entity name.
    """
    logger.debug(f"Scheduling update mapped fields task for entity {entity}.")
    entity: Entity = get_entity_by_name(entity)
    if entity.ongoingMappingUpdateTaskId:
        try:
            APP.control.revoke(
                entity.ongoingMappingUpdateTaskId, terminate=True
            )
        except Exception:
            logger.error(
                f"Error revoking task {entity.ongoingMappingUpdateTaskId} for "
                f"entity {entity.name}. Task may have completed.",
                details=traceback.format_exc(),
            )
    task = execute_celery_task(
        update_mapped_fields.apply_async,
        "cre.update_mapped_fields",
        args=[entity.name],
    )
    connector.collection(Collections.CREV2_ENTITIES).update_one(
        {"name": entity.name},
        {"$set": {"ongoingMappingUpdateTaskId": task.task_id}},
    )


@router.post("/entities/{name}/fields", tags=["CREv2 Entities"])
async def create_field(
    name: str,
    field: EntityFieldIn,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> Entity:
    """Create a new field."""
    entity = connector.collection(Collections.CREV2_ENTITIES).find_one(
        {"name": name}
    )
    if not entity:
        raise HTTPException(404, f"Could not find entity with name {name}.")
    try:
        entity = EntityUpdate(**entity)
        entity.fields = entity.fields + [field]
    except ValidationError as ex:
        raise HTTPException(422, json.loads(ex.json())) from ex

    current_size = len(BSON.encode(entity.model_dump()))
    if current_size > MAX_ENTITY_SIZE_WITH_VALUE_MAP:
        raise HTTPException(
            413,
            f"Entity {entity.name} size limit exceeds the maximum allowed limit of 14 MB, "
            "remove unused fields or value mappings."
        )

    if field.type == EntityFieldType.CALCULATED:
        schedule_update_calculated_fields_task(name)
    if field.type in [EntityFieldType.VALUE_MAP_NUMBER, EntityFieldType.VALUE_MAP_STRING, EntityFieldType.RANGE_MAP]:
        schedule_update_mapping_fields_task(name)
    if field.unique:
        _update_indices(entity, field.name)
    connector.collection(Collections.CREV2_ENTITIES).update_one(
        {"name": entity.name},
        {"$set": entity.model_dump()},
    )
    return entity


@router.patch("/entities/{name}/fields/{field}", tags=["CREv2 Entities"])
async def update_field(
    name: str,
    field: str,
    field_data: EntityFieldIn,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> Entity:
    """Update field."""
    entity = Entity(
        **connector.collection(Collections.CREV2_ENTITIES).find_one(
            {"name": name}
        )
    )
    existing_field: EntityField = next(
        filter(lambda x: x.name == field, entity.fields), None
    )
    entity.fields = list(
        map(
            lambda x: x if x.name != field else field_data,
            entity.fields,
        )
    )
    current_size = len(BSON.encode(entity.model_dump()))
    if current_size > MAX_ENTITY_SIZE_WITH_VALUE_MAP:
        raise HTTPException(
            413,
            f"Entity {entity.name} size limit exceeds the maximum allowed limit of 14 MB, "
            "remove unused fields or value mappings."
        )
    if (
        field_data.type != EntityFieldType.LIST
    ):  # all types except list; lists stay lists
        if (
            not existing_field.coalesceStrategy
            == EntityTypeCoalesceStrategy.MERGE
            and field_data.coalesceStrategy == EntityTypeCoalesceStrategy.MERGE
        ):
            connector.collection(
                f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.name}"
            ).update_many(
                {},
                [
                    {"$set": {field_data.name: [f"${field_data.name}"]}}
                ],  # convert to list
            )
        elif (
            existing_field.coalesceStrategy == EntityTypeCoalesceStrategy.MERGE
            and not field_data.coalesceStrategy
            == EntityTypeCoalesceStrategy.MERGE
        ):
            connector.collection(
                f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.name}"
            ).update_many(
                {},  # all types except list; lists stay lists
                [
                    {
                        "$set": {
                            field_data.name: {"$last": f"${field_data.name}"}
                        }
                    }
                ],  # convert to non-list
            )
    # replace the matching field from the array and get the updated document
    set_field = field_data.model_dump(exclude={"name"})
    # update calculated fields if the expression has changed
    if (
        field_data.type == EntityFieldType.CALCULATED
        and field_data.params.expression != existing_field.params.expression
    ):
        schedule_update_calculated_fields_task(entity.name)
    if field_data.type in [
        EntityFieldType.VALUE_MAP_STRING,
        EntityFieldType.VALUE_MAP_NUMBER,
        EntityFieldType.RANGE_MAP,
    ]:
        schedule_update_mapping_fields_task(entity.name)
    # update indices
    if (not existing_field.unique and field_data.unique) or (
        existing_field.unique and not field_data.unique
    ):
        _update_indices(entity, field_data.name)

    entity = connector.collection(
        Collections.CREV2_ENTITIES
    ).find_one_and_update(
        {"name": name, "fields": {"$elemMatch": {"name": field}}},
        {"$set": {f"fields.$.{k}": v for k, v in set_field.items()}},
        return_document=ReturnDocument.AFTER,
    )
    entity = Entity(**entity)
    return entity


@router.delete("/entities/{name}/fields/{field}", tags=["CREv2 Entities"])
async def delete_field(
    name: str,
    field: str,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> dict:
    """Delete a new field."""
    if connector.collection(Collections.CREV2_ENTITIES).find_one(
        {"fields.params.entity": name, "fields.params.field": field}
    ):
        raise HTTPException(
            400,
            "Can not delete the field as it is referenced by one or more fields.",
        )

    if connector.collection(Collections.CREV2_ENTITIES).find_one(
        {
            "name": name,
            "fields.params.dependencies": field,
            "fields.type": EntityFieldType.CALCULATED
        }
    ):
        raise HTTPException(
            400,
            "Can not delete the field as it is used by one or more calulated fields.",
        )

    if connector.collection(Collections.CREV2_ENTITIES).find_one(
        {
            "name": name,
            "fields.params.field": field,
        }
    ):
        raise HTTPException(
            400,
            "Can not delete the field as it is used by one or more value map/range map fields.",
        )

    if connector.collection(Collections.CREV2_CONFIGURATIONS).find_one(
        {
            "mappedEntities.destination": name,
            "mappedEntities.fields.destination": field,
        }
    ):
        raise HTTPException(
            400,
            "Can not delete the field as it is referenced by one or more configurations.",
        )
    # entity = connector.collection(Collections.CREV2_ENTITIES).find_one({"name": name})

    result = connector.collection(Collections.CREV2_ENTITIES).update_one(
        {"name": name}, {"$pull": {"fields": {"name": field}}}
    )
    if result.matched_count == 0:
        raise HTTPException(404, f"Could not find entity with name {name}.")
    if result.modified_count == 0:
        raise HTTPException(404, f"Could not find field with name {field}.")
    entity: Entity = get_entity_by_name(name)
    entity.fields = list(filter(lambda x: x.name != field, entity.fields))

    _update_indices(entity, field)

    connector.collection(
        f"{Collections.CREV2_ENTITY_PREFIX.value}{name}"
    ).update_many({}, {"$unset": {field: ""}})

    return {"success": True}


@router.delete("/entities/{name}", tags=["CREv2 Entities"])
async def delete_entity(
    name: str, _: User = Security(get_current_user, scopes=["cre_write"])
):
    """Delete an entity."""
    if connector.collection(Collections.CREV2_CONFIGURATIONS).find_one(
        {"mappedEntities.destination": name}
    ):
        raise HTTPException(
            400,
            "Can not delete the entity as it is used in one or more configurations.",
        )
    if connector.collection(Collections.CREV2_ENTITIES).find_one(
        {"fields.params.entity": name}
    ):
        raise HTTPException(
            400,
            "Can not delete the entity as it is referenced by one or more fields.",
        )
    connector.collection(Collections.CREV2_BUSINESS_RULES).delete_many(
        {"entity": name}
    )
    result = connector.collection(Collections.CREV2_ENTITIES).delete_one(
        {"name": name}
    )
    if result.deleted_count == 0:
        raise HTTPException(404, f"Could not find entity with name {name}.")
    # drop the collection with all the records
    connector.collection(
        f"{Collections.CREV2_ENTITY_PREFIX.value}{name}"
    ).delete_many({})
    return {"success": True}


@router.post("/entities/{name}/import", tags=["CREv2 Entities"])
async def import_records_entity(
    name: str,
    file: Annotated[bytes, File(...)],
    mapping: list[str] = Query([]),
    encoding: str = Query("utf-8"),
    delimiter: str = Query(","),
    _: User = Security(get_current_user, scopes=["cre_write"]),
):
    """Import records to an entity."""
    if len(delimiter) != 1:
        raise HTTPException(
            400, "Delimiter must be a single character string."
        )

    def map_record(mappings: dict, fields_types: dict, record: dict) -> dict:
        """Map record."""
        out = {}
        for key, value in mappings.items():
            if fields_types[value] == EntityFieldType.NUMBER:
                try:
                    out[value] = int(record[key])
                except Exception as ex:
                    raise HTTPException(
                        400,
                        f'Invalid value for {value}. "{record[key]}" is not a valid number.',
                    ) from ex
            elif fields_types[value] == EntityFieldType.DATETIME:
                try:
                    out[value] = parser.parse(record[key])
                except Exception as ex:
                    raise HTTPException(
                        400,
                        f'Invalid value for {value}. "{record[key]}" is not a valid datetime.',
                    ) from ex
            elif fields_types[value] == EntityFieldType.LIST:
                out[value] = list(
                    filter(
                        lambda x: x,
                        map(lambda x: x.strip(), record[key].split(",")),
                    )
                )
            elif fields_types[value] == EntityFieldType.BOOLEAN:
                try:
                    raw_value = record[key]
                    if isinstance(raw_value, bool):
                        out[value] = raw_value
                    else:
                        normalized = str(raw_value).strip().lower()
                        if normalized == "true":
                            out[value] = True
                        elif normalized == "false":
                            out[value] = False
                        else:
                            raise ValueError()
                except Exception as ex:
                    raise HTTPException(
                        400,
                        f'Invalid value for {value}. "{record[key]}" is not a valid boolean.',
                    ) from ex
            elif fields_types[value] == EntityFieldType.STRING:
                out[value] = record[key]
            else:
                raise HTTPException(
                    400,
                    f"Can not map with field of type {fields_types[value]} ({value}).",
                )
        return out

    if not mapping:
        raise HTTPException(400, "No mapping provided.")

    try:
        entity = get_entity_by_name(name)
    except Exception as ex:
        raise HTTPException(
            400, f"Could not find entity with name {name}."
        ) from ex
    field_types = {f.name: f.type for f in entity.fields}
    mappings = {i[0]: i[1] for i in map(lambda x: x.split("="), mapping)}
    if set(mappings.values()) - set([f.name for f in entity.fields]):
        raise HTTPException(
            400,
            "One or more of the mapped fields does not exist in the entity.",
        )
    reader = csv.DictReader(
        file.decode(encoding=encoding).split("\n"), delimiter=delimiter
    )
    if set(mappings.keys()) - set(reader.fieldnames):
        raise HTTPException(
            400,
            "One or more of the mapped fields does not exist in the file.",
        )
    try:
        records = [map_record(mappings, field_types, row) for row in reader]
        imported_records_count = import_records(entity.name, records)
        return {"success": True, "count": imported_records_count}
    except HTTPException:
        raise
    except Exception:
        logger.error(
            "Error occurred while parsing the file.",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            400, "Could not read the file. Check logs for more detail."
        )
