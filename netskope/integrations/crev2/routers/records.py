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
from netskope.common.utils.unified_mapping_fields import is_mappable_field

from ..models import (
    Entity,
    EntityFieldType,
    RecordQueryLocator,
    RecordValueLocator,
    get_entity_by_name,
)
from ..tasks.fetch_records import get_normalized_value
from ..utils import (
    THREAT_INDICATORS_ENTITY,
    build_pipeline_from_entity,
    get_entity_collection,
)
from ..utils.threat_indicators_entity import (
    build_threat_indicators_query_schema,
    merge_nested_group,
)

connector = DBConnector()
logger = Logger()
router = APIRouter()


def _field_to_query_def(field, table: str = "") -> dict:
    """Map a CRE entity field to a react-awesome-query-builder field definition.

    ``table`` is the owning entity's collection, which scopes the closed-source
    rule: only the CTE-owned Threat Indicators mirror resolves to ``indicators``.
    """

    # This table maps a field type to its filter WIDGET. The stored primitive
    # for the same types lives in ``ENTITY_FIELD_VALUE_TYPES``
    # (crev2/models/entities.py), which is the source of truth for ``valueType``
    # and for unified mapping's join type check.
    #
    # Mostly they coincide, but a widget is not a primitive and the two are
    # separate on purpose: a Value Map String is a ``select`` widget over stored
    # text, and a Reference is a ``!struct`` widget whose target field's type is
    # not resolved at all (so it has no entry there). A List is NOT such a case
    # -- it is a text widget over stored text, and that table says so.
    #
    # Keep the number/datetime/boolean rows below in step with that table.
    def _map_type(field_type: EntityFieldType) -> str:
        if field_type == EntityFieldType.REFERENCE:
            return "!struct"
        if field_type in [
            EntityFieldType.CALCULATED,
            EntityFieldType.NUMBER,
            EntityFieldType.VALUE_MAP_NUMBER,
        ]:
            return "number"
        if field_type == EntityFieldType.LIST:
            # Filtered as free text, not a dropdown: nothing fills in a List's
            # ``listValues`` on this path -- the Records and Business Rules
            # pages render this schema as served, and the query builder's
            # ``select`` accepts no custom values -- so a ``select`` here
            # reaches the user as an empty dropdown with nothing to pick. Mongo
            # matches a scalar against an array element, so ``equal``/``like``
            # on the bare field work as the user expects.
            #
            # The values are enumerable if a page wants them:
            # ``GET /entities/{name}/records/field/{field}`` runs a ``distinct``,
            # which is how the Multi-Table View Builder offers a populated
            # dropdown for this same field. Threat Indicators Lists stay
            # ``select`` for the equivalent reason -- the UI fills them from
            # live lookups; see ``build_threat_indicators_query_schema``.
            return "text"
        if field_type == EntityFieldType.DATETIME:
            return "datetime"
        if field_type == EntityFieldType.BOOLEAN:
            return "boolean"
        if field_type == EntityFieldType.VALUE_MAP_STRING:
            return "select"
        return "text"

    f = {
        "label": field.label,
        "type": _map_type(field.type),
        # Read by the action-parameter picker, which must not offer a source the
        # rule validators would then reject.
        "mappable": is_mappable_field(field.name, table),
    }

    if field.type == EntityFieldType.VALUE_MAP_STRING:
        # ``type`` above describes the filter widget (a dropdown restricted to
        # the configured mappings), not what the record holds. The mapped value
        # is a plain string, so advertise that separately -- action parameters
        # of type ``text`` match on ``valueType`` and would otherwise reject
        # this field. A List needs no such hint: its widget is ``text`` too.
        f["valueType"] = "text"

    if field.type in [
        EntityFieldType.VALUE_MAP_STRING,
        EntityFieldType.VALUE_MAP_NUMBER,
    ]:
        if field.params and getattr(field.params, "mappings", None):
            f["fieldSettings"] = {
                "listValues": {
                    (m.value if m.value is not None else m.label): (
                        m.value if m.value is not None else m.label
                    )
                    for m in field.params.mappings
                }
            }
    elif field.type == EntityFieldType.NUMBER:
        f["preferWidgets"] = ["number"]
    elif field.type == EntityFieldType.DATETIME:
        f["fieldSettings"] = {
            "timeFormat": "HH:mm:ss",
            "valueFormat": "YYYY-MM-DDTHH:mm:ssZZ",
        }

    return f


def build_schema_from_entity(entity: Entity) -> dict:
    """Build query-builder schema from entity fields.

    Dotted names (e.g. ``sources.severity``, ``sources.destinations.name``)
    are grouped under ``!group`` nodes for ``$elemMatch`` query generation.
    Threat Indicators uses the canonical CTE Threat IOCs schema.
    """
    if entity.name == THREAT_INDICATORS_ENTITY:
        schema = build_threat_indicators_query_schema()
        schema["lastUpdated"] = {
            "label": "Last Updated",
            "type": "datetime",
            "mappable": False,
            "fieldSettings": {
                "timeFormat": "HH:mm:ss",
                "valueFormat": "YYYY-MM-DDTHH:mm:ssZZ",
            },
        }
        return schema

    schema: dict = {}
    group_roots: dict = {}
    table = get_entity_collection(entity.name)

    for field in entity.fields:
        if "." in field.name:
            parts = field.name.split(".")
            root = parts[0]
            if root not in group_roots:
                group_roots[root] = {}
            merge_nested_group(
                group_roots[root],
                parts[1:],
                _field_to_query_def(field, table),
            )
            continue

        f = _field_to_query_def(field, table)

        if field.type == EntityFieldType.REFERENCE:
            f["subfields"] = {}
            ref_entity = get_entity_by_name(field.params.entity)
            ref_table = get_entity_collection(ref_entity.name)
            for ref_field in ref_entity.fields:
                f["subfields"][ref_field.name] = _field_to_query_def(
                    ref_field, ref_table
                )
            f["subfields"]["lastUpdated"] = {
                "label": "Last Updated",
                "type": "datetime",
                "mappable": False,
                "fieldSettings": {
                    "timeFormat": "HH:mm:ss",
                    "valueFormat": "YYYY-MM-DDTHH:mm:ssZZ",
                },
            }

        schema[field.name] = f

    for parent_name, subfields in group_roots.items():
        if parent_name in schema:
            label = schema[parent_name].get("label", parent_name.title())
        else:
            label = parent_name.title()
        schema[parent_name] = {
            "label": label,
            "type": "!group",
            "subfields": subfields,
        }

    schema["lastUpdated"] = {
        "label": "Last Updated",
        "type": "datetime",
        # CE-stamped storage time: filterable, never a mapping/action source.
        "mappable": False,
        "fieldSettings": {
            "timeFormat": "HH:mm:ss",
            "valueFormat": "YYYY-MM-DDTHH:mm:ssZZ",
        },
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
            get_entity_collection(name)
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
    entity_doc = connector.collection(Collections.CREV2_ENTITIES).find_one(
        {"name": name}, {"_id": False}
    )
    if not entity_doc:
        raise HTTPException(404, f"Could not find entity with name {name}.")
    entity = Entity(**entity_doc)
    field_def = next((f for f in entity.fields if f.name == field), None)
    if field_def is None:
        # Also keeps the raw path parameter from being used as a distinct key
        # for keys that are not part of the entity schema.
        raise HTTPException(
            404, f"Could not find field {field} in entity {name}."
        )
    normalization = (
        getattr(field_def.params, "normalization", None)
        if field_def.type == EntityFieldType.STRING and field_def.params
        else None
    )
    # Normalized string fields are stored as nested {"value": ..., "plugins": [...]}
    # documents, so a distinct on the bare field name returns whole sub-documents
    # instead of scalars -- one per distinct plugins combination, which can also
    # blow past Mongo's 16 MB distinct limit. Read the scalar sub-key instead, and
    # only fall back to the bare key for records ingested before normalization was
    # turned on (those still hold a plain string and are migrated on re-ingest).
    if normalization:
        distinct_queries = [
            (f"{field}.value", {}),
            (field, {field: {"$type": "string"}}),
        ]
    else:
        distinct_queries = [(field, {})]
    collection = connector.collection(get_entity_collection(name))
    result = []
    seen = set()
    for distinct_key, distinct_filter in distinct_queries:
        for elem in collection.distinct(distinct_key, distinct_filter):
            # Defensive belt: never let a non-scalar reach the UI as a label.
            # Unwrap the one-level normalized {"value": ...} shape, then drop
            # anything that is still not a scalar (nested dict/list).
            if isinstance(elem, dict):
                elem = elem.get("value")
            if elem is None or isinstance(elem, (dict, list)):
                continue
            if normalization:
                # Value-map lookups compare against the normalized value, so a
                # label offered here has to be normalized too -- pre-normalization
                # records still hold the raw string. Idempotent for the sub-key
                # values, which are already normalized.
                elem = get_normalized_value(elem, normalization)
            if elem in seen:
                continue
            seen.add(elem)
            result.append({"label": elem, "value": None})
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
    if name == THREAT_INDICATORS_ENTITY:
        raise HTTPException(
            400, "Threat Indicators entity is read-only and cannot be modified."
        )
    if isinstance(delete, RecordQueryLocator):
        query = json.loads(delete.query, object_hook=parse_dates)
        result = connector.collection(
            get_entity_collection(name)
        ).delete_many(query)
    elif isinstance(delete, RecordValueLocator):
        result = connector.collection(
            get_entity_collection(name)
        ).delete_many({"_id": {"$in": list(map(ObjectId, delete.ids))}})
    logger.debug(
        f"{result.deleted_count} records(s) from entity {name} deleted "
        f"by {user.username}."
    )
    return {"deleted": result.deleted_count}
