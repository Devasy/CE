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
from netskope.common.api.routers.unified_mapping import (
    delete_mappings_for_collection,
)
from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger

from ..models import (
    Entity,
    EntityField,
    EntityFieldIn,
    EntityFieldOut,
    EntityFieldType,
    EntityIn,
    EntityOut,
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
from ..utils import (get_entity_collection, THREAT_INDICATORS_ENTITY)

router = APIRouter()
connector = DBConnector()
logger = Logger()
UNIQUE_INDEX_NAME = "unique_index"
# How many mapping names the Unique-lock refusal spells out before collapsing the
# rest into a count. A mapping name runs to 256 characters, so a field joined by
# many mappings would otherwise produce a message no toast can show; the refusal
# is logged with every name regardless.
UNIQUE_JOIN_LOCK_NAME_LIMIT = 3


def unique_join_lock_message(mapping_names: list) -> str:
    """The refusal for clearing ``unique`` on a field a mapping joins on.

    Mirrored verbatim by ``uniqueJoinLockMessage`` in the UI's
    unifiedMappingGraph.js, so the Schema Editor's tooltip and this API's 400
    read identically whichever the user reaches first. Callers pass the names
    sorted; ASCII-only mapping names (``_NAME_RE`` on ``UnifiedMappingIn``) sort
    the same in Python and JavaScript, so both sides list them in one order.

    Args:
        mapping_names (list): Names of the mappings joining on the field, at
            least one, in sorted order.

    Returns:
        str: The message, singular or plural, names past
            ``UNIQUE_JOIN_LOCK_NAME_LIMIT`` collapsed into "and N more".
    """
    shown = mapping_names[:UNIQUE_JOIN_LOCK_NAME_LIMIT]
    listed = ", ".join(f"'{name}'" for name in shown)
    hidden = len(mapping_names) - len(shown)
    if hidden:
        listed = f"{listed} and {hidden} more"
    if len(mapping_names) == 1:
        return (
            "Cannot mark the field as non-unique as it is being used in a join "
            f"condition in the Unified Mapping {listed}. Update or delete that "
            "unified mapping to make this field non-unique."
        )
    return (
        "Cannot mark the field as non-unique as it is being used in a join "
        f"condition in the Unified Mappings {listed}. Update or delete those "
        "unified mappings to make this field non-unique."
    )


def _get_next_index_name(index: str) -> str:
    """Get next index name for unique index."""
    if index == UNIQUE_INDEX_NAME:
        return f"{index}_1"
    current_count = int(index[len(f"{UNIQUE_INDEX_NAME}_"):])
    return f"{UNIQUE_INDEX_NAME}_{current_count + 1}"


def _update_indices(entity: Entity, field: str):
    """Update indices for an entity."""
    indices = connector.collection(
        get_entity_collection(entity.name)
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
                get_entity_collection(entity.name)
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
            get_entity_collection(entity.name)
        ).drop_index(current_index[0])


def _entity_out(entity_doc: dict) -> EntityOut:
    """Build the API shape for one entity, enriching each field for mapping.

    The stored ``type`` alone cannot say that a Range Map holds text or that an
    append String holds a list, so the payload carries ``valueType`` and
    ``multiValued`` -- what the IOC field mapping gates on.

    Local import: ``unified_mapping_fields`` lazily imports this package's own
    models, so a module-level import risks a cycle.
    """
    from netskope.common.utils.unified_mapping_fields import (
        entity_field_widget_meta,
        map_type_labels,
    )

    fields = []
    for field_doc in entity_doc.get("fields") or []:
        widget = entity_field_widget_meta(field_doc)
        # Merged, not kwargs: a stored doc already carrying either key would
        # be a duplicate-kwarg TypeError, and the computed value should win.
        fields.append(
            EntityFieldOut(**{
                **field_doc,
                "valueType": widget.get("valueType"),
                "multiValued": widget["multiValued"],
                "mapLabels": map_type_labels(field_doc),
            })
        )
    return EntityOut(**{**entity_doc, "fields": fields})


@router.get("/entities", tags=["CREv2 Entities"])
async def get_entities(
    _: User = Security(get_current_user, scopes=["cre_read"])
) -> list[EntityOut]:
    """Get all entities."""
    return [
        _entity_out(i)
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
    if name == THREAT_INDICATORS_ENTITY:
        raise HTTPException(
            400, "Threat Indicators entity is read-only and cannot be modified."
        )
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
) -> EntityOut:
    """Create a new field."""
    if name == THREAT_INDICATORS_ENTITY:
        raise HTTPException(
            400, "Threat Indicators entity is read-only and cannot be modified."
        )
    entity = connector.collection(Collections.CREV2_ENTITIES).find_one(
        {"name": name}
    )
    if not entity:
        raise HTTPException(404, f"Could not find entity with name {name}.")
    new_label = field.label.lower()
    if any(
        existing.get("label", "").strip().lower() == new_label
        for existing in entity.get("fields", [])
    ):
        raise HTTPException(
            400,
            f"A field with label '{field.label}' already exists.",
        )
    stored_fields = entity.get("fields", [])
    try:
        entity = EntityUpdate(**entity)
        entity.fields = [
            existing.model_copy(update={"name": stored.get("name")})
            for existing, stored in zip(entity.fields, stored_fields)
        ] + [field]
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
    # Same enriched shape as the list endpoint, so a caller reading fields from
    # this response sees the primitive/array facts the field mapping gates on
    # rather than nulls. mode="json" renders enums as the stored strings.
    return _entity_out(entity.model_dump(mode="json"))


def _assert_field_mappings_valid(
    entity: Entity, existing_field: EntityField, field_data: EntityFieldIn
) -> None:
    """Reject a field edit that would break a live rule's IOC field mapping.

    A rule's ``fieldMapping`` is validated at creation and never again, so
    nothing else stops a mapped field being retyped, switched to Append, marked
    unique, or relabelled out from under it. Reuses the rule-save checker.

    Covers both rule kinds: CTE rules address a field by its bare name, unified
    mapping rules by the flattened ``<collection>.<field>`` key.

    Deliberately narrow -- it runs only when ``type``, ``coalesceStrategy``,
    ``unique`` or a map type's authored label set changed, and checks only the
    entries pointing at *this* field, so one rule's pre-existing violation
    cannot block unrelated edits.

    Args:
        entity (Entity): Entity carrying the *proposed* field list.
        existing_field (EntityField): The field as currently stored.
        field_data (EntityFieldIn): The proposed replacement for it.

    Raises:
        HTTPException: 400 naming the rule whose mapping the edit would break.
    """
    # Local imports: cte.models.business_rule imports crev2.models.entities, so
    # a module-level import risks a cycle.
    from netskope.common.utils.unified_mapping_fields import map_type_labels
    from netskope.integrations.cte.models.business_rule import (
        entity_fields_by_name,
        validate_mapping_against_fields,
    )

    # ``None`` for every non-map type, so this adds no trigger for them.
    if (
        existing_field.type == field_data.type
        and existing_field.coalesceStrategy == field_data.coalesceStrategy
        and existing_field.unique == field_data.unique
        and map_type_labels(existing_field.model_dump(mode="json"))
        == map_type_labels(field_data.model_dump(mode="json"))
    ):
        return

    field = field_data.name
    # mode="json" renders enums as the plain strings the Mongo-backed caller
    # yields, so the checker sees one shape either way.
    fields_by_name = entity_fields_by_name(
        [f.model_dump(mode="json") for f in entity.fields]
    )

    def _check(mapping: dict, rule_kind: str, rule_name) -> None:
        """Run the rule-save checker over one rule's narrowed mapping."""
        if not mapping:
            return
        try:
            validate_mapping_against_fields(mapping, fields_by_name)
        except ValueError as ex:
            raise HTTPException(
                400,
                f"Can not update the field '{field}' as it would break the "
                f"field mapping of {rule_kind} '{rule_name}'. {ex}",
            ) from ex

    for rule in connector.collection(Collections.CTE_BUSINESS_RULES).find(
        {"entity": entity.name, "fieldMapping": {"$exists": True, "$ne": {}}},
        {"name": 1, "fieldMapping": 1, "_id": 0},
    ):
        _check(
            {
                ioc_field: spec
                for ioc_field, spec in (rule.get("fieldMapping") or {}).items()
                if _param_references_field(spec, field)
            },
            "CTE business rule",
            rule.get("name"),
        )

    # The checker resolves a reference by its FIRST dotted segment, so a
    # "$<collection>.<field>" spec would look up the collection, miss, and skip
    # the check. The filter already proves the spec means this field, so the
    # bare name is substituted rather than changing the shared checker.
    unified_key = f"{get_entity_collection(entity.name)}.{field}"
    # Narrowed and projected, matching the filter tags.py uses: only a rule that
    # actually carries a field mapping can be broken by this edit, and only its
    # name and mapping are read. Keeps the per-rule tag lookups down too.
    for rule in connector.collection(Collections.UNIFIED_MAPPING_RULES).find(
        {"fieldMapping": {"$exists": True, "$ne": {}}},
        {"name": 1, "fieldMapping": 1, "_id": 0},
    ):
        _check(
            {
                ioc_field: f"${field}"
                for ioc_field, spec in (rule.get("fieldMapping") or {}).items()
                if _param_references_field(spec, unified_key)
            },
            "unified mapping business rule",
            rule.get("name"),
        )


def _mappings_joining_field(collection_name: str, field: str) -> list:
    """Names of every saved unified mapping that joins on this field.

    Matches either side of a join condition. Every mapping naming the field
    blocks the edit on its own, so all of them are collected rather than the
    first: the user needs to know which ones to go and change. Sorted, so the
    message is stable across reads, and de-duplicated, since one mapping can
    reach the same field from two of its joins.
    """
    names = set()
    for mapping in connector.collection(Collections.UNIFIED_MAPPING).find(
        {}, {"name": 1, "joins": 1}
    ):
        name = mapping.get("name")
        if not name:
            continue
        for join in mapping.get("joins") or []:
            condition = join.get("condition") or {}
            if (
                join.get("leftTable") == collection_name
                and condition.get("leftField") == field
            ) or (
                join.get("rightTable") == collection_name
                and condition.get("rightField") == field
            ):
                names.add(name)
                break
    return sorted(names)


def _assert_unique_kept_for_mappings(
    entity_name: str, existing_field: EntityField, field_data: EntityFieldIn
) -> None:
    """Reject clearing ``unique`` on a field a saved unified mapping joins on.

    A join is only allowed when at least one side is unique — the many-to-many
    guard in ``validate_mapping_tables`` — and a saved mapping is never
    re-validated, so clearing the flag afterwards is how a mapping ends up
    matching two non-unique fields and fanning each row out into the
    many-to-many result that guard exists to prevent.

    Any join side is locked, not just the one currently carrying the uniqueness:
    a unique join field is single-valued and matches at most one row, which is
    the row shape the saved mapping (and every rule reading its rows) was built
    against. Turning it off changes that shape whatever the opposite side does.
    Only Yes -> No is refused; making a field unique is always allowed.

    Args:
        entity_name (str): Entity the field belongs to.
        existing_field (EntityField): The field as currently stored.
        field_data (EntityFieldIn): The proposed replacement for it.

    Raises:
        HTTPException: 400 naming the mappings, per ``unique_join_lock_message``.
    """
    if not existing_field.unique or field_data.unique:
        return
    mapping_names = _mappings_joining_field(
        get_entity_collection(entity_name), existing_field.name
    )
    if not mapping_names:
        return
    # Every name goes to the log, where length is no object: the message itself
    # stops at UNIQUE_JOIN_LOCK_NAME_LIMIT, so this is where the full list lives.
    logger.warn(
        f"Refused to clear the unique flag on field '{existing_field.name}' of "
        f"entity '{entity_name}': it is used as a join condition in unified "
        f"mapping(s) {', '.join(mapping_names)}."
    )
    raise HTTPException(400, unique_join_lock_message(mapping_names))


@router.patch("/entities/{name}/fields/{field}", tags=["CREv2 Entities"])
async def update_field(
    name: str,
    field: str,
    field_data: EntityFieldIn,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> EntityOut:
    """Update field."""
    if name == THREAT_INDICATORS_ENTITY:
        raise HTTPException(
            400, "Threat Indicators entity is read-only and cannot be modified."
        )
    entity = connector.collection(Collections.CREV2_ENTITIES).find_one(
        {"name": name}
    )
    if not entity:
        raise HTTPException(404, f"Could not find entity with name {name}.")
    entity = Entity(**entity)
    existing_field: EntityField = next(
        filter(lambda x: x.name == field, entity.fields), None
    )
    if not existing_field:
        raise HTTPException(404, f"Could not find field with name {field}.")
    # EntityFieldIn.label is already whitespace-stripped by its StringConstraints;
    # stored EntityField.label is not, hence the .strip() on other.label below.
    new_label = field_data.label.lower()
    if new_label != existing_field.label.strip().lower() and any(
        other.name != field and other.label.strip().lower() == new_label
        for other in entity.fields
    ):
        raise HTTPException(
            400,
            f"A field with label '{field_data.label}' already exists.",
        )
    # A field's internal name is fixed at creation and never changes on update
    # (the update below excludes "name"). The incoming payload derives a name
    # from the new label, which can diverge from the real stored name. Pin it
    # back to the real name so the record migration and index updates below
    # operate on the correct field key, not the derived one.
    # Use model_copy rather than attribute assignment so this stays correct even
    # if EntityFieldIn later enables validate_assignment (which would otherwise
    # re-derive name from the label and resurrect the diverging-name bug).
    field_data = field_data.model_copy(update={"name": field})
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
    # Before the record migration below: validating after it would convert every
    # document and then reject the request.
    _assert_field_mappings_valid(entity, existing_field, field_data)
    _assert_unique_kept_for_mappings(name, existing_field, field_data)
    if (
        field_data.type != EntityFieldType.LIST
    ):  # all types except list; lists stay lists
        if (
            not existing_field.coalesceStrategy
            == EntityTypeCoalesceStrategy.MERGE
            and field_data.coalesceStrategy == EntityTypeCoalesceStrategy.MERGE
        ):
            connector.collection(
                get_entity_collection(entity.name)
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
                get_entity_collection(entity.name)
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
    # Preserve stored provenance metadata (e.g. ai_suggested) when the client
    # does not send any — the field edit form never includes metadata, and the
    # $set below would otherwise overwrite it with None.
    if field_data.metadata is None and existing_field and existing_field.metadata:
        field_data.metadata = existing_field.metadata
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
    return _entity_out(entity.model_dump(mode="json"))


def _param_references_field(spec, field: str) -> bool:
    """Check whether an action parameter value references a field.

    Action parameters reference fields as "$"-prefixed (possibly dotted)
    strings, e.g. "$value" or "$.foo.bar"; values may also be a list of
    such strings (mirrors how parameters are resolved in _map_params).
    """
    specs = spec if isinstance(spec, list) else [spec]
    for value in specs:
        if not (isinstance(value, str) and value.startswith("$")):
            continue
        path = value[1:].lstrip(".")
        if path == field or path.startswith(f"{field}."):
            return True
    return False


def _action_references_field(actions, field: str) -> bool:
    """Check whether any action's parameters reference a field.

    ``actions`` is a mapping of configuration name to a list of action
    dicts, each having a ``parameters`` mapping.
    """
    for action_list in (actions or {}).values():
        for action in action_list or []:
            for spec in (action.get("parameters") or {}).values():
                if _param_references_field(spec, field):
                    return True
    return False


def _mongo_query_references_field(node, field: str) -> bool:
    """Recursively check whether a business rule mongo query references a field.

    Field names appear as (possibly dotted) keys in the query; MongoDB
    operators are prefixed with "$" and are not field references.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            if key.startswith("$"):
                if _mongo_query_references_field(value, field):
                    return True
            else:
                if key == field or key.startswith(f"{field}."):
                    return True
                if _mongo_query_references_field(value, field):
                    return True
    elif isinstance(node, list):
        for item in node:
            if _mongo_query_references_field(item, field):
                return True
    return False


@router.delete("/entities/{name}/fields/{field}", tags=["CREv2 Entities"])
async def delete_field(
    name: str,
    field: str,
    _: User = Security(get_current_user, scopes=["cre_write"]),
) -> dict:
    """Delete a new field."""
    if name == THREAT_INDICATORS_ENTITY:
        raise HTTPException(
            400, "Threat Indicators entity is read-only and cannot be modified."
        )
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

    for rule in connector.collection(Collections.CREV2_BUSINESS_RULES).find(
        {"entity": name}
    ):
        if _action_references_field(rule.get("actions"), field):
            raise HTTPException(
                400,
                "Can not delete the field as it is referenced by one or more CRE business rule actions.",
            )
        try:
            mongo = json.loads((rule.get("entityFilters") or {}).get("mongo", "{}"))
        except (ValueError, TypeError):
            continue
        if _mongo_query_references_field(mongo, field):
            raise HTTPException(
                400,
                "Can not delete the field as it is referenced by one or more CRE business rules.",
            )

    for rule in connector.collection(Collections.CTE_BUSINESS_RULES).find(
        {"entity": name}
    ):
        mongo_strings = [(rule.get("filters") or {}).get("mongo", "{}")]
        for exception in rule.get("exceptions") or []:
            mongo_strings.append((exception.get("filters") or {}).get("mongo", "{}"))
        for mongo_string in mongo_strings:
            try:
                mongo = json.loads(mongo_string or "{}")
            except (ValueError, TypeError):
                continue
            if _mongo_query_references_field(mongo, field):
                raise HTTPException(
                    400,
                    "Can not delete the field as it is referenced by one or more CTE business rules.",
                )
        for spec in (rule.get("fieldMapping") or {}).values():
            if _param_references_field(spec, field):
                raise HTTPException(
                    400,
                    "Can not delete the field as it is referenced by one or more "
                    "CTE business rule field mappings.",
                )

    # Unified mapping rules reference fields by the flattened "<collection>.<field>"
    # key, so the collection prefix is what scopes the match to this entity.
    collection_name = get_entity_collection(name)
    unified_key = f"{collection_name}.{field}"
    if _mappings_joining_field(collection_name, field):
        raise HTTPException(
            400,
            "Can not delete the field as it is used as a join condition "
            "in one or more unified mappings.",
        )
    for rule in connector.collection(Collections.UNIFIED_MAPPING_RULES).find({}):
        mongo_strings = [(rule.get("filters") or {}).get("mongo", "{}")]
        for exception in rule.get("exceptions") or []:
            mongo_strings.append((exception.get("filters") or {}).get("mongo", "{}"))
        for mongo_string in mongo_strings:
            try:
                mongo = json.loads(mongo_string or "{}")
            except (ValueError, TypeError):
                continue
            if _mongo_query_references_field(mongo, unified_key):
                raise HTTPException(
                    400,
                    "Can not delete the field as it is referenced by one or more "
                    "unified mapping business rules.",
                )
        for spec in (rule.get("fieldMapping") or {}).values():
            if _param_references_field(spec, unified_key):
                raise HTTPException(
                    400,
                    "Can not delete the field as it is referenced by one or more "
                    "unified mapping business rule field mappings.",
                )
        if _action_references_field(rule.get("creActions"), unified_key):
            raise HTTPException(
                400,
                "Can not delete the field as it is referenced by one or more "
                "unified mapping business rule actions.",
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
        get_entity_collection(name)
    ).update_many({}, {"$unset": {field: ""}})

    return {"success": True}


@router.delete("/entities/{name}", tags=["CREv2 Entities"])
async def delete_entity(
    name: str, _: User = Security(get_current_user, scopes=["cre_write"])
):
    """Delete an entity."""
    if name == THREAT_INDICATORS_ENTITY:
        raise HTTPException(
            400, "Threat Indicators entity is read-only and cannot be deleted."
        )
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
    # The entity document goes first, and nothing is cascaded until it is
    # confirmed deleted. Every cascade below matches on the entity name alone, so
    # each can hit state that outlived its entity document -- an orphaned unified
    # mapping is precisely what this endpoint was fixed to stop leaving behind,
    # and it keeps running because /execute replays the stored pipeline without
    # re-reading entity metadata. Cascading before this point would therefore
    # mutate on the way to answering 404. Ordering it this way also settles a
    # concurrent double-delete: only the caller whose delete_one reports a
    # deleted document tears the rest down.
    result = connector.collection(Collections.CREV2_ENTITIES).delete_one(
        {"name": name}
    )
    if result.deleted_count == 0:
        raise HTTPException(404, f"Could not find entity with name {name}.")
    connector.collection(Collections.CREV2_BUSINESS_RULES).delete_many(
        {"entity": name}
    )
    if name != THREAT_INDICATORS_ENTITY:
        connector.collection(Collections.CTE_BUSINESS_RULES).delete_many(
            {"entity": name}
        )
    # Same cascade for the unified mappings joining this entity's records: the
    # records are dropped below, so a mapping over them could never run again and
    # only survives as an orphan that blanks the Unified Join Builder when opened.
    # Deleting the mapping takes its business rules, their sharing/action
    # configurations and its schedules with it. Safe to run with the entity
    # document already gone: the collection name is derived from the entity name,
    # and the join-index reclamation only needs metadata for the collections
    # still referenced by surviving mappings.
    delete_mappings_for_collection(get_entity_collection(name))
    # drop the collection with all the records, but preserve CTE-managed collections
    if name != THREAT_INDICATORS_ENTITY:
        connector.collection(
            get_entity_collection(name)
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
    if name == THREAT_INDICATORS_ENTITY:
        raise HTTPException(
            400, "Threat Indicators entity is read-only and cannot be modified."
        )
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
