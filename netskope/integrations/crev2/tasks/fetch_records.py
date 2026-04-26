"""Fetch records from third party plugins."""

import traceback
from bson import BSON
from datetime import datetime
from typing import Optional

from py_expression_eval import Parser
from pymongo import ReturnDocument

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    SecretDict,
    integration,
    track,
    parse_events,
)
from netskope.integrations.crev2.plugin_base import PluginBase

from ..models import (
    ConfigurationDB,
    Entity,
    EntityFieldType,
    EntityTypeCoalesceStrategy,
    EntityMappingField,
    get_entity_by_name,
)
from .evaluate_records import evaluate_records

RECORDS_BATCH_SIZE = 1000
MAX_ENTITY_SIZE_WITH_VALUE_MAP = 14 * 1024 * 1024  # More than 14MB

connector = DBConnector()
helper = PluginHelper()
logger = Logger()
parser = Parser()


def _end_life(name: str, success: bool):
    connector.collection(Collections.CREV2_CONFIGURATIONS).update_one(
        {"name": name},
        {"$set": {"lastRunAt": datetime.now(), "lastRunSuccess": success}},
    )


@APP.task(name="cre.fetch_records", acks_late=False)
@integration("cre")
@track()
def fetch_records(
    configuration: str,
    data: Optional[list[dict]] = None,
    data_type: str = None,
    sub_type: str = None,
):
    """Fetch records from third party plugins."""
    try:
        logger.debug(f"Starting fetching records for {configuration}.")
        configuration_db = _get_configuration(configuration)
        if not configuration_db:
            return {
                "success": False,
                "message": (
                    f"Could not find CRE configuration with "
                    f"name {configuration}."
                ),
            }
        configuration = configuration_db
        plugin = _get_plugin(configuration)
        if not plugin:
            _end_life(configuration.name, False)
            return {
                "success": False,
                "message": f"Plugin {configuration.plugin} does not exist.",
            }
        if not configuration.active:
            logger.debug(f"{configuration.name} is not active.")
            return {
                "success": False,
                "message": f"{configuration.name} is not active.",
            }
        if isinstance(data, bytes):
            data = (
                parse_events(data, tenant_config_name=configuration.tenant, data_type=data_type, sub_type=sub_type)
                if configuration.tenant
                else parse_events(data, configuration=configuration, data_type=data_type, sub_type=sub_type)
            )
        for mapped_entity in configuration.mappedEntities:
            logger.debug(
                f"Fetching records for {mapped_entity.destination} "
                f"from {configuration.name}."
            )
            plugin.last_run_at = configuration.checkpoints.get(
                mapped_entity.entity
            )  # get the entity specific checkpoint
            if data:
                plugin.data = data
                plugin.data_type = data_type
                plugin.sub_type = sub_type
            updated_checkpoint = datetime.now()
            success = True
            try:
                records = plugin.fetch_records(mapped_entity.entity)
            except Exception:
                success = False
                logger.error(
                    f"Error occurred while fetching records for "
                    f"{mapped_entity.destination} from {configuration.name}.",
                    details=traceback.format_exc(),
                )
                _end_life(configuration.name, False)
            if success:
                # update storage and checkpoint
                connector.collection(Collections.CREV2_CONFIGURATIONS).update_one(
                    {"name": configuration.name},
                    {
                        "$set": {
                            "storage": plugin.storage,
                            f"checkpoints.{mapped_entity.entity}": updated_checkpoint,
                        }
                    },
                )
                if not records:
                    logger.debug(
                        f"No new records found for {mapped_entity.destination} "
                        f"from {configuration.name}."
                    )
                    continue
                logger.debug(
                    f"Fetched {len(records)} records for "
                    f"{mapped_entity.destination} from {configuration.name}."
                )
                mapped_records = _map_records(records, mapped_entity.fields)
                if not mapped_records:
                    continue
                stored_records = _store_records(
                    mapped_entity.destination, mapped_records
                )
                logger.debug(
                    f"Stored {len(stored_records)} records for "
                    f"{mapped_entity.destination} from {configuration.name}."
                )
                _update_calculated_fields(
                    mapped_entity.destination, mapped_entity.fields, stored_records
                )
                _update_mapped_fields(
                    mapped_entity.destination, mapped_entity.fields, stored_records
                )
                logger.debug(
                    f"Updated calculated fields for {mapped_entity.destination}."
                )
        _end_life(configuration.name, True)
    except Exception:
        logger.error(
            f"Error occurred while storing records for {configuration.name}.",
            details=traceback.format_exc(),
        )
        _end_life(configuration.name, False)
    logger.debug(f"Finished fetching records for {configuration.name}.")
    if isinstance(configuration, ConfigurationDB) and not configuration.tenant:
        # is not a netskope configuration; do the update records
        try:
            return update_records(configuration.name)
        except Exception:
            logger.error(
                f"Error occurred while updating records for {configuration.name}.",
                details=traceback.format_exc(),
            )


def _update_mapped_fields(
    destination: str,
    mapped_fields: list[EntityMappingField],
    records: list[dict],
):
    """Update mapped fields.

    Args:
        destination (str): Entity to update the mapped fields in.
        mapped_fields (list[EntityMappingField]): List of fields that
        were changed.
        Only the dependent mapped fields will be updated. ... for all.
        records (list[dict]): List of records to be updated with _id.
    """
    entity = get_entity_by_name(destination)
    if mapped_fields is not ...:
        mapped_fields = set(map(lambda x: x.destination, mapped_fields))
    value_maps = list(
        filter(
            lambda x: x.type in [EntityFieldType.VALUE_MAP_NUMBER, EntityFieldType.VALUE_MAP_STRING]
            and (
                True
                if mapped_fields is ...
                else x.params.field in mapped_fields
            ),
            entity.fields,
        ),
    )
    range_maps = list(
        filter(
            lambda x: x.type == EntityFieldType.RANGE_MAP
            and (
                True
                if mapped_fields is ...
                else x.params.field in mapped_fields
            ),
            entity.fields,
        ),
    )
    prepped_value_maps = {
        f.name: {m.label: m.value for m in f.params.mappings}
        for f in value_maps
    }
    field_mappings = {f.name: f for f in value_maps} | {
        f.name: f for f in range_maps
    }
    prepped_range_maps = {
        f.name: {range(m.gte, m.lte + 1): m.label for m in f.params.mappings}
        for f in range_maps
    }
    # return
    update_value_map = {}
    for record in records:
        for key, value in list(record.items()):
            if isinstance(value, list) and value:
                record[key] = value[
                    -1
                ]  # might be a field with coalstrat merge; use latest value
        set_fields = {}
        push_fields = {}
        for field, mappings in prepped_value_maps.items():
            if field_mappings[field].params.field not in record:
                continue
            if not mappings.get(
                record[field_mappings[field].params.field]
            ):
                field_value_map = {
                    "label": record[field_mappings[field].params.field],
                    "value": None
                } if record[field_mappings[field].params.field] else {}
                if field_value_map and field not in update_value_map:
                    update_value_map[field] = [field_value_map]
                elif field_value_map and field_value_map not in update_value_map[field]:
                    update_value_map[field].append(field_value_map)
            if (
                field_mappings[field].coalesceStrategy
                == EntityTypeCoalesceStrategy.OVERWRITE
            ):
                set_fields[field] = mappings.get(
                    record[field_mappings[field].params.field]
                )
            elif (
                field_mappings[field].coalesceStrategy
                == EntityTypeCoalesceStrategy.MERGE
            ) and (
                value := mappings.get(
                    record[field_mappings[field].params.field]
                )
            ) is not None:
                push_fields[field] = value
        for field, mappings in prepped_range_maps.items():
            if field_mappings[field].params.field not in record:
                continue
            for mapping_range, label in mappings.items():
                if record[field_mappings[field].params.field] in mapping_range:
                    if (
                        field_mappings[field].coalesceStrategy
                        == EntityTypeCoalesceStrategy.OVERWRITE
                    ):
                        set_fields[field] = label
                    elif (
                        field_mappings[field].coalesceStrategy
                        == EntityTypeCoalesceStrategy.MERGE
                    ):
                        push_fields[field] = label
                    break
            else:
                if (
                    field_mappings[field].coalesceStrategy
                    == EntityTypeCoalesceStrategy.OVERWRITE
                ):
                    set_fields[field] = None
                elif (
                    field_mappings[field].coalesceStrategy
                    == EntityTypeCoalesceStrategy.MERGE
                ):
                    push_fields[field] = None
        if set_fields or push_fields:
            connector.collection(
                f"{Collections.CREV2_ENTITY_PREFIX.value}{destination}"
            ).update_one(
                {"_id": record["_id"]},
                ({"$set": set_fields} if set_fields else {})
                | ({"$push": push_fields} if push_fields else {}),
            )
    entity_field = connector.collection(Collections.CREV2_ENTITIES).find_one({"name": destination})
    if not entity_field:
        logger.warn(
            f"Entity {destination} not found, "
            "skipping addition of new incoming mapping values inside field value map."
        )
        return
    current_size = len(BSON.encode(entity_field))
    for field, values_map_ in update_value_map.items():
        for value in values_map_:
            projected_addition = {
                "label": value.get("label"),
                "value": None
            }
            new_size = current_size + len(BSON.encode(projected_addition))
            if new_size > MAX_ENTITY_SIZE_WITH_VALUE_MAP:
                logger.warn(
                    f"Field {destination} size limit exceeds the maximum allowed limit of 14 MB, "
                    "skipping addition of new incoming mapping values inside field value map."
                )
                return
            connector.collection(Collections.CREV2_ENTITIES).update_one(
                {
                    "name": destination,
                    "fields": {
                        "$elemMatch": {
                            "name": field,
                            "params.mappings": {
                                "$not": {
                                    "$elemMatch": {"label": value.get("label")}
                                }
                            }
                        }
                    }
                },
                {
                    "$push": {
                        "fields.$[field].params.mappings": projected_addition
                    }
                },
                array_filters=[
                    {"field.name": field}
                ]
            )
            current_size = new_size


def _update_calculated_fields(
    destination: str,
    updated_fields: list[EntityMappingField],
    records: list[dict],
):
    """Update calculated fields.

    Args:
        destination (str): Destination entity to calculate the fields in.
        updated_fields (list[EntityMappingField]): List of fields that were
        updated.
        records (list[dict]): List of records with _id.
    """
    entity = get_entity_by_name(destination)
    if updated_fields is not ...:
        updated_fields = set(map(lambda x: x.destination, updated_fields))
    calculated_fields = [
        f
        for f in filter(
            # field type is calculated
            lambda field: field.type == EntityFieldType.CALCULATED
            and (
                # that's it.
                True
                if updated_fields is ...
                # and one of the dependency of the calculated field might
                # have been updated
                else set(field.params.dependencies).intersection(
                    updated_fields
                )
            ),
            entity.fields,
        )
    ]
    # TODO: optimize this loop; cache expression parsing
    for record in records:
        for key, value in list(record.items()):
            if isinstance(value, list) and value:
                record[key] = value[
                    -1
                ]  # might be a field with coalstrat merge; use latest value
        set_fields = {}
        push_fields = {}
        for calculated_field in calculated_fields:
            expr = parser.parse(
                calculated_field.params.expression.replace("$", "")
            )
            if not all(
                [
                    (var in record and record[var] is not None)
                    for var in expr.variables()
                ]
            ):  # skip if any of the required variables are not set
                continue
            try:
                if (
                    calculated_field.coalesceStrategy
                    == EntityTypeCoalesceStrategy.OVERWRITE
                ):
                    set_fields[calculated_field.name] = int(
                        expr.evaluate(record)
                    )
                elif (
                    calculated_field.coalesceStrategy
                    == EntityTypeCoalesceStrategy.MERGE
                ):
                    push_fields[calculated_field.name] = int(
                        expr.evaluate(record)
                    )
            except Exception:
                logger.error(
                    f"Could not evaluate expression "
                    f"{calculated_field.params.expression} for a record.",
                    details=traceback.format_exc(),
                )
        connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{destination}"
        ).update_one(
            {"_id": record["_id"]}, {"$set": set_fields, "$push": push_fields}
        )


def _get_unique_fields(entity: Entity) -> list[str]:
    """Get unique fields."""
    return [field.name for field in filter(lambda x: x.unique, entity.fields)]


def _required_fields_exist(record: dict, fields: list[str]) -> bool:
    """Check if the record has at least one of the required fields.

    Args:
        record (dict): Record to be checked.
        fields (list[str]): List of required fields.

    Returns:
        bool: Whether the record has at least one of the required fields or not.
    """
    for field in fields:
        if field not in record or record[field] is None:
            continue
        return True
    return False


@APP.task(name="cre.update_records", acks_late=False)
@integration("cre")
@track()
def update_records(configuration: str):
    """Update records."""
    logger.debug(f"Starting updating records for {configuration}.")
    configuration_db = _get_configuration(configuration)
    if not configuration_db:
        return {
            "success": False,
            "message": (
                f"Could not find CRE configuration with name {configuration}."
            ),
        }
    configuration = configuration_db
    plugin = _get_plugin(configuration)
    if not plugin:
        _end_life(configuration.name, success=False)
        return {
            "success": False,
            "message": f"Plugin {configuration.plugin} does not exist.",
        }
    if not configuration.active:
        logger.debug(f"{configuration.name} is not active.")
        return {
            "success": False,
            "message": f"{configuration.name} is not active.",
        }
    success = True
    for mapped_entity in configuration.mappedEntities:
        logger.debug(
            f"Updating records for {mapped_entity.destination} "
            f"from {configuration.name}."
        )
        plugin_entity = next(
            filter(
                lambda e: e.name == mapped_entity.entity, plugin.get_entities()
            )
        )
        required_fields = list(
            map(
                lambda f: f.name,
                filter(lambda f: f.required, plugin_entity.fields),
            )
        )
        field_mappings = {
            field.source: field.destination for field in mapped_entity.fields
        }
        records = connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{mapped_entity.destination}"
        ).find({}, {field_mappings[field]: True for field in required_fields})
        mapped_records = [
            {
                source: record.get(destination)
                for source, destination in field_mappings.items()
            }
            | {"_id": record["_id"]}
            for record in records
        ]
        before_count = len(mapped_records)
        mapped_records = list(
            filter(
                lambda record: _required_fields_exist(record, required_fields),
                mapped_records,
            )
        )
        if (after_count := len(mapped_records)) != before_count:
            logger.info(
                f"Skipped {before_count - after_count} record(s) due to "
                f"missing values for the required field(s) in "
                f"{mapped_entity.destination} from {configuration.name}."
            )
        try:
            records = plugin.update_records(
                mapped_entity.entity, mapped_records
            )
        except Exception:
            success = False
            logger.error(
                "Error occurred while updating records.",
                details=traceback.format_exc(),
            )
            continue
        logger.debug(
            f"Fetched {len(records)} record updates for "
            f"{mapped_entity.destination} from {configuration.name}."
        )
        try:
            mapped_records = _map_records(records, mapped_entity.fields)
            stored_records = _store_records(
                mapped_entity.destination, mapped_records
            )
            _update_calculated_fields(
                mapped_entity.destination, mapped_entity.fields, stored_records
            )
            _update_mapped_fields(
                mapped_entity.destination, mapped_entity.fields, stored_records
            )
            execute_celery_task(
                evaluate_records.apply_async,
                "cre.evaluate_records",
                args=[
                    mapped_entity.destination,
                    list(map(lambda x: x["_id"], stored_records)),
                ],
            )

        except Exception:
            success = False
            logger.error(
                "Error occurred while updating records.",
                details=traceback.format_exc(),
            )

    _end_life(configuration.name, success=success)
    return {"success": success}


def _store_records(destination: str, records: list) -> list[dict]:
    """Store records in destination."""
    entity = get_entity_by_name(destination)
    updated_records = []
    # TODO: verify for multiple unique fields
    unique_fields = _get_unique_fields(entity)
    fields = {}
    for field in entity.fields:
        fields[field.name] = field
    for record in records:
        set_fields = {}
        set_fields["lastUpdated"] = datetime.now()
        for key, value in list(record.items()):
            if key not in fields:
                continue
            if fields[key].type == EntityFieldType.LIST:
                if (
                    fields[key].coalesceStrategy
                    == EntityTypeCoalesceStrategy.MERGE
                ):
                    if value is None:
                        continue
                    set_fields[key] = {
                        "$setUnion": [{"$ifNull": [f"${key}", []]}, value]
                    }
                    # add_to_set_fields[key] = {
                    #     "$each": value if isinstance(value, list) else [value]
                    # }
                else:
                    set_fields[key] = [] if value is None else value
            else:
                if (
                    fields[key].coalesceStrategy
                    == EntityTypeCoalesceStrategy.MERGE
                ):
                    if value is None:
                        continue
                    # push_fields[key] = value
                    set_fields[key] = {
                        "$concatArrays": [
                            {"$ifNull": [f"${key}", []]},
                            [value],
                        ]
                    }
                else:
                    set_fields[key] = value
        if "_id" in record:
            find = {
                "_id": record["_id"]
            }  # _id is not needed in update; pop it
        else:
            find = {field: record.get(field) for field in unique_fields}
        if not find:
            # if no condition, a new record must be created.
            # `{}` will match with the first record which is not intentd.
            # Have to create a condition that does not match with any records.
            find = {"ne": {"$in": []}}
        original = (
            connector.collection(
                f"{Collections.CREV2_ENTITY_PREFIX.value}{destination}"
            ).find_one(find)
            or {}
        )
        updated = connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{destination}"
        ).find_one_and_update(
            find,
            [
                {"$set": set_fields},
            ],
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if "lastUpdated" in original:
            original.pop("lastUpdated")
        if "lastUpdated" in updated:
            updated.pop("lastUpdated")
        if original != updated:
            updated_records.append(updated)

    return updated_records


def _get_configuration(configuration_name: str) -> Optional[ConfigurationDB]:
    configuration_db = connector.collection(
        Collections.CREV2_CONFIGURATIONS
    ).find_one({"name": configuration_name})
    return ConfigurationDB(**configuration_db) if configuration_db else None


def _get_plugin(configuration: ConfigurationDB) -> Optional[PluginBase]:
    plugin_class = helper.find_by_id(configuration.plugin)
    if plugin_class and configuration.mappedEntities:
        plugin_class.mappedEntities = [mapped_entity.model_dump() for mapped_entity in configuration.mappedEntities]
    return (
        plugin_class(
            configuration.name,
            SecretDict(configuration.parameters),
            configuration.storage,
            None,
            logger,
        )
        if plugin_class
        else None
    )


def _map_records(
    records: list,
    fields: list,
) -> list[dict]:
    """
    Map records from the plugin format to the desired format.

    Args:
        records (list): List of records from the plugin.
        destination (str): Destination entity to map to.
        fields (list): List of fields to map.

    Returns:
        list: List of mapped records.
    """
    mappings = {"_id": "_id"}
    for field in fields:
        mappings[field.source] = field.destination
    mapped_records = []
    for record in records:
        mapped_record = {}
        for key, value in record.items():
            if key in mappings:
                mapped_record[mappings[key]] = value
        mapped_records.append(mapped_record)
    return mapped_records


@APP.task(name="cre.update_calculated_fields", acks_late=True)
def update_calculated_fields(entity: str):
    """Update all the calculated fields.

    Args:
        entity (str): Entity to update the calcualted fields in.
    """
    logger.debug(
        f"Task started to update calculated fields for entity {entity}."
    )
    records = []
    for record in connector.collection(
        f"{Collections.CREV2_ENTITY_PREFIX.value}{entity}"
    ).find({}):
        records.append(record)
        if len(records) == RECORDS_BATCH_SIZE:
            _update_calculated_fields(entity, ..., records)
            records = []
    if records:
        _update_calculated_fields(entity, ..., records)
    logger.debug(
        f"Task finished to update calculated fields for entity {entity}."
    )


@APP.task(name="cre.update_mapped_fields", acks_late=True)
def update_mapped_fields(entity: str):
    """Update all the mapped fields.

    Args:
        entity (str): Entity to update the mapped fields in.
    """
    logger.debug(f"Task started to update mapped fields for entity {entity}.")
    records = []
    for record in connector.collection(
        f"{Collections.CREV2_ENTITY_PREFIX.value}{entity}"
    ).find({}):
        records.append(record)
        if len(records) == RECORDS_BATCH_SIZE:
            _update_mapped_fields(entity, ..., records)
            records = []
    if records:
        _update_mapped_fields(entity, ..., records)
    logger.debug(f"Task finished to update mapped fields for entity {entity}.")


def import_records(entity: str, records: list[dict]) -> int:
    """Import list of records.

    Args:
        entity (str): Entity to import in.
        records (list[dict]): List of records to be imported.
    """
    stored_records = _store_records(entity, records)
    logger.debug(
        f"Stored {len(stored_records)} imported records for {entity}."
    )
    _update_calculated_fields(entity, ..., stored_records)
    _update_mapped_fields(entity, ..., stored_records)
    logger.debug(f"Updated calculated fields for {entity}.")
    return len(stored_records)
