"""Schemas."""
import inspect

from netskope.common.utils import FILTER_TYPES, get_database_fields_schema


def has_source_info_args(cls, method_name, check_args):
    """Check wether push method has required args for sharing source labeling information."""
    method = getattr(cls, method_name, None)
    if method is None:
        return False
    # check for args in method.
    signature = inspect.signature(method)
    for arg in check_args:
        if arg not in signature.parameters:
            return False
    return True


def alert_event_query_schema():
    """Get alert and event query schema.

    Returns:
        tuple[dict[str, dict[str, str]], dict[str, dict[str, str]], dict[str, Any]]: Static Dictionary,
        Raw Dictionary, Query Schema
    """
    STATIC_DICT = {
        "id": {"$ref": "#/definitions/stringFilters"},
        "alertName": {"$ref": "#/definitions/stringFilters"},
        "alertType": {"$ref": "#/definitions/stringFilters"},
        "eventType": {"$ref": "#/definitions/stringFilters"},
        "app": {"$ref": "#/definitions/stringFilters"},
        "appCategory": {"$ref": "#/definitions/stringFilters"},
        "user": {"$ref": "#/definitions/stringFilters"},
        "configuration": {"$ref": "#/definitions/stringFilters"},
        "type": {"$ref": "#/definitions/stringFilters"},
        "rawData_message": {"$ref": "#/definitions/stringFilters"},
        "rawData_plugin": {"$ref": "#/definitions/stringFilters"},
        "rawData_aggregateScoreRange": {"$ref": "#/definitions/stringFilters"},
    }
    RAW_DICT = {}
    DATABASE_FIELDS = get_database_fields_schema()
    RAW_DICT = {
        f"rawData_{key}": value_schema
        for key, value_schema in DATABASE_FIELDS.items()
        if key not in STATIC_DICT.keys()
    }
    RAW_DICT["rawData_aggregateScore"] = {"$ref": "#/definitions/numberFilters"}
    RAW_DICT["timestamp"] = {"$ref": "#/definitions/dateFilters"}

    ALERT_QUERY_SCHEMA = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "definitions": {
            **FILTER_TYPES,
            "searchRoot": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    **STATIC_DICT,
                    **RAW_DICT,
                    "$and": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/searchRoot"},
                    },
                    "$or": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/searchRoot"},
                    },
                    "$nor": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/searchRoot"},
                    },
                },
            },
        },
        "type": "object",
        "$ref": "#/definitions/searchRoot",
        "additionalProperties": False,
    }
    return STATIC_DICT, RAW_DICT, ALERT_QUERY_SCHEMA


def task_query_schema():
    """Get task query schema.

    Args:
        additional_dataitem_fields (List[str]):

    Returns:
        tuple[dict[str, dict[str, str]], dict[str, dict[str, str]], dict[str, Any]]: Static Dictionary,
        Data Item Dictionary, Query Schema
    """
    STATIC_DATAITEM_DICT, RAW_DATAITEM_DICT, _ = alert_event_query_schema()
    STATIC_DICT = {
        "id": {"$ref": "#/definitions/stringFilters"},
        "status": {"$ref": "#/definitions/stringFilters"},
        "approvalStatus": {"$ref": "#/definitions/stringFilters"},
        "link": {"$ref": "#/definitions/stringFilters"},
        "businessRule": {"$ref": "#/definitions/stringFilters"},
        "dataItem_id": {"$ref": "#/definitions/stringFilters"},
        "dataType": {"$ref": "#/definitions/stringFilters"},
        "dataSubType": {"$ref": "#/definitions/stringFilters"},
        "severity": {"$ref": "#/definitions/stringFilters"},
        "configuration": {"$ref": "#/definitions/stringFilters"},
        "createdAt": {"$ref": "#/definitions/dateFilters"},
        "dedupeCount": {"$ref": "#/definitions/numberFilters"},
        "lastUpdatedAt": {"$ref": "#/definitions/dateFilters"},
        "lastSyncedAt": {"$ref": "#/definitions/dateFilters"},
        "syncStatus": {"$ref": "#/definitions/stringFilters"}
    }
    DATA_ITEM_DICT = {
        f"dataItem_{key}": value
        for key, value in STATIC_DATAITEM_DICT.items()
    }
    RAW_DATA_ITEM_DICT = {
        f"dataItem_{key}": value
        for key, value in RAW_DATAITEM_DICT.items()
    }
    TASK_QUERY_SCHEMA = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "definitions": {
            **FILTER_TYPES,
            "searchRoot": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    **STATIC_DICT,
                    **DATA_ITEM_DICT,
                    **RAW_DATA_ITEM_DICT,
                    "$and": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/searchRoot"},
                    },
                    "$or": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/searchRoot"},
                    },
                    "$nor": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/searchRoot"},
                    },
                },
            },
        },
        "type": "object",
        "$ref": "#/definitions/searchRoot",
        "additionalProperties": False,
    }
    return STATIC_DICT, DATA_ITEM_DICT, TASK_QUERY_SCHEMA


def get_task_from_query(query: dict):
    """
    Get tasks from database based on query.

    Args:
        query (dict): Query to filter tasks
    """
    from netskope.common.utils import DBConnector, Collections
    from netskope.integrations.itsm.models import Task
    connector = DBConnector()
    return [
        Task(**t)
        for t in connector.collection(Collections.ITSM_TASKS).find(
            query
        )
    ]
