"""Common routers."""

from netskope.common.utils import DATE_VALUE_SCHEMA

DATE_STRING_FILTERS = {
    "dateFilters": {
        "anyOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "$gt": DATE_VALUE_SCHEMA,
                    "$lt": DATE_VALUE_SCHEMA,
                    "$gte": DATE_VALUE_SCHEMA,
                    "$lte": DATE_VALUE_SCHEMA,
                    "$ne": DATE_VALUE_SCHEMA,
                    "$not": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "$gt": DATE_VALUE_SCHEMA,
                            "$lt": DATE_VALUE_SCHEMA,
                            "$gte": DATE_VALUE_SCHEMA,
                            "$lte": DATE_VALUE_SCHEMA,
                            "$ne": DATE_VALUE_SCHEMA,
                        }
                    },
                },
            },
            {"type": "string"},
        ]
    },
    "stringFilters": {
        "anyOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "$eq": {"type": "string"},
                    "$regex": {"type": "string"},
                    "$in": {"type": "array", "items": {"type": ["string", "null"]}},
                    "$nin": {"type": "array", "items": {"type": ["string", "null"]}},
                    "$not": {"type": "string"},
                    "$ne": {"type": "string"},
                },
            },
            {"type": "string"},
        ]
    },
    "expressionFilters": {
        "anyOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "$gt": {"type": "array"},
                    "$lt": {"type": "array"},
                    "$gte": {"type": "array"},
                    "$lte": {"type": "array"},
                    "$ne": {"type": "array"},
                    "$eq": {"type": "array"}
                },
            }
        ]
    }
}
