"""JSON query schemas."""

from netskope.common.utils import DATE_VALUE_SCHEMA

IMAGE_METADATA_QUERY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "definitions": {
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
                            },
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
                        "$nin": {
                            "type": "array",
                            "items": {"type": ["string", "null"]},
                        },
                        "$not": {"type": "string"},
                        "$ne": {"type": "string"},
                    },
                },
                {"type": "string"},
            ]
        },
        "numberFilters": {
            "anyOf": [
                {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "$gt": {"type": "number"},
                        "$lt": {"type": "number"},
                        "$gte": {"type": "number"},
                        "$lte": {"type": "number"},
                        "$ne": {"type": ["number", "null"]},
                        "$eq": {"type": ["number", "null"]},
                        "$not": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "$gt": {
                                    "type": "number",
                                },
                                "$lt": {
                                    "type": "number",
                                },
                                "$gte": {
                                    "type": "number",
                                },
                                "$lte": {
                                    "type": "number",
                                },
                                "$eq": {
                                    "type": ["number", "null"],
                                },
                                "$ne": {
                                    "type": ["number", "null"],
                                },
                            },
                        },
                    },
                },
                {"type": ["number", "null"]},
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
        },
        "booleanFilters": {
            "anyOf": [
                {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "$eq": {"type": "boolean"},
                        "$ne": {"type": "boolean"},
                    },
                },
                {"type": "boolean"},
            ]
        },
        "arrayFilters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "$in": {"type": "array", "items": {"type": "string"}},
                "$nin": {"type": "array", "items": {"type": "string"}},
            },
        },
        "sourceFilters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "sourcePlugin": {"$ref": "#/definitions/stringFilters"},
                "file": {"$ref": "#/definitions/stringFilters"},
                "path": {"$ref": "#/definitions/stringFilters"},
                "extension": {"$ref": "#/definitions/stringFilters"},
                "sourceType": {"$ref": "#/definitions/stringFilters"},
                "outdated": {"$ref": "#/definitions/booleanFilters"},
                "status": {"$ref": "#/definitions/stringFilters"},
                "fileSize": {"$ref": "#/definitions/numberFilters"},
                "destinationPlugin": {"$ref": "#/definitions/stringFilters"},
                "classifierName": {"$ref": "#/definitions/stringFilters"},
                "trainingType": {"$ref": "#/definitions/stringFilters"},
                "lastShared": {"$ref": "#/definitions/dateFilters"},
                "lastFetched": {"$ref": "#/definitions/dateFilters"},
                "$and": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/sourceFilters"},
                },
                "$or": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/sourceFilters"},
                },
                "$expr": {
                    "type": "object",
                    "$ref": "#/definitions/expressionFilters"
                },
                "$nor": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/sourceFilters"},
                },
            },
        },
        "destinationFilters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"$elemMatch": {"$ref": "#/definitions/sourceFilters"}},
        },
        "searchRoot": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "sourcePlugin": {"$ref": "#/definitions/stringFilters"},
                "file": {"$ref": "#/definitions/stringFilters"},
                "path": {"$ref": "#/definitions/stringFilters"},
                "extension": {"$ref": "#/definitions/stringFilters"},
                "sourceType": {"$ref": "#/definitions/stringFilters"},
                "outdated": {"$ref": "#/definitions/booleanFilters"},
                "fileSize": {"$ref": "#/definitions/numberFilters"},
                "lastFetched": {"$ref": "#/definitions/dateFilters"},
                "sharedWith": {"$ref": "#/definitions/destinationFilters"},
                "$and": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/searchRoot"},
                },
                "$or": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/searchRoot"},
                },
                "$expr": {
                    "type": "object",
                    "$ref": "#/definitions/expressionFilters"
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
