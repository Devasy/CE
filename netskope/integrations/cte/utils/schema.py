"""JSON query schemas."""

from netskope.common.utils import DATE_VALUE_SCHEMA

# for case-insensitive search
INDICATOR_STRING_FIELDS = ["value", "source", "comments"]

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
                    "$expr": {
                        "type": "object",
                        "$ref": "#/definitions/expressionFilters"
                    }
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
                    "$expr": {
                        "type": "object",
                        "$ref": "#/definitions/expressionFilters"
                    },
                },
            },
            {"type": "string"},
        ]
    },
}


INDICATOR_QUERY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "definitions": {
        **DATE_STRING_FILTERS,
        "numberFilters": {
            "anyOf": [
                {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "$gt": {"type": "integer"},
                        "$lt": {"type": "integer"},
                        "$gte": {"type": "integer"},
                        "$lte": {"type": "integer"},
                        "$ne": {"type": ["integer", "null"]},
                        "$eq": {"type": ["integer", "null"]},
                    },
                },
                {"type": ["integer", "null"]},
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
        "retractionResult": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"$ref": "#/definitions/stringFilters"},
                "status": {"$ref": "#/definitions/stringFilters"},
                "$and": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/retractionResult"},
                },
                "$or": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/retractionResult"},
                },
                "$nor": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/retractionResult"},
                },
            }
        },
        "retractionResults": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "$elemMatch": {"$ref": "#/definitions/retractionResult"}
            },
        },
        "sourceFilters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "reputation": {"$ref": "#/definitions/numberFilters"},
                "internalHits": {"$ref": "#/definitions/numberFilters"},
                "externalHits": {"$ref": "#/definitions/numberFilters"},
                "source": {"$ref": "#/definitions/stringFilters"},
                "comments": {"$ref": "#/definitions/stringFilters"},
                "extendedInformation": {"$ref": "#/definitions/stringFilters"},
                "firstSeen": {"$ref": "#/definitions/dateFilters"},
                "lastSeen": {"$ref": "#/definitions/dateFilters"},
                "severity": {"$ref": "#/definitions/arrayFilters"},
                "tags": {"$ref": "#/definitions/arrayFilters"},
                "retracted": {"$ref": "#/definitions/booleanFilters"},
                "retractionDestinations": {"$ref": "#/definitions/retractionResults"},
                "destinations": {"$ref": "#/definitions/retractionResults"},
                "$and": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/sourceFilters"},
                },
                "$or": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/sourceFilters"},
                },
                "$nor": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/sourceFilters"},
                },
                "$expr": {
                    "type": "object",
                    "$ref": "#/definitions/expressionFilters"
                }
            },
        },
        "sourcesFilters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "$elemMatch": {"$ref": "#/definitions/sourceFilters"}
            },
        },
        "searchRoot": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "internalHits": {"$ref": "#/definitions/numberFilters"},
                "externalHits": {"$ref": "#/definitions/numberFilters"},
                "value": {"$ref": "#/definitions/stringFilters"},
                "type": {"$ref": "#/definitions/stringFilters"},
                "test": {"$ref": "#/definitions/booleanFilters"},
                "safe": {"$ref": "#/definitions/booleanFilters"},
                "active": {"$ref": "#/definitions/booleanFilters"},
                "expiresAt": {"$ref": "#/definitions/dateFilters"},
                "sharedWith": {"$ref": "#/definitions/arrayFilters"},
                "sources": {"$ref": "#/definitions/sourcesFilters"},
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
                "$expr": {
                    "type": "object",
                    "$ref": "#/definitions/expressionFilters"
                }
            },
        },
    },
    "type": "object",
    "$ref": "#/definitions/searchRoot",
    "additionalProperties": False,
}
