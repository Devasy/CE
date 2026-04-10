"""Common routers."""


DATE_STRING_FILTERS = {
    "dateFilters": {
        "anyOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "$gt": {"type": "string"},
                    "$lt": {"type": "string"},
                    "$gte": {"type": "string"},
                    "$lte": {"type": "string"},
                    "$ne": {"type": "string"},
                    "$not": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "$gt": {"type": "string"},
                            "$lt": {"type": "string"},
                            "$gte": {"type": "string"},
                            "$lte": {"type": "string"},
                            "$ne": {"type": "string"},
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
}
