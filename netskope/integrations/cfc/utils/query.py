"""Provides utility methods for filter queries."""
import json
from typing import Dict

from netskope.integrations.cfc.models.business_rule import BusinessRuleDB
from netskope.integrations.cfc.utils import parse_dates


def build_mongo_query(
    rule: BusinessRuleDB,
) -> Dict:
    """Build a mongo query for the business rule.

    Args:
        rule (BusinessRuleDB): Business rule to build the query for.

    Returns:
        Dict: Mongo query.
    """
    query = {
        "$and": [
            json.loads(
                rule.filters.mongo,
                object_hook=lambda pair: parse_dates(pair),
            )
        ]
    }
    for mute in rule.exceptions:
        if mute.filters:
            mute_query = json.loads(
                mute.filters.mongo, object_hook=lambda pair: parse_dates(pair)
            )
            query["$nor"] = query.get("$nor", []) + [mute_query]
    return query
