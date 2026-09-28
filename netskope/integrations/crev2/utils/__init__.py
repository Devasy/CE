"""CREv2 utils."""

import json
from copy import deepcopy
from typing import Union

from netskope.common.utils import Collections, parse_dates
from netskope.common.models import PollIntervalUnit

from ..models import Entity, EntityFieldType


NETSKOPE_POLL_INTERVAL = 1
NETSKOPE_POLL_INTERVAL_UNIT = PollIntervalUnit.HOURS

THREAT_INDICATORS_ENTITY = "Threat Indicators"


def get_entity_collection(entity_name: str) -> str:
    """Resolve the Mongo collection name for a CRE entity.

    Threat Indicators data is owned by CTE and stored in the ``indicators``
    collection; all other CRE entities live in ``crev2_entity_<name>``.
    """
    if entity_name == THREAT_INDICATORS_ENTITY:
        return Collections.INDICATORS.value
    return f"{Collections.CREV2_ENTITY_PREFIX.value}{entity_name}"


def get_entity_recency_field(entity_name: str) -> str:
    """Mongo field used for days-window filtering on entity records.

    CTE indicators use ``lastSeen``; CRE-ingested entities use ``lastUpdated``.
    """
    if entity_name == THREAT_INDICATORS_ENTITY:
        return "lastSeen"
    return "lastUpdated"


def build_threat_indicator_source_match(source: str) -> dict:
    """Mongo $elemMatch for a single CTE source configuration."""
    return {"sources": {"$elemMatch": {"source": source}}}


def _collapse_mongo_and(clauses: list) -> dict:
    clauses = [c for c in clauses if c]
    if not clauses:
        return {}
    if len(clauses) == 1:
        return clauses[0]
    return {"$and": clauses}


def build_threat_indicator_business_rule_match(
    entity_filters_mongo: str,
    source_configuration: str,
) -> dict:
    """Combine user entityFilters.mongo with TI source configuration (server-side)."""
    user_query = json.loads(entity_filters_mongo or "{}", object_hook=parse_dates)
    clauses = []
    if user_query:
        clauses.append(user_query)
    clauses.append(build_threat_indicator_source_match(source_configuration))
    return _collapse_mongo_and(clauses)


def build_pipeline_from_entity(entity: Entity) -> list:
    """Generate a pipeline from the given entity containing reference fields.

    Args:
        entity (Entity): The entity for which the pipeline is being built.

    Returns:
        list: The pipeline to lookup and unwind reference fields.
    """
    reference_fields = filter(
        lambda x: x.type == EntityFieldType.REFERENCE, entity.fields
    )
    lookup_pipeline_tmp = [
        [
            {
                "$lookup": {
                    "from": f"{Collections.CREV2_ENTITY_PREFIX.value}{f.params.entity}",
                    "localField": f.name,
                    "foreignField": f.params.field,
                    "as": f.name,
                }
            },
            {
                "$unwind": {
                    "path": f"${f.name}",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {"$unset": f"{f.name}._id"},
        ]
        for f in reference_fields
    ]
    lookup_pipeline_unpacked = []
    for item in lookup_pipeline_tmp:
        lookup_pipeline_unpacked += item
    return lookup_pipeline_unpacked


def is_value_variable(value: str) -> bool:
    """Check if the provided value is a variable.

    Args:
        value (str): Value to be checked.

    Returns:
        bool: Wheather the value is variable or not.
    """
    return value.strip().startswith("$")


def get_latest_value(value: Union[str, int, list]) -> Union[str, int]:
    """Get latest value from the list.

    Args:
        value (Union[str, list]): List of string object.

    Returns:
        str: Value of the latest element in the list.
    """
    if isinstance(value, list):
        if value:
            return value[-1]
        return ""
    return value


def get_latest_values(config: dict, exclude_keys: list = []) -> dict:
    """Get latest values from the config dict.

    Args:
        config (dict): Configuration dict.
        exclude_keys (list, optional): List of keys to be excluded.

    Returns:
        dict: Dictionary with all the latest values.
    """
    config_copy = deepcopy(config)
    for key, value in config_copy.items():
        if key in exclude_keys:
            continue
        config_copy[key] = get_latest_value(value)
    return config_copy


def _choice_label(detail, value):
    """Label snapshotted for this exact value, or None if it no longer matches.

    Matching on the value is what keeps a rewritten parameter from inheriting
    the previous selection's name.
    """
    for choice in detail.value_labels or []:
        if choice.value == value:
            return choice.label
    return None


def _labelled_value(label, value):
    """Render one choice value as "label (value)", or bare when unresolved.

    Scalars only: a plugin may carry a whole remote object as the choice
    value, and flattening that to a string would lose its structure.
    """
    if not label or not isinstance(value, (str, int, float, bool)):
        return value
    if str(label) == str(value):
        return value
    return f"{label} ({value})"


def display_action_parameters(action) -> dict:
    """Action parameters with choice ids named by what the user selected.

    A choice parameter stores only the option value (a group id, say), so a
    raw parameter map reads as opaque ids. ``fieldDetails.value_labels``
    carries the labels resolved at rule save; pair the two so an alert shows
    "test (6f2a9c1e-...)" - readable, without losing the id actually sent.

    Args:
        action (Action): Action whose parameters should be rendered.

    Returns:
        dict: Copy of action.parameters with resolved choice values labelled.
    """
    parameters = dict(action.parameters or {})
    for detail in action.fieldDetails or []:
        if not detail.value_labels or detail.key not in parameters:
            continue
        value = parameters[detail.key]
        if isinstance(value, list):
            parameters[detail.key] = [
                _labelled_value(_choice_label(detail, item), item)
                for item in value
            ]
        else:
            parameters[detail.key] = _labelled_value(
                _choice_label(detail, value), value
            )
    return parameters
