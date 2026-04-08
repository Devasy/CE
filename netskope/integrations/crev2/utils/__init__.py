"""CREv2 utils."""

from typing import Union
from copy import deepcopy
from netskope.common.utils import Collections
from netskope.common.models import PollIntervalUnit

from ..models import Entity, EntityFieldType


NETSKOPE_POLL_INTERVAL = 1
NETSKOPE_POLL_INTERVAL_UNIT = PollIntervalUnit.HOURS


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
