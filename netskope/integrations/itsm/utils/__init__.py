# flake8: noqa
"""ITSM related utils."""

from netskope.common.utils import FILTER_TYPES, DBConnector, Collections
from .constants import ALERTS_EVENT_UNIQUE_CRITERIA
from .schemas import alert_event_query_schema, task_query_schema, get_task_from_query

connector = DBConnector()


def filter_out_none_values(data: dict) -> dict:
    """Filter out keys with None values from a dict.

    Args:
        data (dict): Dictionary to be filtered.

    Returns:
        dict: Filtered dictionary.
    """
    return {k: v for k, v in data.items() if v is not None}
