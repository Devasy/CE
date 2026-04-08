"""
Utility functions for applying custom field mappings in ITSM integrations.

This module provides generic functions to apply custom field mappings at various
points in the ITSM data flow, including:
- After fetching data from source plugins (Netskope → CE)
- Before sending data to destination plugins (CE → Third-party)
- During sync operations (bidirectional mapping)
"""

import copy
import traceback
from typing import List
from netskope.integrations.itsm.models.task import Task
from netskope.integrations.itsm.models.task import TaskStatus, Severity, UpdatedTaskValues
from netskope.integrations.itsm.models.custom_fields import MappingDirection
from netskope.common.utils import (
    Logger,
)

logger = Logger()


def plugin_to_ce_task_map(task, configuration_db):
    """Map plugin (Jira) values in task.status, task.severity, etc. to CE values, and update task.updatedValues.

    Maintains old values from rawData (unmapped), and updated values as mapped.
    Dynamically finds mapping keys by event_field.
    """
    mapping_config = configuration_db.parameters.get("mapping_config", {})
    status_mapping = None
    severity_mapping = None

    for mapping_config_values in mapping_config.values():
        if isinstance(mapping_config_values, dict) and mapping_config_values.get("event_field") == "status":
            status_mapping = {}
            for m in mapping_config_values.get("mappings", []):
                for k, v in m.items():
                    if v and v not in status_mapping:
                        status_mapping[v] = k
        if isinstance(mapping_config_values, dict) and mapping_config_values.get("event_field") == "severity":
            severity_mapping = {}
            for m in mapping_config_values.get("mappings", []):
                for k, v in m.items():
                    if v and v not in severity_mapping:
                        severity_mapping[v] = k

    def map_field(value, mapping_dict, default_enum):
        if value is None:
            return None

        EnumClass = type(default_enum)
        key_to_check = value.value if hasattr(value, 'value') else str(value)
        if mapping_dict and key_to_check in mapping_dict:
            mapped_value = mapping_dict[key_to_check]
            return mapped_value

        if mapping_dict:
            return default_enum.value.capitalize() if key_to_check != "deleted" else key_to_check.capitalize()

        try:
            transformed_data = [
                " ".join(word.capitalize() for word in member.value.split("_"))
                if member.value not in ["notification", "failed"]
                else member.value
                for member in EnumClass
            ]
            if key_to_check in transformed_data:
                return key_to_check

            _ = EnumClass(key_to_check)
            return (
                " ".join(word.capitalize() for word in key_to_check.split("_"))
                if key_to_check != ['notification', 'failed']
                else key_to_check
            )
        except ValueError:
            return default_enum.value.capitalize()

    task.status = map_field(task.status, status_mapping, TaskStatus.NEW)
    task.severity = map_field(task.severity, severity_mapping, Severity.LOW)

    if task.dataItem and task.dataItem.rawData:
        old_status = task.dataItem.rawData.get("status", None)
        old_severity = task.dataItem.rawData.get("severity", None)
        if task.updatedValues:
            task.updatedValues.oldStatus = old_status
            task.updatedValues.status = task.status
            task.updatedValues.oldSeverity = old_severity
            task.updatedValues.severity = task.severity
        else:
            task.updatedValues = UpdatedTaskValues(
                status=task.status,
                oldStatus=old_status,
                oldSeverity=old_severity,
                severity=task.severity,
                assignee=None,
                oldAssignee=task.dataItem.rawData.get("assignee", None),
            )
    return task


def ce_to_tenant_task_map(tasks: List[Task], configuration_db):
    """Map CE values in task.status, task.severity, etc. to tenant/plugin values, including updatedValues fields.

    Returns a copy of the task with mapped fields.
    """
    mapping_config = configuration_db.parameters.get("mapping_config", {})
    status_mapping = None
    severity_mapping = None

    for v in mapping_config.values():
        if isinstance(v, dict) and v.get("event_field") == "status":
            status_mapping = v.get("mappings", [])
        if isinstance(v, dict) and v.get("event_field") == "severity":
            severity_mapping = v.get("mappings", [])

    def map_field(value, mapping, default_enum):
        if value is None:
            return None
        if not mapping:
            return default_enum

        forward_map = {}
        for m in mapping:
            for k, v in m.items():
                if k and k not in forward_map:
                    forward_map[k] = v
        mapped_value = forward_map.get(value, default_enum)

        return mapped_value

    updates_tasks = []
    for task in tasks:
        mapped_task = copy.deepcopy(task)
        mapped_task.status = map_field(task.status, status_mapping, TaskStatus.NEW.value)
        mapped_task.severity = map_field(
            task.severity, severity_mapping, Severity.OTHER.value
        )

        if mapped_task.updatedValues:
            mapped_task.updatedValues.status = map_field(
                task.updatedValues.status, status_mapping, TaskStatus.NEW.value
            )
            mapped_task.updatedValues.severity = map_field(
                task.updatedValues.severity, severity_mapping, Severity.LOW.value.capitalize()
            )
            mapped_task.updatedValues.oldStatus = map_field(
                task.updatedValues.oldStatus, status_mapping, TaskStatus.NEW.value
            )
            mapped_task.updatedValues.oldSeverity = map_field(
                task.updatedValues.oldSeverity, severity_mapping, Severity.LOW.value.capitalize()
            )
        updates_tasks.append(mapped_task)
    return updates_tasks


def apply_custom_mapping(
    data,
    configuration_db,
    direction: MappingDirection = MappingDirection.FORWARD
):
    """Apply custom field mappings to a single data item or a list of items.

    The mapping can be done in a 'forward' (source -> destination) or
    'reverse' (destination -> source) direction.

    Args:
        data: A single data item object or a list of data item objects.
        configuration_db: The configuration object containing mapping rules.
        direction (str): The direction of mapping, either "forward" or "reverse".

    Returns:
        The modified data item or list of data items.
    """
    if not data or not configuration_db:
        return data

    # Normalize input to always work with a list
    is_single_item = not isinstance(data, list)
    data_items = [data] if is_single_item else data

    processed_items = copy.deepcopy(data_items)

    try:
        mapping_config = configuration_db.parameters.get("mapping_config", {})
        if not mapping_config:
            return processed_items[0] if is_single_item else processed_items

        for section in mapping_config.values():
            event_field = section.get("event_field")
            mappings_array = section.get("mappings", [])
            value_map = {}

            for m in mappings_array:
                if isinstance(m, dict):
                    for dest_value, source_value in m.items():
                        if direction == MappingDirection.FORWARD and source_value and source_value not in value_map:
                            value_map[source_value] = dest_value
                        elif dest_value and dest_value not in value_map:  # "reverse"
                            value_map[dest_value] = source_value

            if not event_field or not value_map:
                continue
            default_value = (
                TaskStatus.NEW.value.capitalize() if event_field == "status"
                else Severity.LOW.value.capitalize() if event_field == "severity"
                else "other"
            )
            for item in processed_items:
                raw = item.rawData
                if event_field in raw:
                    orig_value = raw[event_field]
                    raw[event_field] = value_map.get(orig_value, default_value)
        return processed_items[0] if is_single_item else processed_items

    except Exception as e:
        logger.error(
            message=(
                f"Error applying custom mappings for plugin {configuration_db.name}. "
                f"Error: {e}"
            ),
            details=traceback.format_exc(),
        )
        # Return the original, unmodified data on error
        return data
