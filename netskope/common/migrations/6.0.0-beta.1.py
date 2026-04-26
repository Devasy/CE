"""Migrations for 6.0.0 release."""

import traceback
import subprocess
import asyncio
import os

from netskope.common.models import NetskopeField, NetskopeFieldType, FieldDataType
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    RepoManager,
    get_default_policy,
)
from pymongo import UpdateOne
from netskope.common.models.repo import PluginRepo
from netskope.common.models.settings import SettingsDB
from netskope.common.utils.integrations_tasks_scheduler import (
    schedule_or_delete_integrations_tasks,
)

connector = DBConnector()
logger = Logger()
loop = asyncio.get_event_loop()
manager = RepoManager()


def add_ce_logs_fields():
    """Add resolution field for Cloud Exchange Alerts plugin."""
    try:
        connector.collection(Collections.NETSKOPE_FIELDS).update_one(
            {"name": "resolution"},
            {
                "$set": NetskopeField(
                    name="resolution",
                    label="Resolution",
                    type=NetskopeFieldType.ALERT,
                    dataType=FieldDataType.TEXT,
                ).model_dump()
            },
            upsert=True,
        )
        connector.collection(Collections.NETSKOPE_FIELDS).update_one(
            {"name": "details"},
            {
                "$set": NetskopeField(
                    name="details",
                    label="Details",
                    type=NetskopeFieldType.ALERT,
                    dataType=FieldDataType.TEXT,
                ).model_dump()
            },
            upsert=True,
        )
        connector.collection(Collections.NETSKOPE_FIELDS).update_one(
            {"name": "ce_log_type"},
            {
                "$set": NetskopeField(
                    name="ce_log_type",
                    label="CE Log Type",
                    type=NetskopeFieldType.ALERT,
                    dataType=FieldDataType.TEXT,
                ).model_dump()
            },
            upsert=True,
        )
        connector.collection(Collections.NETSKOPE_FIELDS).update_one(
            {"name": "createdAt"},
            {
                "$set": NetskopeField(
                    name="createdAt",
                    label="Created At",
                    type=NetskopeFieldType.ALERT,
                    dataType=FieldDataType.DATETIME,
                ).model_dump()
            },
            upsert=True,
        )
    except Exception as error:
        logger.error(
            "Error occurred while adding resolution/details field for CE logs.",
            details=traceback.format_exc(),
            error_code="CE_1054",
        )
        raise error


def migrate_transform_data():
    """Migrate transformData field value."""
    try:
        for config in connector.collection(Collections.CLS_CONFIGURATIONS).find({}):
            connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
                {"name": config["name"]},
                {
                    "$set": {
                        "parameters.transformData": (
                            "json"
                            if not config.get("parameters", {}).get(
                                "transformData", False
                            )
                            else "cef"
                        )
                    }
                },
            )
    except Exception as error:
        logger.error(
            "Error occurred while migrating transformData field for CLS configurations.",
            details=traceback.format_exc(),
            error_code="CE_1057",
        )
        raise error


def get_destination_label(plugin_name: str) -> str:
    """
    Calculate the display-friendly destination label from a plugin name.

    Args:
        plugin_name: The raw plugin name (e.g., 'jira_itsm').

    Returns:
        A formatted string for use as a destination label (e.g., 'Jira').
    """
    label_mappings = {
        "netskope_itsm": "Netskope Tenant",
        "jira_itsm": "Jira",
        "servicenow_itsm": "ServiceNow",
        "ivanti_itsm": "Ivanti",
        "bmc_helix_itsm": "BMC Helix",
        "manage_engine_service_desk_plus": "Manage Engine",
    }

    if plugin_name in label_mappings:
        return label_mappings[plugin_name]

    return plugin_name.replace("_itsm", "").replace("_", " ").title()


def _process_mapping_section(
    source_data: dict, section_name: str, field_set: set, destination_label: str
):
    """
    Transform a single mapping section from the old format to the new format.

    Args:
        source_data: The dictionary containing the old mapping (e.g., {"status_mapping": {...}}).
        section_name: The name of the section ("status" or "severity").
        field_set: The set to which new custom field names will be added.
        destination_label: The label for the destination plugin.

    Returns:
        A dictionary with the new mapping format, or None if no source mapping was found.
    """
    source_key = f"{section_name}_mapping"
    if not source_data or source_key not in source_data:
        return None

    DEFAULT_STATUS_VALUE_MAP = {
        "New": "new",
        "In Progress": "in_progress",
        "Resolved": "closed",
    }
    mapping = []
    for key, value in source_data[source_key].items():
        formatted_key = " ".join(word.capitalize() for word in key.split("_"))
        mapping.append(
            {
                formatted_key: (
                    DEFAULT_STATUS_VALUE_MAP[value]
                    if destination_label == "Netskope Tenant"
                    and value in DEFAULT_STATUS_VALUE_MAP.keys()
                    else value
                )
            }
        )
        field_set.add(formatted_key)

    del source_data[source_key]

    return {
        "event_field": section_name,
        "destination_label": destination_label,
        "mappings": mapping,
    }


def _sync_custom_fields(
    section_name: str, all_migrated_fields: set, existing_fields_map: dict
):
    """
    Compare migrated fields with existing ones and create only the new fields.

    Args:
        section_name: The name of the section ("status" or "severity").
        all_migrated_fields: A set of all field names found during
        the migration.
        existing_fields_map: A pre-fetched map of existing fields.
    """
    from netskope.integrations.itsm.models.custom_fields import (
        FieldInfo,
        CustomFieldIn,
    )
    from netskope.integrations.itsm.routers.custom_fields import (
        create_custom_field,
    )

    existing_fields = existing_fields_map.get(section_name, set())
    new_fields_to_create = {
        field for field in all_migrated_fields if field.lower() not in existing_fields
    }

    if not new_fields_to_create:
        print(f"No new custom fields to create for '{section_name}'.")
        return

    print(f"Creating new custom fields for '{section_name}': {new_fields_to_create}")
    payload_list = [
        FieldInfo(name=field, is_default=True) for field in new_fields_to_create
    ]
    create_custom_field(field=CustomFieldIn(section=section_name, fields=payload_list))


async def migrate_cto():
    """Migrate the existing CTO configurations to be idempotent (Refactored)."""
    from netskope.integrations.itsm.routers.custom_fields import (
        list_custom_fields,
    )

    # Added migration for CTO, AI, DLP Plugins
    cto_plugins = [
        "netskope_itsm",
        "netskope_provider",
        "jira_itsm",
        "servicenow_itsm",
        "ivanti_itsm",
        "bmc_helix_itsm",
        "manage_engine_service_desk_plus",
        "syslog_service",
        "logs_itsm"
    ]
    for plugin in cto_plugins:
        subprocess.run(
            [
                f'/bin/unzip -o default_plugins.zip "netskope/repos/Default/{plugin}/*"',
            ],
            capture_output=True,
            shell=True,
            cwd="/opt",
            check=True,
        )
        subprocess.run(
            [
                f"find netskope/plugins/ -maxdepth 1 -mindepth 1 -type d -not -name custom_plugins "
                f"-not -name __pycache__ | xargs -I {{}} cp -r netskope/repos/Default/{plugin} {{}}",
            ],
            capture_output=True,
            shell=True,
            cwd="/opt",
            check=True,
        )

    try:
        repo = connector.collection(Collections.PLUGIN_REPOS).find_one(
            {"name": "Default"}
        )
        if repo:
            repo = PluginRepo(**repo)
            result = manager.reset_hard_to_head(repo)
            if isinstance(result, str):
                print(result)
            clean_result = manager.clean_default_repo(repo)
            if isinstance(clean_result, str):
                print(clean_result)
    except Exception:
        print("Error occurred while resetting the head", traceback.format_exc())

    updates = []
    status_fields = set()
    severity_fields = set()
    regex_pattern = "|".join(cto_plugins)
    query = {"plugin": {"$regex": regex_pattern}}
    for config in connector.collection(Collections.ITSM_CONFIGURATIONS).find(query):
        plugin = config["plugin"].split(".")[-2]
        params = config["parameters"]
        updated_mapping = {}

        source_data = (
            params.get("incident_update_config", {})
            if plugin == "netskope_itsm"
            else params.get("mapping_config", {})
        )

        dest_label = get_destination_label(plugin)

        status_map = _process_mapping_section(
            source_data, "status", status_fields, dest_label
        )
        if status_map:
            updated_mapping["status"] = status_map

        severity_map = _process_mapping_section(
            source_data, "severity", severity_fields, dest_label
        )
        if severity_map:
            updated_mapping["severity"] = severity_map

        if updated_mapping:
            params["mapping_config"] = {
                **params.get("mapping_config", {}),
                **updated_mapping,
            }
            updates.append(
                UpdateOne({"_id": config["_id"]}, {"$set": {"parameters": params}})
            )

    if updates:
        print(f"Found {len(updates)} configurations to migrate. Applying updates...")
        connector.collection(Collections.ITSM_CONFIGURATIONS).bulk_write(updates)

        print("Syncing custom fields...")
        existing_fields_map = {
            section.section: {field.name.lower() for field in section.fields}
            for section in list_custom_fields()
        }

        _sync_custom_fields("status", status_fields, existing_fields_map)
        _sync_custom_fields("severity", severity_fields, existing_fields_map)
    else:
        print("No configurations required migration.")


def update_cre_value_map_type():
    """Update CRE fields type."""
    try:
        from netskope.integrations.crev2.models import EntityFieldType

        connector.collection(Collections.CREV2_ENTITIES).update_many(
            {"fields.type": "value_map"},
            {"$set": {"fields.$[field].type": EntityFieldType.VALUE_MAP_NUMBER}},
            array_filters=[{"field.type": "value_map"}],
        )
    except Exception as error:
        logger.error(
            "Error occurred while updating field type for CRE entities.",
            details=traceback.format_exc(),
            error_code="CE_1068",
        )
        raise error


def add_default_password_policy_to_settings():
    """Ensure default password policy exists in settings collection."""
    try:
        default_policy = get_default_policy()
        connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"passwordPolicy": default_policy}}
        )
    except Exception as error:
        logger.error(
            "Error adding default password policy to settings.",
            details=traceback.format_exc(),
            error_code="CE_1061",
        )
        raise error


def remove_use_proxy_from_configs():
    """Remove use_proxy from existing configurations."""
    try:
        modules = {
            "cte": Collections.CONFIGURATIONS,
            "cls": Collections.CLS_CONFIGURATIONS,
            "itsm": Collections.ITSM_CONFIGURATIONS,
            "cre": Collections.CRE_CONFIGURATIONS,
            "crev2": Collections.CREV2_CONFIGURATIONS,
            "cfc": Collections.CFC_CONFIGURATIONS,
            "edm": Collections.EDM_CONFIGURATIONS,
            "grc": Collections.GRC_CONFIGURATIONS,
        }
        for module in modules:
            connector.collection(modules[module]).update_many(
                {}, {"$unset": {"use_proxy": ""}}
            )
    except Exception as e:
        logger.error(
            "Error occurred while removing use_proxy from module configurations.",
            details=traceback.format_exc(),
        )
        raise e


def mandatory_tenant_config():
    """Mandatory tenant configuration."""
    try:
        regex = r"netskope_provider\.main$"
        if (
            connector.collection(Collections.NETSKOPE_TENANTS).count_documents(
                {"plugin": {"$regex": regex, "$options": "i"}}
            )
            == 0
        ):
            connector.collection(Collections.SETTINGS).update_one(
                {},
                {
                    "$set": {
                        "platforms": {
                            "cte": False,
                            "itsm": False,
                            "edm": False,
                            "cfc": False,
                            "grc": False,
                            "cls": False,
                            "cre": False,
                        }
                    }
                },
            )
    except Exception as e:
        logger.error(
            "Error occurred while checking for mandatory tenant configuration.",
            details=traceback.format_exc(),
        )
        raise e


def migrate_itsm_tasks():
    """Execute the database migration to update schedule entries."""
    schedules_collection = connector.collection(Collections.SCHEDULES)

    try:
        schedules_collection.update_many(
            {
                "task": {"$in": ["itsm.sync_states", "itsm.update_incidents"]},
                "kwargs.lock_collection": {"$exists": False},
            },
            {
                "$set": {
                    "kwargs": {
                        "lock_collection": "itsm_configurations",
                        "lock_unique_key": "name",
                        "lock_field": "lockedAt",
                    }
                }
            },
        )
    except Exception as error:
        logger.error(
            "Error occurred while setting default value for required approval.",
            details=traceback.format_exc(),
            error_code="CE_1068",
        )
        raise error


def add_default_value_for_required_approval_cto():
    """Add default value for required approval functionality for CTO module."""
    try:
        from netskope.integrations.itsm.models import TaskRequestStatus

        connector.collection(Collections.ITSM_TASKS).update_many(
            {}, {"$set": {"approvalStatus": TaskRequestStatus.NOT_REQUIRED.value}}
        )
    except Exception as error:
        logger.error(
            "Error occurred while setting default value for required approval.",
            details=traceback.format_exc(),
            error_code="CE_1068",
        )
        raise error


def schedule_audit_requests_task():
    """Schedule itsm.audit_requests task."""
    try:
        schedule_or_delete_integrations_tasks(
            SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
        )
    except Exception as error:
        logger.error(
            "Error occurred while scheduling audit requests task for ITSM module.",
            details=traceback.format_exc(),
            error_code="CE_1068",
        )
        raise error


def update_default_cto_cleanup_query():
    """Update default mapping for cto cleanup query."""
    try:
        connector.collection(Collections.SETTINGS).find_one_and_update(
            {"ticketsCleanupQuery": 'status IN ("closed")'},
            {"$set": {
                "ticketsCleanupMongo": '{"status": {"$in": ["Closed"]}}',
                "ticketsCleanupQuery": 'status IN ("Closed")'
            }
            }
        )
    except Exception as error:
        logger.error(
            "Error occurred while updating the default tickets cleanup query for CTO module.",
            details=traceback.format_exc(),
            error_code="CE_1068",
        )
        raise error


if __name__ == "__main__":
    add_ce_logs_fields()
    migrate_transform_data()
    loop.run_until_complete(migrate_cto())
    update_cre_value_map_type()
    add_default_password_policy_to_settings()
    script_path = os.path.join(os.path.dirname(__file__), "5.1.1-dlp-beta-1.py")
    print("Started migration script for the DLP...")
    proc = subprocess.Popen(f"python {script_path} > /dev/null 2>&1 &".split())
    proc.wait()
    print("Finished migration script for the DLP.")
    remove_use_proxy_from_configs()
    mandatory_tenant_config()
    migrate_itsm_tasks()
    add_default_value_for_required_approval_cto()
    schedule_audit_requests_task()
    update_default_cto_cleanup_query()
    loop.close()
