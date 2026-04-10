"""Migrations for 6.0.0 release."""

import os
import sys
import subprocess
import traceback
import zipfile
import shutil
from pymongo import UpdateOne
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
)

connector = DBConnector()
logger = Logger()
ALERT_EVENT_FIELD_MAPPING = {
    "low": "Low", "medium": "Medium", "high": "High", "critical": "Critical", "informational": "Informational",
    "new": "New", "in_progress": "In Progress", "on_hold": "On Hold", "resolved": "Resolved", "other": "Other",
}
MAX_BATCH_SIZE = 10000


def migrate_dlp_plugins():
    """Migrate the DLP configurations to be idempotent (Refactored)."""
    # Added migration for DLP Plugins
    dlp_plugins = [
        "linux_file_share_cfc",
        "microsoft_file_share_cfc",
        "netskope_cfc",
        "linux_file_share_edm",
        "microsoft_file_share_edm",
        "microsoft_sql_edm",
        "mysql_edm",
        "netskope_edm",
        "netskope_edm_forwarder_receiver",
        "oracledb_edm"
    ]
    base_path = "/opt"
    zip_file_path = os.path.join(base_path, "default_plugins.zip")
    plugins_base_path = os.path.join(base_path, "netskope", "plugins")
    try:
        for plugin in dlp_plugins:
            # Extract specific plugin from zip file
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                # Get all files for this specific plugin
                plugin_files = [f for f in zip_ref.namelist() if f.startswith(f"netskope/repos/Default/{plugin}/")]
                if not plugin_files:
                    continue
                # Extract plugin files to /opt
                for file_path in plugin_files:
                    zip_ref.extract(file_path, base_path)
            # Copy plugin to all plugin directories (excluding custom_plugins and __pycache__)
            source_plugin_path = os.path.join(base_path, "netskope", "repos", "Default", plugin)
            # Find all plugin directories (excluding custom_plugins and __pycache__)
            for item in os.listdir(plugins_base_path):
                item_path = os.path.join(plugins_base_path, item)
                if os.path.isdir(item_path) and item not in ["custom_plugins", "__pycache__"]:
                    destination_path = os.path.join(item_path, plugin)
                    # Remove existing plugin directory if it exists
                    if os.path.exists(destination_path):
                        shutil.rmtree(destination_path)
                    # Copy the plugin directory
                    shutil.copytree(source_plugin_path, destination_path)
        logger.info("EDM and CFC plugins added successfully in Default plugin repository.")
    except Exception as e:
        logger.error(
            "Error occurred while migrating EDM and CFC plugins.",
            details=traceback.format_exc(),
            error_code="CE_1070",
        )
        raise e


def migrate_itsm_alerts_events():
    """Migrates documents in ITSM ALERTS and ITSM EVENTS."""
    try:
        collections_to_process = [Collections.ITSM_ALERTS, Collections.ITSM_EVENTS]
        modified_count = 0
        for collection_name in collections_to_process:
            skip = 0
            while True:
                processed_data = 0
                cursor = (
                    connector.collection(collection_name)
                    .find({})
                    .skip(skip)
                    .limit(MAX_BATCH_SIZE)
                )
                updates = []
                for doc in cursor:
                    skip += 1
                    processed_data += 1
                    raw_data = doc.get("rawData")
                    if not isinstance(raw_data, dict):
                        continue

                    update_payload = {}
                    current_status = raw_data.get("status")
                    current_severity = raw_data.get("severity")

                    if current_status and current_status in ALERT_EVENT_FIELD_MAPPING:
                        new_status = ALERT_EVENT_FIELD_MAPPING[current_status]
                        if new_status != current_status:
                            update_payload["rawData.status"] = new_status

                    if current_severity and current_severity in ALERT_EVENT_FIELD_MAPPING:
                        new_severity = ALERT_EVENT_FIELD_MAPPING[current_severity]
                        if new_severity != current_severity:
                            update_payload["rawData.severity"] = new_severity

                    if update_payload:
                        updates.append(
                            UpdateOne(
                                {"_id": doc["_id"]},
                                {"$set": update_payload}
                            )
                        )
                        modified_count += 1
                if updates:
                    connector.collection(collection_name).bulk_write(updates)
                if skip % 10000 == 0 and processed_data != 0:
                    continue
                else:
                    break
        logger.info(
            f"Migration for Alerts/Events completed successfully. "
            f"Documents modified: {modified_count}."
        )
    except Exception as error:
        logger.error(
            "Error occurred while migrating the ITSM alerts and events.",
            details=traceback.format_exc(),
            error_code="CE_1068",
        )
        raise error


def migrate_itsm_tasks_fields():
    """Migrates all documents in the ITSM_TASKS collection safely by iterating."""
    # collection_name = Collections.ITSM_TASKS
    try:
        modified_count = 0
        skip = 0
        while True:
            processed_data = 0
            cursor = (
                connector.collection(Collections.ITSM_TASKS)
                .find({})
                .skip(skip)
                .limit(MAX_BATCH_SIZE)
            )
            updates = []
            for doc in cursor:
                skip += 1
                processed_data += 1
                update_payload = {}

                data_item = doc.get("dataItem", {})
                raw_data = data_item.get("rawData", {})
                updated_values = doc.get("updatedValues", {})

                fields_to_map = {
                    "status": doc.get("status"),
                    "severity": doc.get("severity"),
                    "dataItem.rawData.status": raw_data.get("status"),
                    "dataItem.rawData.severity": raw_data.get("severity"),
                    "updatedValues.status": updated_values.get("status"),
                    "updatedValues.oldStatus": updated_values.get("oldStatus"),
                    "updatedValues.severity": updated_values.get("severity"),
                    "updatedValues.oldSeverity": updated_values.get("oldSeverity"),
                }

                for field_path, current_value in fields_to_map.items():
                    if current_value and current_value in ALERT_EVENT_FIELD_MAPPING:
                        new_value = ALERT_EVENT_FIELD_MAPPING[current_value]
                        if new_value != current_value:
                            update_payload[field_path] = new_value

                if update_payload:
                    updates.append(
                        UpdateOne(
                            {"_id": doc["_id"]},
                            {"$set": update_payload}
                        )
                    )
                    modified_count += 1
            if updates:
                connector.collection(Collections.ITSM_TASKS).bulk_write(updates)
            if skip % 10000 == 0 and processed_data != 0:
                continue
            else:
                break
        logger.info(
            f"Migration for Tickets completed successfully. "
            f"Documents modified: {modified_count}."
        )

    except Exception as error:
        logger.error(
            "Error occurred while migrating status/severity of the ITSM tasks.",
            details=traceback.format_exc(),
            error_code="CE_1068",
        )
        raise error


if __name__ == "__main__":
    n = len(sys.argv)
    if n > 1 and sys.argv[1] == "migrate_cto_collections":
        try:
            connector.collection(Collections.SETTINGS).update_one(
                {}, {"$set": {"cto_migration_status": False}}
            )
            migrate_itsm_alerts_events()
            migrate_itsm_tasks_fields()
        except Exception:
            logger.error(
                "Error occured while migrating the CTO Alerts/Events and Tickets.",
                details=traceback.format_exc(),
                error_code="CE_1068"
            )
        finally:
            connector.collection(Collections.SETTINGS).update_one(
                {}, {"$set": {"cto_migration_status": True}}
            )
    else:
        migrate_dlp_plugins()
        script_path = os.path.join(os.path.dirname(__file__), "6.0.0.py")
        print("Started migration script for the CTO Alerts/Events and Tickets migrations...")
        proc = subprocess.Popen(
            f"python {script_path} migrate_cto_collections > /dev/null 2>&1 &".split()
        )
