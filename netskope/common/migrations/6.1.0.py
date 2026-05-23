"""Migrations for 6.1.0 release."""

import asyncio
import traceback

from pymongo import UpdateOne
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
)
from netskope.integrations.edm.routers.configurations import update_configuration
from netskope.integrations.edm.models import ConfigurationUpdate
from netskope.common.models.user import User


connector = DBConnector()
logger = Logger()
plugin_helper = PluginHelper()
loop = asyncio.get_event_loop()


def migrate_itsm_configurations_locking_fields():
    """Migrate ITSM configurations locking fields."""
    def get_config_dict(value: any, plugin_metaddata: dict, default_value: any = None) -> dict:
        """Get dictionary based on field name and plugin metadata."""
        if plugin_metaddata.get("pulling_supported", False) and not plugin_metaddata.get("netskope", False):
            return {
                "pull": value,
                "sync": default_value,
                "update": default_value,
            }
        elif plugin_metaddata.get("sharing_supported", False):
            return {
                "pull": default_value,
                "sync": default_value,
                "update": value,
            }
        elif plugin_metaddata.get("receiving_supported", False):
            return {
                "pull": default_value,
                "sync": value,
                "update": default_value,
            }
        else:
            return {
                "pull": default_value,
                "sync": default_value,
                "update": default_value,
            }
    for config in connector.collection(Collections.ITSM_CONFIGURATIONS).find({}):
        PluginClass = plugin_helper.find_by_id(config["plugin"])

        task_status = {
            "startedAt": config.get("task", {}).get("startedAt", None),
            "task_id": config.get("task", {}).get("task_id", None),
            "worker_id": config.get("task", {}).get("worker_id", None),
        }
        # update field changes.
        if not isinstance(config.get("lockedAt"), dict):
            connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
                {"name": config.get("name")},
                {
                    "$set": {
                        "lockedAt": get_config_dict(config.get("lockedAt"), PluginClass.metadata),
                        "lastRunAt": get_config_dict(config.get("lastRunAt"), PluginClass.metadata),
                        "lastRunSuccess": get_config_dict(config.get("lastRunSuccess"), PluginClass.metadata),
                        "task": get_config_dict(task_status, PluginClass.metadata, {})
                    }
                },
                upsert=True,
            )

        # Update lockedAt field for pulling task.
        connector.collection(Collections.SCHEDULES).bulk_write([
            UpdateOne(
                {"task": "itsm.sync_states", "name": f"itsm.{config.get('name')}"},
                {"$set": {"kwargs.lock_field": "lockedAt.sync"}},
            ),
            UpdateOne(
                {"task": "itsm.pull_data_items", "name": f"itsm.{config.get('name')}"},
                {"$set": {"kwargs.lock_field": "lockedAt.pull"}},
            ),
            UpdateOne(
                {"task": "itsm.update_incidents", "name": f"itsm.{config.get('name')}_sharing"},
                {"$set": {"kwargs.lock_field": "lockedAt.update"}},
            ),
        ])


def disable_invalid_edm_configurations():
    """Disable EDM configurations that use deprecated characters in their names."""
    try:
        invalid_configs = list(
            connector.collection(Collections.EDM_CONFIGURATIONS).find(
                {"name": {"$regex": r"[-_]"}, "active": True},
                {"name": 1},
            )
        )
        if not invalid_configs:
            logger.info(
                "No active EDM configurations with '-' or '_' found for migration 6.1.0."
            )
            return

        disabled_count = 0
        for config in invalid_configs:
            config_name = config["name"]
            config_db_dict = connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
                {"name": config_name}
            )
            if not config_db_dict:
                continue
            loop.run_until_complete(
                update_configuration(
                    ConfigurationUpdate(
                        name=config_name,
                        active=False,
                        plugin=config_db_dict["plugin"],
                    ),
                    user=User(username="System", scopes=["edm_write"]),
                )
            )
            disabled_count += 1

        logger.info(
            f"Disabled {disabled_count} EDM configurations becuase restricted characters were found in their names."
        )
    except Exception:
        logger.error(
            "Error occurred while disabling EDM configurations with restricted characters.",
            details=traceback.format_exc(),
        )
        raise


def migrate_cfc_status_values():
    """Migrate CFC status values from old to new format.

    Migrates:
    - 'completed' -> 'success'
    - 'partially_failed' -> 'partial_success'

    This migration updates both:
    - CFC_MANUAL_UPLOAD_CONFIGURATIONS collection (config status and files array)
    - CFC_SHARING collection (sharing status)
    - CFC_IMAGES_METADATA collection (sharedWith array status)
    """
    try:
        # Migrate CFC_MANUAL_UPLOAD_CONFIGURATIONS - files array status
        connector.collection(
            Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
        ).update_many(
            {"files.status": "completed"},
            {"$set": {"files.$[elem].status": "success"}},
            array_filters=[{"elem.status": "completed"}],
        )

        # Migrate CFC_SHARING collection
        connector.collection(Collections.CFC_SHARING).update_many(
            {"status": "completed"},
            {"$set": {"status": "success"}},
        )

        # Migrate CFC_IMAGES_METADATA - sharedWith array status
        connector.collection(Collections.CFC_IMAGES_METADATA).update_many(
            {"sharedWith.status": "completed"},
            {"$set": {"sharedWith.$[elem].status": "success"}},
            array_filters=[{"elem.status": "completed"}],
        )

        # Recompute manual upload configuration status from file outcomes.
        for config in connector.collection(
            Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
        ).find({}, {"files": 1, "status": 1}):
            current_status = config.get("status")
            # Avoid touching in-progress configurations.
            if current_status not in (
                "success",
                "completed",
                "partial_success",
                "partially_failed",
                "failed",
            ):
                continue

            files = config.get("files", [])
            statuses = [f.get("status") for f in files]
            success_statuses = {"success", "completed"}

            if statuses and all(status_value in success_statuses for status_value in statuses):
                final_status = "success"
            elif any(status_value in success_statuses for status_value in statuses):
                final_status = "partial_success"
            else:
                final_status = "failed"

            if final_status != current_status:
                connector.collection(Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS).update_one(
                    {"_id": config["_id"]},
                    {"$set": {"status": final_status}},
                )

    except Exception:
        logger.error(
            "Error occurred while migrating CFC status values.",
            details=traceback.format_exc(),
        )
        raise


def remove_stale_cls_pull_logs_schedules():
    """Remove stale CE Logs pull schedules that have no active SIEM mappings."""
    try:
        stale_schedules = []
        for schedule in connector.collection(Collections.SCHEDULES).find(
            {"task": "common.pull_logs"}
        ):
            schedule_name = schedule.get("name", "")
            if not schedule_name.startswith("cls."):
                continue

            config_name = schedule_name[4:]
            args = schedule.get("args", [])
            if args and len(args) > 0:
                config_name = args[0]

            has_mapping = False
            for rule in connector.collection(Collections.CLS_BUSINESS_RULES).find({}):
                siem_mappings = rule.get("siemMappings", {})
                if config_name in siem_mappings and siem_mappings[config_name]:
                    has_mapping = True
                    break

            if not has_mapping:
                stale_schedules.append(schedule_name)

        if stale_schedules:
            connector.collection(Collections.SCHEDULES).delete_many(
                {"name": {"$in": stale_schedules}}
            )
    except Exception:
        logger.error(
            "Error occurred while removing stale CE Logs pull schedules.",
            details=traceback.format_exc(),
        )
        raise


if __name__ == "__main__":
    migrate_itsm_configurations_locking_fields()
    disable_invalid_edm_configurations()
    remove_stale_cls_pull_logs_schedules()

    # Update EDM hash upload status polling schedule from 1 hour to 10 minutes
    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "edm.poll_edm_hash_upload_status"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "INTERNAL EDM Hashes POLL TASK",
                "enabled": True,
                "args": [],
                "interval": {
                    "every": 10,
                    "period": "minutes",
                },
            }
        },
        upsert=True,
    )

    # Migrate CFC status values from old to new format
    migrate_cfc_status_values()
