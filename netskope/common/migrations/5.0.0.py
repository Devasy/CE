"""Migrations for 5.0.0 release."""

import os
import traceback
from datetime import datetime, UTC
from netskope.common.utils import DBConnector, Collections, RepoManager, delete_duplicate_indicators
from netskope.common.models import PluginRepoIn
from pymongo import ASCENDING, DESCENDING
from bson.son import SON
connector = DBConnector()
manager = RepoManager()

manager.load()


def recreate_index():
    """Re-creating the index for ITSM_TASKS."""
    connector.collection(Collections.ITSM_TASKS).drop_index("id_1")  # Need to unset the unique parameter.
    connector.collection(Collections.ITSM_TASKS).create_index(
        [("id", ASCENDING)],
    )


def create_indexes_on_indicators():
    """Create the indexes on Indicators collection."""
    index_on_value_exist = False
    for index in connector.collection(Collections.INDICATORS).list_indexes():
        if index == SON([('v', 2), ('key', SON([('value', 1)])), ('name', 'value_1'), ('unique', True)]):
            index_on_value_exist = True
            break
    if not index_on_value_exist:
        delete_duplicate_indicators()
        connector.collection(Collections.INDICATORS).create_index(
            [("value", ASCENDING)],
            unique=True,
        )
    connector.collection(Collections.INDICATORS).create_index([("sources.tags", ASCENDING)])
    connector.collection(Collections.INDICATORS).create_index([("sources.lastSeen", DESCENDING)])


def set_repo_to_cls_mappings_collection():
    """Set the repository to cls_mappings_collection."""
    try:
        mapping_files = connector.collection(Collections.CLS_MAPPING_FILES).find({"repo": {"$ne": None}})
        for file in mapping_files:
            connector.collection(Collections.CLS_MAPPING_FILES).delete_one({"name": file["name"], "repo": None})
        connector.collection(Collections.CLS_MAPPING_FILES).update_many(
            {"repo": {"$exists": False}}, {"$set": {"repo": None}})
    except Exception:
        print(traceback.format_exc())


def add_repo_to_cls_configurations():
    """Add repo to CLS configuration."""
    try:
        cls_configurations = connector.collection(Collections.CLS_CONFIGURATIONS).find({
            "attributeMapping": {"$ne": None}})
        for config in cls_configurations:
            mapping = list(connector.collection(Collections.CLS_MAPPING_FILES).find(
                {"name": config["attributeMapping"]}).sort("_id", -1).limit(1))[0]
            connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
                {"_id": config["_id"]}, {"$set": {"attributeMappingRepo": mapping["repo"]}})
    except Exception:
        print(traceback.format_exc())


def update_all_business_rule():
    """Update All business rule in CLS and Add CTEP Rebranded."""
    connector.collection(Collections.CLS_BUSINESS_RULES).update_one(
        {"name": "All"},
        {
            "$set": {
                "filters": {
                    "query": 'alert_type IN ("Compromised Credential", "policy", "malsite", "Malware", "DLP", "Security Assessment", "watchlist", "quarantine", "Remediation", "uba", "ctep", "ips", "c2") || event_type IN ("page", "application", "audit", "infrastructure", "network", "incident")',  # noqa
                    "mongo": '{"$or":[{"alert_type":{"$in":["Compromised Credential","policy","malsite","Malware","DLP","Security Assessment","watchlist","quarantine","Remediation","uba", "ctep", "ips", "c2"]}},{"event_type":{"$in":["page","application","audit","infrastructure","network", "incident"]}}]}',  # noqa
                },
            }
        },
    )


def remove_unused_default_fields():
    """Remove netskope fields which is not present in netskope dataexport schema."""
    connector.collection(Collections.NETSKOPE_FIELDS).delete_many(
        {
            "name": {
                "$in": [
                    "breach_media_reference",
                    "filesize",
                    "lh_custodian_email",
                    "lh_custodian_name",
                    "lh_dest_app",
                    "lh_shared",
                    "lh_dest_instance",
                    "lh_original_filename",
                    "legal_hold_profile_name",
                    "dtection_type",
                    "ns_detection_name",
                    "malware_scanner_result",
                    "sa_rule_remediation",
                    "page_id",
                    "alarm_description",
                    "page_endtime",
                    "md5_list",
                    "alarm_name",
                    "page_starttime",
                    "device_name",
                ]
            }
        }
    )
    for field in connector.collection(Collections.NETSKOPE_FIELDS).find({"name": {"$regex": "^_"}}):
        connector.collection(Collections.NETSKOPE_FIELDS).update_one(
            {"_id": field.get("_id")},
            {"$set": {"label": field.get("name")}}
        )


def modify_settings_for_ha():
    """Modify settings for HA."""
    connector.collection(Collections.CLS_CONFIGURATIONS).update_many({}, {"$unset": {"pid": 1}})


def add_beat_status_to_settings():
    """Add beat status to settings."""
    connector.collection(Collections.SETTINGS).update_one(
        {},
        {
            "$set": {
                "beat_status.node_name": "",
                "beat_status.last_update_time": datetime.now(UTC)
            }
        },
    )


def unset_page_size_cls_config():
    """Unset the page size in cls settings."""
    connector.collection(Collections.SETTINGS).update_one({}, {"$unset": {"cls.pagesize": 1}})


def create_default_repo():
    """Create default repo."""
    if not connector.collection(Collections.PLUGIN_REPOS).find_one({"name": "Default"}):
        print("Creating the Default plugin repository...")
        repo = PluginRepoIn(
            name="Default",
            url=os.environ.get(
                "GIT_URL",
                "https://github.com/netskopeoss/ta_cloud_exchange_plugins.git",
            ),
            username=os.environ.get("GIT_USERNAME", ""),
            password=os.environ.get("GIT_PASSWORD", ""),
            isDefault=True,
        )
        connector.collection(Collections.PLUGIN_REPOS).insert_one(
            repo.model_dump()
        )


if __name__ == "__main__":
    recreate_index()
    update_all_business_rule()
    remove_unused_default_fields()
    set_repo_to_cls_mappings_collection()
    add_repo_to_cls_configurations()
    add_beat_status_to_settings()
    modify_settings_for_ha()
    create_indexes_on_indicators()
    unset_page_size_cls_config()
    create_default_repo()
