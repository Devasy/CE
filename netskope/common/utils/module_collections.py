"""Module to maintain a centralized mapping of collections by module."""

import sys


class ModuleCollections:
    """Mapping of collections by module."""

    # Common collections
    COMMON = {
        "PLUGIN_REPOS": "repos",
        "NETSKOPE_TENANTS": "tenants",
        "NETSKOPE_FIELDS": "netskope_fields",
        "USERS": "users",
        "SETTINGS": "settings",
        "LOGS": "logs",
        "SCHEDULES": "schedules",
        "NOTIFICATIONS": "notifications",
        "TAGS": "tags",
        "TASK_STATUS": "task_status",
        "CLUSTER_HEALTH": "cluster_health",
        "NODE_HEALTH": "node_health",
        "DATA_BATCHES": "data_batches",
        "ITERATOR": "iterator",
    }

    # CTE module collections
    CTE = {
        "INDICATORS": "indicators",
        "CONFIGURATIONS": "configurations",
        "BUSINESS_RULES": "cte_business_rules",
    }

    # CREV2 module collections
    CREV2 = {
        "ENTITIES": "crev2_entities",
        "CONFIGURATIONS": "crev2_configurations",
        "BUSINESS_RULES": "crev2_business_rules",
        "ENTITY_PREFIX": "crev2_entity_",
        "ACTION_LOGS": "crev2_action_logs",
        "ENTITY_USERS": "crev2_entity_Users",
        "ENTITY_DEVICES": "crev2_entity_Devices",
        "ENTITY_APPLICATIONS": "crev2_entity_Applications",
    }

    # CLS module collections
    CLS = {
        "BUSINESS_RULES": "cls_business_rules",
        "CONFIGURATIONS": "cls_configurations",
        "ALERTS": "cls_alerts",
        "TASKS": "cls_tasks",
        "MAPPING_FILES": "cls_mapping_files",
        "WEBTX_METRICS": "webtx_metrics",
    }

    # EDM module collections
    EDM = {
        "BUSINESS_RULES": "edm_business_rules",
        "CONFIGURATIONS": "edm_configurations",
        "MANUAL_UPLOAD_CONFIGURATIONS": "edm_manual_upload_configurations",
        "STATISTICS": "edm_statistics",
        "HASHES_STATUS": "edm_hashes_status",
    }

    # CFC module collections
    CFC = {
        "CONFIGURATIONS": "cfc_configurations",
        "SHARING": "cfc_sharing",
        "IMAGES_METADATA": "cfc_images_metadata",
        "MANUAL_UPLOAD_CONFIGURATIONS": "cfc_manual_upload_configurations",
        "BUSINESS_RULES": "cfc_business_rules",
        "STATISTICS": "cfc_statistics",
    }

    # CTO/ITSM module collections
    CTO = {
        "CONFIGURATIONS": "itsm_configurations",
        "ALERTS": "itsm_alerts",
        "EVENTS": "itsm_events",
        "BUSINESS_RULES": "itsm_business_rules",
        "TASKS": "itsm_tasks",
    }

    # CRE module collections
    CRE = {
        "CONFIGURATIONS": "crev2_configurations",
        "USERS": "crev2_users",
        "LOGS": "crev2_logs",
        "BUSINESS_RULES": "crev2_business_rules",
        "ACTION_LOGS": "crev2_action_logs",
        "ENTITIES": "crev2_entities",
        "USER": "crev2_entity_Users",
        "DEVICE": "crev2_entity_Devices",
        "APPLICATION": "crev2_entity_Applications",
    }


def get_collection_name(module, key):
    """Get collection name from module and key.

    Args:
        module (str): Module name (e.g., 'CREV2', 'CLS')
        key (str): Collection key (e.g., 'BUSINESS_RULES')
    Returns:
        str: Actual collection name in database
    """
    if hasattr(ModuleCollections, module) and key in getattr(ModuleCollections, module):
        return getattr(ModuleCollections, module)[key]
    return None


if __name__ == "__main__":
    module = sys.argv[1].strip()
    key = sys.argv[2].strip()
    collection = get_collection_name(module, key)
    if collection:
        print(collection)
