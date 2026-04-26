"""Contains database related classes."""

import os
import sys
from sys import stderr
from enum import Enum
from typing import Union
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from netskope.common.utils.decorator import graceful_auto_reconnect
from .singleton import Singleton


class Collections(str, Enum):
    """Mongo collections."""

    PLUGIN_REPOS = "repos"
    NETSKOPE_TENANTS = "tenants"
    NETSKOPE_FIELDS = "netskope_fields"

    INDICATORS = "indicators"
    USERS = "users"
    CONFIGURATIONS = "configurations"
    SETTINGS = "settings"
    LOGS = "logs"
    SCHEDULES = "schedules"
    NOTIFICATIONS = "notifications"
    TAGS = "tags"
    TASK_STATUS = "task_status"
    CLUSTER_HEALTH = "cluster_health"
    NODE_HEALTH = "node_health"
    DATA_BATCHES = "data_batches"

    ITSM_CONFIGURATIONS = "itsm_configurations"
    ITSM_ALERTS = "itsm_alerts"
    ITSM_EVENTS = "itsm_events"
    ITSM_BUSINESS_RULES = "itsm_business_rules"
    ITSM_TASKS = "itsm_tasks"
    ITSM_CUSTOM_FIELDS = "itsm_custom_fields"

    CRE_CONFIGURATIONS = "cre_configurations"
    CRE_USERS = "cre_users"
    CRE_LOGGING = "cre_logs"
    CRE_BUSINESS_RULES = "cre_business_rules"
    CRE_ACTION_LOGS = "cre_action_logs"

    CLS_BUSINESS_RULES = "cls_business_rules"
    CLS_CONFIGURATIONS = "cls_configurations"
    CLS_ALERTS = "cls_alerts"
    CLS_TASKS = "cls_tasks"
    CLS_MAPPING_FILES = "cls_mapping_files"
    WEBTX_METRICS = "webtx_metrics"
    CTE_BUSINESS_RULES = "cte_business_rules"
    ITERATOR = "iterator"

    GRC_CONFIGURATIONS = "grc_configurations"
    GRC_APPLICATIONS = "grc_applications"
    GRC_BUSINESS_RULES = "grc_business_rules"

    EDM_BUSINESS_RULES = "edm_business_rules"
    EDM_CONFIGURATIONS = "edm_configurations"
    EDM_MANUAL_UPLOAD_CONFIGURATIONS = "edm_manual_upload_configurations"
    EDM_STATISTICS = "edm_statistics"
    EDM_HASHES_STATUS = "edm_hashes_status"

    CFC_CONFIGURATIONS = "cfc_configurations"
    CFC_SHARING = "cfc_sharing"
    CFC_IMAGES_METADATA = "cfc_images_metadata"
    CFC_MANUAL_UPLOAD_CONFIGURATIONS = "cfc_manual_upload_configurations"
    CFC_BUSINESS_RULES = "cfc_business_rules"
    CFC_STATISTICS = "cfc_statistics"

    CREV2_ENTITIES = "crev2_entities"
    CREV2_CONFIGURATIONS = "crev2_configurations"
    CREV2_BUSINESS_RULES = "crev2_business_rules"
    CREV2_ENTITY_PREFIX = "crev2_entity_"
    CREV2_ACTION_LOGS = "crev2_action_logs"


class CustomCollection:
    """Custom collection with retry mechanism."""

    def __init__(self, collection):
        """Initialize.

        Args:
            collection (Object): pymongo collection object.
        """
        self.collection = collection

    def __getattr__(self, name):
        """Get attribute of collection object."""

        @graceful_auto_reconnect()
        def wrapped(*args, **kwargs):
            return getattr(self.collection, name)(*args, **kwargs)

        return wrapped


class DBConnector(metaclass=Singleton):
    """Singleton database connection class."""

    def __init__(self):
        """Initialize."""
        self._client = {}
        pass

    @property
    def database(self) -> Database:
        """Return the CTE database for the current process.

        Returns:
            Database: Instance of database.
        """
        try:
            if not self._client.get(os.getpid()):
                self._client[os.getpid()] = MongoClient(os.environ["MONGO_CONNECTION_STRING"])
            return self._client[os.getpid()].cte
        except KeyError as ex:
            print(ex, file=stderr)
            # one of the required environment variable is not set
            print(
                "One of the required environment variable is not set",
                file=stderr,
            )
            sys.exit(1)

    def collection(self, name: Union[Collections, str]) -> Collection:
        """Return the specified collection.

        Args:
            name (Collections): Name of the collection to fetch.

        Returns:
            Collection: Specified collection.
        """
        return CustomCollection(self.database[name])


@graceful_auto_reconnect()
def mongo_connection(func, *args, **kwargs):
    """Retry mongo connection on AutoReconnect Error."""
    return func(*args, **kwargs)


def check_mongo_service(mongo_string, username=None, password=None, authMechanism=None, authSource=None):
    """Check the mongo service status."""
    if not username:
        client = MongoClient(
            mongo_string
        )
    else:
        client = MongoClient(
            mongo_string, username=username,
            password=password, authMechanism=authMechanism,
            authSource=authSource
        )

    client.server_info()
    return client
