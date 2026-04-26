"""Migrations for 5.1.1 release."""

import os
import traceback
from pymongo.errors import DuplicateKeyError, OperationFailure
from pymongo import ASCENDING
from netskope.common.utils import DBConnector, Collections, RepoManager, Logger
from netskope.common.models import PluginRepo

from netskope.integrations.crev2.utils import (
    NETSKOPE_POLL_INTERVAL,
    NETSKOPE_POLL_INTERVAL_UNIT,
)

connector = DBConnector()
manager = RepoManager()
logger = Logger()


def clean_default_git_repo():
    """Clean the default git repo to remove untrack files and folders."""
    print("Cleaning Default repo...")
    try:
        repo = connector.collection(Collections.PLUGIN_REPOS).find_one({"name": "Default"})
        repo = PluginRepo(**repo)
        result = manager.clean_default_repo(repo)
        if isinstance(result, str):
            print(result)
    except Exception as e:
        print("Error occurred while Cleaning the Default repo", traceback.format_exc())
        raise e


def field_learning_change():
    """Add newly added fields."""
    print("Adding field datatype in all netskope fields...")
    try:
        connector.collection(Collections.NETSKOPE_FIELDS).update_many(
            {},
            {"$set": {"dataType": "text"}}
        )
    except Exception as e:
        print("Error occurred while updating the netskope fields", traceback.format_exc())
        raise e


def increase_cre_perform_action_interval():
    """Update the intervals for CRE Perform Action tasks."""
    print("Updating intervals for CRE Perform Action tasks...")
    try:
        connector = DBConnector()
        connector.collection(Collections.SCHEDULES).update_one(
            {"task": "cre.perform_action"},
            {
                "$set": {
                    "interval": {
                        "every": 5,
                        "period": "minutes",
                    }
                }
            },
        )
    except Exception as e:
        print(
            "Error occurred while updating the CRE Perform Action tasks",
            traceback.format_exc(),
        )
        raise e


def migrate_cre_netskope_tasks():
    """Update the intervals for CRE Netskope tasks."""
    print("Updating intervals for CRE Netskope tasks...")
    try:
        connector = DBConnector()
        connector.collection(Collections.SCHEDULES).update_many(
            {"task": "cre.update_records"},
            {
                "$set": {
                    "interval": {
                        "every": NETSKOPE_POLL_INTERVAL,
                        "period": NETSKOPE_POLL_INTERVAL_UNIT,
                    }
                }
            },
        )
    except Exception as e:
        print(
            "Error occurred while updating the CRE Netskope tasks",
            traceback.format_exc(),
        )
        raise e


def update_env_variables():
    """Update Class variables from existing set .env variables."""
    print("Updating Class variables as per already set .env variables...")
    try:
        connector = DBConnector()
        if (
            os.environ.get("CTE_DELETE_INACTIVE_INDICATORS", "false").lower()
            == "true"
        ):
            connector.collection(Collections.SETTINGS).update_one(
                {}, {"$set": {"cte.deleteInactiveIndicators ": True}}
            )
    except Exception as e:
        print(
            "Error occurred while updating class variables from .env variables",
            traceback.format_exc(),
        )
        raise e


def create_cre_entities_indexes():
    """Create indexes for default CRE entities."""
    print("Creating CRE entities indexes...")
    try:
        UNIQUE_INDEX_NAME = "unique_index"
        # only work on default entities
        for entity in connector.collection(Collections.CREV2_ENTITIES).find(
            {
                "name": {
                    "$in": [
                        "Applications",
                        "Users",
                        "Devices",
                    ]
                }
            }
        ):
            logger.info(f"Working on entity {entity.get('name')}.")
            try:
                logger.info(
                    f"Trying to drop index {UNIQUE_INDEX_NAME} for entity {entity.get('name')}."
                )
                connector.collection(
                    f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.get('name')}"
                ).drop_index(UNIQUE_INDEX_NAME)
                logger.info(
                    f"Dropped index {UNIQUE_INDEX_NAME} for entity {entity.get('name')}.",
                )
            except OperationFailure:
                logger.error(
                    f"Could not drop index {UNIQUE_INDEX_NAME} as it does not exist.",
                    details=traceback.format_exc(),
                )
            if unique_fields := [
                (f.get('name'), ASCENDING)
                for f in entity.get("fields", [])
                if f.get('unique', False) and f.get('name')
            ]:
                try:
                    logger.info(
                        f"Trying to create index {UNIQUE_INDEX_NAME} for entity {entity.get('name')}."
                    )
                    connector.collection(
                        f"{Collections.CREV2_ENTITY_PREFIX.value}{entity.get('name')}"
                    ).create_index(
                        unique_fields,
                        unique=True,
                        name=UNIQUE_INDEX_NAME,
                    )
                    logger.info(
                        f"Created index {UNIQUE_INDEX_NAME} for entity {entity.get('name')}.",
                    )
                except DuplicateKeyError:
                    logger.error(
                        f"Could not create index {UNIQUE_INDEX_NAME}.",
                    )
                    # set all the field's unique to False
                    connector.collection(
                        Collections.CREV2_ENTITIES
                    ).update_one(
                        {"name": entity.get("name")},
                        {
                            "$set": {
                                "fields": [
                                    {
                                        **f,
                                        "unique": False,
                                        "coalesceStrategy": "overwrite",
                                    }
                                    for f in entity.get("fields", [])
                                ]
                            }
                        },
                    )
                    logger.info(
                        f"Set all the field's unique to False for entity {entity.get('name')}.",
                    )
    except Exception as e:
        logger.error(
            "Error occurred while creating the CRE entities indexes",
            details=traceback.format_exc(),
        )
        raise e


def update_default_business_rules():
    """Update default business rules."""
    try:
        connector.collection(Collections.CLS_BUSINESS_RULES).update_one(
            {"name": "All"},
            {
                "$set": {
                    "filters": {
                        "query": "",
                        "mongo": "{}"
                    },
                }
            },
        )
    except Exception as error:
        logger.error(
            f"Error occurred while updating default business rules. {error}",
            details=traceback.format_exc(),
            error_code="CE_1039"
        )
        raise error


if __name__ == "__main__":
    clean_default_git_repo()
    field_learning_change()
    migrate_cre_netskope_tasks()
    increase_cre_perform_action_interval()
    update_env_variables()
    create_cre_entities_indexes()
    update_default_business_rules()
