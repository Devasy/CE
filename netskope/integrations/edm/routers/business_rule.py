"""Business rule related endpoints."""

from typing import List

from fastapi import APIRouter, HTTPException, Query, Security

from netskope.common.api.routers.auth import get_current_user
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger
from netskope.integrations.edm.models import (
    BusinessRuleDelete,
    BusinessRuleIn,
    BusinessRuleOut,
    BusinessRuleDB,
    StatusType,
)
from netskope.integrations.edm.tasks.plugin_lifecycle_task import execute_plugin

router = APIRouter()
logger = Logger()
connector = DBConnector()


def _clear_destination_config_storage(rule_name: str):
    """Clear all storage stored in destination configuration related to configured source configuration in sharing.

    Args:
        rule_name (str): business_rule.name generated while creation of business rule.
    """
    business_rule = connector.collection(Collections.EDM_BUSINESS_RULES).find_one(
        {"name": rule_name}
    )
    source_config_name = list(business_rule["sharedWith"].keys())[0]
    destination_config_name = list(
        business_rule["sharedWith"][source_config_name].keys()
    )[0]
    storage = connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
        {"name": destination_config_name}, {"storage": True}
    )
    if storage:
        storage = storage["storage"]
    if source_config_name in storage:
        del storage[source_config_name]
    connector.collection(Collections.EDM_CONFIGURATIONS).update_one(
        {"name": destination_config_name}, {"$set": {"storage": storage}}
    )


@router.get("/business_rules", tags=["EDM Business Rules"])
async def get_business_rule(
    _: User = Security(get_current_user, scopes=["edm_read"])
) -> List[BusinessRuleOut]:
    """Get list of business rules."""
    rules = []
    for rule in connector.collection(Collections.EDM_BUSINESS_RULES).find({}):
        rules.append(BusinessRuleOut(**rule))
    return list(reversed(rules))


@router.post("/business_rules", tags=["EDM Business Rules"])
async def create_business_rule(
    rule: BusinessRuleIn,
    _: User = Security(get_current_user, scopes=["edm_write"]),
) -> BusinessRuleOut:
    """Create a business rule."""
    connector.collection(Collections.EDM_BUSINESS_RULES).insert_one(rule.model_dump())
    share_with = rule.sharedWith
    source_config_name = list(share_with.keys())[0]
    source_data = share_with.get(source_config_name, {})
    dest_config_name = list(source_data.keys())[0]
    logger.debug(
        f"EDM sharing configuration successfully created for Source: {source_config_name} "
        f"and Destination: {dest_config_name}."
    )
    return rule


@router.delete("/business_rule", tags=["EDM Business Rules"])
async def delete_business_rule(
    rule: BusinessRuleDelete,
    _: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Delete a business rule."""
    _clear_destination_config_storage(rule.name)
    rule_db = BusinessRuleDB(
        **connector.collection(Collections.EDM_BUSINESS_RULES).find_one(
            {"name": rule.name}
        )
    )
    share_with = rule_db.sharedWith
    source_config_name = list(share_with.keys())[0]
    source_data = share_with.get(source_config_name, {})
    dest_config_name = list(source_data.keys())[0]
    connector.collection(Collections.EDM_BUSINESS_RULES).delete_one({"name": rule.name})
    logger.debug(
        f"EDM sharing configuration successfully deleted for Source: {source_config_name} "
        f"and Destination: {dest_config_name}."
    )
    return {"success": True}


@router.post("/business_rules/sync", tags=["EDM Business Rules"])
async def sync_action(
    sourceConfiguration: str = Query(...),
    _: User = Security(get_current_user, scopes=["edm_read"]),
):
    """Sync business rule.

    Starts the full plugin lifecycle for the source configuration:
    - Pull data from source
    - Generate EDM hashes
    - Upload hashes to ALL destinations with the same source that are ready for sync

    This endpoint finds ALL sharing configurations with the given source and triggers
    sync for those destinations whose status is in [COMPLETED, FAILED, SCHEDULED].
    Destinations that are already in progress (other statuses) will be skipped.

    Args:
        sourceConfiguration (str): Source configuration name.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        dict: Sync success with details about triggered and skipped destinations.
    """
    # Find all rules with the same source configuration
    all_source_rules = list(
        connector.collection(Collections.EDM_BUSINESS_RULES).find(
            {f"sharedWith.{sourceConfiguration}": {"$exists": True}}
        )
    )
    if not all_source_rules:
        raise HTTPException(
            400,
            f"No sharing configuration exists for source {sourceConfiguration}.",
        )

    # Categorize rules by status - ready vs in-progress
    ready_statuses = {StatusType.COMPLETED, StatusType.FAILED, StatusType.SCHEDULED}
    eligible_destinations, skipped_info = [], []

    for rule in all_source_rules:
        rule_db = BusinessRuleDB(**rule)
        dest_config_name = next(iter(rule_db.sharedWith.get(sourceConfiguration, {})), None)
        if not dest_config_name:
            continue

        if rule_db.status in ready_statuses:
            eligible_destinations.append(dest_config_name)
        else:
            skipped_info.append(dest_config_name)

    if not eligible_destinations:
        raise HTTPException(
            400,
            "Skipping sync as hash upload is already in progress.",
        )

    logger.info(
        f"Executing sync for source configuration '{sourceConfiguration}'. "
        f"Destinations to sync: {', '.join(eligible_destinations)}."
    )
    if skipped_info:
        logger.info(
            f"Skipping sync as hash upload is already in progress "
            f"for the following destinations: {', '.join(skipped_info)}."
        )

    # Start the full plugin lifecycle for all eligible destinations
    execute_celery_task(
        execute_plugin.apply_async,
        "edm.execute_plugin",
        with_locks=True,
        args=[sourceConfiguration],  # share_data will check per-rule status
    )
    return {"success": True}
