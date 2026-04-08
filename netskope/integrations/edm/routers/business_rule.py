"""Business rule related endpoints."""
from typing import List

from fastapi import APIRouter, HTTPException, Query, Security
from fastapi.param_functions import Body

from netskope.common.api.routers.auth import get_current_user
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger, PluginHelper
from netskope.integrations.edm.models import (Action, BusinessRuleDelete,
                                              BusinessRuleIn, BusinessRuleOut,
                                              BusinessRuleDB, StatusType)
from netskope.integrations.edm.tasks.share_data import share_data

router = APIRouter()
logger = Logger()
connector = DBConnector()


def _clear_destination_config_storage(rule_name: str):
    """Clear all storage stored in destination configuration related to configured source configuration in sharing.

    Args:
        rule_name (str): business_rule.name generated while creation of business rule.
    """
    business_rule = connector.collection(Collections.EDM_BUSINESS_RULES).find_one(
        {
            "name": rule_name
        }
    )
    source_config_name = list(business_rule["sharedWith"].keys())[0]
    destination_config_name = list(business_rule["sharedWith"][source_config_name].keys())[0]
    storage = connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
        {
            "name": destination_config_name
        },
        {"storage": True}
    )
    if storage:
        storage = storage["storage"]
    if source_config_name in storage:
        del storage[source_config_name]
    connector.collection(Collections.EDM_CONFIGURATIONS).update_one(
        {"name": destination_config_name}, {"$set": {"storage": storage}}
    )


def _sync_netskope_ce_receiver_hashes(source_config_name, storage: dict, kwargs: dict):
    """Sync Netskope CE receiver edm hashes."""
    if not storage:
        logger.debug(
           "Unable to sync as no EDM hashes have been "
           f"received for Netskope CE Receiver: {source_config_name}."
        )
        return
    for ce_identifier in storage:
        if (
            storage[ce_identifier]
            and isinstance(storage[ce_identifier], dict)
            and "edm_hash_folder" in storage[ce_identifier]
        ):
            execute_celery_task(
                share_data.apply_async,
                "edm.share_data",
                args=[source_config_name],
                kwargs={
                    **kwargs,
                    "hash_dict": storage[ce_identifier]
                },
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
    connector.collection(Collections.EDM_BUSINESS_RULES).insert_one(
        rule.model_dump()
    )
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
    connector.collection(Collections.EDM_BUSINESS_RULES).delete_one(
        {"name": rule.name}
    )
    logger.debug(
        f"EDM sharing configuration successfully deleted for Source: {source_config_name} "
        f"and Destination: {dest_config_name}."
    )
    return {"success": True}


@router.post("/business_rules/sync", tags=["EDM Business Rules"])
async def sync_action(
    rule: str = Query(...),
    sourceConfiguration: str = Query(...),
    destinationConfiguration: str = Query(...),
    action: Action = Body(...),
    _: User = Security(get_current_user, scopes=["edm_read"]),
):
    """Sync business rule."""
    rule_dict = connector.collection(Collections.EDM_BUSINESS_RULES).find_one(
        {"name": rule}
    )
    if rule_dict is None:
        raise HTTPException(400, "Requested EDM sharing configuration does not exist.")
    rule_db = BusinessRuleDB(**rule_dict)
    if rule_db.status not in [StatusType.COMPLETED, StatusType.FAILED, StatusType.SCHEDULED]:
        raise HTTPException(
            400,
            f"Sharing process for '{sourceConfiguration}' to "
            f"'{destinationConfiguration}' is already in progress."
            " Please try the sync operation after some time."
        )
    logger.debug(
        f"Sync with EDM sharing: {rule} for configuration \
            {sourceConfiguration} to {destinationConfiguration} is triggered."
    )
    kwargs = {
        "destination_config_name": destinationConfiguration,
        "rule": rule,
        "action": dict(action),
    }
    # Here starting share data task
    source_config = connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
        {"name": sourceConfiguration}
    )
    if PluginHelper.check_plugin_name_with_regex(
        "netskope_edm_forwarder_receiver",
        source_config["plugin"]
    ):
        _sync_netskope_ce_receiver_hashes(
            source_config_name=sourceConfiguration,
            storage=source_config["storage"],
            kwargs=kwargs,
        )
    else:
        execute_celery_task(
            share_data.apply_async,
            "edm.share_data",
            args=[sourceConfiguration],
            kwargs=kwargs,
        )
    return {"success": True}
