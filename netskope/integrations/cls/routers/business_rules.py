"""Business rule related endpoints."""

from typing import List
from celery.result import AsyncResult
from fastapi.param_functions import Query
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from fastapi import APIRouter, Security
from datetime import datetime, timedelta
from uuid import uuid4

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import (
    Logger,
    DBConnector,
    Collections,
    PluginHelper,
    Scheduler,
)
from netskope.common.utils.common_pull_scheduler import schedule_or_delete_common_pull_tasks

from ..tasks.plugin_lifecycle_task import execute_historical
from ..models import (
    BusinessRuleIn,
    BusinessRuleOut,
    BusinessRuleUpdate,
    BusinessRuleDelete,
    BusinessRuleDB,
    ConfigurationDB,
)
from ..utils import schedule_or_delete_third_party_pull_task

helper = PluginHelper()
router = APIRouter()
logger = Logger()
connector = DBConnector()
scheduler = Scheduler()


@router.get("/business_rules", tags=["CLS Business Rules"])
async def get_business_rule(
    user: User = Security(get_current_user, scopes=["cls_read"])
) -> List[BusinessRuleOut]:
    """Get list of business rules."""
    rules = []
    for rule in connector.collection(Collections.CLS_BUSINESS_RULES).find({}):
        rules.append(BusinessRuleOut(**rule))
    return rules


@router.post("/business_rules", tags=["CLS Business Rules"])
async def create_business_rule(
    rule: BusinessRuleIn,
    user: User = Security(get_current_user, scopes=["cls_write"]),
) -> BusinessRuleOut:
    """Create a business rule."""
    connector.collection(Collections.CLS_BUSINESS_RULES).insert_one(rule.model_dump())
    logger.debug(f"CLS business rule {rule.name} successfully created.")
    return rule


def stop_existing_historical_task(siem_mapping_id, source, destination) -> None:
    """Stop existing historical task."""
    if siem_mapping_id:
        task_id = siem_mapping_id.get("task_id")
        if task_id:
            try:
                state = AsyncResult(task_id).state
            except Exception:
                return
            if state not in ["SUCCESS", "FAILURE"]:
                APP.control.revoke(task_id, terminate=True)
                logger.info(f"Stopped existing historical task for {source} to {destination}.")


def _get_new_mappings(
    before: dict, after: dict,
    siem_mapping_ids: dict
) -> dict:
    """Get new mappings.

    Args:
        before (dict): Older mappings.
        after (dict): Newer mappings.
        siem_mapping_ids (dict): SIEM mapping ids.
    Returns:
        dict: Different mappings.
    """
    mappings = {}
    new_siem_mapping_ids = {}
    if siem_mapping_ids:
        new_siem_mapping_ids = siem_mapping_ids.copy()
    for key, values in after.items():
        if key not in before:
            mappings[key] = values
            for value in values:
                new_siem_mapping_ids[f"{key}_{value}"] = {
                    "id": str(uuid4()),
                    "task_id": None
                }
        else:
            for v in values:
                if v not in before[key]:
                    mappings[key] = mappings.get(key, []) + [v]
                    new_siem_mapping_ids[f"{key}_{v}"] = {
                        "id": str(uuid4()),
                        "task_id": None
                    }
    for key, values in before.items():
        if key not in after:
            for value in values:
                if siem_mapping_ids.get(f"{key}_{value}"):
                    stop_existing_historical_task(siem_mapping_ids[f"{key}_{value}"], key, value)
                    del new_siem_mapping_ids[f"{key}_{value}"]
        else:
            for v in values:
                if v not in after[key]:
                    if siem_mapping_ids.get(f"{key}_{v}"):
                        stop_existing_historical_task(siem_mapping_ids[f"{key}_{v}"], key, v)
                        del new_siem_mapping_ids[f"{key}_{v}"]
    return mappings, new_siem_mapping_ids


@router.patch("/business_rule", tags=["CLS Business Rules"])
async def update_business_rule(
    rule: BusinessRuleUpdate,
    user: User = Security(get_current_user, scopes=["cls_write"]),
) -> BusinessRuleOut:
    """Update an existing business rules."""
    before = connector.collection(Collections.CLS_BUSINESS_RULES).find_one(
        {"name": rule.name}
    )
    before = BusinessRuleDB(**before)
    after = BusinessRuleDB(
        **connector.collection(Collections.CLS_BUSINESS_RULES).find_one_and_update(
            {"name": rule.name},
            {"$set": rule.model_dump(exclude_none=True)},
            return_document=ReturnDocument.AFTER,
        )
    )
    new_mappings, new_siem_mapping_ids = _get_new_mappings(
        before.siemMappings, after.siemMappings, before.siemMappingIDs
    )
    after = BusinessRuleDB(
        **connector.collection(Collections.CLS_BUSINESS_RULES).find_one_and_update(
            {"name": rule.name},
            {"$set": {"siemMappingIDs": new_siem_mapping_ids}},
            return_document=ReturnDocument.AFTER,
        )
    )
    if before.name == "All":
        for config in connector.collection(Collections.CLS_CONFIGURATIONS).find(
            {
                "name": {"$in": list(before.siemMappings.keys())},
                "task.task_id": {"$ne": None},
            }
        ):
            logger.debug(
                f"Terminating webtransaction process with task_id {config.get('task_id')}"
            )
            APP.control.revoke(config.get("task", {}).get("task_id", ""), terminate=True, signal='SIGINT')
        for key in after.siemMappings.keys():
            configuration = connector.collection(
                Collections.CLS_CONFIGURATIONS
            ).find_one({"name": key})
            configuration = ConfigurationDB(**configuration)
            if "netskope_webtx.main" in configuration.plugin:
                try:
                    scheduler.schedule(
                        name=f"tenant.{configuration.tenant}.{configuration.name}.webtx",
                        task_name="common.pull",
                        poll_interval=30,
                        poll_interval_unit="seconds",
                        args=["webtx", configuration.tenant],
                        kwargs={"configuration_name": configuration.name}
                    )
                except DuplicateKeyError:
                    pass
    for key, values in new_mappings.items():
        for value in values:
            configuration = connector.collection(
                Collections.CLS_CONFIGURATIONS
            ).find_one({"name": key})
            configuration = ConfigurationDB(**configuration)
            PluginClass = helper.find_by_id(configuration.plugin)
            if not PluginClass.metadata.get(
                "netskope", False
            ) and PluginClass.metadata.get("pull_supported", False):
                # skip historical pull task for 3rd party plugins that support pull
                continue
            if "alerts" in PluginClass.metadata.get(
                "types", []
            ) or "events" in PluginClass.metadata.get("types", []):
                logger.debug(f"Scheduling historical pull for {value} configuration.")
                end_time = datetime.now()
                source = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
                    {"name": key}
                )
                start_time = end_time - timedelta(
                    hours=source.get("parameters", {}).get("hours", 1)
                )
                start_time_alert = end_time - timedelta(
                    days=source.get("parameters", {}).get("days", 7)
                )
                if start_time == end_time and start_time_alert == end_time:
                    logger.info(f"Historical data pull has been skipped for '{key}' plugin,"
                                " because it is disabled from the configuration.")
                else:
                    task = execute_celery_task(
                        execute_historical.apply_async, "cls.execute_historical",
                        args=[key, value, after.name, start_time, start_time_alert, end_time],
                    )
                    after = BusinessRuleDB(
                        **connector.collection(Collections.CLS_BUSINESS_RULES).find_one_and_update(
                            {"name": rule.name},
                            {
                                "$set": {
                                    f"siemMappingIDs.{key}_{value}.task_id": task.task_id,
                                }
                            },
                            return_document=ReturnDocument.AFTER,
                        )
                    )
            elif "logs" in PluginClass.metadata.get("types", []):
                logger.debug(f"Scheduling historical pull for {value} configuration.")
                end_time = datetime.now()
                source = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
                    {"name": key}
                )
                start_time = end_time - timedelta(
                    hours=source.get("parameters", {}).get("days", 7)
                )
                # adding schedular task
                if (
                    connector.collection(Collections.SCHEDULES).find_one(
                        {"name": f"cls.{key}"}
                    )
                    is None
                ):
                    connector.collection(Collections.SCHEDULES).insert_one(
                        {
                            "_cls": "PeriodicTask",
                            "name": f"cls.{key}",
                            "enabled": True,
                            "args": [key],
                            "task": "common.pull_logs",
                            "interval": {
                                "every": 30,
                                "period": "seconds",
                            },
                        }
                    )
                if start_time == end_time:
                    logger.info(f"Historical data pull has been skipped for '{value}' plugin,"
                                " because it is disabled from the configuration.")
                else:
                    task = execute_celery_task(
                        execute_historical.apply_async, "cls.execute_historical",
                        args=[key, value, after.name, start_time, ..., end_time],
                    )
                    after = BusinessRuleDB(
                        **connector.collection(Collections.CLS_BUSINESS_RULES).find_one_and_update(
                            {"name": rule.name},
                            {
                                "$set": {
                                    f"siemMappingIDs.{key}_{value}.task_id": task.task_id,
                                }
                            },
                            return_document=ReturnDocument.AFTER,
                        )
                    )

    after_dict = after.model_dump()
    rule_out = BusinessRuleOut(**after_dict)
    schedule_or_delete_common_pull_tasks()
    schedule_or_delete_third_party_pull_task()
    logger.debug(f"CLS business rule {rule.name} updated.")
    return rule_out


@router.delete("/business_rule", tags=["CLS Business Rules"])
async def delete_business_rule(
    rule: BusinessRuleDelete,
    user: User = Security(get_current_user, scopes=["cls_write"]),
):
    """Delete a business rule."""
    if rule.name == "All":
        logger.error(
            f"Business rule {rule.name} cannot be deleted.",
            error_code="CLS_1002",
        )
        return {"success": False}
    connector.collection(Collections.CLS_BUSINESS_RULES).delete_one({"name": rule.name})
    schedule_or_delete_common_pull_tasks()
    schedule_or_delete_third_party_pull_task()
    logger.debug(f"Business rule {rule.name} has been successfully deleted.")
    return {"success": True}


@router.post("/business_rule/sync", tags=["CLS Business Rules"])
async def sync_business_rule(
    rule: str = Query(...),
    sourceConfiguration: str = Query(...),
    destinationConfiguration: str = Query(...),
    startTime: datetime = Query(...),
    endTime: datetime = Query(...),
    user: User = Security(get_current_user, scopes=["cls_write"]),
):
    """Delete a business rule."""
    logger.debug(
        f"Manual sync added to queue for {rule} business rule from "
        f"{sourceConfiguration} to {destinationConfiguration} for "
        f"{startTime} UTC to {endTime} UTC."
    )
    configuration = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
        {"name": sourceConfiguration}
    )
    configuration = ConfigurationDB(**configuration)
    tenant = configuration.tenant
    tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
        {"name": tenant}
    )
    execute_celery_task(
        execute_historical.apply_async,
        "cls.execute_historical",
        args=[sourceConfiguration, destinationConfiguration, rule, startTime, startTime, endTime, ..., True],
    )
    return {"success": True}
