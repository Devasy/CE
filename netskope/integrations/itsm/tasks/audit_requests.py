"""Audit task requests."""

import traceback
from datetime import datetime
from itertools import groupby
from netskope.common.celery.main import APP
from netskope.common.utils import (
    DBConnector,
    Collections,
    integration,
    Logger,
    PluginHelper,
    track,
)
from netskope.common.utils.secrets_manager import SecretDict
from netskope.integrations.itsm.models import (
    BusinessRuleDB,
    ConfigurationDB,
    Task,
    TaskRequestStatus,
    TaskStatus,
)
from netskope.integrations.itsm.utils.tickets import create_tickets_or_requests

connector = DBConnector()
logger = Logger()
helper = PluginHelper()


def _update_lock(time: datetime):
    connector.collection(Collections.SETTINGS).update_one(
        {}, {"$set": {"itsm.lockedAuditRequestsAt": time}}
    )


@APP.task(name="itsm.audit_requests")
@integration("itsm")
@track()
def audit_requests():
    """Audit requests."""
    scheduled_tasks = connector.collection(Collections.ITSM_TASKS).find(
        {
            "approvalStatus": {
                "$in": [TaskRequestStatus.SCHEDULED.value]
            }
        }
    )
    grouped_tasks_by_config = groupby(
        scheduled_tasks, key=lambda task: task["configuration"]
    )
    for name, tasks in grouped_tasks_by_config:
        configuration = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
            {"name": name}
        )
        if configuration is None:
            logger.error(
                "Error occurred while creating tasks from the requests. "
                f"Ticket Orchestrator configuration {name} no longer exists.",
                error_code="CTO_1048",
            )
            connector.collection(Collections.ITSM_TASKS).update_one(
                {"_id": {"$in": [task["_id"] for task in tasks]}},
                {
                    "$set": {
                        "status": TaskStatus.FAILED.value,
                        "lastUpdatedAt": datetime.now(),
                    }
                }
            )
            continue
        configuration = ConfigurationDB(**configuration)
        if not configuration.active:
            logger.debug(
                f"Ticket Orchestrator configuration {name} is disabled. Skipping the task creation process.",
                error_code="CTO_1050",
            )
            continue
        for task in tasks:
            task_obj = Task(**task)
            data_item = task_obj.dataItem
            rule = task_obj.businessRule
            data_type = task_obj.dataType
            business_rule = connector.collection(
                Collections.ITSM_BUSINESS_RULES
            ).find_one({"name": rule})
            if (
                business_rule is None
            ):  # if business rule deleted then continue with next task.
                logger.error(
                    "Error occurred while creating task from the request. "
                    f"Ticket Orchestrator business rule {rule} no longer exists.",
                    error_code="CTO_1046",  # update the error code.
                )
                connector.collection(Collections.ITSM_TASKS).update_one(
                    {"_id": task["_id"]},
                    {
                        "$set": {
                            "status": TaskStatus.FAILED.value,
                            "lastUpdatedAt": datetime.now(),
                        }
                    },
                )
                continue
            business_rule = BusinessRuleDB(**business_rule)
            if (
                not business_rule.queues
            ):  # if queue is deleted then continue with next task.
                logger.error(
                    "Error occurred while creating task from the request. "
                    f"Queue for Ticket Orchestrator business rule {rule} no longer exists.",
                    error_code="CTO_1047",  # update the error code.
                )
                connector.collection(Collections.ITSM_TASKS).update_one(
                    {"_id": task["_id"]},
                    {
                        "$set": {
                            "status": TaskStatus.FAILED.value,
                            "lastUpdatedAt": datetime.now(),
                        }
                    },
                )
                continue
            try:
                PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
                plugin = PluginClass(
                    configuration.name,
                    SecretDict(configuration.parameters),
                    configuration.storage,
                    configuration.checkpoint,
                    logger,
                )
                result = create_tickets_or_requests(
                    plugin=plugin,
                    data_item=data_item,
                    rule=business_rule,
                    queues=business_rule.queues[configuration.name],
                    configuration=configuration,
                    data_type=data_type,
                    audit_request=True,
                    _id=task["_id"],
                )
                if not result["failed"]:
                    logger.info(
                        "Successfully created/updated task for "
                        f"{data_type.value} with ID {data_item.id} "
                        f"for configuration {configuration.name} from the request."
                    )
            except Exception as ex:
                logger.error(
                    f"Could not create task for {data_type.value} "
                    f"with ID {data_item.id} for configuration {configuration.name} from the request. "
                    f"It will be retried in next run. Error: {ex}",
                    error_code="CTO_1049",
                    details=traceback.format_exc(),
                )
                connector.collection(Collections.ITSM_TASKS).update_one(
                    {"_id": task["_id"]},
                    {
                        "$set": {
                            "status": TaskStatus.FAILED.value,
                            "lastUpdatedAt": datetime.now(),
                            "approvalStatus": TaskRequestStatus.APPROVED.value
                        }
                    },
                )
        _update_lock(datetime.now())
    return {"success": True}
