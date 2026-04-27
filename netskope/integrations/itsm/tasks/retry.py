"""Retry task."""
import os
from netskope.common.celery.main import APP
from bson.objectid import ObjectId
from netskope.common.utils import integration, track, Collections, DBConnector, Logger
from netskope.integrations.itsm.models.task import Task, TaskStatus, TaskRequestStatus
from netskope.integrations.itsm.utils.retry import _create_tickets_for_failed_task
import traceback
from datetime import datetime, timedelta

connector = DBConnector()
logger = Logger()
RETRY_TICKETS_LOCK_CLEAR_TIME = int(os.getenv("RETRY_TICKETS_LOCK_CLEAR_TIME", 1))


@APP.task(name="itsm.retry_tickets")
@integration("itsm")
@track()
def retry_tickets(valid_task_ids):
    """Retry the tasks."""
    try:
        success_tasks = 0
        for task_id in valid_task_ids:
            task_id = ObjectId(task_id)
            task = connector.collection(Collections.ITSM_TASKS).find_one(
                {"_id": task_id}
            )
            if not task:
                continue
            task_modal = Task(**task)
            evaluationResultTime = task_modal.evaluationResultTime
            # lock is not acquired
            if evaluationResultTime is None:
                task_modal.evaluationResultTime = datetime.now() + timedelta(
                    hours=RETRY_TICKETS_LOCK_CLEAR_TIME
                )
                connector.collection(Collections.ITSM_TASKS).update_one(
                    {"_id": task_id}, {"$set": {"evaluationResultTime": task_modal.evaluationResultTime}}
                )
            # lock is aquired.
            else:
                # lock expired the time limit.
                if datetime.now() > evaluationResultTime:
                    task_modal.evaluationResultTime = None
                    connector.collection(Collections.ITSM_TASKS).update_one(
                        {"_id": task_id}, {"$set": {"evaluationResultTime": None}}
                    )
                continue
            if (
                task.get("status") != TaskStatus.FAILED.value
                or task.get("approvalStatus") not in [
                    TaskRequestStatus.SCHEDULED.value,
                    TaskRequestStatus.APPROVED.value,
                    TaskRequestStatus.NOT_REQUIRED.value,
                ]
            ):
                success_tasks += 1
                continue
            task = Task(**task)
            data_item = task.dataItem
            business_rule = task.businessRule
            configuration = task.configuration
            _create_tickets_for_failed_task(
                data_item,
                business_rule,
                task_id,
                configuration,
                data_type=task_modal.dataType,
            )
            connector.collection(Collections.ITSM_TASKS).update_one(
                {"_id": task_id}, {"$set": {"evaluationResultTime": None}}
            )
        logger.info("Retry ticket creation task completed successfully.")
        return {"success": True}
    except Exception:
        logger.error(
            "Error occurred while retrying the ticket creation.",
            error_code="CTO_1028",
            details=traceback.format_exc(),
        )
        return {"success": False}
