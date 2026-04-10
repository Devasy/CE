"""Initializes the celery app."""

from __future__ import absolute_import, unicode_literals

import os
import ssl
import sys
import traceback
from sys import stderr

from bson import ObjectId
from celery import Celery
from celery.signals import worker_init, task_failure
from celery.worker.control import control_command
from datetime import datetime
from kombu import Queue, Exchange

from netskope.common.celery.custom_config import CustomScaler, NoChannelGlobalQoS
from netskope.common.models.other import StatusType
from netskope.common import RABBITMQ_QUORUM_QUEUE_NAME
from netskope.integrations.cls.tasks import TASKS as CLS_TASKS
from netskope.integrations.cte.tasks import TASKS as CTE_TASKS
from netskope.integrations.itsm.tasks import TASKS as ITSM_TASKS
from netskope.integrations.crev2.tasks import TASKS as CREV2_TASKS
from netskope.integrations.edm.tasks import TASKS as EDM_TASKS
from netskope.integrations.cfc.tasks import TASKS as CFC_TASKS
from ..utils.db_connector import Collections, DBConnector
from ..utils.repo_manager import RepoManager
from ..utils import log_mem, release_lock
from netskope.integrations.cls.utils import set_utf8_encoding_flag

try:
    # Create custom SSL context with all advanced settings
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.options |= ssl.OP_NO_TLSv1_1
    ssl_context.options |= ssl.OP_NO_TLSv1
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    ssl_context.maximum_version = ssl.TLSVersion.TLSv1_3

    ssl_context.load_cert_chain(
        certfile='/opt/certs/mongodb_rabbitmq_certs/tls_cert.crt',
        keyfile='/opt/certs/mongodb_rabbitmq_certs/tls_cert_key.key'
    )
    ssl_context.load_verify_locations('/opt/certs/mongodb_rabbitmq_certs/tls_cert_ca.crt')

    BROKER = os.environ["RABBITMQ_CONNECTION_STRING"]
    BACKEND = BROKER.replace("amqps://", "rpc://").replace("amqp://", "rpc://")
    APP = Celery(
        "netskope",
        broker=BROKER,
        backend=BACKEND,
        include=[
            *CTE_TASKS,
            *ITSM_TASKS,
            *CREV2_TASKS,
            *CLS_TASKS,
            *EDM_TASKS,
            *CFC_TASKS,
            "netskope.common.celery.check_updates",
            "netskope.common.celery.delete_logs",
            "netskope.common.celery.delete_tasks",
            "netskope.common.celery.analytics",
            "netskope.common.celery.historical_alerts",
            "netskope.common.celery.pull_logs",
            "netskope.common.celery.heartbeat",
            "netskope.common.celery.pull",
        ],
        broker_use_ssl=ssl_context,
    )

    @control_command()
    def reload_repo_manager(state):
        """Refresh plugin repo manager."""
        print(f"Loading plugins from disk for worker: {state['hostname']} with pid={os.getpid()}")
        manager = RepoManager()
        manager.load()
        manager.helper.refresh()

    @control_command()
    def reload_cls_utf_8_encoding_flag(state):
        """Reload UTF-8 encoding flag."""
        print(f"Reloading UTF-8 encoding flag for worker: {state['hostname']} with pid={os.getpid()}")
        set_utf8_encoding_flag()

    @control_command()
    def reload_environment_variables(state, **kwargs):
        """Reload environment variables."""
        print(f"Reloading Environment variables for worker: {state['hostname']} with pid={os.getpid()}")
        for key, value in kwargs.items():
            os.environ[key] = str(value)
            print(f"Updated environment var: {key}")

    @worker_init.connect
    def init_worker(*args, **kwargs):
        """Initialize workers."""
        set_utf8_encoding_flag()

    @task_failure.connect
    def handle_task_failure(*args, **kwargs):
        """Handle task failure."""
        task_name = kwargs["sender"].name
        log_mem(
            f"Task: '{task_name}' failed. Removing the locks. "
            "Expand the log to see the traceback.",
            details=traceback.format_exc(),
        )
        connector = DBConnector()
        if "uid" in kwargs.get("kwargs"):
            connector.collection(Collections.TASK_STATUS).update_one(
                {"_id": ObjectId(kwargs["kwargs"]["uid"])},
                {
                    "$set": {
                        "status": StatusType.ERROR,
                        "completedAt": datetime.now(),
                    }
                },
            )
        release_lock(kwargs.get("args", []), kwargs.get("kwargs", {}))

    APP.conf["task_serializer"] = "pickle"
    APP.conf["accept_content"] = ["json", "pickle"]
    APP.conf["CELERY_MONGODB_SCHEDULER_DB"] = "cte"
    APP.conf["CELERY_MONGODB_SCHEDULER_COLLECTION"] = Collections.SCHEDULES
    APP.conf["CELERY_MONGODB_SCHEDULER_URL"] = os.environ["MONGO_CONNECTION_STRING"]
    APP.conf["worker_prefetch_multiplier"] = 1
    APP.conf["broker_transport_options"] = {"heartbeat": 120}
    APP.conf["task_track_started"] = True
    APP._conf["broker_connection_retry_on_startup"] = True
    APP.conf.update(
        result_expires=3600,
    )
    APP.conf["task_acks_late"] = True
    APP.conf["task_reject_on_worker_lost"] = True
    APP.conf["task_acks_on_failure_or_timeout"] = False
    APP.conf["worker_cancel_long_running_tasks_on_connection_loss"] = True
    APP.conf["task_queues"] = [
        Queue(
            name=RABBITMQ_QUORUM_QUEUE_NAME.format(i),
            routing_key=RABBITMQ_QUORUM_QUEUE_NAME.format(i),
            durable=True,
            queue_arguments={"x-queue-type": "quorum"},
            exchange=Exchange('CE_EXCHANGE', type='topic')
        )
        for i in [3, 6, 9]
    ]
    APP.steps["consumer"].add(NoChannelGlobalQoS)
    APP.conf["worker_autoscaler"] = CustomScaler

    if __name__ == "__main__":
        APP.start()
except KeyError:
    print("One of the required environment variable is not set", file=stderr)
    sys.exit(1)
