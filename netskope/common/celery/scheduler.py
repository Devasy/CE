"""Custom implementation of the MongoScheduler class."""
import os
import re
import traceback
import mongoengine
from copy import deepcopy
import amqp.exceptions
from celery.exceptions import OperationalError
from celery.beat import ScheduleEntry, SchedulingError, Scheduler
from celery.utils.log import get_logger
from celery import current_app
from mongoengine import NotUniqueError
from netskope.common.models.other import TaskStatus, StatusType
from netskope.common.models.celerybeatmongo import PeriodicTask
from datetime import datetime, timedelta, UTC
from netskope.common.utils import (
    DBConnector,
    Collections,
    flatten,
    Notifier,
    Logger
)
from pymongo import ReturnDocument
from kombu import Queue

from netskope.common.utils.decorator import retry
from netskope.common.utils.disk_free_alarm import check_disk_free_alarm
from netskope.common.utils import get_lock_params
from netskope.common.utils.scheduler import LOCKING_ARGS
from netskope.common import RABBITMQ_QUORUM_QUEUE_NAME
from netskope.common.models import SettingsDB
from .task_priorities import TASK_PRIORITIES
from netskope.common.utils.const import (
    WEBTX_SOFT_TIME_LIMIT,
    ALERT_EVENT_SOFT_TIME_LIMIT,
)

try:
    MAX_WAIT_ON_LOCK_IN_MINUTES = int(
        os.environ.get("PLUGIN_TIMEOUT_MINUTES", 4 * 60)
    )
except ValueError:
    MAX_WAIT_ON_LOCK_IN_MINUTES = 4 * 60

OLD_LOCKING_MECHANISM = os.environ.get("OLD_LOCKING_MECHANISM", "")
CONTAINER_ID = os.environ.get("CE_CONTAINER_ID", None)

MAX_WORKER_WAIT_TIME = os.environ.get("MAX_WORKER_WAIT_TIME", 300)
CUSTOM_MAX_WAIT_ON_LOCK_IN_MINUTES = {
    "common.pull": {
        "alerts": 105,
        "events": 105,
        "webtx": 60
    },
}

connector = DBConnector()
notifier = Notifier()
logger = Logger()


class MongoScheduleEntry(ScheduleEntry):
    """Celery Beat Mongo Class."""

    def __init__(self, task):
        """
        Initialize the MongoScheduleEntry.

        :param task: The PeriodicTask object which we are going to schedule.
        :type task: PeriodicTask
        """
        self._task = task

        self.app = current_app._get_current_object()
        self.name = self._task.name
        self.task = self._task.task

        self.schedule = self._task.schedule

        self.args = self._task.args
        self.kwargs = self._task.kwargs
        self.options = {
            'queue': self._task.queue,
            'exchange': self._task.exchange,
            'routing_key': self._task.routing_key,
            'expires': self._task.expires,
            'soft_time_limit': self._task.soft_time_limit,
            'enabled': self._task.enabled
        }
        if self._task.total_run_count is None:
            self._task.total_run_count = 0
        self.total_run_count = self._task.total_run_count

        if not self._task.last_run_at:
            self._task.last_run_at = self._default_now()
        self.last_run_at = self._task.last_run_at

    def _default_now(self):
        """
        Return the current date and time as a :class:`~datetime.datetime` object.

        This is the default implementation of :meth:`now` that returns the current
        date and time. It's used when no custom :meth:`now` function is provided.

        :rtype: :class:`~datetime.datetime`
        """
        return self.app.now()

    def next(self):
        """
        Return the next beat due for the task.

        :rtype: :class:`~MongoScheduleEntry`

        This method is used by Celery Beat to determine the next time a task should
        be run. It returns a new :class:`~MongoScheduleEntry` object with the
        updated last run time and count.
        """
        self._task.last_run_at = self.app.now()
        self._task.total_run_count += 1
        self._task.run_immediately = False
        return self.__class__(self._task)

    __next__ = next

    def is_due(self):
        """Determine if the task is due to be run now."""
        if not self._task.enabled:
            return False, 5.0   # 5 second delay for re-enable.
        if hasattr(self._task, 'start_after') and self._task.start_after:
            if datetime.now() < self._task.start_after:
                return False, 5.0
        if hasattr(self._task, 'max_run_count') and self._task.max_run_count:
            if (self._task.total_run_count or 0) >= self._task.max_run_count:
                return False, 5.0
        if self._task.run_immediately:
            # figure out when the schedule would run next anyway
            _, n = self.schedule.is_due(self.last_run_at)
            return True, n
        return self.schedule.is_due(self.last_run_at)

    def __repr__(self):
        """
        Return string representation of the MongoScheduleEntry.

        :rtype: str
        """
        return (u'<{0} ({1} {2}(*{3}, **{4}) {{5}})>'.format(  # NOQA
            self.__class__.__name__,
            self.name, self.task, self.args,
            self.kwargs, self.schedule,
        ))  # NOQA

    def reserve(self, entry):
        """
        Reserve a task to be run.

        :param entry: The :class:`~celery.beat.SchedulerEntry` to be reserved.
        :type entry: :class:`~celery.beat.SchedulerEntry`
        :return: The reserved :class:`~celery.beat.SchedulerEntry`.
        :rtype: :class:`~celery.beat.SchedulerEntry`
        """
        new_entry = Scheduler.reserve(self, entry)
        return new_entry

    def save(self):
        """
        Save the state of the task.

        Saves the :class:`~celery.beat.SchedulerEntry`'s state to the database.
        """
        # Check if task still exists in the database
        task = PeriodicTask.objects(id=self._task.id).first()
        if not task:
            return
        if self.total_run_count > self._task.total_run_count:
            self._task.total_run_count = self.total_run_count
        if self.last_run_at and self._task.last_run_at and self.last_run_at > self._task.last_run_at:
            self._task.last_run_at = self.last_run_at
        self._task.run_immediately = False
        try:
            self._task.save(save_condition={})
        except Exception:
            get_logger(__name__).error(traceback.format_exc())


class MongoScheduler(Scheduler):
    """Celery beat scheduler with Mongo backend."""

    #: how often should we sync in schedule information
    #: from the backend mongo database
    UPDATE_INTERVAL = timedelta(seconds=5)

    Entry = MongoScheduleEntry

    Model = PeriodicTask

    def __init__(self, *args, **kwargs):
        """Initialize MongoScheduler class."""
        if hasattr(current_app.conf, "mongodb_scheduler_db"):
            db = current_app.conf.get("mongodb_scheduler_db")
        elif hasattr(current_app.conf, "CELERY_MONGODB_SCHEDULER_DB"):
            db = current_app.conf.CELERY_MONGODB_SCHEDULER_DB
        else:
            db = "celery"

        if hasattr(current_app.conf, "mongodb_scheduler_connection_alias"):
            alias = current_app.conf.get('mongodb_scheduler_connection_alias')
        elif hasattr(current_app.conf, "CELERY_MONGODB_SCHEDULER_CONNECTION_ALIAS"):
            alias = current_app.conf.CELERY_MONGODB_SCHEDULER_CONNECTION_ALIAS
        else:
            alias = "default"

        if hasattr(current_app.conf, "mongodb_scheduler_url"):
            host = current_app.conf.get('mongodb_scheduler_url')
        elif hasattr(current_app.conf, "CELERY_MONGODB_SCHEDULER_URL"):
            host = current_app.conf.CELERY_MONGODB_SCHEDULER_URL
        else:
            host = None

        self._mongo = mongoengine.connect(db, host=host, alias=alias)

        if host:
            # Masking the password in the connection string
            connection_string_without_password = re.sub(r":([^@:]+)@", r":**@", host)
            get_logger(__name__).info("backend scheduler using %s:%s",
                                      connection_string_without_password, self.Model._get_collection().name)
        else:
            get_logger(__name__).info("backend scheduler using %s/%s:%s",
                                      "mongodb://localhost",
                                      db, self.Model._get_collection().name)

        self._schedule = {}
        self._last_updated = None
        Scheduler.__init__(self, *args, **kwargs)
        self.max_interval = (kwargs.get('max_interval')
                             or self.app.conf.CELERYBEAT_MAX_LOOP_INTERVAL or 5)

    def setup_schedule(self):
        """Configure the scheduler from the database."""
        pass

    def requires_update(self):
        """Check whether we should pull an updated schedule from the backend database."""
        if not self._last_updated:
            return True
        return self._last_updated + self.UPDATE_INTERVAL < datetime.now()

    def get_from_database(self):
        """Get the schedule from the database."""
        self.sync()
        d = {}
        for doc in self.Model.objects():
            d[doc.name] = self.Entry(doc)
        return d

    @property
    def schedule(self):
        """Get the schedule from the database if necessary."""
        if self.requires_update():
            self._schedule = self.get_from_database()
            self._last_updated = datetime.now()
        return self._schedule

    def sync(self):
        """Update the schedule from the database."""
        for entry in self._schedule.values():
            entry.save()


def _check_beat_lock():
    """Check for the beat lock."""
    settings = list(
        connector.collection(Collections.SETTINGS).aggregate(
            [
                {
                    "$lookup": {
                        "from": "node_health",
                        "localField": "beat_status.node_name",
                        "foreignField": "worker_id",
                        "as": "result",
                    },
                },
            ]
        )
    )
    settings = settings[0]
    if node_health := settings.get("result", []):
        node_health = node_health[0]
        if node_health.get("check_time") > datetime.now() - timedelta(
            seconds=MAX_WORKER_WAIT_TIME
        ):  # current beat is healthy
            if (
                settings.get("beat_status", {}).get("node_name")
                == CONTAINER_ID
            ):
                return settings
            else:
                return None

    updated_doc = connector.collection(
        Collections.SETTINGS
    ).find_one_and_update(
        {
            "beat_status.node_name": settings.get("beat_status").get(
                "node_name"
            )
        },
        {"$set": {"beat_status.node_name": CONTAINER_ID}},
        return_document=ReturnDocument.AFTER,
    )
    return updated_doc  # this node acquired the lock


class CustomMongoScheduler(MongoScheduler):
    """Celery beat scheduler with Mongo backend."""

    def sync(self):
        """Override to handle race conditions and NotUniqueError."""
        for entry in self._schedule.values():
            try:
                entry.save()
            except NotUniqueError:
                print(f"Could not update duplicate schedule for {entry.name}")
            except Exception as e:
                print(f"Error updating schedule for {entry.name}: {str(e)}")
                # Continue processing other entries even if one fails

    def _get_custom_max_wait_on_lock_in_minutes(self, entry: MongoScheduleEntry):
        """Get custom max wait on lock in minutes."""
        if entry.task in CUSTOM_MAX_WAIT_ON_LOCK_IN_MINUTES:
            data = CUSTOM_MAX_WAIT_ON_LOCK_IN_MINUTES[entry.task]
            if isinstance(data, dict) and entry.args[0] in data:
                return data[entry.args[0]]
            elif isinstance(data, int):
                return data
            else:
                return MAX_WAIT_ON_LOCK_IN_MINUTES
        return MAX_WAIT_ON_LOCK_IN_MINUTES

    def is_due(self, entry):
        """Override is due."""
        settings = _check_beat_lock()
        is_due, next_time_to_run = entry.is_due()
        if settings is None:
            print(
                f"Current beat isn't scheduling the task with name {entry.task} "
                "because another beat has acquired the lock."
            )
            return False, next_time_to_run
        else:
            print(f"Beat will schedule task with name {entry.task} after {next_time_to_run} secs.")
            return is_due, next_time_to_run

    def apply_entry(self, entry: MongoScheduleEntry, producer):
        """Add the task to the queue."""
        soft_time_limit = 0
        settings = SettingsDB(
            **connector.collection(Collections.SETTINGS).find_one({})
        )
        if settings.disk_alarm is False and check_disk_free_alarm():
            notifier.error(
                "Available disk space has reached below the minimum space required by CE. "
                "Free up some disk space to continue regular operations."
            )
            connector.collection(Collections.SETTINGS).update_one(
                {}, {"$set": {"disk_alarm": True}}
            )
        if settings.disk_alarm is True and not check_disk_free_alarm():
            connector.collection(Collections.SETTINGS).update_one(
                {}, {"$set": {"disk_alarm": False}}
            )
        if not is_enabled(entry.task):
            print(
                f"Module is disabled. Not adding {entry.task} task to the queue.",
                end="",
            )
            return
        is_create_task = False
        lock_collection, lock_field, query, lock_field_change = get_lock_params(entry.args, entry.kwargs)
        if (
            lock_collection is not None
        ):
            print(f"Trying to acquire lock for the task '{entry.task}'.", end="")
            try:
                record = flatten(connector.collection(lock_collection).find_one(query))
                # checking lock is acquired or not
                custom_wait_on_lock = self._get_custom_max_wait_on_lock_in_minutes(entry)
                if entry.task == "common.pull":
                    soft_time_limit = ALERT_EVENT_SOFT_TIME_LIMIT
                    if entry.args[0] == "webtx":
                        soft_time_limit = WEBTX_SOFT_TIME_LIMIT
                if (
                    record.get(lock_field) is not None
                    and (datetime.now() - record.get(lock_field)).seconds
                    < custom_wait_on_lock * 60
                ):
                    # Adding failed task to queue
                    if (
                        f"task.{lock_field_change}worker_id" in record
                        and record.get(f'task.{lock_field_change}worker_id') is not None
                    ):
                        node_health = connector.collection(Collections.NODE_HEALTH).find_one(
                            {"worker_id": record.get(f'task.{lock_field_change}worker_id')}
                        )
                        if (
                            node_health is not None
                            and "check_time" in node_health
                            and datetime.now().timestamp()
                            - node_health.get("check_time").timestamp()
                            > MAX_WORKER_WAIT_TIME
                            and OLD_LOCKING_MECHANISM != "true"
                        ):
                            print(
                                f"Creating new task as worker health is "
                                f"not updated for {MAX_WORKER_WAIT_TIME} seconds.", end=""
                            )
                            is_create_task = True
                    else:
                        print(f"The task '{entry.task}' is already in queue.", end="")
                else:
                    if entry.task == "cre.perform_action" and not (
                        (
                            settings.cre.startTime.strftime("%H:%M:%S")
                            < datetime.now(UTC).strftime("%H:%M:%S")
                            < settings.cre.endTime.strftime("%H:%M:%S")
                        )
                        or (
                            settings.cre.endTime.strftime("%H:%M:%S")
                            < settings.cre.startTime.strftime("%H:%M:%S")
                            and not (
                                settings.cre.endTime.strftime("%H:%M:%S")
                                < datetime.now(UTC).strftime("%H:%M:%S")
                                < settings.cre.startTime.strftime("%H:%M:%S")
                            )
                        )
                    ):
                        print("Not scheduled yet.", end="")
                    else:
                        is_create_task = True
                        print("Lock acquired; adding task to queue", end="")
            except AttributeError as ae:
                print(ae)
                print("Could not acquire lock", end="")
            except Exception as ex:
                print(repr(ex))
        else:
            if entry.task == "cre.perform_action" and not (
                (
                    settings.cre.startTime.strftime("%H:%M:%S")
                    < datetime.now(UTC).strftime("%H:%M:%S")
                    < settings.cre.endTime.strftime("%H:%M:%S")
                )
                or (
                    settings.cre.endTime.strftime("%H:%M:%S")
                    < settings.cre.startTime.strftime("%H:%M:%S")
                    and not (
                        settings.cre.endTime.strftime("%H:%M:%S")
                        < datetime.now(UTC).strftime("%H:%M:%S")
                        < settings.cre.startTime.strftime("%H:%M:%S")
                    )
                )
            ):
                print("Not scheduled yet.", end="")
            else:
                is_create_task = True
        if is_create_task:
            # add entry to the database
            uid = _add_entry(entry)
            raw_entry = ScheduleEntry(
                entry.name,
                entry.task,
                entry.last_run_at,
                entry.total_run_count,
                entry.schedule,
                entry.args,
                entry.kwargs | {"uid": uid},
                entry.options,
                app=entry.app,
            )
            queue_name = RABBITMQ_QUORUM_QUEUE_NAME.format(TASK_PRIORITIES.get(entry.task, 3))
            raw_entry.options = {
                "queue": Queue(
                    name=queue_name,
                    durable=True,
                    routing_key=queue_name,
                    queue_arguments={'x-queue-type': 'quorum'}
                ),
            }
            if soft_time_limit > 0:
                raw_entry.options["soft_time_limit"] = soft_time_limit
            retry_count = 3
            is_sent = False
            for _ in range(retry_count+1):
                try:
                    result = super().apply_async(raw_entry, producer=None, advance=False)
                    is_sent = True
                    break
                except SchedulingError as exc:
                    print(
                        f"Error occurred while sending task to RabbitMQ queue, "
                        f"{retry_count} {'retry' if retry_count <= 1 else 'retries'} remaining."
                        f" Message error: {exc}."
                    )
                    retry_count -= 1
                except Exception as exc:
                    print(f"Message error: {exc}")
                    break
            if is_sent:
                result_id = result.id
                print(f'{entry.task} sent. id-> {result_id}')
                if (
                    lock_collection is not None
                ):
                    connector.collection(lock_collection).update_one(
                        query,
                        {
                            "$set": {
                                f"{lock_field}": datetime.now(),
                                f"task.{lock_field_change}task_id": result_id,
                                f"task.{lock_field_change}startedAt": None,
                                f"task.{lock_field_change}worker_id": None,
                            }
                        },
                    )
            return


def is_enabled(name: str) -> bool:
    """Check if module is enabled based on task name.

    Args:
        name (str): Name of the task.

    Returns:
        bool: Whether the module is enabled or not.
    """
    try:
        name = name.split(".")
        if len(name) == 1 or name[0] == "common":
            return True
        prefix = name[0]
        connector = DBConnector()
        settings = connector.collection(Collections.SETTINGS).find_one(
            {f"platforms.{prefix}": True}
        )
        return settings is not None
    except Exception as ex:
        print(repr(ex))
        return True


def call_celery_task(func, *args, **kwargs):
    """Call celery task."""
    settings = SettingsDB(
        **connector.collection(Collections.SETTINGS).find_one({})
    )
    if settings.disk_alarm is False and check_disk_free_alarm():
        notifier.error(
            "Available disk space has reached below the minimum space required by CE. "
            "Free up some disk space to continue regular operations."
        )
        connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"disk_alarm": True}}
        )
    if settings.disk_alarm is True and not check_disk_free_alarm():
        connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"disk_alarm": False}}
        )
    status = TaskStatus(
        name=kwargs["name"],
        status=StatusType.INQUEUE,
        startedAt=datetime.now(),
        args="",  # TODO
    )
    kwargs.pop("name")
    result = connector.collection(Collections.TASK_STATUS).insert_one(
        status.model_dump()
    )
    uid = str(result.inserted_id)
    kwargs["uid"] = uid
    func(*args, **kwargs)


def _add_entry(entry):
    kwargs = deepcopy(entry.kwargs if entry.kwargs else {})
    pop_keys = [
        "lock_collection",
        "lock_field",
        "lock_unique_key",
        "uid",
        "priority",
        "configuration_name",
        "share_new_indicators",
    ]
    for key in pop_keys:
        if key in kwargs:
            kwargs.pop(key)
    status = TaskStatus(
        name=entry.task,
        status=StatusType.INQUEUE,
        startedAt=datetime.now(),
        args=f"({kwargs}, {', '.join(x for x in entry.args if x is not None)})",
    )
    result = connector.collection(Collections.TASK_STATUS).insert_one(
        status.model_dump()
    )
    return str(result.inserted_id)


def execute_celery_task(func, task=None, with_locks=False, **kwargs):
    """Retry celery task on Operational Error."""
    queue_name = RABBITMQ_QUORUM_QUEUE_NAME.format(TASK_PRIORITIES.get(task, 3))
    kwargs["queue"] = Queue(
        name=queue_name,
        durable=True,
        routing_key=queue_name,
        queue_arguments={'x-queue-type': 'quorum'}
    )
    kwargs["kwargs"] = {
        **(kwargs.get("kwargs") or {}),
        **(LOCKING_ARGS.get(task) if (with_locks and LOCKING_ARGS.get(task)) else {}),
    }
    return retry(
        times=float('inf'),
        exceptions=(OperationalError, amqp.exceptions.NotFound),
        sleep=10, log_level="debug", task_name=task
    )(func)(**kwargs)
