"""Celery task decorator."""
import os
import uuid
import traceback
import requests
import socket

from datetime import datetime
from copy import deepcopy
from bson import ObjectId
from memory_profiler import memory_usage
from celery.exceptions import SoftTimeLimitExceeded

from netskope.common.models.other import StatusType
from . import DBConnector, Collections, Logger
from netskope.common.utils.requests_retry_mount import _patched_session_init
from .const import SOCKET_DEFAULT_TIMEOUT

try:
    MAX_WAIT_ON_LOCK_IN_MINUTES = int(
        os.environ.get("PLUGIN_TIMEOUT_MINUTES", 120)
    )
except ValueError:
    MAX_WAIT_ON_LOCK_IN_MINUTES = 120

CE_CONTAINER_ID = os.environ.get("CE_CONTAINER_ID", None)

try:
    timeout = int(os.environ.get("SOCKET_TIMEOUT", SOCKET_DEFAULT_TIMEOUT))
    if timeout < 1:
        timeout = SOCKET_DEFAULT_TIMEOUT
    SOCKET_DEFAULT_TIMEOUT = timeout
except Exception:
    pass


def integration(name):
    """Celery task decorator."""

    def decorator(func):
        def wrapper(*args, **argv):
            connector = DBConnector()
            settings = connector.collection(Collections.SETTINGS).find_one(
                {f"platforms.{name}": True}
            )
            if not settings:
                return {
                    "success": False,
                    "message": f"Module {name} is currently disabled.",
                }
            return func(*args, **argv)

        return wrapper

    return decorator


logger = Logger()


def log_mem(msg="", details=None):
    """Log memory function."""
    pid = os.getpid()
    rss = memory_usage(proc=pid, max_usage=True, backend="psutil", include_children=True, multiprocess=True)
    uss = memory_usage(proc=pid, max_usage=True, backend="psutil_uss", include_children=True, multiprocess=True)
    pss = memory_usage(proc=pid, max_usage=True, backend="psutil_pss", include_children=True, multiprocess=True)
    logger.debug(f"PID: {pid}, USS: {uss:.2f}, RSS: {rss:.2f}, PSS: {pss:.2f} {msg}", details=details)


# Collections storing a per-op lock sub-document (lockedAt.pull/.sync/...), mapped to
# the field each task must write. A message queued before the lock was split carries
# frozen kwargs naming the bare "lockedAt", which would $set over the whole
# sub-document and leave the configuration unmanageable. CLS/CRE/EDM/CFC use a scalar
# lock and are absent here. Inner keys accept a bare or dotted task name.
DICT_LOCK_FIELDS = {
    "configurations": {
        "execute_plugin": "lockedAt.pull",
        "share_indicators": "lockedAt.share",
    },
    "itsm_configurations": {
        "pull_data_items": "lockedAt.pull",
        "sync_states": "lockedAt.sync",
        "update_incidents": "lockedAt.update",
    },
}


def _reconcile_lock_field(lock_collection, lock_field, task_name):
    """Return the lock field for the current schema, ignoring a flattened one.

    Args:
        lock_collection (str): Collection holding the lock.
        lock_field (str): Lock field named by the schedule entry kwargs.
        task_name (str): Task function name, or the full Celery task name.
    """
    if not task_name or not isinstance(lock_field, str) or "." in lock_field:
        return lock_field
    expected = DICT_LOCK_FIELDS.get(lock_collection, {}).get(
        str(task_name).split(".")[-1]
    )
    if not expected:
        return lock_field
    logger.warn(
        f"Task {task_name} was queued with the outdated lock field "
        f"'{lock_field}'; using '{expected}' instead to avoid overwriting the "
        f"lock sub-document of {lock_collection}."
    )
    return expected


def get_lock_params(
    schedule_entry_args: list, schedule_entry_kwargs: dict, task_name: str = None
):
    """Get required fields from schedule entry.

    Args:
        schedule_entry (dict): Schedule entry
        task_name (str): Name of the decorated task function, used to ignore a
            flattened lock field carried by a stale queued message.
    """
    if (
        schedule_entry_kwargs is not None
        and "lock_collection" in schedule_entry_kwargs
        and "lock_unique_key" in schedule_entry_kwargs
        and "lock_field" in schedule_entry_kwargs
    ):
        lock_collection = schedule_entry_kwargs.get("lock_collection")
        unique_key = schedule_entry_kwargs.get("lock_unique_key")
        current_lock_field = _reconcile_lock_field(
            lock_collection, schedule_entry_kwargs.get("lock_field"), task_name
        )
        if not unique_key:
            query = {}
        else:
            query = {f"{unique_key}": schedule_entry_args[-1]}

        # modified lock field to store task id and startedAt field in lock_collection.
        raw_lock_field = current_lock_field.split(".")
        lock_field = f"{raw_lock_field[-1]}." if len(raw_lock_field) > 1 else ""
        return lock_collection, current_lock_field, query, lock_field
    return None, None, None, None


def _heal_flattened_lock_containers(connector, lock_collection, query, lock_field):
    """Rebuild lock containers that were flattened to a scalar.

    Mongo cannot create a subfield inside a scalar: ``$set`` of
    ``lockedAt.sync`` against ``{lockedAt: null}`` raises WriteError 28 and the
    task dies before its body runs. Replacing the scalar with an empty
    sub-document first makes the write succeed; healthy documents do not match,
    so this is a no-op for them.
    """
    if not query:
        # An unselective heal would rebuild an arbitrary document's containers.
        # Only itsm.audit_requests locks without a unique key, and its
        # settings.itsm container is always an object.
        return
    containers = ["task"]
    if "." in str(lock_field):
        containers.append(str(lock_field).split(".")[0])
    # Only an existing non-object blocks the write; an absent field is fine,
    # Mongo creates the whole path. One update per container, so a healthy
    # document matches nothing and no write is issued.
    for container in containers:
        try:
            connector.collection(lock_collection).update_one(
                {
                    **query,
                    container: {"$exists": True},
                    "$nor": [{container: {"$type": "object"}}],
                },
                {"$set": {container: {}}},
            )
        except Exception:
            logger.warn(
                f"Could not rebuild the '{container}' field of "
                f"{lock_collection} before writing the task lock.",
                details=traceback.format_exc(),
            )


def release_lock(args, argv, task_name=None):
    """Release lock."""
    connector = DBConnector()
    lock_collection, lock_field, query, lock_field_change = get_lock_params(
        args, argv, task_name
    )
    if (
        lock_collection is not None
    ):  # unlock after completion
        _heal_flattened_lock_containers(connector, lock_collection, query, lock_field)
        connector.collection(lock_collection).update_one(
            query,
            {
                "$set": {
                    f"{lock_field}": None,
                    f"task.{lock_field_change}startedAt": None,
                    f"task.{lock_field_change}worker_id": None,
                }
            },
        )


def track():
    """Celery locking task decorator."""

    def decorator(func):
        def wrapper(*args, **argv):
            requests.sessions.Session.__init__ = _patched_session_init
            socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)
            uid = str(uuid.uuid1())
            os.environ["CE_TASK_UID"] = uid
            log_mem(f"Method: {func.__name__}, UID: {uid}, Type: start")
            lock_collection, lock_field, query, lock_field_change = get_lock_params(
                args, argv, func.__name__
            )
            try:
                is_completed = False
                is_errored = False
                connector = DBConnector()
                if (
                    lock_collection is not None
                ):
                    _heal_flattened_lock_containers(
                        connector, lock_collection, query, lock_field
                    )
                    connector.collection(lock_collection).update_one(
                        query,
                        {
                            "$set": {
                                f"{lock_field}": datetime.now(),
                                f"task.{lock_field_change}startedAt": datetime.now(),
                                f"task.{lock_field_change}worker_id": CE_CONTAINER_ID,
                            }
                        },
                    )
                kwargs = deepcopy(argv)
                pop_keys = [
                    "lock_collection",
                    "lock_field",
                    "lock_unique_key",
                    "uid",
                    "priority"
                ]
                for key in pop_keys:
                    if key in kwargs:
                        kwargs.pop(key)
                if "uid" in argv:
                    connector.collection(Collections.TASK_STATUS).update_one(
                        {"_id": ObjectId(argv["uid"])},
                        {"$set": {"status": StatusType.INPROGRESS}},
                    )
                ret = func(*args, **kwargs)
                is_completed = True
                if "uid" in argv:
                    connector.collection(Collections.TASK_STATUS).update_one(
                        {"_id": ObjectId(argv["uid"])},
                        {
                            "$set": {
                                "status": StatusType.COMPLETED,
                                "completedAt": datetime.now(),
                            }
                        },
                    )
                log_mem(f"Method: {func.__name__}, UID: {uid}, Type: end")
                return ret
            except SoftTimeLimitExceeded:
                raise
            except Exception as ex:
                is_errored = True
                if "uid" in argv:
                    connector.collection(Collections.TASK_STATUS).update_one(
                        {"_id": ObjectId(argv["uid"])},
                        {
                            "$set": {
                                "status": StatusType.ERROR,
                                "completedAt": datetime.now(),
                            }
                        },
                    )
                log_mem(f"Method: {func.__name__}, UID: {uid}, Type: end")
                return {
                    "success": False,
                    "message": str(repr(ex)),
                    "trace": traceback.format_exc(),
                }
            finally:
                if is_errored or is_completed:
                    release_lock(args, argv, func.__name__)

        return wrapper

    return decorator
