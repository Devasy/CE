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
from netskope.common.utils.requests_retry_mount import override_session_init
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


def get_lock_params(schedule_entry_args: list, schedule_entry_kwargs: dict):
    """Get required fields from schedule entry.

    Args:
        schedule_entry (dict): Schedule entry
    """
    if (
        schedule_entry_kwargs is not None
        and "lock_collection" in schedule_entry_kwargs
        and "lock_unique_key" in schedule_entry_kwargs
        and "lock_field" in schedule_entry_kwargs
    ):
        lock_collection = schedule_entry_kwargs.get("lock_collection")
        unique_key = schedule_entry_kwargs.get("lock_unique_key")
        current_lock_field = schedule_entry_kwargs.get("lock_field")
        if not unique_key:
            query = {}
        else:
            query = {f"{unique_key}": schedule_entry_args[-1]}

        # modified lock field to store task id and startedAt field in lock_collection.
        raw_lock_field = current_lock_field.split(".")
        lock_field = f"{raw_lock_field[-1]}." if len(raw_lock_field) > 1 else ""
        return lock_collection, current_lock_field, query, lock_field
    return None, None, None, None


def release_lock(args, argv):
    """Release lock."""
    connector = DBConnector()
    lock_collection, lock_field, query, lock_field_change = get_lock_params(args, argv)
    if (
        lock_collection is not None
    ):  # unlock after completion
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
            requests.sessions.Session.__init__.__code__ = override_session_init.__code__
            socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)
            uid = str(uuid.uuid1())
            os.environ["CE_TASK_UID"] = uid
            log_mem(f"Method: {func.__name__}, UID: {uid}, Type: start")
            lock_collection, lock_field, query, lock_field_change = get_lock_params(args, argv)
            try:
                is_completed = False
                is_errored = False
                connector = DBConnector()
                if (
                    lock_collection is not None
                ):
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
                    release_lock(args, argv)

        return wrapper

    return decorator
