"""Contains logging related classes."""

import os
from datetime import datetime, timedelta
from enum import Enum
from celery.exceptions import SoftTimeLimitExceeded
from celery import current_task

from .singleton import Singleton
from .db_connector import DBConnector, Collections
from netskope.common.models.log import Log


class LogType(str, Enum):
    """Log type enumerations."""

    INFO = "info"
    ERROR = "error"
    WARNING = "warning"
    DEBUG = "debug"


class Logger(metaclass=Singleton):
    """Class used to log messages."""

    def __init__(self):
        """Initialize a new logger."""
        self._connector = DBConnector()
        self.update_level()
        self._last_updated_level = datetime.now()
        # TODO: use capped collection here
        # self._connector.db.create_collection(Collections.LOGS, {
        #     "capped": True,
        #     "max": 1000
        # })

    def _get_celery_task_id_details(self, details: str = None):
        """Get task id details."""
        try:
            if not details:
                details = ""
            pid_msg = "\nPID: " + str(os.getpid())
            task_id_msg = ""
            ce_task_uid = ""
            if hasattr(current_task, "request") and hasattr(current_task.request, "id") and current_task.request.id:
                task_id_msg = "\nTask ID: " + str(current_task.request.id)
            if os.environ.get("CE_TASK_UID", None):
                ce_task_uid = "\nTask UID: " + str(os.environ.get("CE_TASK_UID", None))
            details = (details + pid_msg + task_id_msg + ce_task_uid).strip()
        except Exception:  # NOSONAR
            pass
        return details

    def update_level(self, should_check_log_level: bool = False):
        """Refresh logLevel from the database."""
        if should_check_log_level and datetime.now() - self._last_updated_level > timedelta(minutes=1):
            settings = self._connector.collection(Collections.SETTINGS).find_one(
                {}
            )
            try:
                self._level = LogType(settings["logLevel"])
            except TypeError:
                self._level = LogType.INFO
            self._last_updated_level = datetime.now()
        elif not should_check_log_level:
            settings = self._connector.collection(Collections.SETTINGS).find_one(
                {}
            )
            try:
                self._level = LogType(settings["logLevel"])
            except TypeError:
                self._level = LogType.INFO

    def info(self, message: str, error_code: str = None, details: str = None, resolution: str = None):
        """Log an information message.

        Args:
            message (str): Message to be logged.
        """
        self.update_level(should_check_log_level=True)
        if self._level not in [LogType.INFO, LogType.DEBUG]:
            return
        details = self._get_celery_task_id_details(details)
        log = Log(
            message=message, type=LogType.INFO, createdAt=datetime.now(),
            errorCode=error_code, details=details, resolution=resolution
        )
        self._connector.collection(Collections.LOGS).insert_one(log.model_dump())

    def debug(self, message: str, error_code: str = None, details: str = None, resolution: str = None):
        """Log a debug message.

        Args:
            message (str): Message to be debugged.
        """
        self.update_level(should_check_log_level=True)
        if self._level not in [LogType.DEBUG]:
            return
        details = self._get_celery_task_id_details(details)
        log = Log(
            message=message, type=LogType.DEBUG, createdAt=datetime.now(),
            errorCode=error_code, details=details, resolution=resolution
        )
        self._connector.collection(Collections.LOGS).insert_one(log.model_dump())

    def warn(self, message: str, error_code: str = None, details: str = None, resolution: str = None):
        """Log a warning message.

        Args:
            message (str): Message to be logged.
        """
        self.update_level(should_check_log_level=True)
        if self._level not in [LogType.WARNING, LogType.INFO, LogType.DEBUG]:
            return
        details = self._get_celery_task_id_details(details)
        log = Log(
            message=message, type=LogType.WARNING, createdAt=datetime.now(),
            errorCode=error_code, details=details, resolution=resolution
        )
        self._connector.collection(Collections.LOGS).insert_one(log.model_dump())

    def error(self, message: str, error_code: str = None, details: str = None, resolution: str = None):
        """Log an error message.

        Args:
            message (str): Message to be logged.
        """
        self.update_level(should_check_log_level=True)
        if self._level not in [LogType.ERROR, LogType.WARNING, LogType.INFO, LogType.DEBUG]:
            return
        details = self._get_celery_task_id_details(details)
        log = Log(
            message=message,
            type=LogType.ERROR,
            createdAt=datetime.now(),
            errorCode=error_code,
            details=details,
            resolution=(
                resolution
                if resolution
                else "No resolution available. Please contact Netskope support for further assistance."
            )
        )
        self._connector.collection(Collections.LOGS).insert_one(log.model_dump())

        if details and "SoftTimeLimitExceeded" in details:
            raise SoftTimeLimitExceeded()
