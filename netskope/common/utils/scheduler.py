"""Class to help with scheduling."""
from typing import List

from .singleton import Singleton
from .db_connector import DBConnector, Collections
from ..models.other import PollIntervalUnit


LOCKING_ARGS = {
    "common.pull": {
        "lock_collection": Collections.NETSKOPE_TENANTS,
        "lock_unique_key": "name",
    },
    "cre.fetch_records": {
        "lock_collection": Collections.CREV2_CONFIGURATIONS,
        "lock_unique_key": "name",
        "lock_field": "lockedAt",
    },
    "cre.update_records": {
        "lock_collection": Collections.CREV2_CONFIGURATIONS,
        "lock_unique_key": "name",
        "lock_field": "lockedAt",
    },
    "cte.execute_plugin": {
        "lock_collection": Collections.CONFIGURATIONS,
        "lock_unique_key": "name",
        "lock_field": "lockedAt.pull",
    },
    "cte.share_indicators": {
        "lock_collection": Collections.CONFIGURATIONS,
        "lock_unique_key": "name",
        "lock_field": "lockedAt.share",
    },
    "itsm.pull_data_items": {
        "lock_collection": Collections.ITSM_CONFIGURATIONS,
        "lock_unique_key": "name",
        "lock_field": "lockedAt",
    },
    "edm.execute_plugin": {
        "lock_collection": Collections.EDM_CONFIGURATIONS,
        "lock_unique_key": "name",
        "lock_field": "lockedAt",
    },
    "itsm.audit_requests": {
        "lock_collection": Collections.SETTINGS,
        "lock_unique_key": None,
        "lock_field": "itsm.lockedAuditRequestsAt",
    },
    "cfc.execute_plugin": {
        "lock_collection": Collections.CFC_CONFIGURATIONS,
        "lock_unique_key": "name",
        "lock_field": "lockedAt",
    },
    "cls.pull": {
        "lock_collection": Collections.CLS_CONFIGURATIONS,
        "lock_unique_key": "name",
        "lock_field": "lockedAt",
    },
    "itsm.sync_states": {
        "lock_collection": Collections.ITSM_CONFIGURATIONS,
        "lock_unique_key": "name",
        "lock_field": "lockedAt",
    },
    "itsm.update_incidents": {
        "lock_collection": Collections.ITSM_CONFIGURATIONS,
        "lock_unique_key": "name",
        "lock_field": "lockedAt",
    },
}


class Scheduler(metaclass=Singleton):
    """Scheduler singleton class."""

    def __init__(self):
        """Initialize scheduler."""
        self.connector = DBConnector()

    def schedule(
        self,
        name: str,
        task_name: str,
        poll_interval: int,
        poll_interval_unit: PollIntervalUnit,
        args: List = [],
        queue: str = None,
        kwargs: dict = {},
    ) -> bool:
        """Schedule a task to run at specified poll interval.

        Args:
            task_name (str): Name of the celery task.
            poll_interval (int): Poll interval.
            poll_interval_unit (PollIntervalUnit): Unit of the poll interval.
            args (List): Parameters to pass to the task.
        """
        if task_name == "common.pull":
            # Is it shared between processes?
            lock_at_field = f"lockedAt.{name.split('.')[-1]}"
            LOCKING_ARGS[task_name]["lock_field"] = lock_at_field

        self.connector.collection(Collections.SCHEDULES).insert_one(
            {
                "_cls": "PeriodicTask",
                "name": name,
                "enabled": True,
                "locked_on": None,
                "args": args,
                "kwargs": {**kwargs, **LOCKING_ARGS.get(task_name, {})},
                "task": task_name,
                "interval": {
                    "every": poll_interval,
                    "period": poll_interval_unit,
                },
                "queue": queue,
            }
        )

    def upsert(
        self,
        name: str,
        task_name: str,
        poll_interval: int,
        poll_interval_unit: PollIntervalUnit,
        args: List = [],
        queue: str = None,
        kwargs: dict = {}
    ):
        """Update an existing schedule.

        Args:
            name (str): Name of the schedule to update.
            poll_interval (int, optional): Poll interval. Defaults to None.
            poll_interval_unit (PollIntervalUnit, optional): Unit of poll interval. Defaults to None.
        """
        if task_name == "common.pull":
            lock_at_field = f"lockedAt.{name.split('.')[-1]}"
            LOCKING_ARGS[task_name]["lock_field"] = lock_at_field

        self.connector.collection(Collections.SCHEDULES).update_one(
            {"name": name},
            {
                "$set": {
                    "_cls": "PeriodicTask",
                    "name": name,
                    "enabled": True,
                    "locked_on": None,
                    "args": args,
                    "kwargs": {**LOCKING_ARGS.get(task_name, {}), **kwargs},
                    "task": task_name,
                    "interval": {
                        "every": poll_interval,
                        "period": poll_interval_unit,
                    },
                    "queue": queue,
                },
            },
            upsert=True,
        )

    def delete(self, name: str):
        """Delete a scheduled task.

        Args:
            name (str): Task name.
        """
        self.connector.collection(Collections.SCHEDULES).delete_one({"name": name})
