"""Integrations tasks scheduler."""
from netskope.common.models import SettingsDB
from .db_connector import DBConnector, Collections

db_connector = DBConnector()


def schedule_or_delete_integrations_tasks(settings: SettingsDB):
    """Schedule integrations tasks."""
    if settings.platforms.get("itsm", False):
        db_connector.collection(Collections.SCHEDULES).update_one(
            {"task": "itsm.audit_requests"},
            {
                "$set": {
                    "_cls": "PeriodicTask",
                    "name": "INTERNAL AUDIT REQUESTS TO CREATE TICKETS",
                    "enabled": True,
                    "args": [],
                    "task": "itsm.audit_requests",
                    "interval": {
                        "every": 5,
                        "period": "minutes",
                    },
                }
            },
            upsert=True,
        )
    else:
        db_connector.collection(Collections.SCHEDULES).delete_one(
            {"task": "itsm.audit_requests"}
        )
