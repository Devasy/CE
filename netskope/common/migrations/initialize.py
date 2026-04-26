"""Database initialization script."""
import uuid
from netskope.common.models import SettingsDB
from netskope.common.utils import DBConnector, Collections, Scheduler

scheduler = Scheduler()

if __name__ == "__main__":
    connector = DBConnector()
    #     if (db.getUser('cteadmin') == null) {
    #     db.createUser({
    #         user: "cteadmin",
    #         pwd: "cteadmin",
    #         roles: [{
    #             role: "readWrite",
    #             db: "cte"
    #         }]
    #     });
    # }
    # connector.collection(Collections.INDICATORS).create_index()
    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "cte.age_indicators"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "INTERNAL INDICATOR AGING TASK",
                "enabled": True,
                "args": [],
                "task": "cte.age_indicators",
                "interval": {
                    "every": 12,
                    "period": "hours",
                },
            }
        },
        upsert=True,
    )
    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "itsm.unmute"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "INTERNAL UNMUTE TASK",
                "enabled": True,
                "args": [],
                "task": "itsm.unmute",
                "interval": {
                    "every": 5,
                    "period": "minutes",
                },
            }
        },
        upsert=True,
    )
    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "itsm.data_cleanup"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "INTERNAL ALERT CLEANUP TASK",
                "enabled": True,
                "args": [],
                "task": "itsm.data_cleanup",
                "interval": {
                    "every": 12,
                    "period": "hours",
                },
            }
        },
        upsert=True,
    )
    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "common.check_updates"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "INTERNAL UPDATE TASK",
                "enabled": True,
                "args": [],
                "task": "common.check_updates",
                "interval": {
                    "every": 12,
                    "period": "hours",
                },
            }
        },
        upsert=True,
    )
    scheduler.upsert(
        name="INTERNAL AUDIT REQUESTS TO CREATE TICKETS",
        task_name="itsm.audit_requests",
        poll_interval=5,
        poll_interval_unit="minutes",
        args=[],
    )
    setting = connector.collection(Collections.SETTINGS).find_one({})
    if not setting:
        connector.collection(Collections.SETTINGS).insert_one(
            {
                "proxy": {
                    "scheme": "http",
                    "server": "",
                    "username": "",
                    "password": "",
                },
                "logLevel": "info",
                "databaseVersion": "2.9.9",
                "alertCleanup": 7,
                "eventCleanup": 7,
                "ticketsCleanupMongo": '{"status": {"$in": ["Closed"]}}',
                "ticketsCleanupQuery": 'status IN ("Closed")',
                "platforms": {"cte": False, "itsm": False, "edm": False, "cfc": False},
            }
        )
        setting = connector.collection(Collections.SETTINGS).find_one({})

    settings = SettingsDB(**setting)
    if settings.uid is None:
        # generate installation id
        print("Generating installation id")
        uid = str(uuid.uuid4().hex)
        connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"uid": uid}}
        )

    admin = connector.collection(Collections.USERS).find_one({"username": "admin"})
    if not admin:
        connector.collection(Collections.USERS).insert_one(
            {
                "username": "admin",
                "password": "$2y$12$RBcV6xWFhHucm4a1YRmQXuEZHqz9NadpMuzIB6xEIXOhg.QzngiiO",  # NOSONAR
                "scopes": ["admin", "read", "write", "me", "api"],
                "tokens": [],
                "firstLogin": True,
            }
        )
    # db.indicators.createIndex({ reputation: -1 })
    # db.indicators.createIndex({ externalHits: -1 })
    # db.indicators.createIndex({ lastSeen: -1 })
