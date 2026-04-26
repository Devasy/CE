"""Migrations for 5.1.1-dlp-beta-1 release."""
from netskope.common.utils import DBConnector, Collections


connector = DBConnector()

if __name__ == "__main__":
    read = [
        "edm_read",
        "cfc_read"
    ]
    write = [
        "edm_write",
        "cfc_write"
    ]
    admin = connector.collection(Collections.USERS).find_one({"username": "admin"})
    if admin:
        scopes = admin.get("scopes", [])
        for scope in read+write:
            if scope not in scopes:
                scopes.append(scope)
        connector.collection(Collections.USERS).update_one(
            {"username": "admin"}, {"$set": {"scopes": scopes}}
        )
    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "edm.age_edm_hashes"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "INTERNAL EDM Hashes AGING TASK",
                "enabled": True,
                "args": [],
                "interval": {
                    "every": 1,
                    "period": "hours",
                },
            }
        },
        upsert=True,
    )
    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "edm.poll_edm_hash_upload_status"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "INTERNAL EDM Hashes POLL TASK",
                "enabled": True,
                "args": [],
                "interval": {
                    "every": 1,
                    "period": "hours",
                },
            }
        },
        upsert=True,
    )
    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "cfc.unmute_business_rules"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "CFC INTERNAL UNMUTE BUSINESS RULE TASK",
                "enabled": True,
                "args": [],
                "task": "cfc.unmute_business_rules",
                "interval": {
                    "every": 5,
                    "period": "minutes",
                },
            }
        },
        upsert=True,
    )
    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "cfc.age_cfc_image_metadata"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "INTERNAL CFC IMAGE METADATA AGING TASK",
                "enabled": True,
                "args": [],
                "task": "cfc.age_cfc_image_metadata",
                "interval": {
                    "every": 12,
                    "period": "hours",
                }
            }
        },
        upsert=True
    )
