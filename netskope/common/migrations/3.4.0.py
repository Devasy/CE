"""Migrations for 3.4.0 release."""

from datetime import datetime, timedelta, UTC
from netskope.common.utils import DBConnector, Collections


if __name__ == "__main__":
    connector = DBConnector()
    for configuration in connector.collection(Collections.CONFIGURATIONS).find(
        {"plugin": "netskope.plugins.Netskope.netskope.main"}
    ):
        print(f"Updating CTE Netskope configuration {configuration['name']}.")
        connector.collection(Collections.CONFIGURATIONS).update_one(
            {"name": configuration["name"]},
            {
                "$set": {
                    "parameters.malware_type": ["MD5", "SHA256"],
                }
            },
        )

    connector.collection(Collections.SCHEDULES).update_one(
        {"name": "CRE SYNC INTERVAL ACTION"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "CRE SYNC INTERVAL ACTION",
                "enabled": True,
                "args": [],
                "task": "cre.perform_action",
                "interval": {
                    "every": 30,
                    "period": "seconds",
                },
            },
        },
        upsert=True,
    )
    connector.collection(Collections.SETTINGS).update_one(
        {},
        {
            "$set": {
                "cre.startTime": (
                    datetime.now(UTC) + timedelta(days=1)
                ).replace(hour=0, minute=0, second=0, microsecond=0),
                "cre.endTime": (datetime.now(UTC) + timedelta(days=1)).replace(
                    hour=23, minute=59, second=59, microsecond=0
                ),
            }
        },
    )
