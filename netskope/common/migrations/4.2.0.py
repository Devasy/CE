"""Migrations for 4.2.0 release."""
from pymongo import DESCENDING

from netskope.common.utils import (Collections, DBConnector, Logger,
                                   RepoManager, get_lock_params)

manager = RepoManager()
connector = DBConnector()
logger = Logger()


def create_indexes_on_collections():
    """Create indexes on collections."""
    connector.collection(Collections.ITSM_ALERTS).create_index(
        [("timestamp", DESCENDING)],
    )
    connector.collection(Collections.ITSM_TASKS).create_index(
        [("timestamp", DESCENDING)],
    )
    connector.collection(Collections.LOGS).create_index(
        [("createdAt", DESCENDING)],
    )


if __name__ == "__main__":
    connector = DBConnector()
    manager.load()
    for repo in manager.repos:
        connector.collection(Collections.PLUGIN_REPOS).update_one(
            {"name": repo.name},
            {"$set": {"hasUpdate": True if manager.get_diff(repo) else False}}
        )

    connector.collection(Collections.SCHEDULES).update_many(
        {}, {"$unset": {"queue": ""}}
    )

    connector.collection(Collections.CLS_BUSINESS_RULES).update_one(
        {"name": "All"},
        {
            "$set": {
                "filters": {
                    "query": 'alert_type IN ("anomaly", "Compromised Credential", "policy", "Legal Hold", "malsite", "Malware", "DLP", "Security Assessment", "watchlist", "quarantine", "Remediation", "uba", "ctep") || event_type IN ("page", "application", "audit", "infrastructure", "network", "incident")',  # noqa
                    "mongo": '{"$or":[{"alert_type":{"$in":["anomaly","Compromised Credential","policy","Legal Hold","malsite","Malware","DLP","Security Assessment","watchlist","quarantine","Remediation","uba", "ctep"]}},{"event_type":{"$in":["page","application","audit","infrastructure","network", "incident"]}}]}',  # noqa
                },
            }
        },
    )
    connector.collection(Collections.NETSKOPE_TENANTS).update_many(
        {}, {"$set": {"forbidden_endpoints": {}}}
    )
    connector.collection(Collections.NOTIFICATIONS).delete_one({"id": "BANNER_ERROR_1002"})
    for custom_schedule in connector.collection(Collections.SCHEDULES).find({}):
        lock_collection, lock_field, query, lock_field_change = get_lock_params(
            custom_schedule.get("args"),
            custom_schedule.get("kwargs", None)
        )
        if (
            lock_collection is not None
        ):
            # clear old lock mechanisms.
            connector.collection(lock_collection).update_one(
                query,
                {
                    "$set": {
                        f"{lock_field}": None,
                        f"task.{lock_field_change}startedAt": None,
                        f'task.{lock_field_change}task_id': None,
                        f'task.{lock_field_change}worker_id': None,
                    }
                }
            )
    create_indexes_on_collections()
    # Add CTEP
    connector.collection(Collections.NETSKOPE_TENANTS).update_many(
        {},
        {"$set": {"first_alert_pull.first_ctep_pull": True}},
    )
