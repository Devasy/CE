"""Migrations for 4.1.0 release."""
from netskope.common.utils import DBConnector, Collections, resolve_secret, delete_duplicate_indicators
from netskope.common.migrations import iterator_migration
from pymongo import ASCENDING
import datetime
from netskope.common.utils.proxy import get_proxy_params
from netskope.common.models.settings import SettingsDB
from netskope_api.iterator.netskope_iterator import NetskopeIterator
from netskope_api.iterator.const import Const
from netskope.common.api import __version__


ALERT = "alert"
RESULT = "result"
TIMESTAMP = "timestamp"
TIMESTAMP_HWM = "timestamp_hwm"
connector = DBConnector()


def get_proxy() -> dict:
    """Get proxy dict."""
    settings = connector.collection(Collections.SETTINGS).find_one({})
    return get_proxy_params(SettingsDB(**settings))


def set_alerts_checkpoint():
    """Set the checkpoints for alert types to pull."""
    for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find(
        {"is_iterator": True}
    ):
        try:
            tenant_name = tenant.get("name").replace(" ", "")
            params = {
                Const.NSKP_TOKEN: resolve_secret(tenant.get("v2token")),
                Const.NSKP_TENANT_HOSTNAME: f"{tenant.get('tenantName')}.goskope.com",
                Const.NSKP_ITERATOR_NAME: f"{tenant_name}_{ALERT}",
                Const.NSKP_USER_AGENT: f"netskope-ce-{__version__}",
                Const.NSKP_EVENT_TYPE: ALERT,
            }
            iterator_pull = NetskopeIterator(params)
            response = iterator_pull.next()
            response = response.json()
            new_alerts = response.get(RESULT, [])
            if new_alerts:  # use first alert data to get the initial timestamp.
                timestamp = new_alerts[0].get("timestamp")
            else:  # use timestamp_hwm field if no data found.
                timestamp = response.get("timestamp_hwm")
            timestamp = datetime.datetime.fromtimestamp(timestamp)
            # set the checkpoint to start pulling after migration.
            connector.collection(Collections.NETSKOPE_TENANTS).update_one(
                {"name": tenant.get("name")},
                {"$set": {"checkpoint.alert": timestamp}},
                upsert=True,
            )
        except Exception as ex:
            print("Error occurred while pulling alerts.")
            print(ex)
            exit(1)


if __name__ == "__main__":
    connector.collection(Collections.NOTIFICATIONS).create_index(
        "id",
        unique=True,
        partialFilterExpression={"id": {"$type": 2}},  # 2=BSON type int for string
    )

    connector.collection(Collections.NOTIFICATIONS).delete_many(
        {"type": {"$in": ["critical", "critical_core"]}}
    )

    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "common.heartbeat"},
        {
            "$set": {
                "_cls": "PeriodicTask",
                "name": "HEARTBEAT LOGS",
                "enabled": True,
                "args": [],
                "task": "common.heartbeat",
                "interval": {
                    "every": 5,
                    "period": "minutes",
                },
            }
        },
        upsert=True,
    )
    connector.collection(Collections.ITSM_CONFIGURATIONS).update_many(
        {"plugin": "netskope.plugins.Netskope.netskope_itsm.main"},
        {
            "$set": {
                "filters": {
                    "query": 'alertType IN ("anomaly", "Compromised Credential", "policy", "Legal Hold", "malsite", "Malware", "DLP", "Security Assessment", "watchlist", "quarantine", "Remediation", "uba")',  # noqa
                    "mongo": '{"alertType":{"$in":["anomaly","Compromised Credential","policy","Legal Hold","malsite","Malware","DLP","Security Assessment","watchlist","quarantine","Remediation","uba"]}}',  # noqa
                }
            }
        },
    )

    delete_duplicate_indicators()
    connector.collection(Collections.INDICATORS).create_index(
        [("value", ASCENDING)],
        unique=True,
    )
    connector.collection(Collections.ITSM_ALERTS).create_index(
        [("id", ASCENDING)],
        unique=True,
    )
    connector.collection(Collections.ITSM_TASKS).create_index(
        [("id", ASCENDING)],
        unique=True,
    )
    read = [
        "cte_read",
        "cto_read",
        "cls_read",
        "cre_read",
        "settings_read",
        "logs",
    ]
    write = [
        "cte_write",
        "cto_write",
        "cre_write",
        "cls_write",
        "settings_write",
    ]
    for user in connector.collection(Collections.USERS).find({}):
        scopes = []
        if {*read, *write} & set(user.get("scopes", [])):
            continue
        if "admin" in user.get("scopes", []):
            scopes.append("admin")
        if "api" in user.get("scopes", []):
            scopes.append("api")
        if "me" in user.get("scopes", []):
            scopes.append("me")
        if "read" in user.get("scopes", []):
            scopes.extend(read)
        if "write" in user.get("scopes", []):
            scopes.extend(write)

        connector.collection(Collections.USERS).update_one(
            {"username": user["username"]}, {"$set": {"scopes": scopes}}
        )

    connector.collection(Collections.CLS_BUSINESS_RULES).update_one(
        {"name": "All"},
        {
            "$set": {
                "filters": {
                    "query": 'alert_type IN ("anomaly", "Compromised Credential", "policy", "Legal Hold", "malsite", "Malware", "DLP", "Security Assessment", "watchlist", "quarantine", "Remediation", "uba") || event_type IN ("page", "application", "audit", "infrastructure", "network", "incident")',  # noqa
                    "mongo": '{"$or":[{"alert_type":{"$in":["anomaly","Compromised Credential","policy","Legal Hold","malsite","Malware","DLP","Security Assessment","watchlist","quarantine","Remediation","uba"]}},{"event_type":{"$in":["page","application","audit","infrastructure","network","incident"]}}]}',  # noqa
                },
            }
        },
    )

    connector.collection(Collections.NOTIFICATIONS).update_one(
        {"id": "BANNER_INFO_1001"},
        {
            "$set": {
                "id": "BANNER_INFO_1001",
                "message": "Netskope security partner SecurityScorecard is offering a promotion for users interested in trying out SSC and Netskope. Click [here](https://securityscorecard.com/netskope) for more information.",  # noqa
                "type": "banner_info",
                "acknowledged": False,
                "createdAt": datetime.datetime.now(),
            },
        },
        upsert=True,
    )

    set_alerts_checkpoint()

    for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find({}):
        tenant_alert = True
        if type(tenant_alert) is not dict:
            connector.collection(Collections.NETSKOPE_TENANTS).update_one(
                {"name": tenant["name"]},
                {
                    "$unset": {
                        "first_alert_pull": True,
                        "first_infrastructure_pull": True,
                        "first_page_pull": True,
                        "first_network_pull": True,
                        "first_audit_pull": True,
                        "first_application_pull": True,
                        "is_iterator": False,
                    },
                },
                upsert=True,
            )
            connector.collection(Collections.NETSKOPE_TENANTS).update_one(
                {"name": tenant["name"]},
                {
                    "$set": {
                        "first_alert_pull": {
                            "first_Compromised Credential_pull": tenant_alert,
                            "first_malsite_pull": tenant_alert,
                            "first_Malware_pull": tenant_alert,
                            "first_DLP_pull": tenant_alert,
                            "first_Security Assessment_pull": tenant_alert,
                            "first_watchlist_pull": tenant_alert,
                            "first_quarantine_pull": tenant_alert,
                            "first_Remediation_pull": tenant_alert,
                            "first_uba_pull": tenant_alert,
                            "first_policy_pull": tenant_alert,
                        },
                        "first_event_pull": {
                            "first_infrastructure_pull": tenant.get(
                                "first_infrastructure_pull"
                            ),
                            "first_page_pull": tenant.get("first_page_pull"),
                            "first_network_pull": tenant.get("first_network_pull"),
                            "first_audit_pull": tenant.get("first_audit_pull"),
                            "first_application_pull": tenant.get(
                                "first_application_pull"
                            ),
                            "first_incident_pull": True,
                        },
                        "is_checkpoint_used_incident": False,
                        "alert_types": [
                            "Compromised Credential",
                            "policy",
                            "malsite",
                            "Malware",
                            "DLP",
                            "Security Assessment",
                            "watchlist",
                            "quarantine",
                            "Remediation",
                            "uba",
                        ],
                    },
                },
                upsert=True,
            )
    for tenant_schedules in connector.collection(Collections.SCHEDULES).find(
        {"task": {"$in": ["common.pull_alerts", "common.pull_events"]}}
    ):
        connector.collection(Collections.SCHEDULES).update_one(
            {"name": tenant_schedules.get("name")},
            {"$set": {"queue": tenant_schedules.get("args")[0].replace(" ", "_")}},
            upsert=True,
        )
    iterator_migration()
