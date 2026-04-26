"""Migrations for 4.0.0 release."""

import subprocess
import json
import os
import re
import sys
import uuid
from pymongo import ASCENDING, InsertOne
from netskope.common.models.other import NetskopeField, NetskopeFieldType, FieldDataType
from netskope.common.utils import DBConnector, Collections
from netskope.integrations.cte.models.business_rule import BusinessRuleDB
from netskope.integrations.cte.models.indicator import (
    IndicatorDB,
    IndicatorSourceDB,
)


FIELDS = [
    "id",
    "appcategory",
    "user",
    "type",
    "configuration",
    "srcip",
    "dstip",
    "app",
    "site",
    "nsdeviceuid",
    "activity",
    "ccl",
    "cci",
    "managementID",
    "url",
    "event_type",
    "shared_credential_user",
    "userkey",
    "breach_media_reference",
    "email_source",
    "breach_score",
    "breach_date",
    "matched_username",
    "hostname",
    "device",
    "os",
    "browser",
    "access_method",
    "device_classification",
    "mime_type",
    "policy",
    "md5",
    "sha256",
    "object",
    "filesize",
    "instance_id",
    "dlp_incident_id",
    "dlp_file",
    "act_user",
    "dlp_rule",
    "dlp_rule_count",
    "dlp_profile",
    "lh_custodian_email",
    "lh_custodian_name",
    "lh_dest_app",
    "lh_shared",
    "instance",
    "file_path",
    "lh_dest_instance",
    "lh_original_filename",
    "legal_hold_profile_name",
    "modified",
    "referer",
    "action",
    "malsite_id",
    "malicious",
    "app_session_id",
    "malsite_category",
    "page",
    "threat_match_field",
    "local_md5",
    "local_sha256",
    "dtection_type",
    "malware_id",
    "ns_detection_name",
    "detection_engine",
    "malware_scanner_result",
    "malware_type",
    "malware_profile",
    "q_original_filename",
    "q_app",
    "quarantine_profile",
    "transaction_id",
    "quarantine_file_name",
    "q_admin",
    "q_instance",
    "file_type",
    "sa_profile_name",
    "sa_rule_name",
    "account_id",
    "iaas_asset_tags",
    "sa_rule_remediation",
    "asset_object_id",
    "sa_rule_severity",
    "severity",
    "risk_level_id",
    "dlp_rule_severity",
    "message",
    "justification_type",
    "justification_reason",
    "aggregateScore",
    "aggregateScoreRange",
    "alert_type",
    "server_packets",
    "tunnel_id",
    "session_duration",
    "supporting_data",
    "page_id",
    "alarm_description",
    "srcport",
    "protocol",
    "policy_name",
    "end_time",
    "page_endtime",
    "md5_list",
    "uba_inst1",
    "serial",
    "server_bytes",
    "detection_type",
    "timestamp",
    "risk_level",
    "audit_log_event",
    "appcategory",
    "tunnel_type",
    "domain",
    "client_packets",
    "alert_name",
    "breach_media_references",
    "alarm_name",
    "dstport",
    "page_starttime",
    "start_time",
    "network_session_id",
    "severity_level",
    "client_bytes",
    "os_version",
    "uba_inst2",
    "device_name",
    "metric_value",
    "uba_ap2",
    "uba_ap1",
    "tunnel_up_time",
    "policy_actions",
    "file_size",
    "traffic_type",
]

IOC_SOURCE_DATA = {
    "reputation",
    "source",
    "comments",
    "extendedInformation",
    "firstSeen",
    "lastSeen",
    "severity",
    "tags",
}

RECURESE = {"$and", "$or"}


def migrate_mongo_query(input):
    """Migrate mongo query."""
    if isinstance(input, str):
        input = json.loads(input)
    output = {}

    for key, val in input.items():
        if key in ["$and", "$or"]:
            output[key] = []
            for sub_query in val:
                output[key].append(migrate_mongo_query(sub_query))
        elif key in IOC_SOURCE_DATA:
            if output.get("sources", None) is not None:
                output["sources"]["$elemMatch"][key] = val
            else:
                output["sources"] = {"$elemMatch": {key: val}}
        else:
            output[key] = val

    return output


def generate_token(pattern, token1=[]):
    """Generate token."""
    while True:
        token = str(uuid.uuid4())
        if token not in pattern and token not in token1:
            return token


def migrate_filter_query(input):
    """Migrate filter query."""
    regex = (
        "(?P<operand1>[sources.]*internalHits|[sources.]*firstSeen|[sources.]*severity"
        "|[sources.]*comments|[sources.]*lastSeen|[sources.]*externalHits"
        "|[sources.]*reputation|[sources.]*extendedInformation|[sources.]*source|[sources.]*tags"
        "|type|value|safe|sources|active|sharedWith|test|expiresAt) "
        "(?P<operator>Like|Is equal|=|!=|<|<=|>|>=|==|IN|NOT IN) "
        '(?P<operand2>(?:\\w+|".*?")|(?:\\([^\\)]+\\)))'  # nosonar
    )

    token1 = generate_token(input)
    token2 = generate_token(input, [token1])
    token3 = generate_token(input, [token1, token2])
    token4 = generate_token(input, [token1, token2, token3])

    input = input.replace("\\\\", token1)
    input = input.replace('\\"', token2)
    input = input.replace("\\(", token3)
    input = input.replace("\\)", token4)
    mat = re.finditer(regex, input)

    indexes = []
    for i in mat:
        if i.group(1) in IOC_SOURCE_DATA and not i.group(1).startswith(
            "sources."
        ):
            indexes.append(i.regs[1][0])

    for idx in indexes[::-1]:
        input = input[:idx] + "sources." + input[idx:]

    input = input.replace(token4, "\\)")
    input = input.replace(token3, "\\(")
    input = input.replace(token2, '\\"')
    input = input.replace(token1, "\\\\")

    return input


def migrate_indicator():
    """Migrate indicators to latest model."""
    try:
        connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"migration_status": False}}
        )

        if "indicators_old" in connector.database.list_collection_names():
            indicators = connector.collection("indicators_old").find()
        else:
            indicators = connector.collection(Collections.INDICATORS).find()

        indicators = list(indicators)

        if (
            len(indicators) > 0
            and "indicators_old"
            not in connector.database.list_collection_names()
        ):
            connector.collection("indicators").rename("indicators_old")

        for i in range(0, len(indicators), 1000):
            connector.collection(Collections.INDICATORS).bulk_write(
                requests=[
                    InsertOne(
                        document={
                            **(IndicatorDB(**indicator).model_dump()),
                            "sources": [IndicatorSourceDB(**indicator).model_dump()],
                        },
                    )
                    for indicator in indicators[i: i + 1000]
                ]
            )

        connector.collection("indicators_old").drop()
        print("Completed migration script for the Indicator migrations...")
    except Exception as e:
        print("Error occured while migrating IOCs: ", e)
    finally:
        connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"migration_status": True}}
        )


if __name__ == "__main__":
    n = len(sys.argv)

    if n > 1 and sys.argv[1] == "migrate_iocs":
        connector = DBConnector()
        migrate_indicator()
    else:
        connector = DBConnector()
        connector.collection(Collections.CRE_USERS).create_index(
            [("uid", ASCENDING), ("type", ASCENDING)], unique=True
        )
        script_path = os.path.join(os.path.dirname(__file__), "4.0.0.py")
        print("Started migration script for the Indicator migrations...")
        proc = subprocess.Popen(
            f"python {script_path} migrate_iocs > /dev/null 2>&1 &".split()
        )

        # fields to be added in database
        for field in FIELDS:
            connector.collection(Collections.NETSKOPE_FIELDS).update_one(
                {"name": field},
                {
                    "$set": NetskopeField(
                        name=field,
                        label=field.replace("_", " ").title().strip(),
                        type=NetskopeFieldType.ALERT,
                        dataType=FieldDataType.TEXT,
                    ).model_dump()
                },
                upsert=True,
            )

        try:
            rules = connector.collection(Collections.CTE_BUSINESS_RULES).find()

            for rule in rules:
                rule["filters"]["mongo"] = json.dumps(
                    migrate_mongo_query(rule["filters"]["mongo"])
                )
                rule["filters"]["query"] = migrate_filter_query(
                    rule["filters"]["query"]
                )

                exceptions = rule["exceptions"]
                updated_exceptions = []

                for exception in exceptions:
                    if exception.get("filters", None) is not None:
                        exception["filters"]["mongo"] = json.dumps(
                            migrate_mongo_query(exception["filters"]["mongo"])
                        )
                        exception["filters"]["query"] = migrate_filter_query(
                            exception["filters"]["query"]
                        )

                    updated_exceptions.append(exception)

                rule["exceptions"] = updated_exceptions

                b_rule = BusinessRuleDB(**rule)

                connector.collection(
                    Collections.CTE_BUSINESS_RULES
                ).update_one(
                    {"name": b_rule.name},
                    {"$set": b_rule.model_dump()},
                )
        except Exception as e:
            print(e)
            raise

        connector.collection(Collections.GRC_APPLICATIONS).create_index(
            [("applicationName", ASCENDING), ("source", ASCENDING)],
            unique=True,
        )

        connector.collection(Collections.SCHEDULES).update_one(
            {"task": "grc.unmute"},
            {
                "$set": {
                    "_cls": "PeriodicTask",
                    "name": "GRC INTERNAL UNMUTE TASK",
                    "enabled": True,
                    "args": [],
                    "task": "grc.unmute",
                    "interval": {
                        "every": 5,
                        "period": "minutes",
                    },
                }
            },
            upsert=True,
        )
        connector.collection(Collections.GRC_BUSINESS_RULES).update_one(
            {"name": "All"},
            {
                "$set": {
                    "name": "All",
                    "filters": {
                        "query": 'ccl IN ("excellent","high","medium","low","poor")',
                        "mongo": '{"ccl":{"$in":["poor","low","medium","high","excellent"]}}',
                    },
                    "exceptions": [],
                    "muted": False,
                    "unmuteAt": None,
                    "sharedWith": {},
                    "isDefault": True,
                }
            },
            upsert=True,
        )
        settings = connector.collection(Collections.SETTINGS).find_one({})
        platforms = settings.get("platforms")
        platforms["grc"] = False
        connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"platforms": platforms}}
        )

        for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find(
            {}
        ):
            connector.collection(Collections.NETSKOPE_TENANTS).update_one(
                {"name": tenant["name"]},
                {
                    "$set": {
                        "first_alert_pull": True,
                        "first_infrastructure_pull": True,
                        "first_page_pull": True,
                        "first_network_pull": True,
                        "first_audit_pull": True,
                        "first_application_pull": True,
                        "is_iterator": False,
                        "is_checkpoint_used_application": False,
                        "is_checkpoint_used_audit": False,
                        "is_checkpoint_used_infrastructure": False,
                        "is_checkpoint_used_network": False,
                        "is_checkpoint_used_page": False,
                        "is_checkpoint_used_alert": False,
                    }
                },
                upsert=True,
            )
