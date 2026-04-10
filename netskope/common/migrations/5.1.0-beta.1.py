"""Migrations for 5.1.0-beta.1 release."""

import asyncio
import os
import subprocess
import traceback
from datetime import datetime
from typing import Dict, List, Union
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING, IndexModel, UpdateOne

from netskope.common.models import (
    NetskopeField,
    NetskopeFieldType,
    PollIntervalUnit,
    FieldDataType
)
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    RepoManager,
    Scheduler,
)
from netskope.common.utils.common_pull_scheduler import (
    schedule_or_delete_common_pull_tasks,
)
from netskope.common.utils.plugin_provider_helper import convert_tenant_format
from netskope.common.utils.scheduler import LOCKING_ARGS
from netskope.common.utils.webtx_notifier import (
    add_or_acknowledge_webtx_disabled_banner,
)
from netskope.integrations.crev2.routers.entities import (
    schedule_update_calculated_fields_task,
)
from netskope.integrations.itsm.routers.configurations import (
    update_configuration,
)
from netskope.integrations.itsm.models.configuration import ConfigurationUpdate

connector = DBConnector()
manager = RepoManager()
scheduler = Scheduler()
logger = Logger()
loop = asyncio.get_event_loop()
plugin_helper = PluginHelper()


def update_plugin_ids():
    """Update plugin ids."""
    print("Migrate plugins to use Default repository instead of Netskope.")
    try:
        # update plugin ids
        plugin_id_mappings = {
            Collections.CONFIGURATIONS: ["netskope"],
            Collections.ITSM_CONFIGURATIONS: ["netskope_itsm"],
            Collections.CRE_CONFIGURATIONS: ["netskope_cre"],
            Collections.CLS_CONFIGURATIONS: [
                "netskope_cls",
                "netskope_webtx",
                "syslog_service",
            ],
            Collections.GRC_CONFIGURATIONS: ["netskope_grc"],
        }

        for collection, plugin_ids in plugin_id_mappings.items():
            for plugin_id in plugin_ids:
                connector.collection(collection).update_many(
                    {"plugin": f"netskope.plugins.Netskope.{plugin_id}.main"},
                    {
                        "$set": {
                            "plugin": f"netskope.plugins.Default.{plugin_id}.main"
                        }
                    },
                )

    except Exception:
        logger.warn(
            "Error occurred while updating the Netskope plugins' ID",
            details=traceback.format_exc(),
        )
        raise


def replace_netskope_plugins():
    """Replace Netskope plugins."""
    try:
        import shutil
        import zipfile
        zip_files = []
        with zipfile.ZipFile("/opt/netskope_plugins.zip", "r") as zip_ref:
            zip_files = zip_ref.namelist()
            zip_ref.extractall("/opt/netskope/plugins/Default/")

        for relative_path in zip_files:
            full_path = os.path.join("/opt/netskope/plugins/Default/", relative_path)
            if not os.path.exists(full_path):
                raise Exception(f"Not able to find extracted file {full_path}.")

        directory_path = "/opt/netskope/plugins/Netskope"
        if os.path.exists(directory_path):
            print("Removing the old Netskope plugins directory")
            shutil.rmtree(directory_path)
    except Exception as e:
        logger.warn(
            f"Error occurred while replacing the Netskope plugins, {e}",
            details=traceback.format_exc(),
        )
        raise e


def update_netskope_tenants():
    """Update Netskope tenants."""
    for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find({}):
        if "tenantName" not in tenant.keys():
            continue
        new_tenant = convert_tenant_format(tenant)
        connector.collection(Collections.NETSKOPE_TENANTS).replace_one(
            {"name": tenant["name"]}, new_tenant
        )
        PluginClass = plugin_helper.find_by_id(new_tenant["plugin"])
        metadata = PluginClass.metadata
        for data_type in metadata["data"]:
            if data_type == "webtx":
                continue
            scheduler.schedule(
                name=f"tenant.{tenant['name']}.{data_type}",
                task_name="common.pull",
                poll_interval=tenant.get("pollInterval"),
                poll_interval_unit=tenant.get("pollIntervalUnit"),
                args=[data_type, tenant["name"]],
            )


def migrate_plugins_for_new_parameter():
    """Migrate plugins for new parameter."""
    try:
        for plugin in connector.collection(Collections.CLS_CONFIGURATIONS).find(
            {"plugin": "netskope.plugins.Default.netskope_cls.main"}
        ):
            tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": plugin["tenant"]})
            if "alert_types" not in tenant.keys():
                continue
            connector.collection(Collections.CLS_CONFIGURATIONS).update_one(
                {"name": plugin["name"]},
                {
                    "$set": {
                        "parameters.alert_types": list(tenant["alert_types"]),
                        "parameters.days": tenant["initialRange"],
                    },
                },
            )

        for plugin in connector.collection(Collections.CREV2_CONFIGURATIONS).find(
            {"plugin": "netskope.plugins.Default.netskope_ztre.main"}
        ):
            tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": plugin["tenant"]})
            if "initialRange" not in tenant.keys():
                continue
            connector.collection(Collections.CREV2_CONFIGURATIONS).update_one(
                {"name": plugin["name"]},
                {
                    "$set": {
                        "parameters.days": tenant["initialRange"],
                    },
                },
            )

        for plugin in connector.collection(Collections.ITSM_CONFIGURATIONS).find(
            {"plugin": "netskope.plugins.Default.netskope_itsm.main"}
        ):
            tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": plugin["tenant"]})
            if "alert_types" not in tenant.keys():
                continue
            connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
                {"name": plugin["name"]},
                {
                    "$set": {
                        "parameters.params.alert_types": list(tenant["alert_types"]),
                        "parameters.params.days": tenant["initialRange"],
                    },
                },
            )

        for plugin in connector.collection(Collections.CONFIGURATIONS).find(
            {"plugin": "netskope.plugins.Default.netskope.main"},
        ):
            tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                {"name": plugin["tenant"]}
            )
            required_types = []
            if plugin["parameters"]["threat_data_type"] in ["Both", "Malware"]:
                required_types.extend(plugin["parameters"]["malware_type"])
            if plugin["parameters"]["threat_data_type"] in ["Both", "URL"]:
                required_types.append("URL")
            if "initialRange" not in tenant.keys():
                continue
            connector.collection(Collections.CONFIGURATIONS).update_one(
                {"name": plugin["name"]},
                {
                    "$set": {
                        "parameters.threat_data_type": required_types,
                        "parameters.days": tenant["initialRange"],
                    },
                    "$unset": {"parameters.malware_type": ""},
                },
            )
    except Exception as e:
        logger.warn(
            "Error occurred while migrating plugins for new parameters",
            details=traceback.format_exc(),
        )
        raise e


def schedule_share_analytics_in_user_agent():
    """Schedule User-Agent task."""
    connector.collection(Collections.SCHEDULES).update_one(
        {"task": "common.share_analytics_in_user_agent"},
        {
            "$set": {
                "task": "common.share_analytics_in_user_agent",
                "_cls": "PeriodicTask",
                "name": "SHARE ANALYTICS IN USER AGENT",
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


async def migrate_ztre():
    """Schedule CREv2 unmute task."""
    from netskope.integrations.crev2.models import (
        CalculatedTypeParams,
        ValueMapTypeParams,
        ValueMapMappingNumber,
        ConfigurationIn,
        EntityFieldIn,
        EntityFieldType,
        EntityIn,
        EntityMapping,
        EntityMappingField,
        EntityTypeCoalesceStrategy,
    )
    from netskope.integrations.crev2.routers.configurations import (
        create_configuration,
    )
    from netskope.integrations.crev2.routers.entities import (
        create_entity,
        create_field,
    )

    def get_field_name(label: str) -> str:
        return "_".join(label.strip().lower().split(" "))

    ztre_plugins = [
        "netskope_ztre",
        "crowdstrike_ztre",
        "okta_ztre",
        "crowdstrike_identity_protect_ztre",
        "microsoft_entra_id_ztre",
        "mimecast_ztre",
    ]

    for plugin in ztre_plugins:
        subprocess.run(
            [
                f'/bin/unzip -o default_plugins.zip "netskope/repos/Default/{plugin}/*"',
            ],
            capture_output=True,
            shell=True,
            cwd="/opt",
            check=True,
        )
        subprocess.run(
            [
                f"find netskope/plugins/ -maxdepth 1 -mindepth 1 -type d -not -name custom_plugins "
                f"-not -name __pycache__ | xargs -I {{}} cp -r netskope/repos/Default/{plugin} {{}}",
            ],
            capture_output=True,
            shell=True,
            cwd="/opt",
            check=True,
        )

    connector.collection(Collections.USERS).update_one(
        {"username": "admin"},
        {"$addToSet": {"scopes": {"$each": ["cre_read", "cre_write"]}}},
    )
    result = connector.collection(Collections.SCHEDULES).update_one(
        {"task": "cre.unmute"},
        {
            "$set": {
                "task": "cre.unmute",
                "name": "CREv2 INTERNAL UNMUTE TASK",
            }
        },
    )
    if result.modified_count > 0:
        print("Migrated cre.unmute task...")
    result = connector.collection(Collections.SCHEDULES).delete_one(
        {"task": "cre.calculate_aggregate"}
    )
    if result.deleted_count > 0:
        print("Deleted cre.calculate_aggregate task...")
    result = connector.collection(Collections.SCHEDULES).update_one(
        {"task": "cre.perform_action"},
        {
            "$set": {
                "task": "cre.perform_action",
                "name": "CREv2 SYNC INTERVAL ACTION",
            }
        },
    )
    if result.modified_count > 0:
        print("Updated cre.perform_action task...")
    result = connector.collection(Collections.SCHEDULES).delete_one(
        {"task": "grc.unmute"}
    )
    if result.deleted_count > 0:
        print("Deleted grc.unmute task...")

    connector.collection(Collections.SCHEDULES).delete_many(
        {"task": "cre.fetch_records"}
    )
    connector.collection(Collections.SCHEDULES).delete_many(
        {"task": "grc.get_application_details"}
    )

    USERS_ENTITY = "Users"
    DEVICES_ENTITY = "Devices"
    APPS_ENTITY = "Applications"

    CRE_CONFIGS = list(
        connector.collection(Collections.CRE_CONFIGURATIONS).find({})
    )
    ARE_CONFIGS = list(
        connector.collection(Collections.GRC_CONFIGURATIONS).find({})
    )
    # cre_names = list(map(lambda c: c["name"], CRE_CONFIGS))

    print(f"Creating {USERS_ENTITY} entity...")
    try:
        await create_entity(
            EntityIn(
                name=USERS_ENTITY,
                fields=[
                    EntityFieldIn(
                        label="Email",
                        type=EntityFieldType.STRING,
                        unique=True,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    )
                ],
            )
        )
    except ValueError:
        print("Entity already exists...")

    print(f"Creating {DEVICES_ENTITY} entity...")
    try:
        await create_entity(
            EntityIn(
                name=DEVICES_ENTITY,
                fields=[
                    EntityFieldIn(
                        label="ID",
                        type=EntityFieldType.STRING,
                        unique=True,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    )
                ],
            )
        )
    except ValueError:
        print("Entity already exists...")

    print(f"Creating {APPS_ENTITY} entity...")
    try:
        await create_entity(
            EntityIn(
                name=APPS_ENTITY,
                fields=[
                    EntityFieldIn(
                        label="Netskope ID",
                        type=EntityFieldType.STRING,
                        unique=True,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                    EntityFieldIn(
                        label="Name",
                        type=EntityFieldType.STRING,
                        unique=True,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                    EntityFieldIn(
                        label="Vendor",
                        type=EntityFieldType.STRING,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                    EntityFieldIn(
                        label="CCI",
                        type=EntityFieldType.NUMBER,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                    EntityFieldIn(
                        label="CCL",
                        type=EntityFieldType.STRING,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                    EntityFieldIn(
                        label="Category",
                        type=EntityFieldType.STRING,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                    EntityFieldIn(
                        label="Users",
                        type=EntityFieldType.LIST,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.MERGE,
                    ),
                    EntityFieldIn(
                        label="Deep Link",
                        type=EntityFieldType.STRING,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                    EntityFieldIn(
                        label="Custom Tags",
                        type=EntityFieldType.LIST,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                    EntityFieldIn(
                        label="Discovery Domains",
                        type=EntityFieldType.LIST,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                    EntityFieldIn(
                        label="Steering Domains",
                        type=EntityFieldType.LIST,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                ],
            )
        )
    except ValueError:
        print("Entity already exists...")

    fields = {}
    device_fields = {}
    for configuration in CRE_CONFIGS:
        plugin = configuration["plugin"].split(".")[-2]
        print(f"Migrating URE {configuration['name']} configuration...")
        if plugin == "netskope_cre":
            score_field = f"{configuration['name']} UBA Score"
            try:
                fields[get_field_name(score_field)] = {
                    "configuration": configuration["name"],
                    "transform": lambda x: x,
                }
                await create_field(
                    USERS_ENTITY,
                    EntityFieldIn(
                        label=score_field,
                        type=EntityFieldType.NUMBER,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{score_field} field already exists...")
            plugin = (
                configuration["plugin"]
                .replace("Netskope", "Default")
                .replace("netskope_cre", "netskope_ztre")
            )
            try:
                await create_configuration(
                    ConfigurationIn(
                        name=f"URE {configuration['name']}",
                        active=False,
                        plugin=plugin,
                        tenant=configuration["tenant"],
                        pollInterval=30,
                        pollIntervalUnit=PollIntervalUnit.SECONDS,
                        parameters={"initial_range": 1, "days": 7},
                        mappedEntities=[
                            EntityMapping(
                                entity="Users",
                                destination=USERS_ENTITY,
                                fields=[
                                    EntityMappingField(
                                        source="email", destination="email"
                                    ),
                                    EntityMappingField(
                                        source="ubaScore",
                                        destination=get_field_name(
                                            score_field
                                        ),
                                    ),
                                ],
                            )
                        ],
                    )
                )
            except Exception:
                print("Configuration already exists...")
        elif plugin == "crowdstrike_cre":
            score_field = f"{configuration['name']} Assessment Score"
            device_fields[get_field_name(score_field)] = {
                "configuration": configuration["name"],
                "transform": lambda x: None if x is None else x // 10,
            }
            normalized_score_field = (
                f"{configuration['name']} Normalized Score"
            )
            try:
                await create_field(
                    DEVICES_ENTITY,
                    EntityFieldIn(
                        label=score_field,
                        type=EntityFieldType.NUMBER,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{score_field} field already exists...")
            try:
                await create_field(
                    DEVICES_ENTITY,
                    EntityFieldIn(
                        label=normalized_score_field,
                        type=EntityFieldType.CALCULATED,
                        params=CalculatedTypeParams(
                            expression=f"${'_'.join(score_field.strip().lower().split(' '))} * 10"
                        ),
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{normalized_score_field} field already exists...")
            plugin = configuration["plugin"].replace(
                "crowdstrike_cre", "crowdstrike_ztre"
            )
            try:
                configuration["parameters"]["maximum_score"] = (
                    configuration["parameters"]["maximum_score"] // 10
                )
                await create_configuration(
                    ConfigurationIn(
                        name=f"URE {configuration['name']}",
                        active=False,
                        plugin=plugin,
                        tenant=configuration["tenant"],
                        pollInterval=configuration["pollInterval"],
                        pollIntervalUnit=configuration["pollIntervalUnit"],
                        parameters=configuration["parameters"],
                        mappedEntities=[
                            EntityMapping(
                                entity="Agents",
                                destination=DEVICES_ENTITY,
                                fields=[
                                    EntityMappingField(
                                        source="Host ID", destination="id"
                                    ),
                                    EntityMappingField(
                                        source="Overall Assessment Score",
                                        destination=get_field_name(
                                            score_field
                                        ),
                                    ),
                                ],
                            )
                        ],
                    )
                )
            except Exception:
                print("Configuration already exists...")
        elif plugin == "okta_cre":
            plugin = configuration["plugin"].replace("okta_cre", "okta_ztre")
            id_field = f"{configuration['name']} ID"
            status_field = f"{configuration['name']} Status"
            try:
                await create_field(
                    APPS_ENTITY,
                    EntityFieldIn(
                        label=id_field,
                        type=EntityFieldType.STRING,
                        unique=True,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{id_field} field already exists...")
            try:
                await create_field(
                    APPS_ENTITY,
                    EntityFieldIn(
                        label=status_field,
                        type=EntityFieldType.STRING,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{status_field} field already exists...")
            try:
                await create_configuration(
                    ConfigurationIn(
                        name=f"URE {configuration['name']}",
                        active=False,
                        plugin=plugin,
                        tenant=configuration["tenant"],
                        pollInterval=configuration["pollInterval"],
                        pollIntervalUnit=configuration["pollIntervalUnit"],
                        parameters=configuration["parameters"],
                        mappedEntities=[
                            EntityMapping(
                                entity="Applications",
                                destination=APPS_ENTITY,
                                fields=[
                                    EntityMappingField(
                                        source="ID",
                                        destination=get_field_name(id_field),
                                    ),
                                    EntityMappingField(
                                        source="Name",
                                        destination="name",
                                    ),
                                    EntityMappingField(
                                        source="Status",
                                        destination=get_field_name(
                                            status_field
                                        ),
                                    ),
                                ],
                            )
                        ],
                    )
                )
            except Exception:
                print("Configuration already exists...")
        elif plugin == "crowdstrike_identity_protect":
            score_field = f"{configuration['name']} Risk Score"
            fields[get_field_name(score_field)] = {
                "configuration": configuration["name"],
                "transform": lambda x: None if x is None else 1 - (x / 1000),
            }
            normalized_score_field = (
                f"{configuration['name']} Normalized Score"
            )
            plugin = configuration["plugin"].replace(
                "crowdstrike_identity_protect",
                "crowdstrike_identity_protect_ztre",
            )

            try:
                await create_field(
                    USERS_ENTITY,
                    EntityFieldIn(
                        label=score_field,
                        type=EntityFieldType.NUMBER,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{score_field} field already exists...")
            try:
                await create_field(
                    USERS_ENTITY,
                    EntityFieldIn(
                        label=normalized_score_field,
                        type=EntityFieldType.CALCULATED,
                        params=CalculatedTypeParams(
                            expression=f"(1 - ${get_field_name(score_field)}) * 1000"
                        ),
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{normalized_score_field} field already exists...")
            try:
                await create_configuration(
                    ConfigurationIn(
                        name=f"URE {configuration['name']}",
                        active=False,
                        plugin=plugin,
                        tenant=configuration["tenant"],
                        pollInterval=configuration["pollInterval"],
                        pollIntervalUnit=configuration["pollIntervalUnit"],
                        parameters=configuration["parameters"],
                        mappedEntities=[
                            EntityMapping(
                                entity="Users",
                                destination=USERS_ENTITY,
                                fields=[
                                    EntityMappingField(
                                        source="Email Address",
                                        destination="email",
                                    ),
                                    EntityMappingField(
                                        source="Risk Score",
                                        destination=get_field_name(
                                            score_field
                                        ),
                                    ),
                                ],
                            )
                        ],
                    )
                )
            except Exception:
                print("Configuration already exists...")
        elif plugin == "microsoft_azure_AD":
            score_field = f"{configuration['name']} Risk Level"

            def transform(value):
                """Transform value into risk level."""
                if value is None:
                    return None
                elif value == 875:
                    return "low"
                elif value == 625:
                    return "medium"
                elif value == 375:
                    return "high"

            fields[get_field_name(score_field)] = {
                "configuration": configuration["name"],
                "transform": transform,
            }
            normalized_score_field = (
                f"{configuration['name']} Normalized Score"
            )
            plugin = configuration["plugin"].replace(
                "microsoft_azure_AD",
                "microsoft_entra_id_ztre",
            )

            try:
                await create_field(
                    USERS_ENTITY,
                    EntityFieldIn(
                        label=score_field,
                        type=EntityFieldType.STRING,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{score_field} field already exists...")
            try:
                await create_field(
                    USERS_ENTITY,
                    EntityFieldIn(
                        label=normalized_score_field,
                        type=EntityFieldType.VALUE_MAP_NUMBER,
                        params=ValueMapTypeParams(
                            field=get_field_name(score_field),
                            mappings=[
                                ValueMapMappingNumber(label="low", value=875),
                                ValueMapMappingNumber(label="medium", value=625),
                                ValueMapMappingNumber(label="high", value=375),
                            ],
                        ),
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{normalized_score_field} field already exists...")
            try:
                await create_configuration(
                    ConfigurationIn(
                        name=f"URE {configuration['name']}",
                        active=False,
                        plugin=plugin,
                        tenant=configuration["tenant"],
                        pollInterval=configuration["pollInterval"],
                        pollIntervalUnit=configuration["pollIntervalUnit"],
                        parameters=configuration["parameters"],
                        mappedEntities=[
                            EntityMapping(
                                entity="Users",
                                destination=USERS_ENTITY,
                                fields=[
                                    EntityMappingField(
                                        source="User Email",
                                        destination="email",
                                    ),
                                    EntityMappingField(
                                        source="Risk Level",
                                        destination=get_field_name(
                                            score_field
                                        ),
                                    ),
                                ],
                            )
                        ],
                    )
                )
            except Exception:
                print("Configuration already exists...")
        elif plugin == "mimecast_cre":
            score_field = f"{configuration['name']} Risk"

            def transform(value):
                """Transform value into risk level."""
                if value is None:
                    return None
                elif value == 800:
                    return "A"
                elif value == 600:
                    return "B"
                elif value == 400:
                    return "C"
                elif value == 200:
                    return "D"
                elif value == 1:
                    return "F"

            fields[get_field_name(score_field)] = {
                "configuration": configuration["name"],
                "transform": transform,
            }
            normalized_score_field = (
                f"{configuration['name']} Normalized Score"
            )
            plugin = configuration["plugin"].replace(
                "mimecast_cre",
                "mimecast_ztre",
            )

            try:
                await create_field(
                    USERS_ENTITY,
                    EntityFieldIn(
                        label=score_field,
                        type=EntityFieldType.STRING,
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{score_field} field already exists...")
            try:
                await create_field(
                    USERS_ENTITY,
                    EntityFieldIn(
                        label=normalized_score_field,
                        type=EntityFieldType.VALUE_MAP_NUMBER,
                        params=ValueMapTypeParams(
                            field=get_field_name(score_field),
                            mappings=[
                                ValueMapMappingNumber(label="A", value=800),
                                ValueMapMappingNumber(label="B", value=600),
                                ValueMapMappingNumber(label="C", value=400),
                                ValueMapMappingNumber(label="D", value=200),
                                ValueMapMappingNumber(label="F", value=1),
                            ],
                        ),
                        unique=False,
                        coalesceStrategy=EntityTypeCoalesceStrategy.OVERWRITE,
                    ),
                )
            except Exception:
                print(f"{normalized_score_field} field already exists...")
            try:
                await create_configuration(
                    ConfigurationIn(
                        name=f"URE {configuration['name']}",
                        active=False,
                        plugin=plugin,
                        tenant=configuration["tenant"],
                        pollInterval=configuration["pollInterval"],
                        pollIntervalUnit=configuration["pollIntervalUnit"],
                        parameters=configuration["parameters"],
                        mappedEntities=[
                            EntityMapping(
                                entity="Users",
                                destination=USERS_ENTITY,
                                fields=[
                                    EntityMappingField(
                                        source="User Email",
                                        destination="email",
                                    ),
                                    EntityMappingField(
                                        source="User Risk",
                                        destination=get_field_name(
                                            score_field
                                        ),
                                    ),
                                ],
                            )
                        ],
                    )
                )
            except Exception:
                print("Configuration already exists...")
    for configuration in ARE_CONFIGS:
        plugin = configuration["plugin"].split(".")[-2]
        print(f"Migrating ARE {configuration['name']} configuration...")
        if plugin == "netskope_grc":
            plugin = (
                configuration["plugin"]
                .replace("Netskope", "Default")
                .replace("netskope_grc", "netskope_ztre")
            )
            try:
                await create_configuration(
                    ConfigurationIn(
                        name=f"ARE {configuration['name']}",
                        active=False,
                        plugin=plugin,
                        tenant=configuration["tenant"],
                        pollInterval=30,
                        pollIntervalUnit=PollIntervalUnit.SECONDS,
                        parameters={"initial_range": 1, "days": 7},
                        mappedEntities=[
                            EntityMapping(
                                entity="Applications",
                                destination=APPS_ENTITY,
                                fields=[
                                    EntityMappingField(
                                        source="applicationId",
                                        destination="netskope_id",
                                    ),
                                    EntityMappingField(
                                        source="applicationName",
                                        destination="name",
                                    ),
                                    EntityMappingField(
                                        source="vendor", destination="vendor"
                                    ),
                                    EntityMappingField(
                                        source="cci", destination="cci"
                                    ),
                                    EntityMappingField(
                                        source="ccl", destination="ccl"
                                    ),
                                    EntityMappingField(
                                        source="categoryName",
                                        destination="category",
                                    ),
                                    EntityMappingField(
                                        source="users", destination="users"
                                    ),
                                    EntityMappingField(
                                        source="deepLink",
                                        destination="deep_link",
                                    ),
                                    EntityMappingField(
                                        source="customTags",
                                        destination="custom_tags",
                                    ),
                                    EntityMappingField(
                                        source="discoveryDomains",
                                        destination="discovery_domains",
                                    ),
                                    EntityMappingField(
                                        source="steeringDomains",
                                        destination="steering_domains",
                                    ),
                                ],
                            )
                        ],
                    )
                )
            except Exception:
                print("Configuration already exists...")

    # migrate user records
    print("Migrating user records...")
    count = 0
    for record in connector.collection(Collections.CRE_USERS).find(
        {"type": "user"}
    ):
        scores = {
            record["source"]: record.get("current")
            for record in record.get("scores", [])
        }
        connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{USERS_ENTITY}"
        ).insert_one(
            {
                "email": record["uid"],
            }
            | {
                key: value["transform"](scores.get(value["configuration"]))
                for key, value in fields.items()
            }
        )
        count += 1
    print(f"Migrated {count} user record(s)...")

    # migrate user records
    print("Migrating device records...")
    count = 0
    for record in connector.collection(Collections.CRE_USERS).find(
        {"type": "host"}
    ):
        scores = {
            record["source"]: record.get("current")
            for record in record.get("scores", [])
        }
        connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{DEVICES_ENTITY}"
        ).insert_one(
            {
                "id": record["uid"],
            }
            | {
                key: value["transform"](scores.get(value["configuration"]))
                for key, value in device_fields.items()
            }
        )
        count += 1
    print(f"Migrated {count} device record(s)...")

    if count:
        schedule_update_calculated_fields_task(DEVICES_ENTITY)

    # migrate application records
    print("Migrating application records...")
    count = 0
    for record in connector.collection(Collections.GRC_APPLICATIONS).find({}):
        connector.collection(
            f"{Collections.CREV2_ENTITY_PREFIX.value}{APPS_ENTITY}"
        ).insert_one(
            {
                "name": record.get("applicationName"),
                "netskope_id": str(record.get("applicationId")),
                "category": record.get("categoryName"),
                "cci": record.get("cci"),
                "ccl": record.get("ccl"),
                "custom_tags": record.get("customTags", []),
                "deep_link": record.get("deepLink"),
                "discovery_domains": record.get("discoveryDomains", []),
                "lastUpdated": record.get("updatedTime"),
                "vendor": record.get("vendor"),
                "users": record.get("users", []),
            }
        )
        count += 1
    print(f"Migrated {count} application record(s)...")


def delete_old_tasks():
    """Delete old tasks."""
    try:
        connector.collection(Collections.SCHEDULES).delete_many(
            {
                "task": {
                    "$in": [
                        "cls.webtx_logs",
                        "common.pull_alerts",
                        "common.pull_events",
                    ]
                }
            }
        )
    except Exception:
        logger.error(
            "Error occurred while deleting old tasks from schedules.",
            details=traceback.format_exc(),
            error_code="CE_1035"
        )


def disable_webtx_plugins_and_add_banner():
    """Disable webtx plugins and add banner."""
    try:
        connector.collection(Collections.CLS_CONFIGURATIONS).update_many(
            {
                "plugin": "netskope.plugins.Default.netskope_webtx.main",
                "active": True,
                "$or": [{"tenant": {"$exists": False}}, {"tenant": None}],
            },
            {"$set": {"active": False}},
        )
        message = add_or_acknowledge_webtx_disabled_banner()
        if message:
            print(message)
    except Exception:
        raise


def create_sharing_task_for_cte_configurations():
    """Create schedule sharing tasks for cte configurations."""
    for config in connector.collection(Collections.CONFIGURATIONS).find({}):
        PluginClass = plugin_helper.find_by_id(config["plugin"])
        if PluginClass.metadata.get("netskope", False):
            config["pollInterval"] = 60
            config["pollIntervalUnit"] = "minutes"

        task_status = {
            "startedAt": config.get("task", {}).get("startedAt", None),
            "task_id": config.get("task", {}).get("task_id", None),
            "worker_id": config.get("task", {}).get("worker_id", None),
        }
        # update field changes.
        if not isinstance(config.get("lockedAt"), dict):
            connector.collection(Collections.CONFIGURATIONS).update_one(
                {"name": config.get("name")},
                {
                    "$set": {
                        "lockedAt": {
                            "pull": config.get("lockedAt"),
                            "share": None,
                        },
                        "lastRunAt": {
                            "pull": config.get("lastRunAt"),
                            "share": None,
                        },
                        "lastRunSuccess": {
                            "pull": config.get("lastRunSuccess"),
                            "share": None,
                        },
                        "pollInterval": config.get("pollInterval"),
                        "pollIntervalUnit": config.get("pollIntervalUnit"),
                        "task": {"pull": task_status},
                        "tagsAggregateStrategy": "append",
                    }
                },
                upsert=True,
            )

        # Update lockedAt field for pulling task.
        connector.collection(Collections.SCHEDULES).update_one(
            {"task": "cte.execute_plugin", "name": config.get("name")},
            {"$set": {"kwargs.lock_field": "lockedAt.pull"}},
        )

        # create sharing task.
        if PluginClass.metadata.get("push_supported", False):
            connector.collection(Collections.SCHEDULES).update_one(
                {"name": config.get("name") + "_share"},
                {
                    "$set": {
                        "_cls": "PeriodicTask",
                        "name": config.get("name") + "_share",
                        "enabled": True,
                        "args": [
                            None,
                            config.get("name"),
                        ],
                        "kwargs": {
                            **LOCKING_ARGS.get("cte.share_indicators", {}),
                            **{"share_new_indicators": True},
                        },
                        "task": "cte.share_indicators",
                        "interval": {
                            "every": config.get("pollInterval"),
                            "period": config.get("pollIntervalUnit"),
                        },
                    },
                },
                upsert=True,
            )


def add_data_type_in_cto_tasks():
    """Add data type in cto tasks."""
    try:
        connector.collection(Collections.ITSM_TASKS).update_many(
            {},
            {
                "$rename": {
                    "alert": "dataItem"
                },
                "$set": {
                    "dataType": "alert"
                }
            }
        )
        connector.collection(Collections.ITSM_TASKS).update_many(
            {},
            {
                "$rename": {
                    "dataItem.rawAlert": "dataItem.rawData"
                }
            }
        )
        # Adding default lastUpdatedAt as now in migration as there may be older tasks in the system and if will set
        # it to the creation time many tickets can start cleaning up in the first cleanup and might loose wanted data.
        connector.collection(Collections.ITSM_TASKS).update_many(
            {},
            [
                {
                    "$set": {
                        "dataSubType": "$dataItem.alertType",
                        "lastUpdatedAt": datetime.now()
                    }
                }
            ]
        )
    except Exception as error:
        logger.error(
            f"Error occurred while updating CTO tickets' data type: {error}",
            details=traceback.format_exc(),
            error_code="CE_1034"
        )
        raise error


def update_itsm_schedules_after_events_support():
    """Update itsm schedules after events support."""
    try:
        connector.collection(Collections.SCHEDULES).update_one(
            {"task": "itsm.delete_alerts"},
            {"$set": {"task": "itsm.data_cleanup"}},
        )
        connector.collection(Collections.SCHEDULES).update_many(
            {"task": "itsm.pull_alerts"},
            {"$set": {"task": "itsm.pull_data_items"}},
        )
    except Exception as error:
        logger.error(
            "Error occurred while updating CTO schedules after events support.",
            details=traceback.format_exc(),
            error_code="CE_1033"
        )
        raise error


def iterate_and_replace_raw_alert(data: Union[Dict, List, str, None]):
    """Iterate and replace raw alert."""
    if isinstance(data, list):
        return [iterate_and_replace_raw_alert(value) for value in data]
    elif isinstance(data, dict):
        return {
            key.replace("rawAlert_", "rawData_"): iterate_and_replace_raw_alert(value)
            for key, value in data.items()
        }
    elif isinstance(data, str):
        return data.replace("rawAlert_", "rawData_")
    return data


def update_itsm_business_rules_after_events_support():
    """Update itsm business rules after events support."""
    try:
        rules = connector.collection(Collections.ITSM_BUSINESS_RULES).find({})
        updated_count = 0
        for rule in rules:
            if "alertFilters" in rule:
                rule["filters"] = iterate_and_replace_raw_alert(rule["alertFilters"])
                del rule["alertFilters"]
                if rule.get("dedupeRules"):
                    for idx, dup_rule in enumerate(rule["dedupeRules"]):
                        if dup_rule.get("filters"):
                            dup_rule["filters"] = iterate_and_replace_raw_alert(dup_rule["filters"])
                        if dup_rule.get("dedupeFields"):
                            dup_rule["dedupeFields"] = iterate_and_replace_raw_alert(dup_rule["dedupeFields"])
                        rule["dedupeRules"][idx] = dup_rule
                if rule.get("muteRules"):
                    for idx, mute_rule in enumerate(rule["muteRules"]):
                        if mute_rule.get("filters"):
                            mute_rule["filters"] = iterate_and_replace_raw_alert(mute_rule["filters"])
                        if dup_rule.get("dedupeFields"):
                            mute_rule["dedupeFields"] = iterate_and_replace_raw_alert(mute_rule["dedupeFields"])
                        rule["muteRules"][idx] = mute_rule
                result = connector.collection(Collections.ITSM_BUSINESS_RULES).update_one(
                    {"_id": rule["_id"]},
                    {
                        "$set": rule,
                        "$unset": {"alertFilters": 1}
                    }
                )
                updated_count += result.modified_count
        logger.info(f"{updated_count} CTO Business Rules updated out of {len(list(rules))} as part of 5.1.0 migration.")
    except Exception as error:
        logger.error(
            "Error occurred while updating CTO business rules after events support.",
            details=traceback.format_exc(),
            error_code="CE_1032"
        )
        raise error


def update_itsm_configurations_after_events_support():
    """Update itsm configurations after events support."""
    try:
        configurations = connector.collection(Collections.ITSM_CONFIGURATIONS).find(
            {
                "tenant": {
                    "$ne": None
                }
            }
        )
        updated_count = 0
        for configuration in configurations:
            configuration.setdefault("parameters", {}).setdefault("params", {}).setdefault("filters", [])
            configuration["parameters"]["params"]["filters"] = iterate_and_replace_raw_alert(configuration["filters"])
            configuration["parameters"]["params"]["event_types"] = []
            configuration["parameters"]["params"]["hours"] = 0
            configuration.setdefault("parameters", {}).setdefault("incident_update_config", {})
            configuration["parameters"]["incident_update_config"]["user_email"] = ""
            configuration["parameters"]["incident_update_config"]["status_mapping"] = {
                "new": "New",
                "in_progress": "In Progress",
                "deleted": "Resolved",
                "closed": "Resolved"
            }
            configuration["parameters"]["incident_update_config"]["severity_mapping"] = {
                "low": "Low",
                "medium": "Medium",
                "high": "High",
                "critical": "Critical"
            }
            configuration["updateIncidents"] = False
            del configuration["filters"]
            result = connector.collection(
                Collections.ITSM_CONFIGURATIONS
            ).update_one(
                {"_id": configuration["_id"]},
                {"$set": configuration, "$unset": {"filters": 1}},
            )
            loop.run_until_complete(
                update_configuration(
                    ConfigurationUpdate(
                        name=configuration["name"],
                        active=False,
                        plugin=configuration["plugin"],
                    )
                )
            )
            updated_count += result.modified_count
        logger.info(
            f"{updated_count} Netskope CTO Configurations updated out of "
            f"{len(list(configurations))} as part of 5.1.0 migration."
        )
    except Exception as error:
        logger.error(
            "Error occurred while updating CTO configurations after events support.",
            details=traceback.format_exc(),
            error_code="CE_1031"
        )
        raise error

    try:
        for configuration in connector.collection(
            Collections.ITSM_CONFIGURATIONS
        ).find(
            {
                "plugin": {"$regex": r"\.servicenow_itsm\.main$"},
                "parameters.mapping_config": None,
            }
        ):
            connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
                {"name": configuration["name"]},
                {
                    "$set": {
                        "parameters.mapping_config": {
                            "severity_mapping": {
                                "critical": "1",
                                "high": "1",
                                "low": "3",
                                "medium": "2",
                            },
                            "status_mapping": {
                                "closed": "7",
                                "deleted": "7",
                                "in_progress": "2",
                                "new": "1",
                                "on_hold": "3",
                            },
                        },
                    }
                },
            )
            loop.run_until_complete(
                update_configuration(
                    ConfigurationUpdate(
                        name=configuration["name"],
                        active=False,
                        plugin=configuration["plugin"],
                    )
                )
            )
        for configuration in connector.collection(
            Collections.ITSM_CONFIGURATIONS
        ).find(
            {
                "plugin": {"$regex": r"\.jira_itsm\.main$"},
                "parameters.mapping_config": None,
            }
        ):
            connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
                {"name": configuration["name"]},
                {
                    "$set": {
                        "parameters.mapping_config": {
                            "status_mapping": {
                                "new": "New",
                                "in_progress": "In Progress",
                                "closed": "Closed"
                            }
                        }
                    }
                },
            )
            loop.run_until_complete(
                update_configuration(
                    ConfigurationUpdate(
                        name=configuration["name"],
                        active=False,
                        plugin=configuration["plugin"],
                    )
                )
            )
    except Exception as error:
        logger.error(
            "Error occurred while updating CTO configurations after events support.",
            details=traceback.format_exc(),
            error_code="CE_1031",
        )
        raise error


def create_indexes_on_events_collection():
    """Create indexes on events collection."""
    try:
        indexes = [
            IndexModel(
                [("id", ASCENDING)],
                unique=True
            ),
            IndexModel(
                [("timestamp", DESCENDING)]
            )
        ]
        connector.collection(Collections.ITSM_EVENTS).create_indexes(
            indexes
        )
    except Exception as error:
        logger.error(
            "Error occurred while creating index on events collection.",
            details=traceback.format_exc(),
            error_code="CE_1030"
        )
        raise error


def update_alerts_after_events_support():
    """Update raw alert after events support."""
    try:
        connector.collection(Collections.ITSM_ALERTS).update_many(
            {},
            {
                "$rename": {
                    "rawAlert": "rawData"
                }
            }
        )
    except Exception as error:
        logger.error(
            "Error occurred while running migration for alerts.",
            details=traceback.format_exc(),
            error_code="CE_1036"
        )
        raise error


def remove_security_scorecard_banner():
    """Remove SecurityScorecard banner."""
    connector.collection(Collections.NOTIFICATIONS).delete_one(
        {"id": "BANNER_INFO_1001"}
    )


def update_cls_siem_mappings():
    """Update CLS SIEM mappings."""
    try:
        rules = connector.collection(Collections.CLS_BUSINESS_RULES).find({})
        updates = []
        for rule in rules:
            updates.append(
                UpdateOne(
                    {"_id": rule["_id"]},
                    {
                        "$set": {
                            "siemMappingIDs": (
                                {
                                    f"{key}_{value}": {
                                        "id": str(uuid4()),
                                        "task_id": None
                                    }
                                    for key, values in rule["siemMappings"].items()
                                    for value in values
                                }
                                if rule.get("siemMappings")
                                else {}
                            )
                        }
                    }
                )
            )
        updates = connector.collection(Collections.CLS_BUSINESS_RULES).bulk_write(updates)
    except Exception as error:
        logger.error(
            "Error occurred while updating CLS SIEM mappings.",
            details=traceback.format_exc(),
            error_code="CE_1037"
        )
        raise error


def add_error_code_field_for_itsm_alerts():
    """Add errorCode field for Cloud Exchange Alerts plugin."""
    try:
        field = "errorCode"
        lable = "Error Code"
        connector.collection(Collections.NETSKOPE_FIELDS).update_one(
            {"name": field},
            {
                "$set": NetskopeField(
                    name=field,
                    label=lable,
                    type=NetskopeFieldType.ALERT,
                    dataType=FieldDataType.TEXT
                ).model_dump()
            },
            upsert=True,
        )
    except Exception as error:
        logger.error(
            "Error occurred while adding error code field for itsm alerts.",
            details=traceback.format_exc(),
            error_code="CE_1038"
        )
        raise error


def update_default_business_rules():
    """Update default business rules."""
    try:
        connector.collection(Collections.CLS_BUSINESS_RULES).update_one(
            {"name": "All"},
            {
                "$set": {
                    "filters": {
                        "query": 'alert_type IN ("Compromised Credential", "policy", "malsite", "Malware", "DLP", "Security Assessment", "watchlist", "quarantine", "Remediation", "uba", "ctep", "ips", "c2") || event_type IN ("page", "application", "audit", "infrastructure", "network", "incident", "endpoint")',  # noqa
                        "mongo": '{"$or":[{"alert_type":{"$in":["Compromised Credential","policy","malsite","Malware","DLP","Security Assessment","watchlist","quarantine","Remediation","uba", "ctep", "ips", "c2"]}},{"event_type":{"$in":["page","application","audit","infrastructure","network", "incident", "endpoint"]}}]}',  # noqa
                    },
                }
            },
        )
    except Exception as error:
        logger.error(
            f"Error occurred while updating default business rules. {error}",
            details=traceback.format_exc(),
            error_code="CE_1039"
        )


def add_default_event_cleanup_in_settings():
    """Add default event cleanup in settings."""
    try:
        connector.collection(Collections.SETTINGS).find_one_and_update(
            {
                "eventCleanup": {"$exists": False}
            },
            {
                "$set": {
                    "eventCleanup": 7
                }
            }
        )
    except Exception as error:
        logger.error(
            "Error occurred while adding default event cleanup in settings.",
            details=traceback.format_exc(),
            error_code="CE_1040"
        )
        raise error


if __name__ == "__main__":
    replace_netskope_plugins()
    delete_old_tasks()
    update_plugin_ids()
    migrate_plugins_for_new_parameter()
    update_netskope_tenants()
    create_sharing_task_for_cte_configurations()
    disable_webtx_plugins_and_add_banner()
    remove_security_scorecard_banner()
    schedule_or_delete_common_pull_tasks()
    loop.run_until_complete(migrate_ztre())
    schedule_share_analytics_in_user_agent()
    # Migrations For Event support in CTO module
    add_data_type_in_cto_tasks()
    update_itsm_schedules_after_events_support()
    update_itsm_business_rules_after_events_support()
    update_itsm_configurations_after_events_support()
    create_indexes_on_events_collection()
    update_alerts_after_events_support()
    add_default_event_cleanup_in_settings()
    # Migration to add SIEM mapping ids in CLS
    update_cls_siem_mappings()
    add_error_code_field_for_itsm_alerts()
    update_default_business_rules()
    loop.close()
