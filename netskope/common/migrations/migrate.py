"""Database migration script."""

import asyncio
import shutil
import json
import os
import sys
import subprocess
import urllib.parse
import traceback

from netskope.common.utils import (
    DBConnector,
    Collections,
    RepoManager,
    PluginHelper,
    PluginStatus,
)
from netskope.common.models.settings import ProxyIn
from datetime import datetime
from netskope.common.models.repo import PluginRepo, PluginRepoIn
from netskope.common.api.routers.repos import (
    REPO_STORAGE_PATH,
    _disable_configurations,
    update_plugins_updated_at,
)


# Newer versions should be prepended to this list
ALL_VERSIONS = [
    "6.1.0",
    "6.0.1",
    "6.0.0",
    "6.0.0-beta.1",
    "5.1.2",
    "5.1.1-dlp-beta-1",
    "5.1.1",
    "5.1.0",
    "5.1.0-beta.2",
    "5.1.0-beta.1",
    "5.0.1",
    "5.0.0",
    "4.2.0",
    "4.1.0",
    "4.0.0",
    "3.4.0",
    "3.3.3",
    "3.3.1",
    "3.3.0",
    "3.2.0",
    "3.1.5",
    "3.1.3",
    "3.1.0",
    "3.0.0",
]
LATEST_VERSION = ALL_VERSIONS[0]

if __name__ == "__main__":
    from netskope.common.models import SettingsDB

    try:
        connector = DBConnector()
        manager = RepoManager()
        helper = PluginHelper()
        settings = connector.collection(Collections.SETTINGS).find_one({})
        # script_path = os.path.join(
        #     os.path.dirname(__file__), "migrate_to_quorum_queues.py"
        # )
        # proc = subprocess.Popen(
        #     f"python {script_path} > /dev/null 2>&1 &".split()
        # )
        if settings is None:
            print("Performing database initialization.")
            script_path = os.path.join(
                os.path.dirname(__file__), "initialize.py"
            )
            proc = subprocess.run(f"python {script_path}", shell=True)
            if proc.returncode != 0:
                print(
                    "Database initialization script returned a non-zero exit code."
                )
                sys.exit(1)
            settings = connector.collection(Collections.SETTINGS).find_one({})

        migration_history = settings.get("migrationHistory", [])
        fresh_install = not settings.get("firstDeploymentTime")
        if not settings.get("firstDeploymentTime"):
            firstDeploymentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            firstDeploymentTime = settings.get("firstDeploymentTime")
        currentUpTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        connector.collection(Collections.SETTINGS).update_one(
            {},
            {
                "$set": {
                    "system_cores": int(os.getenv("SYSTEM_CORE")),
                    "firstDeploymentTime": firstDeploymentTime,
                    "currentUpTime": currentUpTime,
                    "analyticsServerConnectivity": True,  # Setting connectivity always True to disable the UI banner
                }
            },
        )
        settings = SettingsDB(**settings)
        if not settings.shareAnalytics:
            connector.collection(Collections.SETTINGS).update_one(
                {},
                {"$set": {"shareAnalytics": True}}
            )
        if not connector.collection(Collections.PLUGIN_REPOS).find_one({"name": "Default"}):
            print("Creating the Default plugin repository...")
            repo = PluginRepoIn(
                name="Default",
                url=os.environ.get(
                    "GIT_URL",
                    "https://github.com/netskopeoss/ta_cloud_exchange_plugins.git",
                ),
                username=os.environ.get("GIT_USERNAME", ""),
                password=os.environ.get("GIT_PASSWORD", ""),
                isDefault=True,
            )
            connector.collection(Collections.PLUGIN_REPOS).insert_one(
                repo.model_dump()
            )
        print("Resetting Default repo head")
        try:
            connector = DBConnector()
            repo = connector.collection(Collections.PLUGIN_REPOS).find_one({"name": "Default"})
            if repo:
                repo = PluginRepo(**repo)
                result = manager.reset_hard_to_head(repo)
                if isinstance(result, str):
                    print(result)
                clean_result = manager.clean_default_repo(repo)
                if isinstance(clean_result, str):
                    print(clean_result)
        except Exception:
            print("Error occurred while resetting the head", traceback.format_exc())

        try:
            package = "azure_service_bus"
            default_repo_model = PluginRepo(
                **connector.collection(Collections.PLUGIN_REPOS).find_one(
                    {"name": "Default"}
                )
            )
            default_repo_path = os.path.join(
                manager.get_dir(default_repo_model), package
            )
            for repo in connector.collection(Collections.PLUGIN_REPOS).find({}):
                print(f"Migrating Azure Service Bus plugin for {repo['name']}")
                diffs = []
                repo_model = PluginRepo(**repo)
                repo_path = os.path.join(manager.get_dir(repo_model), package)
                plugin_path = os.path.join(manager.get_plugin_dir(repo_model), package)
                typing_extension_lib_repo_path = os.path.join(
                    repo_path,
                    "lib",
                    "typing_extensions.py",
                )
                typing_extension_lib_plugin_path = os.path.join(
                    plugin_path,
                    "lib",
                    "typing_extensions.py",
                )
                plugin = f"netskope.plugins.{repo_model.name}.{package}.main"
                PluginClass = helper.find_by_id(plugin)  # NOSONAR
                if PluginClass is None:
                    # continue migration for repos.
                    print("plugin Class not found for Azure Service Bus plugin")

                # update plugin
                # 1. if plugin is not in repo
                if not os.path.exists(typing_extension_lib_repo_path):
                    print("typing_extensions.py not found in repo for Azure Service Bus plugin")
                # 2. if plugin is in repo but not in plugins, then load in repo only.
                if os.path.exists(
                    typing_extension_lib_repo_path
                ) and not os.path.exists(typing_extension_lib_plugin_path):
                    print(
                        "typing_extensions.py not found in plugins for Azure Service Bus plugin"
                    )
                    if repo_model.name == "Default":
                        print("skipping default repo, moving from default to default.")
                        continue
                    print(
                        f"Copying updated Azure Service Bus plugin to repo: {repo_model.name} "
                        f"from {default_repo_model.name}"
                    )
                    shutil.rmtree(repo_path)
                    shutil.copytree(default_repo_path, repo_path)
                    plugin_info = manager.get_diff(repo_model)
                    error_msg = [
                        error.get("error_msg")
                        for error in plugin_info
                        if "error_msg" in error
                    ]
                    if error_msg:
                        print(
                            f"error encountered while loading Azure Service Bus plugin {error_msg[0]}"
                        )
                        continue

                    connector.collection(Collections.PLUGIN_REPOS).update_one(
                        {"name": repo_model.name},
                        {
                            "$set": {
                                "updates": {"action": "pull_updates"},
                                "hasUpdate": True if plugin_info else False,
                            }
                        },
                    )
                # 3. if plugin is either in repo or loaded in plugins, then update both repo and plugin.
                elif os.path.exists(typing_extension_lib_plugin_path) or os.path.exists(typing_extension_lib_repo_path):
                    print("typing_extensions.py found in either repo or plugins for Azure Service Bus plugin")
                    print(
                        f"Updating Azure Service Bus plugin with id={plugin}."
                    )
                    if repo_model.name != "Default":
                        shutil.rmtree(repo_path)
                        shutil.copytree(default_repo_path, repo_path)

                    shutil.rmtree(plugin_path)
                    shutil.copytree(default_repo_path, plugin_path)
                    status = PluginStatus.MODIFIED
                    manager._add_plugin(
                        repo_model, package, manager._get_head_hash(repo_model)
                    )
                    manifest_path = os.path.join(
                        os.path.join(REPO_STORAGE_PATH, repo_model.name),
                        package,
                        "manifest.json",
                    )
                    manifest = None
                    if os.path.exists(manifest_path):
                        manifest = json.load(open(manifest_path))
                    user = ""
                    asyncio.run(_disable_configurations(plugin, manifest, diffs, user))

                helper.refresh()
                connector.collection(Collections.PLUGIN_REPOS).update_one(
                    {"name": repo_model.name},
                    {
                        "$set": {
                            "updates": {
                                "action": "update_plugins",
                                "metadata": {"plugins": [plugin]},
                            },
                            "hasUpdate": (
                                True if manager.get_diff(repo_model) else False
                            ),
                        }
                    },
                )
                update_plugins_updated_at()
        except Exception as e:
            print(
                f"Exception encountered while executing migration. Error: {e} {traceback.format_exc()}"
            )
            raise e

        settings = connector.collection(Collections.SETTINGS).find_one({})
        settings_in = SettingsDB(**settings)

        https_proxy = os.environ.get("HTTPS_PROXY", None)
        if https_proxy is not None and len(https_proxy) > 0:
            scheme = "https" if https_proxy.startswith("https://") else "http"
            parsed_url = urllib.parse.urlparse(https_proxy)
            if (
                parsed_url.username is not None
                and parsed_url.password is not None
            ):
                username = parsed_url.username
                password = parsed_url.password
            else:
                username = ""
                password = ""
            if parsed_url.port:
                server = f"{parsed_url.hostname}:{parsed_url.port}"
            else:
                server = parsed_url.hostname
            username = urllib.parse.unquote_plus(username)
            password = urllib.parse.unquote_plus(password)

            proxy = ProxyIn(
                scheme=scheme,
                server=server,
                username=username,
                password=password,
            )

            connector.collection(Collections.SETTINGS).update_one(
                {}, {"$set": {"proxy": proxy.model_dump()}}
            )
        # Resetting proxy
        else:
            proxy = ProxyIn(
                scheme="http",
                server="",
                username="",
                password="",
            )
            connector.collection(Collections.SETTINGS).update_one(
                {}, {"$set": {"proxy": proxy.model_dump()}}
            )

        settings = connector.collection(Collections.SETTINGS).find_one({})
        settings = SettingsDB(**settings)
        old_version = settings.databaseVersion
        if fresh_install and settings.databaseVersion != LATEST_VERSION:
            old_version = "0.0.0"
        print(f"Current database version is {settings.databaseVersion}.")

        if settings.databaseVersion == LATEST_VERSION:
            print("Current database version is same as the latest version.")
            sys.exit(0)
        if settings.databaseVersion not in ALL_VERSIONS:
            # this version is not in the list; may be older than 3.0.0
            # run all the migration scripts in descending order
            ALL_VERSIONS.append(settings.databaseVersion)
        for i in range(
            ALL_VERSIONS.index(settings.databaseVersion) - 1, -1, -1
        ):
            migrate_to = ALL_VERSIONS[i]
            script_path = os.path.join(
                os.path.dirname(__file__), f"{migrate_to}.py"
            )
            print(f"Running initialization script for {migrate_to} version.")
            proc = subprocess.run(f"python {script_path}", shell=True)
            if proc.returncode != 0:
                print(
                    f"Initialization script for {migrate_to} returned a non-zero exit code."
                )
                sys.exit(1)
            connector.collection(Collections.SETTINGS).update_one(
                {}, {"$set": {"databaseVersion": migrate_to}}
            )

            if migrate_to == LATEST_VERSION:
                migration_history.append(
                    {
                        "oldVersion": old_version,
                        "currentVersion": migrate_to,
                        "migrationTime": datetime.now(),
                    }
                )

        connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"migrationHistory": migration_history}}
        )
    except Exception:
        print("An error occurred while performing database migration.")
        raise
