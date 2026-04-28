"""Plugin repo check update task."""

import json
import os
import re
import traceback

import netskope.common
import netskope.common.celery
import netskope.common.celery.analytics
import netskope.repos
from netskope.common.celery.analytics import convert_to_hex
from .main import APP
from ..models import SettingsDB
from ..utils import (
    RepoManager,
    Notifier,
    Logger,
    DBConnector,
    Collections,
    PluginStatus,
    track,
)

logger = Logger()
notifier = Notifier()
connector = DBConnector()
manager = RepoManager()
db_connector = DBConnector()

REPO_STORAGE_PATH = netskope.repos.__path__[0]


def get_plugin_version(repo: str, package: str) -> str:
    """Get the version of a plugin as a hex string."""
    manifest_path = os.path.join(
        os.path.join(REPO_STORAGE_PATH, repo), package, "manifest.json"
    )
    if os.path.exists(manifest_path):
        manifest = json.load(open(manifest_path))
        version = manifest["version"]
        match = re.match(r"(\d+\.\d+\.\d+)", version)
        if match:
            version_parts = match.group(1).split(".")
            hex_version_parts = [convert_to_hex(int(part), 1) for part in version_parts]
            return "".join(hex_version_parts)
    return ""


def check_beta_plugin_upgrades() -> None:
    """Check for plugin updates in the Beta repo and mark them as modified if newer than the GA version."""
    try:
        GA_repo = None
        for repo in manager.repos:
            if repo.name == "Default":
                manager.pull_updates(repo)
                GA_repo = repo
        for repo in manager.repos:
            plugin_updates_tracker = set()
            if (
                repo.url.startswith(
                    "https://github.com/netskopeoss/ta_cloud_exchange_beta_plugins"
                )
                or repo.url.startswith(
                    "https://github.com/crestdatasystems/ta_cloud_exchange_plugins_beta"
                )
                or repo.url.startswith(
                    "https://github.com/crestdatasystems/ta_cloud_exchange_plugins_hotfix_repo"
                )
            ):
                plugin_updates = []
                config_collections = [
                    Collections.CLS_CONFIGURATIONS,
                    Collections.CONFIGURATIONS,
                    Collections.ITSM_CONFIGURATIONS,
                    Collections.CREV2_CONFIGURATIONS,
                    Collections.EDM_CONFIGURATIONS,
                    Collections.CFC_CONFIGURATIONS,
                ]
                for collection in config_collections:
                    plugins = db_connector.collection(collection).find({})
                    for plugin in plugins:
                        if plugin.get("plugin") is None:
                            continue
                        if plugin.get("plugin").split(".")[-3] == repo.name:
                            package = plugin.get("plugin").split(".")[-2]
                            if package is None:
                                continue
                            if get_plugin_version(
                                repo.name, package
                            ) <= get_plugin_version(GA_repo.name, package) and (
                                plugin.get("plugin") not in plugin_updates_tracker
                            ):
                                plugin_id = manager._get_id(repo, package)
                                PluginClass = manager.helper.find_by_id(plugin_id)
                                current_plugin_version = None
                                if PluginClass and PluginClass.metadata.get("version"):
                                    current_plugin_version = PluginClass.metadata.get(
                                        "version"
                                    )
                                plugin_update = {
                                    "status": PluginStatus.MODIFIED,
                                    **manager._load_plugin_info(
                                        GA_repo,
                                        package,
                                        PluginStatus.MODIFIED,
                                        current_plugin_version,
                                    ),
                                }
                                plugin_update["id"] = plugin.get("plugin")
                                plugin_updates.append(plugin_update)
                                plugin_updates_tracker.add(plugin.get("plugin"))

                db_connector.collection(Collections.PLUGIN_REPOS).update_one(
                    {"name": repo.name},
                    {"$set": {"plugin_migrates": plugin_updates}},
                )

    except Exception:
        logger.error(
            "Error occurred while checking for plugin updates.",
            details=traceback.format_exc(),
            error_code="CE_1003",
        )


@APP.task(name="common.check_updates")
@track()
def check_updates() -> dict:
    """Check for plugin updates for all repos."""
    settings = SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
    out = {"plugins": True}

    if settings.enableUpdateChecking is False:
        return out
    logger.info("Checking for plugin updates.")
    check_beta_plugin_upgrades()
    try:
        out["plugins"] = True
        for repo in manager.repos:
            has_before = True if manager.get_diff(repo) else False
            if manager.pull_updates(repo):
                if not has_before and manager.get_diff(repo):
                    logger.info(f"Updates are available for plugin repo {repo.name}.")
                    notifier.info(f"Updates are available for plugin repo {repo.name}.")
                    connector.collection(Collections.PLUGIN_REPOS).update_one(
                        {"name": repo.name}, {"$set": {"hasUpdate": True}}
                    )
            else:
                notifier.error(
                    f"Error occurred while checking for updates in plugin "
                    f"repo {repo.name}. Check logs for more information."
                )
                out["plugins"] = False
    except Exception:
        logger.error(
            "Error occurred while checking for plugin updates.",
            details=traceback.format_exc(),
            error_code="CE_1006",
            resolution="""\nEnsure that,\n        1. The plugin repo is accessible from the Cloud Exchange.\n        2. Credentials for the plugin repo are correct.\n""",  # noqa
        )
        out["plugins"] = False
    return out
