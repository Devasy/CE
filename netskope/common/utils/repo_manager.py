"""Plugin repo manager class."""

import os
import json
import shutil
import datetime
import glob
from enum import Enum
from importlib import import_module
from typing import List
from subprocess import Popen, PIPE
from urllib.parse import urlparse, quote_plus
import traceback
import netskope.plugins
import netskope.repos
from .db_connector import DBConnector, Collections
from .logger import Logger
from .requests_retry_mount import MaxRetryExceededException, popen_retry_mount
from . import Singleton, PluginHelper
from ..models.repo import PluginRepo
from netskope.common.api import __version__
from netskope.common.utils.const import MODULES_MAP


BR_MAPPING = {
    "CLS": {
        "collection": Collections.CLS_BUSINESS_RULES,
        "import_path": "netskope.integrations.cls.models.business_rule",
    },
    "CTE": {
        "collection": Collections.CTE_BUSINESS_RULES,
        "import_path": "netskope.integrations.cte.models.business_rule",
    },
    "ITSM": {
        "collection": Collections.ITSM_BUSINESS_RULES,
        "import_path": "netskope.integrations.itsm.models.business_rule",
    },
}

REPO_STORAGE_PATH = netskope.repos.__path__[0]
PLUGIN_PATH = netskope.plugins.__path__[0]
PLUGINS_UPDATED_AT = datetime.datetime.now()


class PluginStatus(str, Enum):
    """Plugin status enumeration."""

    ADDED = "added"
    MODIFIED = "modified"
    REMOVED = "removed"


class RepoManager(metaclass=Singleton):
    """RepoManager class."""

    def __init__(self):
        """Initialize RepoManager class."""
        self._repos = []
        self.logger = Logger()

        # add the default repo
        import netskope.plugins.custom_plugins
        import netskope.plugins.Default

        self.helper = PluginHelper()
        packages = [
            netskope.plugins.custom_plugins,
            netskope.plugins.Default,
        ]
        self.helper.add_packages(packages).refresh()
        for package in packages:
            repo_name = package.__package__.split(".")[2]
            if repo_name == "custom_plugins":
                repo_name = None
            for mapping in glob.glob(
                os.path.join(package.__path__[0], "*", "mappings.json")
            ):
                self.import_mapping_file(mapping, repo_name)

        self.load()

    def load(self, cache=False):
        """Load repos from DB and all the plugins along with it."""
        global PLUGINS_UPDATED_AT
        self._repos = []
        connector = DBConnector()
        # only fetch the repos that are not soft deleted
        no_refresh = False
        plugins_updated_at = None
        if cache:
            settings_doc = connector.collection(Collections.SETTINGS).find_one({})
            plugins_updated_at = settings_doc.get("pluginsUpdatedAt")
            no_refresh = True
            if plugins_updated_at is not None:
                no_refresh = PLUGINS_UPDATED_AT >= plugins_updated_at
        for repo in connector.collection(Collections.PLUGIN_REPOS).find({}):
            self.add_repo(PluginRepo(**repo), cache=no_refresh)
        if cache and plugins_updated_at:
            PLUGINS_UPDATED_AT = plugins_updated_at

    def add_repo(self, repo: PluginRepo, repo_exists=True, cache=False):
        """Add new repo."""
        repo_dir = self.get_dir(repo)
        plugin_dir = self.get_plugin_dir(repo)
        if not repo_exists and (os.path.exists(repo_dir) or os.path.isdir(plugin_dir)):
            self.logger.debug(f"Deleting plugin repo directory for {repo.name}.")
            if os.path.isdir(repo_dir):
                shutil.rmtree(repo_dir)
            if os.path.isdir(plugin_dir):
                shutil.rmtree(plugin_dir)

        if os.path.isdir(self.get_dir(repo)):
            if not list(filter(lambda r: r.name == repo.name, self._repos)):
                self._repos.append(repo)
        else:
            self.logger.debug(f"Cloning new plugin repo {repo.name}.")
            url = urlparse(str(repo.url))
            from . import resolve_secret

            url = url._replace(
                netloc=f"{quote_plus(repo.username)}:{quote_plus(resolve_secret(repo.password))}@{url.netloc}"
            ).geturl()
            clone_process = Popen(
                ["git", "clone", f"{url}", "-q", "--", self.get_dir(repo)],
                stdin=PIPE,
                stderr=PIPE,
            )
            try:
                popen_retry_mount(clone_process, True)
            except MaxRetryExceededException:
                self.logger.error(
                    f"Max retry exceeded while cloning plugin repo {repo.name}"
                )
                raise Exception(
                    f"Max retry exceeded while cloning plugin repo {repo.name}"
                )
            if clone_process.returncode != 0:
                _, stderr = clone_process.communicate()
                self.logger.error(
                    f"Error occurred while cloning plugin repo {repo.name}.",
                    details=stderr.decode("utf-8").strip(),
                    error_code="CE_1014",
                    resolution="""\nEnsure that,\n        1. The repo url is correct.\n        2. The repo is reachable.\n""",  # noqa
                )
                stdout, stderr = clone_process.communicate()
                self.logger.error(stderr.decode("utf-8").strip())
                raise Exception(stderr.decode("utf-8").strip())
            self._repos.append(repo)
            self.logger.debug(f"Plugin repo {repo.name} cloned successfully.")
            self.pull_updates(repo)
            self.load_all_plugins(repo)
        if os.getenv("LOAD_REPO", "").lower() == "true":
            self.helper.add_packages([import_module(f"netskope.plugins.{repo.name}")])
            self.helper.refresh(cache=cache)
        return True

    def _add_plugin(self, repo: PluginRepo, package, commit):
        repo.plugins[package] = commit
        connector = DBConnector()
        connector.collection(Collections.PLUGIN_REPOS).update_one(
            {"name": repo.name}, {"$set": {"plugins": repo.plugins}}
        )

    def _remove_plugin(self, repo: PluginRepo, package):
        repo.plugins.pop(package, None)
        connector = DBConnector()
        connector.collection(Collections.PLUGIN_REPOS).update_one(
            {"name": repo.name}, {"$set": {"plugins": repo.plugins}}
        )

    def _get_head_hash(self, repo: PluginRepo):
        hash_process = Popen(
            ["git", "rev-parse", "HEAD"],
            cwd=self.get_dir(repo),
            stdout=PIPE,
            stderr=PIPE,
        )
        try:
            popen_retry_mount(hash_process, True)
            if hash_process.returncode != 0:
                self.logger.error("Error occurred while getting the HEAD.")
            stdout, _ = hash_process.communicate()
            return stdout.decode("utf-8").strip()
        except MaxRetryExceededException:
            self.logger.error("Max retry exceeded while getting the HEAD.")

    def get_active_plugins(self, repo):
        """Get active plugins."""
        try:
            plugins = []
            connector = DBConnector()
            plugin_list = list(
                connector.collection(Collections.CLS_CONFIGURATIONS).aggregate(
                    [
                        {"$unionWith": Collections.CRE_CONFIGURATIONS},
                        {"$unionWith": Collections.ITSM_CONFIGURATIONS},
                        {"$unionWith": Collections.CONFIGURATIONS},
                        {"$unionWith": Collections.GRC_CONFIGURATIONS},
                    ]
                )
            )
            for plugin in plugin_list:
                if plugin.get("plugin").split(".")[-3] == repo.name:
                    plugins.append(plugin.get("plugin").split(".")[-2])
        except Exception:
            self.logger.error(
                "Error occurred while loading active plugins.",
                details=traceback.format_exc(),
            )
        return plugins

    def validate_minimum_version(self, repo, package):
        """Validate minimum version."""
        manifest_path = os.path.join(self.get_dir(repo), package, "manifest.json")
        if not os.path.exists(manifest_path):
            return False
        manifest = json.load(open(manifest_path))
        if not manifest:
            return False

        if "minimum_version" in manifest:
            if manifest["minimum_version"] > str(__version__):
                return False

        if "minimum_provider_version" in manifest:
            provider_path = os.path.join(
                REPO_STORAGE_PATH,
                repo.name,
                manifest["provider_id"],
                "manifest.json",
            )
            provider_manifest = None
            if not os.path.exists(provider_path):
                return False
            provider_manifest = json.load(open(provider_path))
            if not provider_manifest:
                return False
            return manifest["minimum_provider_version"] <= provider_manifest["version"]

        return True

    def load_all_plugins(self, repo: PluginRepo) -> bool:
        """Load all the plugins from a repo that has already been cloned."""
        if os.path.exists(self.get_plugin_dir(repo)):
            self.logger.debug("Plugin directory already exists. Skipping copying.")
            return
        os.mkdir(self.get_plugin_dir(repo))  # create the dir to load plugins into
        active_plugins = self.get_active_plugins(repo)
        for package in os.listdir(self.get_dir(repo)):
            if package == ".git":
                continue
            if not os.path.isdir(os.path.join(self.get_dir(repo), package)):
                continue
            checkout = False
            is_valid = self.validate_minimum_version(repo, package)
            if not is_valid:
                if package in repo.plugins and package in active_plugins:
                    checkout = True
                    self._checkout(repo, repo.plugins[package])
                else:
                    continue

            shutil.copytree(
                os.path.join(self.get_dir(repo), package),
                os.path.join(self.get_plugin_dir(repo), package),
            )
            mappings = os.path.join(
                os.path.join(self.get_plugin_dir(repo), package),
                "mappings.json",
            )
            try:
                if os.path.exists(mappings):
                    self.import_mapping_file(mappings, repo.name)
            except Exception:
                self.logger.error(
                    "Error occurred while importing mapping file.",
                    details=traceback.format_exc(),
                    error_code="CE_1015",
                )
            self._add_plugin(repo, package, self._get_head_hash(repo))
            if checkout:
                self._checkout(repo, "-")
        with open(
            os.path.join(self.get_plugin_dir(repo), "__init__.py"), "w"
        ) as init_file:
            init_file.write('"""Auto generated."""\n')

    def get_dir(self, repo: PluginRepo) -> str:
        """Get repo directory path."""
        return os.path.join(REPO_STORAGE_PATH, repo.name)

    def get_plugin_dir(self, repo: PluginRepo) -> str:
        """Get path to the dir where plugins are stored and loaded from."""
        return os.path.join(PLUGIN_PATH, repo.name)

    def pull_updates(self, repo: PluginRepo) -> bool:
        """Fetch all the latest commits without applying them."""
        self.logger.debug(f"Fetching updates for plugin repo {repo.name}.")
        _ = self.reset_hard_to_head(repo)
        _ = self.clean_default_repo(repo)
        fetch_process = Popen(
            ["git", "pull", "origin"],
            cwd=self.get_dir(repo),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        try:
            popen_retry_mount(fetch_process, True)
        except MaxRetryExceededException:
            self.logger.error(
                f"Max retry exceeded while fetching updates for plugin repo {repo.name}"
            )
            return False
        if fetch_process.returncode != 0:
            _, stderr = fetch_process.communicate()
            self.logger.error(
                f"Error occurred while fetching updates for plugin repo {repo.name}.",
                details=stderr.decode("utf-8"),
                error_code="CE_1016",
            )
            _, stderr = fetch_process.communicate()
            self.logger.error(stderr.decode("utf-8"))
            return False
        self.logger.debug(f"Updates fetched successfully for plugin repo {repo.name}.")
        return True

    def delete_repo(self, repo: PluginRepo):
        """Delete a repo."""
        repo_dir, plugin_dir = self.get_dir(repo), self.get_plugin_dir(repo)
        # delete only if both repo and plugin dir exist
        if (
            os.path.exists(repo_dir)
            and os.path.isdir(repo_dir)
            and os.path.exists(plugin_dir)
            and os.path.isdir(plugin_dir)
        ):
            self.logger.debug(f"Deleting plugin repo directory for {repo.name}.")
            shutil.rmtree(repo_dir)
            shutil.rmtree(plugin_dir)
            self.repos.remove(repo)
            self.helper.refresh()
            self.logger.debug(
                f"Plugin repo directory for {repo.name} deleted successfully."
            )

    def get_diff(self, repo: PluginRepo):
        """Get diff between existing (loaded) and latest repo."""
        repo_path = self.get_dir(repo)
        plugin_path = self.get_plugin_dir(repo)
        changes = []
        for path in glob.glob(os.path.join(repo_path, "*", "main.py")):
            package = os.path.basename(os.path.dirname(path))
            if not os.path.exists(os.path.join(plugin_path, package)):
                if not PluginHelper.check_for_excluded_plugin(path):
                    # new plugin has been added
                    changes.append(
                        {
                            "status": PluginStatus.ADDED,
                            **self._load_plugin_info(repo, package, PluginStatus.ADDED),
                        }
                    )
        for path in glob.glob(os.path.join(plugin_path, "*", "main.py")):
            package = os.path.basename(os.path.dirname(path))
            if not os.path.exists(os.path.join(repo_path, package)):
                # a plugin has been now removed
                changes.append(
                    {
                        "status": PluginStatus.REMOVED,
                        **self._load_plugin_info(repo, package, PluginStatus.REMOVED),
                    }
                )
            else:
                existing_hashes = self._get_dir_hashes(
                    os.path.join(plugin_path, package)
                )
                previous_hashes = self._get_dir_hashes(os.path.join(repo_path, package))
                if len(existing_hashes) != len(previous_hashes) or set(
                    existing_hashes
                ) != set(previous_hashes):
                    if not PluginHelper.check_for_excluded_plugin(
                        os.path.join(plugin_path, package, "main.py")
                    ):
                        plugin_id = self._get_id(repo, package)
                        PluginClass = self.helper.find_by_id(plugin_id)
                        current_plugin_version = None
                        if PluginClass and PluginClass.metadata.get("version"):
                            current_plugin_version = PluginClass.metadata.get("version")
                        changes.append(
                            {
                                "status": PluginStatus.MODIFIED,
                                **self._load_plugin_info(
                                    repo,
                                    package,
                                    PluginStatus.MODIFIED,
                                    current_plugin_version,
                                ),
                            }
                        )
        return changes

    def update_plugin(self, repo: PluginRepo, plugin_id: str) -> PluginStatus:
        """Update individual plugin from a repo."""
        package = plugin_id.split(".")[-2]
        repo_path = os.path.join(self.get_dir(repo), package)
        plugin_path = os.path.join(self.get_plugin_dir(repo), package)
        status = None
        mappings = os.path.join(plugin_path, "mappings.json")
        if os.path.exists(repo_path) and not os.path.exists(plugin_path):
            # new plugin added
            shutil.copytree(repo_path, plugin_path)
            self.logger.debug(
                f"Adding new plugin with id={plugin_id} from repo {repo.name}."
            )
            status = PluginStatus.ADDED
            self._add_plugin(repo, package, self._get_head_hash(repo))
            try:
                if os.path.exists(mappings):
                    self.import_mapping_file(mappings, repo.name)
            except Exception:
                self.logger.error(
                    "Error occurred while importing mapping file.",
                    details=traceback.format_exc(),
                    error_code="CE_1015",
                )
        elif not os.path.exists(repo_path) and os.path.exists(plugin_path):
            # plugin removed
            shutil.rmtree(plugin_path)
            self.logger.debug(f"Removing plugin with id={plugin_id}.")
            status = PluginStatus.REMOVED
            self._remove_plugin(repo, package)
        elif os.path.exists(repo_path) and os.path.exists(
            plugin_path
        ):  # assume plugin has been modified; replace with new one
            shutil.rmtree(plugin_path)
            shutil.copytree(repo_path, plugin_path)
            self.logger.debug(
                f"Updating plugin with id={plugin_id} from repo {repo.name}."
            )
            status = PluginStatus.MODIFIED
            try:
                if os.path.exists(mappings):
                    self.import_mapping_file(mappings, repo.name)
            except Exception:
                self.logger.error(
                    "Error occurred while importing mapping file.",
                    details=traceback.format_exc(),
                    error_code="CE_1015",
                )
            self._add_plugin(repo, package, self._get_head_hash(repo))
        self.helper.refresh()
        return status

    def _load_plugin_info(
        self,
        repo: PluginRepo,
        package: str,
        status: PluginStatus,
        current_plugin_version: str = None,
    ) -> dict:
        """Load plugin details from manifest and return a dict."""
        changelog_path = None
        if status in [PluginStatus.ADDED, PluginStatus.MODIFIED]:
            manifest_path = os.path.join(self.get_dir(repo), package, "manifest.json")
            changelog_path = os.path.join(self.get_dir(repo), package, "CHANGELOG.md")
        if status == PluginStatus.REMOVED:
            manifest_path = os.path.join(
                self.get_plugin_dir(repo), package, "manifest.json"
            )
        out = {"id": self._get_id(repo, package), "changelog": None}
        out["category"] = None
        try:
            manifest = json.load(open(manifest_path))
            out["name"] = manifest["name"]
            out["version"] = manifest["version"]
            out["category"] = manifest.get("module")
            if "minimum_version" not in manifest.keys():
                out["minimum_version"] = None
            else:
                out["minimum_version"] = f"{str(manifest['minimum_version'])}"
            if "provider_id" in manifest:
                out["minimum_provider_version"] = manifest["minimum_provider_version"]
                out["provider_id"] = manifest["provider_id"]
                try:
                    provider_path = os.path.join(
                        self.get_plugin_dir(repo),
                        manifest["provider_id"],
                        "manifest.json",
                    )
                    provider_info = json.load(open(provider_path))
                    out["provider_version"] = provider_info["version"]
                except FileNotFoundError:
                    out["provider_version"] = None
            if changelog_path:
                with open(changelog_path, "r") as changelog_file:
                    out["changelog"] = changelog_file.read()
                if current_plugin_version:
                    from . import get_change_log_till_version

                    out["changelog"] = get_change_log_till_version(
                        out["changelog"], current_plugin_version, out["version"]
                    )
        except FileNotFoundError:
            pass
        except json.JSONDecodeError:
            self.logger.error(
                f"Error occurred while parsing manifest.json for {package}.",
                details=traceback.format_exc(),
                error_code="CE_1017",
            )
        except KeyError as err:
            out["error_msg"] = (
                f"Error occurred due to missing {err} field in manifest.json for {package}."
            )
            self.logger.error(
                f"Error occurred due to missing {err} field in manifest.json for {package}.",
                details=traceback.format_exc(),
                error_code="CE_1056",
            )
        if not out["category"]:
            out["category"] = self.helper.find_integration_by_id(out["id"]) or ""
            out["category"] = MODULES_MAP.get(out["category"].lower())
        return out

    def _get_dir_hashes(self, path: str) -> List[str]:
        """Get hashes of all the items in a directory."""
        proc_find = Popen(
            ["find", path, "-type", "f", "-not", "-path", "'*__pycache__*'"],
            stdout=PIPE,
        )
        proc_grep = Popen(
            ["grep", "-v", "__pycache__"], stdout=PIPE, stdin=proc_find.stdout
        )
        proc_hash = Popen(
            ["git", "hash-object", "--stdin-paths"],
            stdin=proc_grep.stdout,
            stdout=PIPE,
            stderr=PIPE,
        )
        proc_find.stdout.close()
        proc_grep.stdout.close()
        try:
            stdout, _ = popen_retry_mount(proc_hash, False)
            if proc_hash.returncode != 0:
                return []
            hashes = stdout.decode("utf-8").strip().split("\n")
            return hashes
        except MaxRetryExceededException:
            return []

    def _checkout(self, repo: PluginRepo, commit: str):
        proc_checkout = Popen(
            ["git", "checkout", commit], cwd=self.get_dir(repo), stdout=PIPE, stdin=PIPE
        )
        try:
            popen_retry_mount(proc_checkout, True)
        except MaxRetryExceededException:
            self.logger.error(f"Max retry exceeded while checking out commit {commit}")
            return False
        if proc_checkout.returncode != 0:
            self.logger.error(f"Error occurred while checking out commit {commit}.")
            return False
        return True

    def validate_git_credentials(self, repo: PluginRepo) -> bool:
        """Validate git credentials."""
        proc_update = Popen(
            ["git", "fetch"],
            cwd=self.get_dir(repo),
            stdout=PIPE,
            stderr=PIPE,
        )
        try:
            _, stderr = popen_retry_mount(proc_update, False)
        except MaxRetryExceededException:
            self.logger.error(
                f"Max retry exceeded while validating credentials for repo {repo.name}."
            )
        if proc_update.returncode != 0:
            self.logger.error(
                f"Error occrred while validating credentials for repo {repo.name}."
            )
            self.logger.error(stderr)
            return False
        return True

    def update(self, repo: PluginRepo, validate_creds=False) -> tuple[bool, bool]:
        """Update plugin repo."""
        from . import resolve_secret

        url = urlparse(str(repo.url))
        url = url._replace(
            netloc=f"{quote_plus(repo.username)}:{quote_plus(resolve_secret(repo.password))}@{url.netloc}"
        ).geturl()
        proc_update = Popen(
            ["git", "remote", "set-url", "origin", url],
            cwd=self.get_dir(repo),
            stdout=PIPE,
            stderr=PIPE,
        )
        try:
            _, stderr = popen_retry_mount(proc_update, False)
        except MaxRetryExceededException:
            self.logger.error(
                f"Max retry exceeded while updating origin for repo {repo.name}."
            )
        if proc_update.returncode != 0:
            self.logger.error(
                f"Error occrred while updating origin for repo {repo.name}."
            )
            self.logger.error(stderr)
            return False, False
        if validate_creds and not self.validate_git_credentials(repo):
            return True, False
        match = [r for r in self.repos if r.name == repo.name]
        if not match:
            return True, True
        match = match.pop()
        self.repos.remove(match)
        self.repos.append(repo)
        return True, True

    def _get_id(self, repo: PluginRepo, package: str) -> str:
        """Get plugin id."""
        return f"netskope.plugins.{repo.name}.{package}.main"

    def import_mapping_file(self, path: str, repo_name: str):
        """Import mapping files in database.

        Args:
            path (str): path of mapping file.
        """
        connector = DBConnector()
        f = open(path)
        mapping_file = json.load(f)
        if isinstance(mapping_file, dict):
            mapping_files = [mapping_file]
        if isinstance(mapping_file, list):
            mapping_files = mapping_file
        from ...integrations.cls.models.mappings import (
            MappingIn,
            MappingFileUpdate,
        )

        for data in mapping_files:
            file = connector.collection(Collections.CLS_MAPPING_FILES).find_one(
                {"name": data["name"].strip(), "repo": repo_name}
            )
            if file is None:
                connector.collection(Collections.CLS_MAPPING_FILES).insert_one(
                    MappingIn(
                        name=data["name"],
                        jsonData=data["jsonData"],
                        isDefault=data.get("isDefault", True),
                        repo=repo_name,
                        formatOptionsMapping=data.get("formatOptionsMapping", None),
                    ).model_dump()
                )
            else:
                if file["jsonData"] != data["jsonData"] or file.get(
                    "formatOptionsMapping"
                ) != data.get("formatOptionsMapping", None):
                    mappings = MappingFileUpdate(repo=repo_name, **data)
                    connector.collection(Collections.CLS_MAPPING_FILES).update_one(
                        {"name": mappings.name, "repo": mappings.repo},
                        {
                            "$set": {
                                "jsonData": mappings.jsonData,
                                "formatOptionsMapping": mappings.formatOptionsMapping,
                            }
                        },
                    )

    def switch_branch(self, repo: PluginRepo, branch: str) -> bool:
        """Switch the branch for given plugin repo.

        Args:
            repo (PluginRepo): Repo to switch the branch og.
            branch (str): Name of the branch.

        Returns:
            bool: Whether the switch was successful or not.
        """
        if type(branch) is not str:
            raise TypeError("Argument branch must be a str object.")
        switch_process = Popen(
            ["git", "checkout", branch],
            cwd=self.get_dir(repo),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        try:
            popen_retry_mount(switch_process, True)
        except MaxRetryExceededException:
            self.logger.error(
                f"Max retry exceeded while switching branch for plugin repo {repo.name}."
            )
            return False
        if switch_process.returncode != 0:
            self.logger.error(
                f"Error occurred while switching branch for plugin repo {repo.name}."
            )
            _, stderr = switch_process.communicate()
            self.logger.error(stderr.decode("utf-8"))
            return False
        shutil.rmtree(self.get_plugin_dir(repo))
        self.load_all_plugins(repo)
        return True

    @property
    def repos(self) -> List[PluginRepo]:
        """Get list of available repos."""
        return self._repos

    def reset_hard_to_head(self, repo: PluginRepo):
        """Reset the repo to the head."""
        reset_process = Popen(
            ["git", "reset", "--hard"],
            cwd=self.get_dir(repo),
            stdout=PIPE,
            stderr=PIPE,
        )
        try:
            popen_retry_mount(reset_process, True)
            if reset_process.returncode != 0:
                self.logger.error(f"Error occurred while resetting the HEAD for repo {repo.name}")
            stdout, _ = reset_process.communicate()
            return stdout.decode("utf-8").strip()
        except MaxRetryExceededException:
            self.logger.error(f"Max retry exceeded while resetting to the HEAD for repo {repo.name}")

    def clean_default_repo(self, repo: PluginRepo):
        """Reset the repo to the head."""
        reset_process = Popen(
            ["git", "clean", "-df"],
            cwd=self.get_dir(repo),
            stdout=PIPE,
            stderr=PIPE,
        )
        try:
            popen_retry_mount(reset_process, True)
            if reset_process.returncode != 0:
                self.logger.error(f"Error occurred while cleaning untracted files and directories for repo {repo.name}")
            stdout, _ = reset_process.communicate()
            return stdout.decode("utf-8").strip()
        except MaxRetryExceededException:
            self.logger.error(f"Max retry exceeded while cleaning untracted files and directories for repo {repo.name}")
