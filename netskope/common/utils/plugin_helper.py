"""Provides a class for plugin related functionalities."""

import glob
import re
import sys
import traceback
from importlib import import_module
from os.path import dirname, join, sep
from typing import List, Dict

from netskope.common.utils import Logger, Singleton

from .plugin_base import PluginBase


class PluginHelper(metaclass=Singleton):
    """Singleton plugin helper class."""

    EXCLUDE_IMPORT_ERROR_REGEX = r"^.*netskope\.integrations\.(cre|grc)([\.'\"\s\t].*)?$"

    def __init__(self, plugin_pkg=None):
        """Initialize.

        Args:
            plugin_pkg (module): The module which contains all the plugin
            packages
        """
        self._pkg = [] if plugin_pkg is None else plugin_pkg
        self._modules = {}
        self._logger = Logger()
        self._exclude_import_error_matcher = re.compile(self.EXCLUDE_IMPORT_ERROR_REGEX)
        self.refresh()  # first time loading

    def refresh(self, cache=False):
        """Reload all the plugins.

        Use if a plugin is changed or new plugin added. Should be hooked up
        with an API endpoint to reload plugins on demand.
        """
        if cache:
            self._logger.debug("Skipping plugin refresh as cache is enabled.")
            return self
        # import all plugin packages
        PluginBase.plugins = {
            "cte": [],
            "itsm": [],
            "cls": [],
            "cre": [],
            "edm": [],
            "cfc": [],
            "provider": [],
        }
        PluginBase.supported_types = {
            "cte": {
                "alerts": set([]),
                "events": set([]),
            },
            "itsm": {
                "alerts": set([]),
                "events": set([]),
            },
            "cls": {
                "alerts": set([]),
                "events": set([]),
            },
            "cre": {
                "alerts": set([]),
                "events": set([]),
            },
        }
        for plugin in self._plugin_packages():
            try:
                try:
                    cur_module = plugin[: -len("main")]
                    for key in list(sys.modules.keys()):
                        if key.startswith(cur_module):
                            del sys.modules[key]
                    self._modules[plugin] = import_module(plugin)
                except ModuleNotFoundError as error:
                    if self._exclude_import_error_matcher.match(str(error)):
                        continue
                    raise error from error
                except ImportError as error:
                    if self._exclude_import_error_matcher.match(str(error)):
                        continue
                    raise error from error
            except Exception:
                self._logger.error(
                    f"Error occurred while importing plugin {plugin}.",
                    details=traceback.format_exc(),
                    error_code="CE_1013",
                )
        return self

    def _plugin_packages(self) -> List[str]:
        """Get list of all the packages that contains main.py.

        Returns:
            list: List of absolute package names.
        """
        # create a list of plugin package strings
        plugin_packages = []
        for package in self._pkg:
            if len(package.__name__.split(".")) == 3:
                plugin_packages.extend(
                    [
                        f"{package.__name__}." + ".".join(p.split(sep)[-2:])[:-3]
                        for p in glob.glob(join(dirname(package.__file__), "*", "main.py"))
                    ]
                )
            else:
                plugin_packages.extend(
                    [
                        f"{package.__name__}." + "main"
                    ]
                )
        return list(set(plugin_packages))

    def find_by_id(self, _id, validate=False) -> PluginBase:
        """Get the plugin class by name."""
        try:
            for integration in self.plugins:
                for plugin in self.plugins[integration]:
                    if plugin.__module__ == _id:
                        return plugin
            repo = _id.split(".")[-3]
            plugin = _id.split(".")[-2]
            self.add_packages([import_module(f"netskope.plugins.{repo}.{plugin}")])
            self.refresh()
            for integration in self.plugins:
                for plugin in self.plugins[integration]:
                    if plugin.__module__ == _id:
                        return plugin
        except ModuleNotFoundError:
            if validate:
                raise
        return None

    def find_integration_by_id(self, _id) -> str:
        """Get the plugin class by name."""
        for integration in self.plugins:
            for plugin in self.plugins[integration]:
                if plugin.__module__ == _id:
                    return integration
        return None

    def add_packages(self, packages):
        """Add packages to the internal plugin package list."""
        for package in packages:
            if package not in self._pkg:
                self._pkg.append(package)
        return self

    @property
    def plugins(self) -> List[PluginBase]:
        """Get a list of all mounted plugins.

        Returns:
            list: List of all the mounted plugin classes.
        """
        return PluginBase.plugins

    @property
    def supported_types(self) -> Dict[str, List]:
        """Get a list of all mounted plugins.

        Returns:
            list: List of all the mounted plugin classes.
        """
        return PluginBase.supported_types

    @staticmethod
    def is_syslog_service_plugin(plugin_name: str) -> bool:
        """Check if plugin is syslog service / Cloud Exchange logs plugin."""
        pattern = r"^netskope\.plugins\..+\.syslog_service\.main$"
        return re.match(pattern, plugin_name) is not None

    @staticmethod
    def is_netskope_provider_plugin(plugin_name: str) -> bool:
        """Check if plugin is Netskope Tenant plugin."""
        pattern = r"^netskope\.plugins\..+\.netskope_provider\.main$"
        return re.match(pattern, plugin_name) is not None

    @staticmethod
    def check_plugin_name_with_regex(plugin_name: str, plugin_id: str) -> bool:
        """Check if plugin name matches the regex."""
        pattern = rf"^netskope\.plugins\..+\.{plugin_name}\.main$"
        return re.match(pattern, plugin_id) is not None

    @staticmethod
    def check_for_excluded_plugin(plugin_path: str) -> bool:
        """Check if plugin is excluded."""
        compiler = re.compile(PluginHelper.EXCLUDE_IMPORT_ERROR_REGEX)
        excluded = False
        with open(plugin_path, "r") as plugin_file:
            for line in plugin_file.readlines():
                if compiler.match(line):
                    excluded = True
                    break
        return excluded
