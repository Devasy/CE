"""Provides plugin implementation related classes."""

import base64
import inspect
import json
import traceback
from datetime import datetime
from os.path import dirname, isfile, join

from netskope.common.models.other import ActionType
from netskope.common.utils import DBConnector, Logger, Notifier, Collections
from netskope.common.utils.proxy import get_proxy_params
from netskope.common.models.settings import SettingsDB

connector = DBConnector()


def get_proxy() -> dict:
    """Get proxy dict."""
    settings = connector.collection(Collections.SETTINGS).find_one({})
    return get_proxy_params(SettingsDB(**settings))


class PluginMount(type):
    """Meta class that registers all the plugin mounts."""

    def __init__(cls, name, base, attrs):
        """Initialize."""
        if not hasattr(cls, "plugins"):
            cls.plugins = {
                "cte": [],
                "itsm": [],
                "cls": [],
                "cre": [],
                "provider": [],
                "edm": [],
                "cfc": [],
            }
            cls.supported_types = {
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
        else:
            if cls.load_metadata():  # add only if manifest is parsed successfully
                cls.plugins[cls.integration].append(cls)
                if cls.integration in cls.supported_types and hasattr(
                    cls, "supported_subtypes"
                ):
                    cls.supported_types[cls.integration][
                        "alerts"
                    ] = cls.supported_types[cls.integration]["alerts"].union(
                        set(cls.supported_subtypes.get("alerts", []) or [])
                    )
                    cls.supported_types[cls.integration][
                        "events"
                    ] = cls.supported_types[cls.integration]["events"].union(
                        set(cls.supported_subtypes.get("events", []) or [])
                    )


class PluginBase(metaclass=PluginMount):
    """Base plugin class."""

    def __init__(
        self,
        name,
        configuration,
        storage,
        last_run_at,
        logger,
        use_proxy=True,
        ssl_validation=True,
        data=None,
        data_type=None,
        sub_type=None,
        plugin_type=None,
    ):
        """Initialize.

        Args:
            name (str): Plugin configuration name.
            configuration (dict): Configuration dictionary.
            storage (dict): Storage dictionary.
            last_run_at (datetime): Last run checkpoint
            logger (Logger): An initialized logger object.
        """
        self._storage = storage
        self._configuration = configuration
        self._last_run_at = last_run_at
        self._logger = logger
        self._notifier = Notifier()
        self._ssl_validation = ssl_validation
        self._use_proxy = use_proxy
        self._name = name
        self._data = data
        self._data_type = data_type
        self._sub_type = sub_type
        self._plugin_type = plugin_type

    @classmethod
    def load_metadata(cls):
        """Load the metadata of a plugin.

        Returns:
            bool: Indicates if the metadata was loaded successfully or not.
        """
        try:
            inspect_module = inspect.getmodule(cls)
            repo = inspect_module.__package__.split(".")[2]
            plugin_dir = dirname(inspect_module.__file__)
            if not hasattr(cls, "metadata"):  # if it has not already been loaded
                manifest_path = join(plugin_dir, "manifest.json")
                if not isfile(manifest_path):
                    return False
                cls.metadata = json.load(  # load from the manifest.json file
                    open(manifest_path)
                )
                cls.metadata["repo_name"] = repo
                # load the plugin
                with open(join(plugin_dir, "icon.png"), "rb") as icon_file:
                    icon = base64.b64encode(icon_file.read())
                    cls.metadata["icon"] = icon
                if cls.metadata.get("netskope", False):
                    supported_types = cls.metadata.get("supported_subtypes")
                    if supported_types and (
                        supported_types.get("alerts") or supported_types.get("events")
                    ):
                        setattr(cls, "supported_subtypes", supported_types)
            return True
        except Exception:
            logger = Logger()
            logger.error(
                "Error while loading plugin. Could not parse manifest.",
                details=traceback.format_exc(),
                error_code="CE_1012",
            )
            return False

    @property
    def logger(self) -> Logger:
        """Logger object.

        Returns:
            cte.utils.logger.Logger: Initialized logger object.
        """
        return self._logger

    @property
    def storage(self) -> dict:
        """Get the persistent storage for the configuration.

        Returns:
            dict: Storage dictionary.
        """
        return self._storage

    @property
    def configuration(self) -> dict:
        """Get the configuration.

        Returns:
            dict: The dictionary containing configuration parameters.
        """
        return self._configuration

    @property
    def last_run_at(self) -> datetime:
        """Get the last run checkpoint.

        Returns:
            datetime: Time indicating the last successful run of the pull method.
        """
        return self._last_run_at

    @last_run_at.setter
    def last_run_at(self, last_run_at: datetime):
        """Set the last run checkpoint.

        Args:
            last_run_at (datetime): datetime indicating the successful run time.
        """
        self._last_run_at = last_run_at

    @property
    def proxy(self):
        """Get the proxy dictionary."""
        # Returning proxy from database.
        # settings from environment variables
        # (https://requests.readthedocs.io/en/latest/user/advanced/#proxies)
        proxy = get_proxy()
        if not proxy.get("http"):
            proxy["http"] = ""
        if not proxy.get("https"):
            proxy["https"] = ""
        return proxy

    @property
    def notifier(self) -> Notifier:
        """Get the notifier object."""
        return self._notifier

    @property
    def name(self) -> str:
        """Get plugin configuration name."""
        return self._name

    @property
    def plugin_type(self) -> str:
        """Get plugin type."""
        return self._plugin_type

    @property
    def ssl_validation(self) -> bool:
        """Get the SSL cert validation flag..

        Returns:
            bool: Boolean indicating whether SSL certificate validation is enabled or not.
        """
        return self._ssl_validation

    @ssl_validation.setter
    def ssl_validation(self, ssl_validation: bool):
        """Set the SSL cert validation flag.

        Args:
            ssl_validation (bool): Boolean to enable/disbale SSL certificate validation.
        """
        self._ssl_validation = ssl_validation

    @property
    def use_proxy(self) -> bool:
        """Use proxy getter."""
        return self._use_proxy

    @use_proxy.setter
    def use_proxy(self, use_proxy: bool) -> None:
        """Use proxy setter, always set to True."""
        self._use_proxy = True

    @property
    def data(self) -> list:
        """Get the data.

        Returns:
            list: List of data returned from the pull method.
        """
        if isinstance(self._data, bytes):
            from netskope.common.utils import parse_events
            self._data = parse_events(self._data)
        return self._data.get("result", []) if isinstance(self._data, dict) else self._data

    @data.setter
    def data(self, data: list):
        """Set the list of data.

        Args:
            data (list): list of data.
        """
        self._data = data

    @property
    def data_type(self) -> str:
        """Get the data type.

        Returns:
            str: data type.
        """
        return self._data_type

    @data_type.setter
    def data_type(self, data_type: str):
        """Set the data type.

        Args:
            data type (list): data type.
        """
        self._data_type = data_type

    @property
    def sub_type(self) -> str:
        """Get the sub type.

        Returns:
            list: sub type.
        """
        return self._sub_type

    @sub_type.setter
    def sub_type(self, sub_type: str):
        """Set the sub type.

        Args:
            data (list): sub type.
        """
        self._sub_type = sub_type

    @classmethod
    def get_dynamic_fields(self) -> list:
        """Pull fields from plugins."""
        raise NotImplementedError

    def cleanup(self, action_type: str = ActionType.DELETE.value):
        """Cleanup the plugin."""
        raise NotImplementedError()

    def parse_data(self, events: bytes, data_type: str, sub_type: str):
        """Parse pulled data."""
        raise NotImplementedError()
