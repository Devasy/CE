"""Provides plugin implementation related classes."""

from typing import List, Union
from pydantic import BaseModel

from netskope.common.utils import PluginBase as CommonPluginBase


class PluginMount(type):
    """Meta class that registers all the plugin mounts."""

    def __init__(cls, name, base, attrs):
        """Initialize."""
        if not hasattr(cls, "plugins"):
            cls.plugins = []
        else:
            if cls.load_metadata():  # add only if manifest is parsed successfully
                cls.plugins.append(cls)


class ValidationResult(BaseModel):
    """Validation result model (returned by Plugin.validate method)."""

    message: str
    success: bool = False
    checkpoint: Union[dict, None]


class PushResult(BaseModel):
    """Push result model (returned by Plugin.push method)."""

    message: str
    already_shared: bool = False
    success: bool = False


class PluginBase(CommonPluginBase):
    """CTE plugin base class."""

    integration = "provider"

    def pull(self, data_type, sub_types) -> List:
        """Pull the data_type and it sub_types from the tenant.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            List: List of data to be stored on the platform.
        """
        raise NotImplementedError()

    def validate(self, configuration: dict) -> ValidationResult:
        """Validate the configuration parameters dict.

        Args:
            configuration (dict): Dictionary containing all the requested parameters.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            ValidationResult: Result indicating validation outcome and message.
        """
        raise NotImplementedError()

    def cleanup(self) -> None:
        """Remove all related dependencies of the record before its deletion, ensuring data integrity.

        Args:
            None

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            None
        """
        raise NotImplementedError()

    def share_analytics_in_user_agent(self, tenant_name: str, user_agent_analytics: str, analytics_type: str) -> bool:
        """Share analytics to the tenant using the share_analytics_in_user_agent method.

        Args:
            tenant_name (str): Name of the tenant.
            user_agent_analytics (str): Analytics to be shared.
            analytics_type (str): Type of analytics to be shared.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            bool: Result of the operation.
        """
        raise NotImplementedError()

    def parse_data(self, events: bytes, data_type: str, sub_type: str):
        """Parse pulled data."""
        raise NotImplementedError()
