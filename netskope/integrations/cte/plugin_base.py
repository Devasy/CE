"""Provides plugin implementation related classes."""

from typing import Dict, List, Tuple
from pydantic import BaseModel

from netskope.common.utils import PluginBase as CommonPluginBase

from .models.indicator import Indicator
from .models.business_rule import ActionWithoutParams, Action


class PluginMount(type):
    """Meta class that registers all the plugin mounts."""

    def __init__(cls, name, base, attrs):
        """Initialize."""
        if not hasattr(cls, "plugins"):
            cls.plugins = []
        else:
            if (
                cls.load_metadata()
            ):  # add only if manifest is parsed successfully
                cls.plugins.append(cls)


class ValidationResult(BaseModel):
    """Validation result model (returned by Plugin.validate method)."""

    message: str
    success: bool = False
    disabled: bool = False


class PushResult(BaseModel):
    """Push result model (returned by Plugin.push method)."""

    message: str
    already_shared: bool = False
    success: bool = False
    should_run_cleanup: bool = False
    failed_iocs: list = []


class PluginBase(CommonPluginBase):
    """CTE plugin base class."""

    integration = "cte"

    def __init__(
        self,
        name,
        configuration,
        storage,
        last_run_at,
        logger,
        use_proxy=True,
        ssl_validation=True,
    ):
        """Initialize."""
        super().__init__(
            name,
            configuration,
            storage,
            last_run_at,
            logger,
            use_proxy=use_proxy,
            ssl_validation=ssl_validation,
        )
        self._sub_checkpoint = None

    @property
    def sub_checkpoint(self):
        """Get sub-checkpoint."""
        return self._sub_checkpoint

    @sub_checkpoint.setter
    def sub_checkpoint(self, value):
        """Set sub-checkpoint."""
        self._sub_checkpoint = value

    def pull(self) -> List[Indicator]:
        """Pull indicators from 3rd party source.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            List[Indicator]: List of indicators to be stored on the platform.
        """
        raise NotImplementedError()

    def push(
        self,
        indicators,
        action_dict: Dict,
        source: str = None,
        business_rule: str = None,
        plugin_name: str = None
    ):
        """Push the indicators to the 3rd party platform.

        Args:
            indicators (generator): Generator of indicators to be pushed.
            action : Action dictionary to be used while pushing iocs.
            source: Source configuration name of indicator.
            business_rule: Qualified business rule name.
            plugin_name: Integration Name.

        Raises:
            NotImplementedError: If the method is not implemented.
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

    def get_actions(self) -> List[ActionWithoutParams]:
        """Get list of supported actions.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            List[Action]: List of actions.
        """
        raise NotImplementedError()

    def get_action_fields(self, action: Action) -> List:
        """Get list of fields to be rendered in UI.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            List: List of fields to be rendered.
        """
        raise NotImplementedError()

    def validate_action(self, action: Action) -> ValidationResult:
        """Validate action parameters.

        Args:
            action (Action): Action object.

        Returns:
            ValidationResult: Validation result object.
        """
        raise NotImplementedError()

    def get_modified_indicators(self, indicators) -> Tuple[List[str], bool]:
        """Return retracted indicators from platform.

        Args:
            indicators (generator): Generator of indicators to be checked for retraction.

        Raises:
            NotImplementedError: _description_

        Returns:
            List[str]: List of retracted indicators values.
            to be marked on the platform.
        """
        raise NotImplementedError()

    def retract_indicators(self, indicators, action_config_list: List[Action]) -> ValidationResult:
        """Retract IoC(s) from plugin.

        Args:
            indicators (generator): Generator of indicators to be retracted.

        Returns:
            ValidationResult: Validation result object.
        """
        raise NotImplementedError()

    def run_action_cleanup(self) -> ValidationResult:
        """Validate action parameters.

        Args:
            action (Action): Action object.

        Returns:
            ValidationResult: Validation result object.
        """
        raise NotImplementedError()
