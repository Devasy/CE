"""Provides plugin implementation related classes."""

from typing import Dict, List, Optional
from pydantic import BaseModel

from netskope.common.utils import PluginBase as CommonPluginBase

from .models.business_rule import ActionWithoutParams, Action


class ValidationResult(BaseModel):
    """Validation result model (returned by Plugin.validate method)."""

    message: str
    success: bool = False


class PushResult(BaseModel):
    """Push result model (returned by Plugin.push method)."""

    success: bool = False
    apply_success: bool = False
    message: str
    file_id: Optional[str] = None
    upload_id: Optional[str] = None


class PluginBase(CommonPluginBase):
    """EDM plugin base class."""

    integration = "edm"

    def pull(self):
        """Pull data to be hashed from 3rd party source, sanitises it and hash it.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        raise NotImplementedError()

    def push(self, source_config_name: str = None, action_dict: Dict = {}) -> PushResult:
        """Push the EDM Hashed data to Netskope EDM.

        Args:
            source_config_name (str): Source configuration name.
            action : Action dictionary to be used while pushing iocs.

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

    def get_fields(self, name: str, configuration: dict) -> List:
        """Get list of available fields in a dynamic step.

        Args:
            name (str): The name of the dynamic step for which to fetch fields.
            configuration (ConfigurationIn): The configuration input for the dynamic step.

        Returns:
            List: List of fields.
        """
        raise NotImplementedError()

    def validate_step(
        self, name: str
    ) -> ValidationResult:
        """Validate given step from the configuration.

        Args:
            name (str): Name of the step.

        Returns:
            ValidationResult: Validation result.
        """
        raise NotImplementedError()

    def pull_sample_data(self) -> Dict:
        """Pull sample data from 3rd party source.

        Returns:
            Dict: Sample data.
        """
        raise NotImplementedError()
