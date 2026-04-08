"""Provides plugin implementation related classes of CFC module."""

from typing import Dict, List, Union

from pydantic import BaseModel

from netskope.common.utils import PluginBase as CommonPluginBase
from netskope.integrations.cfc.models import (
    Action, ActionWithoutParams, DirectoryConfigurationMetadataOut)


class ValidationResult(BaseModel):
    """Validation result model (returned by Plugin.validate method)."""

    message: str
    success: bool = False
    data: Union[Dict, None] = None


class PushResult(BaseModel):
    """Push result model (returned by Plugin.push method)."""

    message: str
    mapping: Dict
    invalid_files: Union[List[Dict], None]
    success: bool = False


class PullResult(BaseModel):
    """Pull result model (returned by Plugin.pull method)."""

    metadata: List[Dict]
    success: bool = False


class PluginBase(CommonPluginBase):
    """CFC plugin base class."""

    integration = "cfc"

    def pull(self) -> PullResult:
        """Pull data to be hashes from 3rd party source and store metadata for CFC.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        raise NotImplementedError()

    def push(
        self,
        action_dict: Dict,
        hashes: List[Dict],
        mapping: Dict
    ) -> PushResult:
        """Push the CFC Hashes to Netskope Tenant.

        Args:
            action_dict (Dict): Action dictionary to be used while pushing CFC hashes.
            hashes: (List[Dict]): List of CFC hashes. Each hash dictionary contains \
                Hash File Name and Hash File Path.
            mapping: (Dict): Mapping dictionary of a sharing for which CFC hashes to be shared. \
            contains Business Rule name, Classifier Name, Classifier ID and training type

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        raise NotImplementedError()

    def validate_configuration_parameters(self) -> ValidationResult:
        """Validate the configuration parameters dict.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            ValidationResult: Result indicating validation outcome and message.
        """
        raise NotImplementedError()

    def validate_directory_configuration(self) -> ValidationResult:
        """Validate the directory configuration.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            ValidationResult: Directory configuration with verification result.
        """
        raise NotImplementedError()

    def fetch_images_metadata(self) -> DirectoryConfigurationMetadataOut:
        """Fetch images metadata for the plugin directory configuration.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            DirectoryConfigurationMetadataOut: Directory configuration metadata.
        """
        raise NotImplementedError()

    def get_actions(self) -> List[ActionWithoutParams]:
        """Get list of supported actions.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            List[ActionWithoutParams]: List of actions.
        """
        raise NotImplementedError()

    def get_action_fields(self, action: Action) -> List:
        """Get list of fields to be rendered in UI.

        Args:
            action (Action): Action object.

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

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            ValidationResult: Validation result object.
        """
        raise NotImplementedError()

    def get_fields(self, name: str, configuration: dict) -> List:
        """Get list of available fields in a dynamic step.

        Args:
            name (str): The name of the dynamic step for which to fetch fields.
            configuration (dict): The configuration input for the dynamic step.

        Raises:
            NotImplementedError: If the method is not implemented.

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
