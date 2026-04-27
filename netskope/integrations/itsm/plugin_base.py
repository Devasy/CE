"""Provides plugin implementation related classes."""
from typing import List, Dict, Union
from pydantic import BaseModel, Field

from .models import Alert, Event
from .models.custom_fields import CustomFieldsSectionWithMappings

from netskope.common.utils import PluginBase as CommonPluginBase

from netskope.integrations.itsm.models import FieldMapping, Task, Queue


class ValidationResult(BaseModel):
    """Validation result model (returned by Plugin.validate_step method)."""

    message: str
    success: bool = False


class PushResult(BaseModel):
    """Push result model (returned by Plugin.push method)."""

    message: str
    success: bool = False
    results: Dict[str, List[Task]]


class MappingField(BaseModel):
    """Mapping field model."""

    label: str = Field(...)
    value: str = Field(...)
    updateAble: bool = Field(False)


class PluginBase(CommonPluginBase):
    """ITSM plugin base class."""

    integration = "itsm"

    def pull(self) -> List:
        """Pull indicators from 3rd party source.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            List[Indicator]: List of indicators to be stored on the platform.
        """
        raise NotImplementedError()

    def validate_step(
        self, name: str, configuration: dict
    ) -> ValidationResult:
        """Validate given step from the configuration.

        Args:
            name (str): Name of the step.
            configuration (dict): The entire configuration dictionary.

        Returns:
            ValidationResult: Validation result.
        """
        raise NotImplementedError()

    def pull_alerts(self, query: str = "") -> Union[List[Alert], List[Event]]:
        """Get list of alerts or events from the platform.

        Returns:
            Union[List[Alert], List[Event]]: List of alerts/events fetched.
        """
        raise NotImplementedError()

    def create_task(
        self,
        alert: Union[Alert, Event],
        mappings: Dict,
        queue: Queue
    ) -> Task:
        """Create task from the alert or event.

        Args:
            alert (Union[Alert, Event]): Alert/Event.
        """
        raise NotImplementedError()

    def update_task(
        self,
        task: Task,
        alert: Union[Alert, Event],
        mappings: Dict,
        queues: Queue,
        upsert_task: bool = False
    ) -> Task:
        """Update task from the alert or event.

        Args:
            alert (Union[Alert, Event]): Alert/Event.
        """
        raise NotImplementedError()

    def sync_states(self, tasks: List[Task]) -> List[Task]:
        """Sync states of tasks."""
        raise NotImplementedError()

    def sync_incidents(self, incidents: List[Task]) -> PushResult:
        """Sync incidents back to platform."""
        raise NotImplementedError()

    def get_available_fields(self, configuration: dict) -> List[MappingField]:
        """Get the list of all the available fields for mapping.

        Returns:
            List[MappingField]: List of mapping fields.
        """
        raise NotImplementedError()

    def get_default_mappings(self, configuration: dict) -> Dict[str, List[FieldMapping]]:
        """Get default mappings for a configuration.

        Returns:
            List[FieldMapping]: List of field mappings.
        """
        raise NotImplementedError()

    def get_queues(self) -> List[Queue]:
        """Get list of all queues.

        Returns:
            List[str]: List of queues.
        """
        raise NotImplementedError()

    def get_fields(self, name: str, configuration: dict) -> List:
        """Get list of available fields in a dynamic step.

        Returns:
            List: List of fields.
        """
        raise NotImplementedError()

    def process_webhooks(self, query_params: dict, headers: dict, body: bytes) -> tuple:
        """Process incoming webhook requests.

        Args:
            query_params (dict): Incoming Query parameters.
            headers (dict): Request headers.
            body (bytes): Request body.

        Returns:
            List[Task]: List of task.
            Response: Webhook response.
        """
        raise NotImplementedError()

    def get_default_custom_mappings(self) -> list[CustomFieldsSectionWithMappings]:
        """
        Get default custom field mappings with values for this plugin.

        Returns:
            list[CustomFieldsSectionWithMappings]: List of sections with
            field-to-value mappings
        """
        raise NotImplementedError()
