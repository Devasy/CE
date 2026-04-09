"""CREv2 plugin base."""
from pydantic import BaseModel
from typing import Optional, List


from .models.entities import EntityFieldType
from .models.business_rules import Action, ActionWithoutParams
from netskope.common.utils import PluginBase as CommonPluginBase


class ValidationResult(BaseModel):
    """Validation result model (returned by Plugin.validate method)."""

    message: str
    success: bool = False


class ActionResult(BaseModel):
    """Action result model (returned by Plugin.execute_actions method)."""

    message: str
    success: bool = False
    failed_action_ids: Optional[List[object]] = []


class EntityField(BaseModel):
    """Plugin entity field model."""

    name: str
    type: EntityFieldType
    required: bool = False


class Entity(BaseModel):
    """Plugin entity model."""

    name: str
    fields: list[EntityField]


class PluginBase(CommonPluginBase):
    """CREv2 plugin base class."""

    integration = "cre"

    def get_entities(self) -> list[Entity]:
        """Get all entities."""
        raise NotImplementedError

    def validate(self, configuration: dict) -> ValidationResult:
        """Validate the provided configuration."""
        raise NotImplementedError

    def get_fields(self, step: str, configuration: dict) -> list:
        """Get list of available fields in a dynamic step."""
        raise NotImplementedError

    def fetch_records(self, entity: str) -> list:
        """Pull records from 3rd party source."""
        raise NotImplementedError

    def update_records(self, entity: str, records: list[dict]) -> list:
        """Update the given records."""
        raise NotImplementedError

    def get_actions(self) -> list[ActionWithoutParams]:
        """Get list of actions."""
        return []

    def get_action_params(self, action) -> list:
        """Get list of action parameters."""
        return []

    def validate_action(self, action: Action) -> ValidationResult:
        """Validate action parameters."""
        raise NotImplementedError

    def execute_action(self, action: Action):
        """Execute an action with the given paramters."""
        raise NotImplementedError

    def execute_actions(self, actions: list[Action]) -> Optional[ActionResult]:
        """Execute the actions with the given parameters.

        Returns:
            Optional[ActionResult]: Result with partial success information.
                                  If None, all actions are considered successful.
        """
        raise NotImplementedError

    def revert_action(self, action: Action):
        """Execute an action with the given paramters."""
        raise NotImplementedError
