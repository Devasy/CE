"""Business rule related models."""

from datetime import datetime
from typing import Annotated, Optional, Union

from pydantic import BaseModel, Field, StringConstraints, field_validator

from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    SecretDict,
)

from .configurations import ConfigurationDB

connector = DBConnector()
helper = PluginHelper()
logger = Logger()


def validate_actions(cls, v: dict, values, **kwargs):
    """Validate action configurations exist."""
    values = values.data
    if v is None:
        return
    if "name" not in values:
        raise ValueError("name is required.")
    previous = connector.collection(Collections.CREV2_BUSINESS_RULES).find_one(
        {"name": values["name"]}
    )
    for key, actions in v.items():
        if (
            actions
            and connector.collection(
                Collections.CREV2_CONFIGURATIONS
            ).find_one({"name": key})
            is None
        ):
            raise ValueError(f"Configuration {key} does not exist.")
        for action in actions:
            if action.model_dump() not in previous["actions"].get(
                key, []
            ):  # new or updated
                config = ConfigurationDB(
                    **connector.collection(
                        Collections.CREV2_CONFIGURATIONS
                    ).find_one({"name": key})
                )
                PluginClass = helper.find_by_id(config.plugin)  # NOSONAR
                plugin = PluginClass(
                    config.name,
                    SecretDict(config.parameters),
                    config.storage,
                    config.checkpoints,
                    logger,
                )
                if config.mappedEntities:
                    plugin.mappedEntities = [mapped_entity.model_dump() for mapped_entity in config.mappedEntities]
                result = plugin.validate_action(action)
                if not result.success:
                    raise ValueError(result.message)
    return v


class EntityFilters(BaseModel):
    """Entity related filters."""

    query: str = Field("")
    mongo: str = Field("{}")


class Action(BaseModel):
    """Action model."""

    label: str
    value: str
    parameters: dict = Field({})
    generateAlert: bool = Field(False)
    performLater: bool = Field(False)
    requireApproval: bool = Field(False)
    performRevert: Optional[bool] = False


class ActionWithoutParams(BaseModel):
    """Action model."""

    label: str
    value: str


class BusinessRuleIn(BaseModel):
    """Business rule creation model."""

    name: Annotated[str, StringConstraints(strip_whitespace=True)]

    @field_validator("name")
    @classmethod
    def _validate_name_is_unique(cls, v: str):
        """Validate that name is unique."""
        v = v.strip()
        if (
            connector.collection(Collections.CREV2_BUSINESS_RULES).find_one(
                {"name": v}
            )
            is not None
        ):
            raise ValueError(
                "A business rule with the same name already exists."
            )
        return v

    entity: str

    @field_validator("entity")
    @classmethod
    def _validate_entity(cls, v: str):
        """Validate that vlaue is a valid entity type."""
        if (
            connector.collection(Collections.CREV2_ENTITIES).find_one(
                {"name": v}
            )
            is None
        ):
            raise ValueError(f"Entity with name '{v}' does not exist.")
        return v

    entityFilters: EntityFilters = Field(EntityFilters())
    actions: dict[str, list[Action]] = Field(dict())

    @field_validator("actions")
    @classmethod
    def overwrite_actions(cls, v: dict):
        """Overwrite actions while creation."""
        return {}

    muted: bool = Field(False)

    @field_validator("muted")
    @classmethod
    def validate_not_muted(cls, v):
        """Validate that the rule is not muted."""
        if v is True:
            raise ValueError("Can not create a muted business rule.")
        return v

    unmuteAt: Union[datetime, None] = Field(None)


class BusinessRuleUpdate(BaseModel):
    """Business rule update model."""

    name: str

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str):
        """Validate that name is unique."""
        if (
            connector.collection(Collections.CREV2_BUSINESS_RULES).find_one(
                {"name": v}
            )
            is None
        ):
            raise ValueError("A business rule with the name does not exist.")
        return v

    entity: str = Field(None)
    entityFilters: EntityFilters = Field(None)
    actions: dict[str, list[Action]] = Field(None)
    _validate_actions = field_validator("actions")(validate_actions)
    muted: Union[bool, None] = Field(None)
    unmuteAt: Union[datetime, None] = Field(None)


class BusinessRuleOut(BaseModel):
    """Business rule out model."""

    name: str
    entity: str
    entityFilters: EntityFilters
    actions: dict[str, list[Action]] = Field(dict())
    muted: Union[bool, None] = Field(None)
    unmuteAt: Union[datetime, None] = Field(None)


class BusinessRuleDB(BaseModel):
    """Business rule database model."""

    name: str
    entity: str
    entityFilters: EntityFilters
    actions: dict[str, list[Action]] = Field(dict())
    lastEvals: list[str] = Field([])
    muted: bool = Field(False)
    unmuteAt: Union[datetime, None] = Field(None)


class BusinessRuleDelete(BaseModel):
    """Business rule delete model."""

    name: str
