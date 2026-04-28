"""Business rule related schemas."""
import json
from datetime import datetime
from typing import List, Dict, Union, Optional
from pydantic import field_validator, StringConstraints, BaseModel, Field
from jsonschema import validate, ValidationError
from netskope.common.utils import (
    DBConnector,
    Collections,
    PluginHelper,
    Logger,
    SecretDict,
)
from netskope.common.models import TenantDB
from ..utils.schema import INDICATOR_QUERY_SCHEMA
from . import ConfigurationDB
from typing_extensions import Annotated


connector = DBConnector()
helper = PluginHelper()
logger = Logger()


def validate_same_destination_config(
    current_source,
    source,
    destination,
    current_business_rule,
    rule_name,
    nsconfig_name,
    current_action,
    actions,
    tenant_name
):
    """
    Check if two business rules have the same destination configuration.

    Args:
    - current_source (str): The source configuration of the current rule.
    - source (str): The source configuration of the rule to check.
    - destination (str): The name of the destination configuration.
    - current_business_rule (str): The name of the current rule.
    - rule_name (str): The rule_name to check.
    - nsconfig_name (str): The name of the Netskope configuration.
    - current_action (Action): The action of the current rule.
    - actions (list[dict]): The actions of the rule to check.
    - tenant_name (str): The name of the tenant.

    Returns:
    - bool: If the two rules have the same destination configuration, it returns True, otherwise False.
    """
    config = connector.collection(Collections.CONFIGURATIONS).find_one(
        {"name": destination}
    )
    PluginClass = helper.find_by_id(config.get("plugin"))  # NOSONAR
    plugin = PluginClass(
        config.get("name"),
        SecretDict(config.get("parameters")),
        {},
        config.get("checkpoint"),
        logger,
    )
    if config and plugin.metadata.get("netskope", False):
        unique_dest_keys = {
            "url": {
                "unique_key": "list",
                "fallback": "create",
                "fallback_key": "name"
            },
            "file": {
                "unique_key": "file_list"
            },
            "private_app": {
                "unique_key": "private_app_name",
                "fallback": "create",
                "fallback_key": "name"
            }
        }
        if connector.collection(Collections.NETSKOPE_TENANTS).find_one(
            {"name": config.get("tenant")}
        ).get("parameters", {}).get("tenantName").strip().strip(
            "/"
        ) == tenant_name.strip().strip(
            "/"
        ):
            if (
                source == current_source
                and current_business_rule == rule_name
                and nsconfig_name == destination
            ):
                return False
            for action in actions:
                if isinstance(action, Action):
                    action = action.model_dump()
                if current_action.value == action.get("value"):
                    unique_keys = unique_dest_keys.get(
                        current_action.value, {}
                    )
                    unique_key = unique_keys.get("unique_key")
                    fallback = unique_keys.get("fallback")
                    fallback_key = unique_keys.get("fallback_key")
                    current_action_dest_value = (
                        current_action.parameters.get(unique_key)
                        if current_action.parameters.get(unique_key) != fallback
                        else current_action.parameters.get(fallback_key)
                    )
                    other_action_dest_value = (
                        action.get("parameters", {}).get(unique_key)
                        if action.get("parameters", {}).get(unique_key) != fallback
                        else action.get("parameters", {}).get(fallback_key)
                    )
                    if current_action_dest_value == other_action_dest_value:
                        return True
    return False


def is_destination_same(
    current_action: object,
    current_source: str,
    current_business_rule: str,
    current_sharings: dict,
    nsconfig_name: str,
    tenant_name: str
):
    """Validate if two actions has same tenant destination."""
    for rule in connector.collection(Collections.CTE_BUSINESS_RULES).find({}):
        sharings = rule.get("sharedWith", {})
        if rule.get("name") == current_business_rule:
            sharings = current_sharings
        if not sharings:
            continue
        for source, dest_dict in sharings.items():
            for destination, actions in dest_dict.items():
                result = validate_same_destination_config(
                    current_source,
                    source,
                    destination,
                    current_business_rule,
                    rule.get("name"),
                    nsconfig_name,
                    current_action,
                    actions,
                    tenant_name
                )
                if result is True:
                    return True
    return False


def validate_sharedWith(cls, v: Dict[str, List[str]], values, **kwargs):
    """Validate action configurations exist."""
    values = values.data
    if v is None:
        return
    if "name" not in values:
        raise ValueError("name is required.")
    previous = connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
        {"name": values["name"]}
    )
    for source, dest_dict in v.items():
        for key, actions in dest_dict.items():
            if (
                actions
                and connector.collection(Collections.CONFIGURATIONS).find_one(
                    {"name": key}
                )
                is None
            ):
                raise ValueError(f"CTE configuration {key} does not exist.")
            for action in actions:
                if action.model_dump() not in (previous["sharedWith"].get(source, {})).get(
                    key, []
                ):  # new or updated
                    config = ConfigurationDB(
                        **connector.collection(Collections.CONFIGURATIONS).find_one(
                            {"name": key}
                        )
                    )
                    PluginClass = helper.find_by_id(config.plugin)  # NOSONAR
                    plugin = PluginClass(
                        config.name,
                        SecretDict(config.parameters),
                        {},
                        config.checkpoint,
                        logger,
                    )
                    if plugin.metadata.get("netskope", False):
                        tenant = TenantDB(
                            **connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                                {"name": config.tenant}
                            )
                        )
                        same_ns_dest = is_destination_same(
                            current_action=action,
                            current_source=source,
                            current_business_rule=previous["name"],
                            current_sharings=v,
                            nsconfig_name=config.name,
                            tenant_name=tenant.parameters.get("tenantName")
                        )
                        # write to same urllist, file hash list or privateapp
                        if same_ns_dest:
                            raise ValueError(
                                "The same URL List, File Hash List or Private App List is already configured "
                                "in one of the sharing configurations. Please use another list "
                                "to avoid conflicts."
                            )
                    result = plugin.validate_action(action)
                    if not result.success:
                        raise ValueError(result.message)
    return v


class Action(BaseModel):
    """Action model."""

    label: str
    value: str
    parameters: Dict = Field({})


class ActionWithoutParams(BaseModel):
    """Action model."""

    label: str
    value: str
    patch_supported: Optional[bool] = None


class Filters(BaseModel):
    """Sharing filters model."""

    query: str = Field("")
    mongo: str = Field("{}")

    @field_validator("mongo")
    @classmethod
    def validate_mongo_query(cls, v):
        """Validate mongo query."""
        try:
            validate(json.loads(v), INDICATOR_QUERY_SCHEMA)
        except ValidationError as ex:
            raise ValueError(f"Invalid query provided. {ex.message}.")
        except Exception:
            raise ValueError("Could not parse the query.")
        return v


class Exceptions(BaseModel):
    """Mute rule model."""

    name: str = Field(...)
    filters: Union[Filters, None] = Field(None)
    tags: Union[List[str], None] = Field(None)

    @field_validator("tags")
    @classmethod
    def validate_tags_fields(cls, v, values, **kwargs):
        """Validate dedupe fields."""
        values = values.data
        if values.get("filters") is None and v is None:
            raise ValueError("filters and tags can not both be empty.")
        if None not in [v, values.get("filters")]:
            raise ValueError("filters and tags can not both be set.")
        return v


class BusinessRuleIn(BaseModel):
    """Business rule model."""

    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)] = Field(...)

    @field_validator("name")
    @classmethod
    def validate_is_unique(cls, v):
        """Validate that the name is unique."""
        if (
            connector.collection(Collections.CTE_BUSINESS_RULES).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"A business rule with the name {v} already exists.")
        return v

    filters: Filters = Field(Filters())
    exceptions: List[Exceptions] = Field([])
    muted: bool = Field(False)

    @field_validator("muted")
    @classmethod
    def validate_not_muted(cls, v):
        """Validate that the rule is not muted."""
        if v is True:
            raise ValueError("Can not create a muted business rule.")
        return v

    unmuteAt: Union[datetime, None] = Field(None)
    sharedWith: Dict[str, Dict[str, List[Action]]] = Field({})
    _validate_sharedWith = field_validator("sharedWith")(
        validate_sharedWith
    )


class BusinessRuleUpdate(BaseModel):
    """Business rule model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_exists(cls, v):
        """Validate that the name exists."""
        if (
            connector.collection(Collections.CTE_BUSINESS_RULES).find_one({"name": v})
            is None
        ):
            raise ValueError("No business rule with this name exists.")
        return v

    filters: Union[Filters, None] = Field(None)

    exceptions: Union[List[Exceptions], None] = Field(None)

    @field_validator("exceptions")
    @classmethod
    def validate_is_mute_rules_unique(cls, v):
        """Validate that the exceptions is unique."""
        rule_names = set()
        if v is None:
            return None
        for rule in v:
            rule_names.add(rule.name)
        if len(rule_names) != len(v):
            raise ValueError("An exceptions with the same name already exists.")
        return v

    muted: Union[bool, None] = Field(None)
    unmuteAt: Union[datetime, None] = Field(None)

    @field_validator("unmuteAt")
    @classmethod
    def validate_unmute_time(cls, v, values, **kwargs):
        """Validate unmuteAt time."""
        values = values.data
        if values["muted"] is False:
            return None
        if v is None:
            raise ValueError(
                "Unmute time must be set in order to mute the business rule."
            )
        if v < datetime.now():
            raise ValueError("Unmute time can not be in past.")
        return v

    sharedWith: Union[Dict[str, Dict[str, List[Action]]], None] = Field(None)
    _validate_sharedWith = field_validator("sharedWith")(
        validate_sharedWith
    )


class BusinessRuleOut(BaseModel):
    """Business rule out model."""

    name: str = Field(...)
    muted: Union[bool, None] = Field(None)
    unmuteAt: Union[datetime, None] = Field(None)
    filters: Union[Filters, None] = Field(None)
    exceptions: Union[List[Exceptions], None] = Field(None)
    sharedWith: Union[Dict[str, Dict[str, List[Action]]], None] = Field(None)


class BusinessRuleDelete(BaseModel):
    """Delete business rule model."""

    name: str = Field(...)

    @field_validator("name")
    @classmethod
    def validate_exists(cls, v):
        """Validate that the name exists."""
        if (
            connector.collection(Collections.CTE_BUSINESS_RULES).find_one({"name": v})
            is None
        ):
            raise ValueError("No business rule with this name exists.")
        return v


class BusinessRuleDB(BaseModel):
    """Database business rule model."""

    name: str = Field(...)
    filters: Filters = Field(...)
    exceptions: List[Exceptions] = Field(...)
    muted: bool = Field(...)
    unmuteAt: Union[datetime, None] = Field(None)
    sharedWith: Dict[str, Dict[str, List[Action]]] = Field(...)
