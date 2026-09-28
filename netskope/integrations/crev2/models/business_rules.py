"""Business rule related models."""

import traceback
from datetime import datetime, timezone
from typing import Annotated, Any, Optional, Union

from pydantic import BaseModel, Field, StringConstraints, field_validator, model_validator

from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    SecretDict,
)
from netskope.common.utils.unified_mapping_fields import (
    unmappable_action_sources,
    unmappable_source_message,
)

from .configurations import ConfigurationDB

THREAT_INDICATORS_ENTITY = "Threat Indicators"

connector = DBConnector()
helper = PluginHelper()
logger = Logger()


def _strip_field_details(action_dict: dict) -> dict:
    """Return an action dict without the server-managed fieldDetails key."""
    return {k: v for k, v in action_dict.items() if k != "fieldDetails"}


_CHOICE_FIELD_TYPES = ("choice", "multichoice")


def _resolve_choice_labels(field: dict, value):
    """Selected choice value(s) paired with the label each was shown as.

    Each label is stored against its own value rather than by position, so a
    parameter later rewritten to a different value simply finds no match -
    an audit record can never assert a name that was not the one selected.

    Args:
        field (dict): One plugin get_action_params field dict.
        value: The value stored for that field in action.parameters.

    Returns:
        Optional[list[ActionChoiceLabel]]: Resolved pairs, or None when
        nothing resolves - a non-choice field, a "$field" source (which
        resolves per record at run time), or an option dropped since the
        rule was saved.
    """
    if field.get("type") not in _CHOICE_FIELD_TYPES:
        return None
    choices = [c for c in (field.get("choices") or []) if isinstance(c, dict)]
    if not choices:
        return None
    resolved = []
    for item in value if isinstance(value, list) else [value]:
        # Linear scan, not a dict: a choice value may be unhashable (some
        # plugins carry the whole remote object as the value).
        for choice in choices:
            if choice.get("value") == item and choice.get("key"):
                resolved.append(
                    ActionChoiceLabel(value=item, label=choice["key"])
                )
                break
    return resolved or None


def _snapshot_action_field_details(plugin, action):
    """Snapshot key/label/visibility of the action's parameter fields.

    Also pairs each choice field's selected value with its display label, so
    the Action Logs page and CTO alerts can name what the user picked.

    The snapshot is cosmetic display metadata; failures must never block a
    business rule save, hence the broad exception handling.

    Args:
        plugin: Instantiated plugin for the action's configuration.
        action (Action): Action whose parameter fields should be snapshotted.

    Returns:
        Optional[list[ActionFieldDetail]]: Stripped field metadata, or None
        if the plugin call fails.
    """
    try:
        fields = plugin.get_action_params(action) or []
        parameters = action.parameters or {}
        return [
            ActionFieldDetail(
                key=field["key"],
                label=field.get("label", field["key"]),
                show_in_action_config=field.get("show_in_action_config", True),
                value_labels=_resolve_choice_labels(
                    field, parameters.get(field["key"])
                ),
            )
            for field in fields
            if isinstance(field, dict) and field.get("key")
        ]
    except Exception:
        logger.warn(
            f"Could not fetch action field details for action '{action.value}'. "
            "Action log labels will fall back to formatted parameter keys.",
            details=traceback.format_exc(),
        )
        return None


def validate_actions(cls, v: dict, values, **kwargs):
    """Validate action configurations exist."""
    values = values.data
    if v is None:
        return
    if "name" not in values:
        raise ValueError("name is required.")
    # Imported here, not at module scope: ``crev2.utils`` imports ``crev2.models``.
    from ..utils import get_entity_collection

    previous = connector.collection(Collections.CREV2_BUSINESS_RULES).find_one(
        {"name": values["name"]}
    )
    # Scoped to the rule's entity, which is what a bare ``$field`` parameter
    # resolves against. The name alone cannot decide this: Threat Indicators
    # declares ``sharedWith`` as its own field yet CE writes it, while a CRE
    # entity may declare that same name in the schema editor and own it.
    # The stored entity is the authority: it is immutable (_reject_entity_change)
    # and that guard is a model validator, so it has not run yet here. The
    # payload's own is the fallback for the delete-between-reads race only.
    entity = (previous or {}).get("entity") or values.get("entity")
    # No entity resolvable (a delete-between-reads race): no table and no
    # declared names keeps every closed name closed, rather than opening them.
    table = ""
    declared = set()
    if entity:
        table = get_entity_collection(entity)
        entity_doc = connector.collection(Collections.CREV2_ENTITIES).find_one(
            {"name": entity}, {"fields.name": 1}
        )
        # ``lastUpdated`` is stamped onto every CRE record, so a CRE entity
        # opens only the names its own schema declares.
        declared = {
            field.get("name")
            for field in (entity_doc or {}).get("fields") or []
            if isinstance(field, dict)
        }
    # Checked before any plugin is instantiated: a closed source is a payload
    # error, not something the destination plugin should be asked about.
    closed = unmappable_action_sources(v, table, declared)
    if closed:
        raise ValueError(unmappable_source_message(closed))
    for key, actions in v.items():
        if (
            actions
            and connector.collection(
                Collections.CREV2_CONFIGURATIONS
            ).find_one({"name": key})
            is None
        ):
            raise ValueError(f"Configuration {key} does not exist.")
        # previous is present on the update path (BusinessRuleUpdate.name is
        # validated to exist before this runs); guard against None only for
        # the narrow delete-between-reads race.
        stored_actions = (previous or {}).get("actions", {}).get(key, [])
        stripped_stored = [_strip_field_details(a) for a in stored_actions]
        # One plugin instance per configuration key — every new/updated
        # action under this key shares it, so the configuration is read
        # and the plugin instantiated at most once.
        config = None
        plugin = None
        for action in actions:
            current = _strip_field_details(action.model_dump())
            if current in stripped_stored:  # unchanged
                # Carry forward the server-side snapshot; never trust the
                # client-echoed fieldDetails.
                stored = stored_actions[stripped_stored.index(current)]
                # fieldDetails == [] is a valid snapshot (action with no
                # parameters) — only null/missing means "never snapshotted".
                details = stored.get("fieldDetails")
                action.fieldDetails = (
                    [ActionFieldDetail(**d) for d in details]
                    if details is not None
                    else None
                )
                continue
            # new or updated
            if plugin is None:
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
            action.fieldDetails = _snapshot_action_field_details(plugin, action)
            connector.collection(Collections.CREV2_CONFIGURATIONS).update_one(
                {"name": config.name},
                {"$set": {"storage": plugin.storage or {}}},
            )
    return v


class EntityFilters(BaseModel):
    """Entity related filters."""

    query: str = Field("")
    mongo: str = Field("{}")


class ActionChoiceLabel(BaseModel):
    """One selected choice value and the label it was shown as."""

    value: Any
    label: str


class ActionFieldDetail(BaseModel):
    """Snapshot of an action parameter field's display metadata."""

    key: str
    label: str
    # snake_case on purpose: mirrors the plugin get_action_params field
    # dicts and what the UI already reads.
    show_in_action_config: Optional[bool] = True
    # What the user picked, for a value that is otherwise an opaque id: one
    # entry per selected option, each naming the value it labels.
    value_labels: Optional[list[ActionChoiceLabel]] = None


class Action(BaseModel):
    """Action model."""

    label: str
    value: str
    parameters: dict = Field({})
    generateAlert: bool = Field(False)
    performLater: bool = Field(False)
    requireApproval: bool = Field(False)
    performRevert: Optional[bool] = False
    fieldDetails: Optional[list[ActionFieldDetail]] = None


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
    sourceConfiguration: Optional[str] = Field(
        None,
        description="CTE source configuration name (Threat Indicators rules only).",
    )

    @field_validator("actions")
    @classmethod
    def overwrite_actions(cls, v: dict):
        """Overwrite actions while creation."""
        return {}

    @model_validator(mode="after")
    def validate_threat_indicator_source(self):
        """Threat Indicator rules must declare source configuration on the payload."""
        if (
            self.entity == THREAT_INDICATORS_ENTITY
            and not self.sourceConfiguration
        ):
            raise ValueError(
                "sourceConfiguration is required for Threat Indicators business rules."
            )
        return self

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
    sourceConfiguration: Optional[str] = Field(None)
    _validate_actions = field_validator("actions")(validate_actions)
    muted: Union[bool, None] = Field(None)
    unmuteAt: Union[datetime, None] = Field(None)

    @model_validator(mode="after")
    def _validate_mute_state(self):
        """Validate the mute state, mirroring the CTE rule contract.

        Muting always has an end time - the only indefinite mute is the one
        applied when the CTE module is turned off, which is written directly and
        never through this model. Unmuting clears the time so no stale unmute
        time is left behind.
        """
        if self.muted is None:
            return self
        if self.muted is False:
            self.unmuteAt = None
            return self
        if self.unmuteAt is None:
            raise ValueError(
                "Unmute time must be set in order to mute the business rule."
            )
        # Unmute times are stored and compared as naive UTC, so an offset sent by
        # a client ("...Z", "+05:30") is converted rather than compared as-is -
        # comparing aware to naive raises TypeError and fails with a 500.
        if self.unmuteAt.tzinfo is not None:
            self.unmuteAt = self.unmuteAt.astimezone(timezone.utc).replace(
                tzinfo=None
            )
        if self.unmuteAt < datetime.now():
            raise ValueError("Unmute time can not be in past.")
        return self

    @model_validator(mode="after")
    def _reject_entity_change(self):
        """Reject changing the entity of an existing rule.

        The entity a rule is built on drives its filters, action validation and
        record evaluation, so it is immutable after creation. The UI disables the
        entity selector on edit; this guards the same invariant on the API, which
        would otherwise let an entity change slip through the ``$set`` update.
        """
        if self.entity is None:
            return self
        stored = connector.collection(
            Collections.CREV2_BUSINESS_RULES
        ).find_one({"name": self.name})
        stored_entity = (stored or {}).get("entity")
        if stored_entity is not None and self.entity != stored_entity:
            raise ValueError(
                "The entity of a business rule cannot be changed after creation."
            )
        return self


class BusinessRuleOut(BaseModel):
    """Business rule out model."""

    name: str
    entity: str
    entityFilters: EntityFilters
    actions: dict[str, list[Action]] = Field(dict())
    sourceConfiguration: Optional[str] = Field(None)
    muted: Union[bool, None] = Field(None)
    unmuteAt: Union[datetime, None] = Field(None)
    # True when the rule was auto-disabled because the CTE module is off. Only
    # applies to Threat Indicators rules (their data is owned by CTE). Such a
    # rule is muted and locked until CTE is re-enabled.
    disabledByCte: bool = Field(False)

    @model_validator(mode="before")
    def _report_configured_mute_state(cls, values):
        """Report the mute state a user configured, not the lock's system mute.

        Locking a rule (CTE module turned off) mutes it so it stops evaluating
        and moves the user's own mute state into ``cteMuteSnapshot``. Reporting
        the document as-is would present every locked rule as muted, so the
        snapshotted state - exactly what ``restore_ti_business_rules`` writes
        back - is substituted here instead. The lock itself is reported through
        ``disabledByCte``. The snapshot only exists while the rule is locked, so
        unlocked rules are unaffected.

        A snapshotted unmute time keeps running while CTE is down, so a mute
        whose deadline has already elapsed is over and is reported as unmuted -
        the same conclusion ``restore_ti_business_rules`` reaches when CTE comes
        back.
        """
        if not isinstance(values, dict):
            return values
        snapshot = values.get("cteMuteSnapshot")
        if isinstance(snapshot, dict):
            muted = bool(snapshot.get("muted"))
            unmute_at = snapshot.get("unmuteAt") if muted else None
            if isinstance(unmute_at, datetime) and unmute_at <= datetime.now():
                muted, unmute_at = False, None
            values = {**values, "muted": muted, "unmuteAt": unmute_at}
        return values


class BusinessRuleDB(BaseModel):
    """Business rule database model."""

    name: str
    entity: str
    entityFilters: EntityFilters
    actions: dict[str, list[Action]] = Field(dict())
    sourceConfiguration: Optional[str] = Field(None)
    lastEvals: list[str] = Field([])
    muted: bool = Field(False)
    unmuteAt: Union[datetime, None] = Field(None)
    disabledByCte: bool = Field(False)


class BusinessRuleDelete(BaseModel):
    """Business rule delete model."""

    name: str
