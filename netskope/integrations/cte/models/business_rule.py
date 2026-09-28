"""Business rule related schemas."""
import json
from datetime import datetime, timezone
from typing import List, Dict, Union, Optional
from pydantic import (
    field_validator,
    model_validator,
    StringConstraints,
    BaseModel,
    Field,
)
from jsonschema import validate, ValidationError
from netskope.common.utils import (
    DBConnector,
    Collections,
    PluginHelper,
    Logger,
    SecretDict,
)
from netskope.common.models import TenantDB
from netskope.common.utils.unified_mapping_fields import (
    IOC_FIELD_VALUE_TYPES,
    IOC_MULTI_VALUED_FIELDS,
    entity_field_widget_meta,
    missing_tags,
    no_value_reason,
    map_type_labels,
    unknown_tag_message,
    validate_bounded_labels,
)
from ..utils.constants import CTE_NO_ACTION_VALUE
from ..utils.schema import INDICATOR_QUERY_SCHEMA
from ..utils.entity import THREAT_INDICATORS_ENTITY
from . import ConfigurationDB
from typing_extensions import Annotated


connector = DBConnector()
helper = PluginHelper()
logger = Logger()

DEFAULT_UNIQUE_DEST_KEYS = {
    "file": {
        "unique_key": "file_list"
    },
    "private_app": {
        "unique_key": "private_app_name",
        "fallback": "create",
        "fallback_key": "name"
    }
}


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
        config.get("storage") or {},
        config.get("checkpoint"),
        logger,
    )
    if config and plugin.metadata.get("netskope", False):
        unique_dest_keys = plugin.metadata.get("unique_dest_keys", DEFAULT_UNIQUE_DEST_KEYS)
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
                    if (
                        current_action_dest_value is not None
                        and other_action_dest_value is not None
                        and current_action_dest_value == other_action_dest_value
                    ):
                        return True
    return False


def _iter_sharing_entries(rule):
    """Yield ``(source, destination, actions)`` for every sharing entry of a rule.

    Threat Indicators rules keep their sharings under ``sharedWith`` keyed by the
    source configuration; CRE-entity rules keep theirs under ``creShare`` with no
    source layer. CRE-entity entries are yielded with a ``None`` source so both
    kinds can be compared uniformly by :func:`validate_same_destination_config`.
    """
    for source, dest_dict in (rule.get("sharedWith") or {}).items():
        for destination, actions in (dest_dict or {}).items():
            yield source, destination, actions
    for destination, actions in (rule.get("creShare") or {}).items():
        yield None, destination, actions


def is_destination_same(
    current_action: object,
    current_source: Optional[str],
    current_business_rule: str,
    current_sharedWith: Optional[dict],
    current_creShare: Optional[dict],
    nsconfig_name: str,
    tenant_name: str
):
    """Validate if two sharing configurations target the same Netskope destination.

    Scans both Threat Indicators (``sharedWith``) and CRE-entity (``creShare``)
    sharings across every business rule, so the same destination (for example a
    file hash list on a tenant) cannot be configured twice regardless of whether
    each configuration belongs to a Threat Indicators or a CRE-entity rule. The
    in-flight values are substituted for the rule currently being validated.
    """
    for rule in connector.collection(Collections.CTE_BUSINESS_RULES).find({}):
        if rule.get("name") == current_business_rule:
            rule = {
                "name": current_business_rule,
                "sharedWith": current_sharedWith or {},
                "creShare": current_creShare or {},
            }
        for source, destination, actions in _iter_sharing_entries(rule):
            if not actions:
                continue
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


def validate_sharing_action(
    action,
    source,
    destination,
    rule_name,
    current_sharedWith,
    current_creShare,
):
    """Validate a single sharing action against its destination plugin.

    Loads the destination plugin and applies both destination-level checks:

    1. Netskope destination uniqueness (Netskope destinations only) - the same
       destination (such as the same file hash list or private app) must not
       already be configured in another sharing configuration.
    2. Plugin-side action parameter validation via ``plugin.validate_action``,
       which enforces plugin rules the action form cannot express (for example
       the Netskope plugin requiring 'TCP Ports' when 'TCP' is selected in
       Protocol).

    Shared by the Threat Indicators (``sharedWith``) and CRE-entity
    (``creShare``) sharing validators so both paths reject the same invalid
    configurations at save time instead of failing at push time.

    Args:
        action (Action): The sharing action being validated.
        source (str | None): Source configuration for TI rules; ``None`` for
            CRE-entity rules, which have no source layer.
        destination (str): Destination configuration name the action targets.
        rule_name (str): Name of the business rule being validated.
        current_sharedWith (dict | None): In-flight ``sharedWith`` map, or ``None``.
        current_creShare (dict | None): In-flight ``creShare`` map, or ``None``.

    Raises:
        ValueError: If the same destination action is already configured in
            another sharing configuration, or the destination plugin rejects the
            action parameters.
    """
    config = ConfigurationDB(
        **connector.collection(Collections.CONFIGURATIONS).find_one(
            {"name": destination}
        )
    )
    PluginClass = helper.find_by_id(config.plugin)  # NOSONAR
    plugin = PluginClass(
        config.name,
        SecretDict(config.parameters),
        config.storage,
        config.checkpoint,
        logger,
    )
    if plugin.metadata.get("netskope", False):
        tenant = TenantDB(
            **connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                {"name": config.tenant}
            )
        )
        if is_destination_same(
            current_action=action,
            current_source=source,
            current_business_rule=rule_name,
            current_sharedWith=current_sharedWith,
            current_creShare=current_creShare,
            nsconfig_name=config.name,
            tenant_name=tenant.parameters.get("tenantName"),
        ):
            # The error specifies what action it triggered
            raise ValueError(
                f"'{action.label}' is already configured with the same parameter value "
                "in another sharing configuration. Please choose a different "
                f"parameter value for '{action.label}' to avoid conflicts."
            )
    result = plugin.validate_action(action)
    if not result.success:
        raise ValueError(result.message)


def validate_sharedWith(cls, v: Dict[str, List[str]], values, **kwargs):
    """Validate the Threat Indicators sharing config.

    Checks that every destination configuration exists, that an action is not
    repeated for a source/destination pair, that 'No Action' has Generate Alert
    enabled, and - for new or updated actions - defers the destination-level
    checks (Netskope uniqueness and plugin action-parameter validation) to
    :func:`validate_sharing_action`.
    """
    values = values.data
    if v is None:
        return
    if "name" not in values:
        raise ValueError("name is required.")
    previous = connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
        {"name": values["name"]}
    ) or {}
    previous_sharedWith = previous.get("sharedWith", {}) or {}
    for source, dest_dict in v.items():
        for key, actions in dest_dict.items():
            if key == source:
                raise ValueError(
                    "Destination configuration cannot be the same as "
                    "Source configuration."
                )
            if (
                actions
                and connector.collection(Collections.CONFIGURATIONS).find_one(
                    {"name": key}
                )
                is None
            ):
                raise ValueError(f"CTE configuration {key} does not exist.")
            # A sharing config is uniquely identified by rule + source +
            # destination + action; the same action must not repeat for a
            # source/destination pair (mirrors the duplicate check in the
            # Sharing UI).
            seen_actions = set()
            for action in actions:
                if action.value in seen_actions:
                    raise ValueError(
                        "Sharing Configuration for given Business Rule and "
                        "Configuration already exists."
                    )
                seen_actions.add(action.value)
                if action.value == CTE_NO_ACTION_VALUE:
                    # Core-level pseudo-action: nothing is pushed, so there is
                    # no plugin-side validation or Netskope destination
                    # conflict to check.
                    if not action.generateAlert:
                        raise ValueError(
                            "Generate Alert must be enabled when 'No Action' "
                            "is selected as the target."
                        )
                    continue
                if action.model_dump() not in (previous_sharedWith.get(source, {})).get(
                    key, []
                ):  # new or updated
                    # ``current_creShare`` is deliberately ``None``: a rule
                    # carries either ``sharedWith`` or ``creShare``, never both.
                    validate_sharing_action(
                        action=action,
                        source=source,
                        destination=key,
                        rule_name=values["name"],
                        current_sharedWith=v,
                        current_creShare=None,
                    )
    return v


class Action(BaseModel):
    """Action model."""

    label: str
    value: str
    parameters: Dict = Field({})
    generateAlert: bool = Field(False)


def validate_creShare(entity, sharedWith, creShare, name):
    """Validate CRE-entity sharing config and its mutual exclusivity with sharedWith.

    Args:
        entity (str | None): Business-rule entity name.
        sharedWith (dict | None): Threat Indicators sharing map.
        creShare (dict | None): CRE-entity sharing map (dest config -> actions).
        name (str): Name of the business rule being validated.

    Raises:
        ValueError: If TI/CRE sharing fields are mixed, a destination config is
            missing, the same action repeats for a destination, 'No Action' is
            selected without Generate Alert enabled, a Netskope destination
            action is already configured in another sharing configuration, or the
            destination plugin rejects the action parameters.
    """
    is_ti = entity is None or entity == THREAT_INDICATORS_ENTITY
    if is_ti:
        if creShare:
            raise ValueError(
                "creShare is only supported for CRE-entity business rules."
            )
        return creShare
    # CRE entity rule.
    if sharedWith:
        raise ValueError(
            "sharedWith is only supported for Threat Indicators business rules; "
            "use creShare for CRE-entity rules."
        )
    previous = connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
        {"name": name}
    ) or {}
    previous_creShare = previous.get("creShare", {}) or {}
    for destination, actions in (creShare or {}).items():
        if (
            connector.collection(Collections.CONFIGURATIONS).find_one(
                {"name": destination}
            )
            is None
        ):
            raise ValueError(
                f"Destination configuration '{destination}' does not exist."
            )
        # A CRE-entity sharing config is uniquely identified by rule +
        # destination + action; the same action must not repeat for a
        # destination (mirrors the duplicate check in the Sharing UI).
        seen_actions = set()
        for action in actions or []:
            if action.value in seen_actions:
                raise ValueError(
                    "Sharing Configuration for given Business Rule and "
                    "Configuration already exists."
                )
            seen_actions.add(action.value)
            if action.value == CTE_NO_ACTION_VALUE:
                # Core-level pseudo-action: nothing is pushed, so the sharing
                # would be a no-op without alert generation (mirrors the
                # Threat Indicators check in ``validate_sharedWith``), and
                # there is no Netskope destination to conflict with.
                if not action.generateAlert:
                    raise ValueError(
                        "Generate Alert must be enabled when 'No Action' "
                        "is selected as the target."
                    )
                continue
            if action.model_dump() not in previous_creShare.get(
                destination, []
            ):  # new or updated
                validate_sharing_action(
                    action=action,
                    source=None,
                    destination=destination,
                    rule_name=name,
                    current_sharedWith=None,
                    current_creShare=creShare,
                )
    return creShare


# The only IOC fields a rule may map, mirroring the rows the UI's IocFieldMapping
# renders. Every other Indicator field is CE-managed and must not be rule-settable.
MAPPABLE_IOC_FIELDS = frozenset(IOC_FIELD_VALUE_TYPES)

_BOOLEAN_STATIC_VALUES = {"true", "false"}

# Inclusive bounds a numeric IOC value must fall in, mirroring the UI's number
# inputs. ``expiresAt`` is a number of days; ``reputation`` mirrors the
# ``ge=1, le=10`` bound on ``Indicator.reputation``.
# Source of truth for both directions: the save-time check on *static* values
# (:func:`_validate_static_value`) and the share-time clamp on values resolved
# from a CRE record field (``_coerce_numeric_ioc_value`` in the share task). A
# ``$``-reference cannot be range-checked here because its value is only known
# per record, so the share task substitutes ``NUMERIC_IOC_DEFAULTS`` instead.
NUMERIC_IOC_RANGES = {
    "expiresAt": (1, 365),
    "reputation": (1, 10),
}

# Days applied when a CRE-entity mapping omits ``expiresAt``, so indicators
# shared from CRE records always age out. Matches the UI's pre-filled default.
DEFAULT_EXPIRES_AT_DAYS = 90

# Reputation applied when a mapped value is out of range. Mirrors the
# ``Indicator.reputation`` field default, so a clamped record ends up with the
# same reputation an unmapped one would have.
DEFAULT_REPUTATION = 5

# Value substituted when a mapped numeric IOC value falls outside its
# ``NUMERIC_IOC_RANGES`` bound. Used by the share task, which cannot reject a
# bad per-record value without dropping the whole record.
NUMERIC_IOC_DEFAULTS = {
    "expiresAt": DEFAULT_EXPIRES_AT_DAYS,
    "reputation": DEFAULT_REPUTATION,
}


def _referenced_field_name(spec):
    """Top-level entity field name a ``$``-reference spec points at, else None.

    Static specs (``fixed:``/bare literals) and non-string specs return None; a
    dotted reference (``$host.name``) yields its top-level field (``host``).
    """
    if isinstance(spec, str) and spec.startswith("$"):
        return spec[1:].lstrip(".").split(".")[0]
    return None


def _static_literal(spec):
    """Literal a static mapping spec carries.

    Mirrors ``_resolve_field_spec`` in the share task: ``fixed:<v>`` yields
    ``<v>`` with the marker stripped, and any other value is a bare literal.
    Only meaningful for specs that are not ``$``-references.
    """
    if isinstance(spec, str) and spec.startswith("fixed:"):
        return spec[len("fixed:"):]
    return spec


def _is_boolean_ioc_field(ioc_field):
    """Return True when an IOC field holds a boolean (mirrors the UI's isBooleanIocField)."""
    return "boolean" in IOC_FIELD_VALUE_TYPES.get(ioc_field, ())


def _validate_static_value(ioc_field, spec):
    """Validate a static mapping literal against the UI's control for that field.

    Boolean fields (``test``, ``safe``) accept only ``true``/``false``;
    ``expiresAt`` and ``reputation`` must be numbers inside their inclusive
    range. Any other IOC field's static value is free-form.

    Args:
        ioc_field (str): IOC field the literal is mapped to.
        spec (str | int | float): Static spec, with or without ``fixed:``.

    Raises:
        ValueError: If the literal is not one the UI could have produced.
    """
    literal = _static_literal(spec)
    if ioc_field in IOC_MULTI_VALUED_FIELDS:
        # A static tag is as knowable now as a map type's authored set, and
        # Indicator.tags rejects a tag CTE does not have -- dropping the whole
        # record. Checked here so the two routes to Tags behave the same.
        unknown = missing_tags([literal])
        if unknown:
            raise ValueError(unknown_tag_message(ioc_field, literal, unknown))
        return
    if _is_boolean_ioc_field(ioc_field):
        if str(literal).strip().lower() not in _BOOLEAN_STATIC_VALUES:
            raise ValueError(
                f"The '{ioc_field}' field mapping must be 'true' or "
                f"'false'; got '{literal}'."
            )
        return
    bounds = NUMERIC_IOC_RANGES.get(ioc_field)
    if bounds is None:
        return
    minimum, maximum = bounds
    try:
        number = float(literal)
    except (TypeError, ValueError):
        number = None
    if number is None or not minimum <= number <= maximum:
        raise ValueError(
            f"The '{ioc_field}' field mapping must be a number between "
            f"{minimum} and {maximum}; got '{literal}'."
        )


def validate_mapping_keys(field_mapping):
    """Reject IOC fields a rule is not allowed to map.

    Every other check is a keyed lookup that no-ops on an unrecognised IOC field,
    so without this a caller could map a CE-managed one the UI never offers --
    and ``build_indicators_from_records`` passes each key straight to
    ``Indicator(**ioc_kwargs)``, so ``retracted``/``active`` would take effect.

    Args:
        field_mapping (dict | None): IOC field -> mapping spec.

    Raises:
        ValueError: If any key is outside :data:`MAPPABLE_IOC_FIELDS`.
    """
    unsupported = sorted(set(field_mapping or {}) - MAPPABLE_IOC_FIELDS)
    if unsupported:
        raise ValueError(
            f"Field Mapping cannot map {', '.join(unsupported)}; only "
            f"{', '.join(sorted(MAPPABLE_IOC_FIELDS))} may be mapped."
        )


def entity_fields_by_name(fields):
    """Index an entity's field documents by their ``name``.

    Args:
        fields (Iterable[dict] | None): Entity field documents.

    Returns:
        dict: ``{field name: field document}``.
    """
    return {field.get("name"): field for field in fields or []}


def validate_mapping_against_fields(field_mapping, fields_by_name):
    """Check a mapping's ``$``-references against a set of CRE entity fields.

    Shared by rule save (:func:`validate_field_mapping`, stored fields) and the
    CRE schema editor's field edit (``_assert_field_mappings_valid``, *proposed*
    fields), so the two cannot drift.

    Gates on what a field STORES, applying the same rules
    ``validate_rule_field_mapping`` applies to a unified mapping rule: ``tags``
    takes any source of the right ELEMENT primitive (the share task wraps a
    scalar), every other IOC field needs ONE value of a known accepted
    primitive, the field must hold a value at all (:func:`no_value_reason`), and
    a map type's authored set must satisfy it (:func:`validate_bounded_labels`).

    References the caller's field set cannot resolve are skipped; static specs
    are checked by :func:`_validate_static_value` instead.

    Args:
        field_mapping (dict | None): IOC field -> mapping spec.
        fields_by_name (dict): Entity field name -> field document, from
            :func:`entity_fields_by_name`.

    Raises:
        ValueError: If a reference points at a field holding the wrong primitive
            or the wrong number of values, at one that can hold no value, or at a
            bounded field whose values the IOC field cannot accept.
    """
    for ioc_field, spec in (field_mapping or {}).items():
        field_name = _referenced_field_name(spec)
        if not field_name:
            continue
        matched = fields_by_name.get(field_name)
        if matched is None:
            continue
        labels = map_type_labels(matched)
        no_value = no_value_reason(
            matched.get("type"), matched.get("unique"), labels
        )
        if no_value:
            raise ValueError(
                f"The '{ioc_field}' field mapping cannot reference "
                f"'{field_name}': {no_value} and holds no value."
            )
        bounded = validate_bounded_labels(ioc_field, field_name, labels)
        if bounded:
            raise ValueError(bounded)
        widget = entity_field_widget_meta(matched)
        accepted = IOC_FIELD_VALUE_TYPES.get(ioc_field)
        primitive = widget.get("valueType")
        if ioc_field in IOC_MULTI_VALUED_FIELDS:
            # An unknown primitive is refused here too, not only for the scalar
            # fields below: a Reference reports none, and letting it through
            # would ship whatever its target holds straight into Indicator.tags.
            if accepted and not primitive:
                raise ValueError(
                    f"The '{ioc_field}' field mapping must reference a field "
                    f"holding {' or '.join(sorted(accepted))}; what "
                    f"'{field_name}' holds is not known."
                )
            if accepted and primitive not in accepted:
                raise ValueError(
                    f"The '{ioc_field}' field mapping must reference a field "
                    f"holding {' or '.join(sorted(accepted))}; '{field_name}' "
                    f"holds {primitive}."
                )
            continue
        if widget["multiValued"]:
            raise ValueError(
                f"The '{ioc_field}' field mapping must reference a field "
                f"holding one value; '{field_name}' holds a list (a List field, "
                "or one using the append strategy), so it can only be mapped to "
                "a list field such as Tags."
            )
        if accepted and not primitive:
            raise ValueError(
                f"The '{ioc_field}' field mapping must reference a field "
                f"holding {' or '.join(sorted(accepted))}; what '{field_name}' "
                "holds is not known."
            )
        if accepted and primitive not in accepted:
            raise ValueError(
                f"The '{ioc_field}' field mapping must reference a field "
                f"holding {' or '.join(sorted(accepted))}; '{field_name}' holds "
                f"{primitive}."
            )


def validate_field_mapping(entity, field_mapping):
    """Validate the rule-level IOC field mapping against the rule's entity.

    The mapping maps IOC fields (``value``, ``type`` and optional fields) to CRE
    entity field references (``$<path>``), ``fixed:`` literals, or bare static
    values; some IOC fields (e.g. ``expiresAt``, stored by the UI as a number of
    days) are numeric. It is required for CRE-entity rules and forbidden for
    Threat Indicators rules.

    ``$``-references are checked by :func:`validate_mapping_against_fields`.
    ``value`` carries no uniqueness requirement: records sharing a value are
    collapsed into one indicator by the share task.

    A *static* spec is instead checked by :func:`_validate_static_value` against
    the choices the UI's static control offers for that IOC field.

    ``expiresAt`` is required for a CRE-entity rule; a mapping that omits it is
    returned with ``DEFAULT_EXPIRES_AT_DAYS`` filled in, so callers must use the
    returned mapping rather than assume the argument was validated in place.

    Args:
        entity (str | None): Business-rule entity name.
        field_mapping (dict | None): Rule-level IOC field mapping.

    Returns:
        dict: The mapping to persist — the argument, plus a defaulted
            ``expiresAt`` when it was absent.

    Raises:
        ValueError: If a Threat Indicators rule carries a mapping, a CRE-entity
            rule does not map both ``value`` and ``type``, any key is not a
            mappable IOC field, any mapping references a field the entity does
            not declare (a CE-managed name included), a field of an unusable
            type or an ``append``-strategy entity field, or a static value is
            outside what its IOC field accepts.
    """
    is_ti = entity is None or entity == THREAT_INDICATORS_ENTITY
    if is_ti:
        if field_mapping:
            raise ValueError(
                "fieldMapping is only supported for CRE-entity business rules."
            )
        return field_mapping or {}
    mapping = field_mapping or {}
    validate_mapping_keys(mapping)
    if not mapping.get("value") or not mapping.get("type"):
        raise ValueError(
            "A CRE-entity business rule must map both 'value' and 'type' "
            "to a CRE entity field."
        )
    # ``expiresAt`` is required; an omitted one is defaulted rather than
    # rejected so callers that predate the requirement keep working. An
    # explicitly bad value still fails the static check below.
    if "expiresAt" not in mapping:
        mapping = {**mapping, "expiresAt": DEFAULT_EXPIRES_AT_DAYS}
    # Only ``$``-references need the entity schema; static literals are checked
    # against the same choices the UI's static controls offer.
    has_reference = False
    for ioc_field, spec in mapping.items():
        if _referenced_field_name(spec):
            has_reference = True
        else:
            _validate_static_value(ioc_field, spec)
    if not has_reference:
        return mapping
    entity_doc = connector.collection(Collections.CREV2_ENTITIES).find_one(
        {"name": entity}
    )
    fields_by_name = entity_fields_by_name((entity_doc or {}).get("fields"))
    # Rejected here rather than in validate_mapping_against_fields, whose other
    # caller passes a deliberately partial field set (see its docstring). Both
    # sides are top-level names: EntityField.validate_name rewrites "." to "_",
    # so only the seeded Threat Indicators entity has dotted fields -- and a
    # Threat Indicators rule returned above, before any of this.
    #
    # This is also what closes the CE-managed names (UNMAPPABLE_SOURCE_FIELDS):
    # CE stamps them onto the record, so they are never in the entity's schema
    # unless a user declared one in the schema editor -- and then it is theirs.
    referenced = {_referenced_field_name(spec) for spec in mapping.values()}
    # Falsy names are dropped for the same reason has_reference above ignores
    # them: a bare "$" is a static literal, not a reference.
    unknown = sorted(n for n in referenced if n and n not in fields_by_name)
    if unknown:
        raise ValueError(
            f"Field Mapping references field(s) not present in entity "
            f"'{entity}': {', '.join(unknown)}."
        )
    validate_mapping_against_fields(mapping, fields_by_name)
    return mapping


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
        """Validate that the mongo query is parseable JSON.

        The indicator-schema (``INDICATOR_QUERY_SCHEMA``) check is applied at the
        business-rule level only for Threat Indicators rules; CRE-entity rules
        carry queries built from the CRE entity schema, so they are validated as
        generic JSON here (see ``validate_entity_filters``).
        """
        try:
            json.loads(v)
        except Exception:
            raise ValueError("Could not parse the query.")
        return v


def validate_entity_filters(entity, filters, exceptions):
    """Validate filter queries against the indicator schema for TI rules only.

    Args:
        entity (str): Business-rule entity name.
        filters (Filters | None): Primary rule filter.
        exceptions (list[Exceptions] | None): Mute exceptions carrying filters.

    Raises:
        ValueError: If a Threat Indicators query does not match the indicator
            query schema.
    """
    if entity is not None and entity != THREAT_INDICATORS_ENTITY:
        # CRE-entity queries are built from the entity schema, not the indicator
        # schema; the Filters validator already confirmed they are valid JSON.
        return
    queries = []
    if filters is not None and filters.mongo:
        queries.append(filters.mongo)
    for exception in exceptions or []:
        if exception.filters is not None and exception.filters.mongo:
            queries.append(exception.filters.mongo)
    for mongo in queries:
        try:
            validate(json.loads(mongo), INDICATOR_QUERY_SCHEMA)
        except ValidationError as ex:
            raise ValueError(f"Invalid query provided. {ex.message}.")
        except ValueError:
            raise
        except Exception:
            raise ValueError("Could not parse the query.")


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

    entity: str = Field(THREAT_INDICATORS_ENTITY)
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
    creShare: Dict[str, List[Action]] = Field({})
    # Values are str/int/float only, never None: a mapping value is either a
    # ``$<field>`` reference, a ``fixed:`` literal, a bare static value, or a
    # numeric (e.g. expiresAt days). Posting a key with a null value is a 422 by
    # design; the UI drops cleared optional rows before submit rather than
    # sending null.
    fieldMapping: Dict[str, Union[str, int, float]] = Field({})

    @model_validator(mode="after")
    def validate_entity_config(self):
        """Validate query schema, sharing config and field mapping against the rule's entity."""
        validate_entity_filters(self.entity, self.filters, self.exceptions)
        validate_creShare(self.entity, self.sharedWith, self.creShare, self.name)
        # Reassigned so a defaulted ``expiresAt`` is persisted with the rule.
        self.fieldMapping = validate_field_mapping(
            self.entity, self.fieldMapping
        )
        return self


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

    entity: Union[str, None] = Field(None)
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
        # Unmute times are stored and compared as naive UTC, so an offset sent by
        # a client ("...Z", "+05:30") is converted rather than compared as-is -
        # comparing aware to naive raises TypeError and fails with a 500.
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        if v < datetime.now():
            raise ValueError("Unmute time can not be in past.")
        return v

    @model_validator(mode="after")
    def _require_unmute_time_when_muting(self):
        """Reject muting a rule without an unmute time.

        ``validate_unmute_time`` above only runs when the payload carries
        ``unmuteAt``, so a payload that sets ``muted`` alone would otherwise
        store a rule muted with no end time. Every mute has an end time (the
        only indefinite mute is the one applied when the CRE module is turned
        off, which is written directly and never through this model).
        """
        if self.muted and self.unmuteAt is None:
            raise ValueError(
                "Unmute time must be set in order to mute the business rule."
            )
        return self

    sharedWith: Union[Dict[str, Dict[str, List[Action]]], None] = Field(None)
    _validate_sharedWith = field_validator("sharedWith")(
        validate_sharedWith
    )
    creShare: Union[Dict[str, List[Action]], None] = Field(None)
    fieldMapping: Union[Dict[str, Union[str, int, float]], None] = Field(None)

    @model_validator(mode="after")
    def validate_entity_config(self):
        """Validate query schema and sharing config against the effective entity.

        The entity is immutable after creation: a business rule's query, sharing
        and evaluation are all bound to its entity, so changing it on update
        (only possible through the API, the UI disables the field) would leave
        the rule internally inconsistent. When ``entity`` is omitted on update
        the stored rule's entity is used so the conditional indicator-schema
        check stays correct. The field mapping
        is create-time-only (like the entity in the UI): providing it on update
        is rejected, and when the entity is provided the stored mapping is
        re-validated against it so an entity flip cannot leave the rule in an
        invalid state.
        """
        if self.fieldMapping is not None:
            raise ValueError(
                "fieldMapping can only be configured when creating a "
                "business rule."
            )
        stored = connector.collection(Collections.CTE_BUSINESS_RULES).find_one(
            {"name": self.name}
        )
        stored_entity = (stored or {}).get("entity", THREAT_INDICATORS_ENTITY)
        if self.entity is not None and self.entity != stored_entity:
            raise ValueError(
                "The entity of a business rule cannot be changed after creation."
            )
        effective_entity = self.entity if self.entity is not None else stored_entity
        validate_entity_filters(effective_entity, self.filters, self.exceptions)
        validate_creShare(
            effective_entity, self.sharedWith, self.creShare, self.name
        )
        if self.entity is not None:
            validate_field_mapping(
                self.entity, (stored or {}).get("fieldMapping") or {}
            )
        return self


class BusinessRuleOut(BaseModel):
    """Business rule out model."""

    name: str = Field(...)
    entity: str = Field(THREAT_INDICATORS_ENTITY)
    muted: Union[bool, None] = Field(None)
    unmuteAt: Union[datetime, None] = Field(None)
    filters: Union[Filters, None] = Field(None)
    exceptions: Union[List[Exceptions], None] = Field(None)
    sharedWith: Union[Dict[str, Dict[str, List[Action]]], None] = Field(None)
    creShare: Union[Dict[str, List[Action]], None] = Field(None)
    fieldMapping: Union[Dict[str, Union[str, int, float]], None] = Field(None)
    # True when the rule was auto-disabled because the CRE module is off. Such a
    # rule is muted and locked (no edit/mute/sync/delete) until CRE is re-enabled.
    disabledByCre: bool = Field(False)

    @model_validator(mode="before")
    def _report_configured_mute_state(cls, values):
        """Report the mute state a user configured, not the lock's system mute.

        Locking a rule (CRE module turned off) mutes it so it stops sharing and
        moves the user's own mute state into ``creMuteSnapshot``. Reporting the
        document as-is would present every locked rule as muted, so the
        snapshotted state - exactly what ``restore_cre_entity_business_rules``
        writes back - is substituted here instead. The lock itself is reported
        through ``disabledByCre``. The snapshot only exists while the rule is
        locked, so unlocked rules are unaffected.

        A snapshotted unmute time keeps running while CRE is down, so a mute
        whose deadline has already elapsed is over and is reported as unmuted -
        the same conclusion ``restore_cre_entity_business_rules`` reaches when
        CRE comes back.
        """
        if not isinstance(values, dict):
            return values
        snapshot = values.get("creMuteSnapshot")
        if isinstance(snapshot, dict):
            muted = bool(snapshot.get("muted"))
            unmute_at = snapshot.get("unmuteAt") if muted else None
            if isinstance(unmute_at, datetime) and unmute_at <= datetime.now():
                muted, unmute_at = False, None
            values = {**values, "muted": muted, "unmuteAt": unmute_at}
        return values


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
    entity: str = Field(THREAT_INDICATORS_ENTITY)
    filters: Filters = Field(...)
    exceptions: List[Exceptions] = Field(...)
    muted: bool = Field(...)
    unmuteAt: Union[datetime, None] = Field(None)
    sharedWith: Dict[str, Dict[str, List[Action]]] = Field(...)
    creShare: Dict[str, List[Action]] = Field(...)
    # Defaults to {} so pre-change documents (e.g. Threat Indicators rules that
    # never carried a mapping) still parse in the share task and test endpoint.
    fieldMapping: Dict[str, Union[str, int, float]] = Field({})
    disabledByCre: bool = Field(False)
