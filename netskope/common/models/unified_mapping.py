"""Pydantic models for the unified mapping (multi-collection merge) feature."""

import json
import re
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Union

from fastapi import HTTPException
from pydantic import BaseModel, Field, PrivateAttr, field_validator, model_validator

from .other import PollIntervalUnit

_NAME_RE = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9 _\-]*[a-zA-Z0-9])*$")

# Same conversion table the shared poll-interval validator uses to measure an
# interval in seconds before range-checking it.
_SYNC_INTERVAL_MULTIPLIER = {
    PollIntervalUnit.SECONDS: 1,
    PollIntervalUnit.MINUTES: 60,
    PollIntervalUnit.HOURS: 60 * 60,
    PollIntervalUnit.DAYS: 60 * 60 * 24,
}


class MatchOperator(str, Enum):
    """Supported match operators for join conditions and filters.

    Join conditions are restricted to EQUALS (enforced during validation); all
    operators remain available for post-join filters.
    """

    EQUALS = "equals"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IS_EMPTY = "is_empty"
    IS_NOT_EMPTY = "is_not_empty"


class MatchMode(str, Enum):
    """Row selection mode when executing a unified mapping.

    ALL          — every base/left row (matched and unmatched).
    MATCHED_ONLY — only rows whose joined right side is present.
    UNMATCHED_ONLY — only rows whose joined right side is null.
    """

    ALL = "all"
    MATCHED_ONLY = "matched_only"
    UNMATCHED_ONLY = "unmatched_only"


class JoinCondition(BaseModel):
    """Single field-pair condition within a join rule."""

    leftField: str = Field(
        ...,
        description="Field name on the selected left table, e.g. 'value' or 'sources.source'.",
    )
    rightField: str = Field(..., description="Field name on the right table.")
    operator: MatchOperator = Field(MatchOperator.EQUALS, description="Match operator.")

    @field_validator("leftField", "rightField")
    @classmethod
    def validate_field_not_blank(cls, v: str, info) -> str:
        """Left/right field names must not be empty."""
        if not v:
            side = "Left" if info.field_name == "leftField" else "Right"
            raise HTTPException(400, f"{side} field must not be empty in a join condition.")
        return v

    @field_validator("operator")
    @classmethod
    def validate_operator_is_equals(cls, operator: MatchOperator) -> MatchOperator:
        """Join conditions support exact match only."""
        if operator != MatchOperator.EQUALS:
            raise HTTPException(
                400,
                "Join conditions support exact match only. "
                f"Operator '{operator.value}' is not allowed in a join.",
            )
        return operator


class JoinRule(BaseModel):
    """Describes one ordered join step in a unified mapping."""

    leftTable: str = Field(
        ..., description="Collection name already present in the result (the left side of this join)."
    )
    rightTable: str = Field(..., description="Collection name of the right-side table to attach.")
    condition: JoinCondition = Field(..., description="The field-pair condition for this join.")

    @field_validator("rightTable")
    @classmethod
    def validate_right_table_not_blank(cls, v: str) -> str:
        """Right table must not be empty."""
        if not v:
            raise HTTPException(400, "Right table must not be empty.")
        return v


_ALLOWED_FILTER_OPERATORS = {
    # Logical
    "$and", "$or", "$nor", "$not",
    # Comparison
    "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin",
    # Element
    "$exists", "$type",
    # Evaluation — regex only. Deliberately excludes $where, $expr, $function,
    # $accumulator, $jsonSchema: none of those are needed by a query-builder
    # filter, and several allow arbitrary server-side JavaScript execution.
    "$regex", "$options",
    # Array
    "$all", "$elemMatch", "$size",
}


def _assert_filter_operators_allowed(node) -> None:
    """Recursively reject any MongoDB operator key not in the explicit allowlist.

    This filter is user-controlled JSON fed almost directly into a $match stage
    at execute time (see _filter_match_stage in the unified_mapping router).
    Without this, a caller could submit an operator like $where or $function
    (arbitrary server-side JavaScript) via the preview endpoint. Allowlisting
    (reject-by-default) rather than blocklisting a handful of known-dangerous
    names, so an operator not explicitly vetted is rejected by default instead
    of silently passed through.
    """
    if isinstance(node, list):
        for item in node:
            _assert_filter_operators_allowed(item)
        return
    if not isinstance(node, dict):
        return
    for key, value in node.items():
        if key.startswith("$") and key not in _ALLOWED_FILTER_OPERATORS:
            raise HTTPException(400, f"Filter operator '{key}' is not supported.")
        _assert_filter_operators_allowed(value)


class ViewFilter(BaseModel):
    """Query-builder filter applied after the join.

    Mirrors the CREv2 business-rule ``entityFilters`` shape:
      * ``query`` — human-readable filter string (for display / reload).
      * ``mongo`` — the MongoDB query as a JSON string, applied as a $match.
    Field keys are the flattened result keys ("tableName.fieldName"); base-table
    keys are translated to top-level paths at execute time.

    Preview/execute-time only — validated whenever present, but never persisted
    with the saved mapping (not stored by _persist_mapping, not returned on
    UnifiedMappingOut). A saved mapping always executes unfiltered; only a preview
    request can supply an ad-hoc filter.
    """

    query: str = Field("", description="Human-readable filter string from the query builder.")
    mongo: str = Field("{}", description="MongoDB query (JSON string) from the query builder.")

    @field_validator("mongo")
    @classmethod
    def validate_mongo_is_json_object(cls, v: str) -> str:
        """Require a JSON object using only allowed operators, when present.

        Blank values and the default '{}' are ignored.
        """
        stripped = (v or "").strip()
        if not stripped or stripped == "{}":
            return v
        try:
            parsed = json.loads(stripped)
        except (ValueError, TypeError):
            raise HTTPException(400, "Filter query is not valid JSON.")
        if not isinstance(parsed, dict):
            raise HTTPException(400, "Filter query must be a JSON object.")
        _assert_filter_operators_allowed(parsed)
        return v


class UnifiedMappingIn(BaseModel):
    """Incoming payload to create a unified mapping."""

    name: str = Field(..., min_length=1, max_length=256, description="Human-readable mapping name.")
    baseTable: str = Field(..., description="Collection name of the left (base) table.")
    joins: list[JoinRule] = Field(..., min_length=1, description="At least one join rule is required.")
    columns: list[str] = Field(
        default=[],
        description=(
            "Column selection in 'table.field' format. Empty means all columns are "
            "selected; when set, only those columns are saved and used wherever the "
            "mapping's pipeline is consumed. Selecting a 'sources.*' column also makes "
            "the pipeline $unwind that array, which multiplies rows."
        ),
    )
    filter: ViewFilter = Field(
        default_factory=ViewFilter,
        description=(
            "MongoDB query-builder filter (JSON string). Validated whenever present, "
            "but only applied on preview — create/update accept it for a consistent "
            "request shape but never persist it; a saved mapping always executes unfiltered."
        ),
    )
    caseInsensitive: bool = Field(
        False,
        description=(
            "When True, all join comparisons ignore letter case (e.g. 'Host-A' matches 'host-a'). "
            "The aggregate runs with a case-insensitive collation (locale 'en', strength 2) and "
            "join indexes are created with the same collation (um_join_*_ci) so they are usable. "
            "When False (default), joins are case-sensitive and use plain indexes — faster index "
            "creation and compatible with all MongoDB versions without collation support."
        ),
    )
    matchMode: MatchMode = Field(
        MatchMode.ALL,
        description=(
            "The mapping's saved default match mode (all/matched_only/unmatched_only), "
            "persisted with the mapping and loaded by the UI on open. A '/execute' "
            "call may override it per-request via the match_mode query param "
            "without changing this saved value."
        ),
    )
    # Declared before syncInterval so the unit is already validated and present
    # in info.data when the interval is range-checked — the same ordering every
    # module's pollIntervalUnit/pollInterval pair uses for its own validator.
    syncIntervalUnit: PollIntervalUnit = Field(
        ...,
        description="Unit of the syncInterval parameter.",
    )
    syncInterval: int = Field(
        ...,
        ge=1,
        description=(
            "Interval at which the mapping's scheduled sharing task "
            "(cte.um_share_indicators) runs. Required — every mapping is created "
            "with its schedule."
        ),
    )

    @field_validator("syncInterval")
    @classmethod
    def validate_sync_interval(cls, value: int, info) -> int:
        """Bound the interval the way every module bounds its poll interval.

        Same shape as ``validate_poll_interval`` in
        ``netskope.common.utils.validators``, but with a higher floor: the value
        is converted to seconds and must land between 30 minutes and 1 year.

        Reimplemented rather than imported because that module imports
        ``netskope.common.models``, so importing it from a model inside that same
        package would close an import cycle — settings.py defers its
        ``..utils.proxy`` imports for the same reason.
        """
        unit = info.data.get("syncIntervalUnit")
        if unit is None:
            # syncIntervalUnit failed its own validation; that error is enough.
            return value
        interval_in_seconds = value * _SYNC_INTERVAL_MULTIPLIER[unit]
        if not 30 * 60 <= interval_in_seconds <= (60 * 60 * 24 * 365):
            raise ValueError(
                "Sync interval must be between 30 minutes and 1 year."
            )
        return value

    @field_validator("name")
    @classmethod
    def validate_name(cls, name: str) -> str:
        """Strip whitespace and enforce the shared plugin-config name pattern."""
        name = name.strip()
        if not name:
            raise ValueError("Mapping name must not be blank.")
        if not _NAME_RE.match(name):
            raise ValueError(
                "Mapping name should start and end with an alpha-numeric character "
                "and can include alpha-numeric characters, dashes, underscores and spaces."
            )
        return name

    @field_validator("baseTable")
    @classmethod
    def validate_base_table_not_blank(cls, v: str) -> str:
        """Require a start/base table."""
        if not v:
            raise HTTPException(400, "A start/base table is required for a Unified Join Builder.")
        return v

    @model_validator(mode="after")
    def validate_join_structure(self) -> "UnifiedMappingIn":
        """Ordered join references: no self-joins, no duplicate/out-of-order tables."""
        available_tables: set[str] = {self.baseTable}
        for join in self.joins:
            if join.leftTable not in available_tables:
                raise HTTPException(
                    400,
                    f"Left table '{join.leftTable}' must be the base table or a table "
                    "joined in an earlier step.",
                )
            if join.rightTable == self.baseTable:
                raise HTTPException(
                    400,
                    f"Right table '{join.rightTable}' cannot be the same as the base table.",
                )
            if join.rightTable in available_tables:
                raise HTTPException(
                    400,
                    f"Right table '{join.rightTable}' is already used in this unified mapping.",
                )
            available_tables.add(join.rightTable)
        return self


# ---------------------------------------------------------------------------
# Unified mapping validation against live DB state
#
# ``UnifiedMappingIn`` itself stays free of these: it is also the model a STORED
# mapping is rebuilt into (``_doc_to_mapping_in`` in the router, used by
# ``/execute``), and re-validating a saved mapping against current DB state on
# every render would start rejecting mappings that were valid when saved. The
# request models below add the DB-aware checks — the same separation CTE and
# CREv2 keep between ``BusinessRuleIn``/``BusinessRuleUpdate`` (validated) and
# ``BusinessRuleDB`` (rebuilt from storage, no validators).
#
# The module-enabled gate (``cte_platform_enabled``) and the per-user
# ``cte_read``/``cte_write`` check stay in the router, matching how CTE and
# CREv2 keep ``is_platform_enabled`` in their create endpoints while all field
# validation lives in the model.
# ---------------------------------------------------------------------------


def cte_platform_enabled() -> bool:
    """Return whether the Threat Exchange (CTE) module is enabled tenant-wide.

    This is the Settings > General "platforms" toggle — independent of any single
    user's ``cte_read``/``cte_write`` scope. An admin can carry every scope on their
    token yet have turned CTE off tenant-wide because it isn't licensed/used, and in
    that case indicators must stay off-limits for everyone, not just users lacking
    the scope. Fails closed (returns False) if the settings document is missing.
    """
    from ..utils import Collections, DBConnector

    settings_doc = DBConnector().collection(Collections.SETTINGS).find_one({})
    if not settings_doc:
        return False
    return bool((settings_doc.get("platforms") or {}).get("cte", False))


def get_allowed_mapping_collections(cte_enabled: bool) -> set:
    """Return the set of all currently allowed collection names.

    Excludes the Threat Indicators CRE entity — it's a hand-maintained mirror of
    indicator data for CRE's own business-rule builder (not kept in sync with the
    Indicator model), so the native ``indicators`` collection is the accurate
    source for the same data.

    Also excludes ``indicators`` outright when ``cte_enabled`` is False (the CTE
    platform is disabled tenant-wide) — this is independent of the calling user's
    scopes, which this validation has no visibility into, so a tenant-disabled
    module can never be joined into a mapping regardless of who is building it.
    The per-user cte_read/cte_write check happens separately in the router.
    """
    from ..utils import Collections, DBConnector
    from netskope.integrations.cte.utils.entity import THREAT_INDICATORS_ENTITY

    names = set()
    if cte_enabled:
        names.add(Collections.INDICATORS.value)
    prefix = Collections.CREV2_ENTITY_PREFIX.value
    for entity in DBConnector().collection(Collections.CREV2_ENTITIES).find(
        {}, {"name": 1, "_id": 0}
    ):
        if entity["name"] == THREAT_INDICATORS_ENTITY:
            continue
        names.add(f"{prefix}{entity['name']}")
    return names


def mapping_collection_fields(collection_name: str) -> list:
    """Return field metadata for a collection (raises 404 if unknown)."""
    from ..utils import Collections
    from ..utils.unified_mapping_fields import get_unified_mapping_collection_fields

    fields = get_unified_mapping_collection_fields(collection_name)
    if not fields and collection_name != Collections.INDICATORS.value:
        raise HTTPException(404, f"Collection '{collection_name}' not found or has no fields.")
    return fields


def validate_mapping_tables(
    mapping: "UnifiedMappingIn", cte_enabled: bool, fields_cache: dict
) -> None:
    """Validate collection names and field existence/uniqueness against live DB state.

    ``fields_cache`` — see ``cached_table_fields`` in unified_mapping_fields. This
    reads every table the mapping touches, and index creation and pipeline building
    then read the same ones; the cache is populated here and handed back to the
    router (via ``_UnifiedMappingValidated.fields_cache``) so a request still makes
    one read per table rather than one per phase.
    """
    from ..utils.unified_mapping_fields import join_value_types_compatible

    allowed = get_allowed_mapping_collections(cte_enabled)

    if mapping.baseTable not in allowed:
        raise HTTPException(400, f"Start/Base table '{mapping.baseTable}' is not an allowed collection.")

    def field_meta(table: str) -> dict:
        # Populated here rather than via cached_table_fields so an unknown collection
        # still raises mapping_collection_fields' 404 on the first read of that table.
        if table not in fields_cache:
            fields_cache[table] = {
                field["name"]: field for field in mapping_collection_fields(table)
            }
        return fields_cache[table]

    for join in mapping.joins:
        left_table = join.leftTable
        if left_table not in allowed:
            raise HTTPException(400, f"Left table '{left_table}' is not an allowed collection.")
        if join.rightTable not in allowed:
            raise HTTPException(400, f"Right table '{join.rightTable}' is not an allowed collection.")

        left_fields = field_meta(left_table)
        right_fields = field_meta(join.rightTable)
        condition = join.condition
        if condition.leftField not in left_fields:
            raise HTTPException(
                400, f"Left field '{condition.leftField}' is not present in table '{left_table}'."
            )
        if condition.rightField not in right_fields:
            raise HTTPException(
                400,
                f"Right field '{condition.rightField}' is not present in right table '{join.rightTable}'.",
            )
        # Many-to-many guard: at least one side of the join must be a unique field.
        if not left_fields[condition.leftField].get("unique") and not right_fields[condition.rightField].get("unique"):
            raise HTTPException(
                400,
                f"Cannot join on two non-unique fields ('{left_table}.{condition.leftField}' and "
                f"'{join.rightTable}.{condition.rightField}'). At least one side must be a unique field.",
            )
        # Datatype guard: $lookup equality is BSON type-sensitive, so a
        # text↔number join matches nothing at all. Left unvalidated it saves,
        # indexes and runs, and the only symptom is an empty result table.
        left_value_type = left_fields[condition.leftField].get("valueType")
        right_value_type = right_fields[condition.rightField].get("valueType")
        if not join_value_types_compatible(left_value_type, right_value_type):
            raise HTTPException(
                400,
                f"Cannot join '{left_table}.{condition.leftField}' ({left_value_type}) with "
                f"'{join.rightTable}.{condition.rightField}' ({right_value_type}) — the fields "
                "store different data types, so the join would never match. Pick fields that "
                "store the same type.",
            )


def assert_unique_mapping_name(name: str, exclude_id=None) -> None:
    """Reject a duplicate mapping name (optionally excluding the mapping being updated)."""
    from ..utils import Collections, DBConnector

    query: dict = {"name": name}
    if exclude_id is not None:
        query["_id"] = {"$ne": exclude_id}
    if DBConnector().collection(Collections.UNIFIED_MAPPING).find_one(query):
        raise HTTPException(400, f"A unified mapping named '{name}' already exists.")


class _UnifiedMappingValidated(UnifiedMappingIn):
    """A mapping payload checked against live DB state.

    Carries the table/field validation every request-bound mapping needs. The
    ``cte_enabled`` flag and the field cache it builds are exposed so the router
    can reuse both instead of recomputing them — the cache in particular is what
    keeps a request at one field read per table across validation, index creation
    and pipeline building.
    """

    _fields_cache: dict = PrivateAttr(default_factory=dict)
    _cte_enabled: bool = PrivateAttr(default=False)

    @model_validator(mode="after")
    def validate_tables(self) -> "_UnifiedMappingValidated":
        """Check every referenced collection and join field against the database."""
        self._cte_enabled = cte_platform_enabled()
        validate_mapping_tables(self, self._cte_enabled, self._fields_cache)
        return self

    @property
    def fields_cache(self) -> dict:
        """Field metadata read during validation, for the caller's later phases."""
        return self._fields_cache

    @property
    def cte_enabled(self) -> bool:
        """Whether the CTE platform was enabled when this payload was validated."""
        return self._cte_enabled


class UnifiedMappingPreview(_UnifiedMappingValidated):
    """Draft mapping executed without being saved — no name-uniqueness check."""


class UnifiedMappingUpdate(_UnifiedMappingValidated):
    """Mapping payload for an update.

    Uniqueness is not checked here: the name is immutable (the endpoint restores
    the stored one) and the check needs the mapping's own id to exclude itself,
    which only the router has.
    """


class UnifiedMappingCreate(_UnifiedMappingValidated):
    """Mapping payload for a create, including the name-uniqueness check."""

    @model_validator(mode="after")
    def validate_name_is_unique(self) -> "UnifiedMappingCreate":
        """Reject a name already taken, after the tables have been validated.

        Runs after ``validate_tables`` (pydantic runs a base class's "after"
        validators before a subclass's), preserving the order these checks ran
        in while they lived in the router.
        """
        assert_unique_mapping_name(self.name)
        return self


class FieldMeta(BaseModel):
    """Metadata for a single collection field.

    Every key ``get_all_unified_mapping_collections`` produces must be declared
    here: ``/collections`` returns ``list[CollectionMeta]``, so an undeclared key
    is silently DROPPED from the response -- the metadata layer looks correct, the
    backend's own validation (which reads the helpers directly) looks correct, and
    only the UI is left blind. ``/unified-mapping/rules/fields`` returns
    ``list[dict]`` and so has no such filter, which is why a missing key here
    shows up as "the rules form works but the join builder doesn't".
    """

    name: str
    label: str
    type: str
    unique: bool = False
    normalized: bool = False
    # The stored EntityFieldType behind the widget ``type``. Read by the CRE
    # action form's parameter picker (UM_SOURCE_TYPE_TO_WIDGET in the UI).
    sourceType: Optional[str] = None
    # None for indicator fields, which have no CRE coalesce strategy.
    coalesceStrategy: Optional[str] = None
    # The stored primitive; None when unknown (a Reference). Drives the join
    # datatype guard, mirrored client-side in unifiedMappingGraph.js.
    valueType: Optional[str] = None
    # Whether the record holds an array: a List, or any type using the append
    # coalesce strategy. Has no client-side fallback -- `type` cannot imply it.
    multiValued: bool = False
    # Whether the field may be a field-mapping / action-parameter source: False
    # for the CE-managed fields (see UNMAPPABLE_SOURCE_FIELDS).
    mappable: bool = True
    # Query-builder options for a select widget (a Value Map String's configured
    # value -> label mappings), read by the Filters section's selectListValues.
    fieldSettings: Optional[dict] = None
    # The authored value set of a Value Map String / Range Map, else None. ``[]``
    # means the map has no mappings, so it can never match and stores nothing --
    # which is why the field-mapping editor excludes it.
    mapLabels: Optional[list] = None
    nested_array: Optional[str] = None
    subKey: Optional[str] = None
    values: Optional[list[str]] = None


class CollectionMeta(BaseModel):
    """Metadata for an available collection."""

    collection: str
    label: str
    fields: list[FieldMeta]


class UnifiedMappingOut(BaseModel):
    """Outgoing representation of a saved unified mapping."""

    id: str
    name: str
    baseTable: str
    joins: list[JoinRule]
    columns: list[str]
    query: list[dict]
    createdAt: datetime
    updatedAt: datetime
    caseInsensitive: bool = False
    matchMode: MatchMode = MatchMode.ALL
    syncInterval: Optional[int] = None
    syncIntervalUnit: Optional[PollIntervalUnit] = None
    lastRunAt: Optional[datetime] = Field(
        None, description="When the mapping's scheduled share task last ran."
    )
    lastRunSuccess: Optional[bool] = Field(
        None, description="Whether the last scheduled share run succeeded."
    )
    lastActionRunAt: Optional[datetime] = Field(
        None,
        description=(
            "When the mapping's scheduled CRE action task last ran. Separate "
            "from lastRunAt because sharing and actions run on independent "
            "schedules and either module can be disabled on its own."
        ),
    )
    lastActionRunSuccess: Optional[bool] = Field(
        None, description="Whether the last scheduled CRE action run succeeded."
    )


class ExecuteResult(BaseModel):
    """Result of executing a unified mapping pipeline (one page of rows)."""

    fields: list[dict]
    rows: list[dict]
    skip: int
    limit: int
    # null only when the request asked to skip counting (with_counts=false), which a
    # caller does when it already holds these numbers — see the endpoints' with_counts
    # parameter. Any request that doesn't opt out gets all three, as before.
    total: Optional[int] = Field(
        None, description="Total rows for the active match mode (pre-pagination)."
    )
    total_matched: Optional[int] = Field(
        None, description="Rows whose joined right side is present."
    )
    total_unmatched: Optional[int] = Field(
        None, description="Rows whose joined right side is null."
    )
    sources_expanded: bool


class UmShareAction(BaseModel):
    """Sharing action for a unified-mapping business rule (destination action).

    The IOC field mapping used to turn a rule's joined rows into indicators
    lives on the rule itself (``UnifiedMappingRuleIn.fieldMapping``), not per
    action — every action/destination on a rule shares the same rows, so a
    single rule-level mapping is enough (mirrors the CRE-entity business rule
    precedent, which moved an equivalent per-action mapping to the rule).
    """

    label: str = Field(...)
    value: str = Field(...)
    parameters: Dict = Field({})
    generateAlert: bool = Field(
        False,
        description="Raise a CTO alert for every indicator shared by this action.",
    )


class UmCreAction(BaseModel):
    """CRE action for a unified-mapping business rule (per destination configuration).

    Structurally mirrors the CREv2 ``Action`` model rather than importing it:
    ``crev2.models`` imports ``netskope.common.utils``, which imports this
    module, so importing the other way would close an import cycle. The CRE task
    rebuilds a real ``Action`` from these fields before executing. Same
    precedent as ``UmShareAction`` mirroring the CTE action shape.

    ``performRevert`` is deliberately absent: revert-on-unmatch needs a
    per-record ``lastEvals`` anchor a joined row does not have.
    """

    label: str = Field(...)
    value: str = Field(...)
    parameters: Dict = Field({})
    performLater: bool = Field(
        False,
        description=(
            "Defer the action to the CRE maintenance window instead of "
            "performing it during this cycle."
        ),
    )
    requireApproval: bool = Field(
        False,
        description="Queue the action for manual approval before it is performed.",
    )
    generateAlert: bool = Field(
        False,
        description="Raise a CRE alert for every row this action is performed on.",
    )


class UnifiedMappingRuleException(BaseModel):
    """Mute exception for a unified mapping rule (mirrors the CTE Exceptions shape)."""

    name: str = Field(...)
    filters: Union[ViewFilter, None] = Field(None)
    tags: Union[List[str], None] = Field(None)

    @field_validator("tags")
    @classmethod
    def validate_tags_fields(cls, v, values, **kwargs):
        """Validate that exactly one of filters/tags is set."""
        values = values.data
        if values.get("filters") is None and v is None:
            raise ValueError("filters and tags can not both be empty.")
        if None not in [v, values.get("filters")]:
            raise ValueError("filters and tags can not both be set.")
        return v


# ---------------------------------------------------------------------------
# Unified mapping business rule validation
#
# These live here rather than in the router so a rule payload is validated
# wherever it is constructed, matching how the CTE and CREv2 business rules do
# it (``validate_sharedWith`` / ``validate_creShare`` / ``validate_field_mapping``
# in ``cte/models/business_rule.py`` are module-level functions attached to the
# models via validators, and the routers just persist an already-valid rule).
#
# They raise ``HTTPException(400, ...)`` rather than ``ValueError``. Pydantic
# only wraps ``ValueError``/``AssertionError``, so an ``HTTPException`` escapes
# validation untouched and FastAPI renders it as a 400 with a plain string
# ``detail`` — exactly what these checks returned while they lived in the
# router, and what the models here already do (see ``validate_operator_is_equals``
# and ``_assert_filter_operators_allowed``). A ``ValueError`` would instead
# surface as a 422 with a list-shaped ``detail``, changing the contract the UI
# reads.
#
# Every DB/integration import is function-local: this module is imported by
# ``netskope.common.models.__init__``, which ``netskope.common.utils`` imports
# in turn, so a module-level import of either would close a cycle. Same reason
# ``settings.py`` defers its ``..utils.proxy`` imports.
# ---------------------------------------------------------------------------


def get_rule_mapping_doc(mapping_name: str) -> dict:
    """Return the stored mapping document a rule references (400 if missing)."""
    from ..utils import Collections, DBConnector

    mapping_doc = DBConnector().collection(Collections.UNIFIED_MAPPING).find_one(
        {"name": mapping_name}
    )
    if not mapping_doc:
        raise HTTPException(400, f"Unified mapping '{mapping_name}' does not exist.")
    return mapping_doc


def _mapping_fields_by_key(mapping_doc: dict) -> dict:
    """Return ``{"table.field": field_meta}`` for every field a mapping's rules can use.

    Derived from the same field list the ``/rules/fields`` endpoint serves, so
    what a rule is allowed to reference and what the builder offers can never
    drift apart. The whole entry is kept, not just one key: an IOC field's
    accepted sources are decided by ``valueType``, ``multiValued``, ``unique``
    and ``mapLabels`` together (see ``IOC_FIELD_VALUE_TYPES``,
    ``no_value_reason`` and ``validate_bounded_labels`` in
    ``common/utils/unified_mapping_fields.py``).
    """
    from ..utils.unified_mapping_fields import get_unified_mapping_fields

    return {
        field["key"]: field
        for field in get_unified_mapping_fields(mapping_doc)
    }


def _parse_rule_filter_mongo(raw: str, label: str) -> dict:
    """Parse a rule/exception mongo filter string (400 on invalid JSON)."""
    raw = (raw or "").strip()
    if not raw or raw == "{}":
        return {}
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        raise HTTPException(400, f"{label} query is not valid JSON.")
    if not isinstance(parsed, dict):
        raise HTTPException(400, f"{label} query must be a JSON object.")
    return parsed


def validate_rule_filters(filters, exceptions, mapping_doc: dict) -> None:
    """Validate a rule's filter and exception queries against the mapping's fields."""
    from ..utils.unified_mapping_exec import collect_filter_keys
    from ..utils.unified_mapping_fields import get_unified_mapping_fields

    allowed = {field["key"] for field in get_unified_mapping_fields(mapping_doc)}
    labelled_queries = [("Filter", filters.mongo)]
    for exception in exceptions or []:
        if exception.filters is not None:
            labelled_queries.append(
                (f"Exception '{exception.name}'", exception.filters.mongo)
            )
    for label, raw in labelled_queries:
        parsed = _parse_rule_filter_mongo(raw, label)
        if not parsed:
            continue
        referenced: set = set()
        collect_filter_keys(parsed, referenced)
        unknown = sorted(referenced - allowed)
        if unknown:
            raise HTTPException(
                400,
                f"{label} query references field(s) not present in mapping "
                f"'{mapping_doc['name']}': {', '.join(unknown)}. Use flattened "
                "'table.field' keys.",
            )


# ``IOC_FIELD_VALUE_TYPES`` and ``IOC_MULTI_VALUED_FIELDS`` moved to
# common/utils/unified_mapping_fields.py, now the single source for "can this
# field feed this IOC field?" -- the CTE business rule reads the same two.
# Imported locally below, like every other ``..utils`` use here (cycle avoidance).


def validate_rule_mapping_payload(field_mapping: dict) -> None:
    """Reject a field mapping that must never be stored, whatever the rule shares.

    Both checks answer a question about the mapping alone -- which IOC fields it
    sets, and which fields it reads -- so unlike the type checks in
    ``validate_rule_field_mapping`` neither may wait for ``cteShare``. A mapping
    that reaches value+type locks (``_mapping_is_complete``), so one stored with
    a bad key or a closed source would be rejected the moment sharing was added
    and could no longer be edited: delete-and-recreate as the only way out.
    Hence a single unconditional call from ``validate_rule_config``.

    The key rule is the CTE business rule model's, so both modules answer that
    question the same way. Raised as a 400 like every other check in this file.
    """
    from netskope.integrations.cte.models.business_rule import validate_mapping_keys
    from ..utils.unified_mapping_fields import (
        unmappable_source_fields,
        unmappable_source_message,
    )

    try:
        validate_mapping_keys(field_mapping)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    closed = unmappable_source_fields((field_mapping or {}).values())
    if closed:
        raise HTTPException(400, unmappable_source_message(closed))


def validate_rule_field_mapping(field_mapping: dict, mapping_doc: dict) -> dict:
    """Require 'value' and 'type' once a rule actually needs to share indicators.

    fieldMapping itself is optional on a unified mapping rule, and editable until
    it is complete (a rule meant only for a future CRE action never needs one) —
    but the moment cteShare is non-empty, the rule must be able to turn its
    joined rows into indicators, so value/type become mandatory right here
    rather than at rule save time.

    ``$table.field`` specs are checked against the mapping's real fields — the
    stored primitive it holds and whether it holds an array — and static literals
    against the CTE business rule model's own bounds, so a mapping the share task
    would reject per row cannot be saved. Returns the mapping with a defaulted
    ``expiresAt``, so callers must use the returned value.

    Which IOC fields may be mapped, and which fields may feed them, is not asked
    here: those hold whether or not a rule shares, so
    ``validate_rule_mapping_payload`` answers them on every save instead.
    """
    from netskope.integrations.cte.models.business_rule import (
        DEFAULT_EXPIRES_AT_DAYS,
        _validate_static_value,
    )
    from ..utils.unified_mapping_fields import (
        IOC_FIELD_VALUE_TYPES,
        IOC_MULTI_VALUED_FIELDS,
        no_value_reason,
        validate_bounded_labels,
    )

    field_mapping = field_mapping or {}
    if not field_mapping.get("value") or not field_mapping.get("type"):
        raise HTTPException(
            400,
            "This rule needs a Field Mapping with both 'Value' and 'Type' set "
            "before it can be shared to a CTE destination. Go to Universal "
            "Schema Builder > Business Rules, edit this rule, and complete "
            "its Field Mapping section.",
        )
    if "expiresAt" not in field_mapping:
        field_mapping = {**field_mapping, "expiresAt": DEFAULT_EXPIRES_AT_DAYS}
    fields_by_key = _mapping_fields_by_key(mapping_doc)
    unknown = sorted(
        spec[1:]
        for spec in field_mapping.values()
        if isinstance(spec, str) and spec.startswith("$") and spec[1:] not in fields_by_key
    )
    if unknown:
        raise HTTPException(
            400,
            f"Field Mapping references field(s) not present in mapping "
            f"'{mapping_doc['name']}': {', '.join(unknown)}. Use flattened "
            "'table.field' keys.",
        )
    for ioc_field, spec in field_mapping.items():
        if isinstance(spec, str) and spec.startswith("$"):
            key = spec[1:]
            field = fields_by_key[key]
            accepted = IOC_FIELD_VALUE_TYPES.get(ioc_field)
            primitive = field.get("valueType")
            # Resolves to None on every row, silently leaving the IOC field at
            # its default -- rejected whatever its primitive says.
            no_value = no_value_reason(
                field.get("sourceType"),
                field.get("unique"),
                field.get("mapLabels"),
            )
            if no_value:
                raise HTTPException(
                    400,
                    f"The '{ioc_field}' field mapping cannot reference "
                    f"'{key}': {no_value} and holds no value.",
                )
            bounded = validate_bounded_labels(
                ioc_field, key, field.get("mapLabels")
            )
            if bounded:
                raise HTTPException(400, bounded)
            if ioc_field in IOC_MULTI_VALUED_FIELDS:
                # The share task wraps a scalar, so only the ELEMENT type
                # matters -- but an unknown primitive (a Reference) is refused
                # here as it is for the scalar fields below, since whatever its
                # target holds would go straight into Indicator.tags.
                if accepted and not primitive:
                    raise HTTPException(
                        400,
                        f"The '{ioc_field}' field mapping must reference a field "
                        f"holding {' or '.join(sorted(accepted))}; what '{key}' "
                        "holds is not known.",
                    )
                if accepted and primitive not in accepted:
                    raise HTTPException(
                        400,
                        f"The '{ioc_field}' field mapping must reference a field "
                        f"holding {' or '.join(sorted(accepted))}; '{key}' holds "
                        f"{primitive}.",
                    )
                continue
            if field.get("multiValued"):
                # The share task drops every record a list reaches (CTE_1101),
                # so nothing ships; checked first as an append String is text.
                raise HTTPException(
                    400,
                    f"The '{ioc_field}' field mapping must reference a field "
                    f"holding one value; '{key}' holds a list (a List field, or "
                    "one using the append strategy), so it can only be mapped to "
                    "a list field such as Tags.",
                )
            if accepted and not primitive:
                raise HTTPException(
                    400,
                    f"The '{ioc_field}' field mapping must reference a field "
                    f"holding {' or '.join(sorted(accepted))}; what '{key}' holds "
                    "is not known.",
                )
            if accepted and primitive not in accepted:
                raise HTTPException(
                    400,
                    f"The '{ioc_field}' field mapping must reference a field "
                    f"holding {' or '.join(sorted(accepted))}; '{key}' holds "
                    f"{primitive}.",
                )
            continue
        try:
            _validate_static_value(ioc_field, spec)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
    return field_mapping


def validate_cte_share(cte_share: dict, field_mapping: dict, mapping_doc: dict) -> dict:
    """Validate every cteShare destination configuration and action.

    Returns the field mapping to persist (``expiresAt`` defaulted when absent).
    """
    from ..utils import Collections, DBConnector, Logger, PluginHelper, SecretDict
    from netskope.integrations.cte.utils.constants import CTE_NO_ACTION_VALUE
    from netskope.integrations.cte.models import ConfigurationDB

    connector = DBConnector()
    helper = PluginHelper()
    logger = Logger()
    if cte_share:
        field_mapping = validate_rule_field_mapping(field_mapping, mapping_doc)
    for destination, actions in (cte_share or {}).items():
        config_doc = connector.collection(Collections.CONFIGURATIONS).find_one(
            {"name": destination}
        )
        if config_doc is None:
            raise HTTPException(
                400, f"Destination configuration '{destination}' does not exist."
            )
        configuration = ConfigurationDB(**config_doc)
        PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR
        if PluginClass is None:
            raise HTTPException(
                400,
                f"Plugin with id='{configuration.plugin}' for destination "
                f"configuration '{destination}' does not exist.",
            )
        plugin = PluginClass(
            configuration.name,
            SecretDict(configuration.parameters),
            configuration.storage,
            configuration.checkpoint,
            logger,
            ssl_validation=configuration.sslValidation,
        )
        for action in actions:
            if action.value == CTE_NO_ACTION_VALUE:
                # Core-level pseudo-action: nothing is pushed to the destination
                # plugin, so the share would be a no-op without alert generation
                # (mirrors validate_creShare in the CTE business rule model).
                # Raised as a 400 like every other check here — while this lived
                # in the router it was a bare ValueError, which FastAPI does not
                # translate, so the request died as a 500 instead.
                if not action.generateAlert:
                    raise HTTPException(
                        400,
                        "Generate Alert must be enabled when 'No Action' "
                        "is selected as the target.",
                    )
                continue
            result = plugin.validate_action(action)
            if not result.success:
                raise HTTPException(400, result.message)
    return field_mapping


def validate_cre_actions(cre_actions: dict) -> None:
    """Validate every creActions CRE configuration and action.

    Mirrors CREv2's own ``validate_actions`` (crev2/models/business_rules.py):
    each action is validated by the destination plugin, and the plugin's storage
    is persisted afterwards because ``validate_action`` may populate it (e.g.
    caching a looked-up remote id).

    Unlike ``cteShare``, no ``fieldMapping`` is required — a CRE action resolves
    its own ``$<table>.<field>`` parameters against the joined row instead of
    building an indicator out of it.
    """
    from ..utils import Collections, DBConnector, Logger, PluginHelper, SecretDict
    from ..utils.unified_mapping_fields import (
        unmappable_action_sources,
        unmappable_source_message,
    )
    from netskope.integrations.crev2.models import Action, ConfigurationDB

    # Checked before any plugin is instantiated: a closed source is a payload
    # error, not something a destination plugin should be asked about.
    closed = unmappable_action_sources(cre_actions)
    if closed:
        raise HTTPException(400, unmappable_source_message(closed))

    connector = DBConnector()
    helper = PluginHelper()
    logger = Logger()
    for config_name, actions in (cre_actions or {}).items():
        config_doc = connector.collection(
            Collections.CREV2_CONFIGURATIONS
        ).find_one({"name": config_name})
        if config_doc is None:
            raise HTTPException(
                400, f"CRE configuration '{config_name}' does not exist."
            )
        configuration = ConfigurationDB(**config_doc)
        PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR
        if PluginClass is None:
            raise HTTPException(
                400,
                f"Plugin with id='{configuration.plugin}' for CRE configuration "
                f"'{config_name}' does not exist.",
            )
        plugin = PluginClass(
            configuration.name,
            SecretDict(configuration.parameters),
            configuration.storage,
            configuration.checkpoints,
            logger,
        )
        if configuration.mappedEntities:
            plugin.mappedEntities = [
                mapped_entity.model_dump()
                for mapped_entity in configuration.mappedEntities
            ]
        for action in actions:
            result = plugin.validate_action(Action(**action.model_dump()))
            if not result.success:
                raise HTTPException(400, result.message)
        connector.collection(Collections.CREV2_CONFIGURATIONS).update_one(
            {"name": configuration.name},
            {"$set": {"storage": plugin.storage or {}}},
        )


class _UnifiedMappingRuleBase(BaseModel):
    """Fields and payload validation shared by the create and update rule models.

    Split create/update the way the CTE business rule models are (``BusinessRuleIn``
    carries the name-uniqueness check, ``BusinessRuleUpdate`` does not), since a
    rule being updated legitimately already exists under its own name.
    """

    name: str = Field(..., min_length=1, max_length=256, description="Human-readable rule name.")
    view: str = Field(
        ...,
        description="Name of the unified mapping this rule filters (immutable reference).",
    )
    filters: ViewFilter = Field(
        default_factory=ViewFilter,
        description="Query-builder filter over the flattened unified keys ('table.field').",
    )
    exceptions: List[UnifiedMappingRuleException] = Field(
        [], description="Mute exceptions excluded from sharing via $nor."
    )
    muted: bool = Field(False)
    unmuteAt: Optional[datetime] = Field(
        None,
        description=(
            "When set alongside muted=True, the rule is automatically "
            "unmuted once this UTC timestamp passes "
            "(common.unmute_unified_mapping)."
        ),
    )
    cteShare: Dict[str, List[UmShareAction]] = Field(
        {},
        description="CTE sharing configuration: destination configuration name -> actions.",
    )
    creActions: Dict[str, List[UmCreAction]] = Field(
        {},
        description=(
            "CRE action configuration: CRE configuration name -> actions to "
            "perform on this rule's joined rows. Independent of 'cteShare' — a "
            "rule may do either, both, or neither. Unlike CTE sharing (which "
            "re-shares any row whose data moved), each CRE action fires ONCE "
            "per distinct joined row and only fires again if that row stops "
            "matching the rule and later matches again."
        ),
    )
    fieldMapping: Dict[str, Union[str, int, float]] = Field(
        {},
        description=(
            "Rule-level IOC field mapping: maps the shareable IOC fields "
            "('value', 'type' and optional fields) to unified row keys "
            "('$<table.field>' specs, 'fixed:<value>' literals, or bare "
            "static values) so this rule's joined rows can be turned into "
            "indicators. Optional at creation, and editable until 'value' and "
            "'type' are both set — a rule intended only for a future CRE action "
            "never needs one. It is required only once 'cteShare' is non-empty "
            "(enforced where cteShare itself is validated, not here), since "
            "only then does a rule actually need to produce indicators."
        ),
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Require a non-blank name.

        Unlike the mapping name (``_NAME_RE``), rule names allow ``/`` — the UI
        uses it as a folder-path separator, matching the CRE/CTE business
        rule convention (``BusinessRuleIn.name`` there has no character
        restriction beyond whitespace-stripping).
        """
        v = v.strip()
        if not v:
            raise ValueError("Rule name must not be blank.")
        return v

    @model_validator(mode="after")
    def _validate_mute_state(self) -> "_UnifiedMappingRuleBase":
        """Validate the mute state, mirroring the CTE/CRE rule contract.

        Muting always has an end time: the unmute sweep only ever matches
        documents with an ``unmuteAt``, so ``muted=True`` without one would be a
        permanent mute that nothing can clear. Unmuting clears the time so no
        stale unmute time is left behind.

        Declared before ``validate_rule_config`` so a bad mute payload is
        rejected without first loading the mapping and its destination plugins.
        """
        if not self.muted:
            self.unmuteAt = None
            return self
        if self.unmuteAt is None:
            raise HTTPException(
                400, "Unmute time must be set in order to mute the business rule."
            )
        # Unmute times are stored and compared as naive UTC (the sweep compares
        # against a naive datetime.now()), so an offset sent by a client
        # ("...Z", "+05:30") is converted rather than compared as-is - comparing
        # aware to naive raises TypeError and fails with a 500.
        if self.unmuteAt.tzinfo is not None:
            self.unmuteAt = self.unmuteAt.astimezone(timezone.utc).replace(
                tzinfo=None
            )
        if self.unmuteAt <= datetime.now():
            # An elapsed mute is over. Unlike CTE/CRE - whose PATCH is a partial
            # update, so an ordinary edit never resends unmuteAt - this rule's
            # PATCH is a full replace and the UI resends the stored unmuteAt on
            # every edit. Rejecting a past time would make a muted rule
            # un-editable in the window between unmuteAt passing and the next
            # sweep, so treat it the way the rule already reports itself: over.
            self.muted, self.unmuteAt = False, None
        return self

    @model_validator(mode="after")
    def validate_rule_config(self) -> "_UnifiedMappingRuleBase":
        """Validate the rule's filters, field mapping and sharing/action config.

        Same order the checks ran in while they lived in the router, so a
        payload with several problems still reports the same first one:
        the referenced mapping must exist, then its filters, then what the field
        mapping sets and reads, then ``cteShare`` (which is what makes
        ``fieldMapping`` mandatory), then ``creActions``.
        """
        mapping_doc = get_rule_mapping_doc(self.view)
        validate_rule_filters(self.filters, self.exceptions, mapping_doc)
        validate_rule_mapping_payload(self.fieldMapping)
        # Reassigned so a defaulted ``expiresAt`` is persisted with the rule.
        self.fieldMapping = validate_cte_share(
            self.cteShare, self.fieldMapping, mapping_doc
        )
        validate_cre_actions(self.creActions)
        return self


class UnifiedMappingRuleIn(_UnifiedMappingRuleBase):
    """Incoming payload to create a unified mapping business rule."""

    @field_validator("muted")
    @classmethod
    def validate_not_muted(cls, v: bool) -> bool:
        """Reject creating an already-muted rule.

        Matches the CTE and CRE business rule contract: a rule is always created
        unmuted and muted via a follow-up PATCH, so there is no way to create a
        rule that never shares or acts.
        """
        if v is True:
            raise HTTPException(400, "Can not create a muted business rule.")
        return v

    @field_validator("name")
    @classmethod
    def validate_is_unique(cls, v: str) -> str:
        """Reject a name already taken by another rule.

        Runs after ``validate_name`` (so it tests the stripped name) and before
        ``validate_rule_config``, matching the router's original order — a
        duplicate name is reported without first loading destination plugins.
        """
        from ..utils import Collections, DBConnector

        if DBConnector().collection(Collections.UNIFIED_MAPPING_RULES).find_one(
            {"name": v}
        ):
            raise HTTPException(
                400, f"A unified mapping business rule named '{v}' already exists."
            )
        return v


class UnifiedMappingRuleUpdate(_UnifiedMappingRuleBase):
    """Incoming payload to update a unified mapping business rule.

    Same payload validation as create minus the name-uniqueness check: the rule
    being updated already exists, and its name is immutable anyway (the endpoint
    identifies the rule by query parameter and restores the stored name).
    """


class UnifiedMappingRuleOut(BaseModel):
    """Outgoing representation of a saved unified mapping business rule."""

    id: str
    name: str
    view: str
    filters: ViewFilter = Field(default_factory=ViewFilter)
    exceptions: List[UnifiedMappingRuleException] = Field([])
    muted: bool = False
    unmuteAt: Optional[datetime] = None
    disabledByCre: bool = False
    cteShare: Dict[str, List[UmShareAction]] = Field({})
    creActions: Dict[str, List[UmCreAction]] = Field({})
    fieldMapping: Dict[str, Union[str, int, float]] = Field({})
    createdAt: datetime
    updatedAt: datetime
    lastPerformed: Dict[str, datetime] = Field(
        {},
        description=(
            "Recency checkpoint per sharing/action target: when this rule's rows "
            "were last successfully processed for that target. CTE sharing keys "
            "these by destination configuration name; CRE actions key them by "
            "'<configuration>|<action>' so each action tracks its own window."
        ),
    )
