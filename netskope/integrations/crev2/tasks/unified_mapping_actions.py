"""Perform CRE actions on the joined rows of a unified mapping business rule.

Unified mapping rules join several collections, so a "row" is a tuple of
documents rather than one record. That breaks CRE's usual act-once mechanism:
``cre.evaluate_records`` records each fired action in a ``lastEvals`` array ON
the entity record, and a joined row has no single document to carry one. This
module keeps the same act-once guarantee using a separate marker ledger
(``unified_mapping_markers``) keyed by the row's identity hash — see
``netskope.common.utils.unified_mapping_exec.row_hash``.

How a cycle qualifies rows (mirrors the CTE unified-mapping share task rather
than CRE's reactive, record-list-driven evaluation, because no single
"this record changed" event exists for a joined row):

  stored join pipeline -> recency (``lastUpdated`` vs. this action's checkpoint)
  -> matched-only -> rule filter -> mute exceptions

Every constituent document — base or joined — is stamped with ``lastUpdated``
at CE storage time, so the recency stage bounds the scan to rows something
actually touched. That is what keeps a large, mostly-idle mapping cheap: the
per-cycle cost tracks churn, not total size.

Semantics differ from CTE sharing on purpose. CTE re-shares any recency-eligible
row (a re-pushed IOC is harmless). A CRE action is not idempotent — reassigning
a risk score or quarantining a device twice is wrong — so each action fires ONCE
per distinct joined row, and only fires again if that row stops matching the rule
and later matches again.

Out of scope in this iteration (deliberate, documented in the plan): revert on
unmatch (``cre.evaluate_records``'s ``_execute_undos`` needs the per-record
``lastEvals`` anchor a joined row lacks) and ``generateAlert``.
"""

import random
import string
import traceback
from datetime import datetime, timedelta
from functools import partial
from typing import Optional

from bson.objectid import ObjectId
from pymongo import DeleteMany, ReturnDocument, UpdateOne
from pymongo.errors import BulkWriteError

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import SettingsDB
from netskope.common.utils import (
    Collections,
    DBConnector,
    PrefixedLogger,
    cto_alerts_enabled,
    integration,
    log_cto_alerts_skipped,
    track,
)
from netskope.integrations.itsm.models import Alert
from netskope.integrations.itsm.tasks.pull_data_items import store_cre_alerts
from netskope.common.utils.unified_mapping_exec import (
    build_sync_pipeline,
    exception_match_stage,
    mapping_unwinds_sources,
    matched_only_stage,
    recency_match_stage,
    resolve_normalized_row_values,
    row_hash,
    row_identity_stage,
    row_ids,
    rule_match_stage,
)

from ..models import Action, ActionLogStatus
from ..utils import display_action_parameters
from .evaluate_records import (
    _is_within_action_window,
    _log_action,
    _map_params,
    execute_actions_batch,
)

connector = DBConnector()
logger = PrefixedLogger("[Unified Mapping]")

# Rows are materialized, acted on and marked one batch at a time so peak memory
# tracks the batch, not the size of the match set. 1000 mirrors
# evaluate_records.RECORD_BATCH_SIZE and is still a useful batch size for
# plugins that implement batched execute_actions.
ROW_BATCH_SIZE = 1000

# Ceiling on identifiers per `$in` so a high-churn cycle cannot build a single
# oversized query document.
_ID_CHUNK_SIZE = 1000

# Markers scanned per pass by the pruning task.
_MARKER_SCAN_SIZE = 5000

# Ceiling on markers examined per prune run; the rest resume next run.
_PRUNE_MARKERS_PER_RUN = 200000

# How long a mapping's prune lock is honoured before it counts as abandoned.
_PRUNE_LOCK_TIMEOUT = timedelta(hours=1)


def um_rule_key(configuration: str, action_value: str) -> str:
    """Return the ledger/checkpoint key identifying one rule action target.

    A rule may drive several actions across several CRE configurations, and each
    must fire (and be checkpointed) independently — the same row legitimately
    gets a "tag" action once and a "score" action once.
    """
    return f"{configuration}|{action_value}"


def _chunks(items: list, size: int = _ID_CHUNK_SIZE):
    """Yield ``items`` in lists of at most ``size``."""
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _agg_options(mapping_doc: dict) -> dict:
    """Return aggregate options for a mapping (disk use, collation).

    A case-insensitive mapping must run with the same collation its
    ``um_join_*_ci`` indexes were built with, otherwise MongoDB cannot use them
    and the join degrades to a collection scan.
    """
    options: dict = {"allowDiskUse": True}
    if mapping_doc.get("caseInsensitive"):
        options["collation"] = {"locale": "en", "strength": 2}
    return options


def _candidate_pipeline(mapping_doc: dict, checkpoint=None) -> list:
    """Return the joined-row pipeline, before any rule filter.

    Shared prefix of both passes a cycle makes: the rule-filtered pass that
    finds rows to act on, and the identity pass that finds rows which stopped
    matching. ``checkpoint`` bounds a manual run's days window; a scheduled run
    passes None and scans every matching row, as ``cre.evaluate_records`` does —
    the ledger, not a time window, is what stops an action repeating.
    """
    return (
        build_sync_pipeline(mapping_doc)
        + (recency_match_stage(mapping_doc, checkpoint) if checkpoint else [])
        + matched_only_stage(mapping_doc)
    )


def _matched_pipeline(mapping_doc: dict, rule_doc: dict, checkpoint=None) -> list:
    """Return the pipeline yielding full rows that currently match the rule."""
    base_table = mapping_doc["baseTable"]
    sources_expanded = mapping_unwinds_sources(mapping_doc)
    return (
        _candidate_pipeline(mapping_doc, checkpoint)
        + rule_match_stage(rule_doc, base_table, sources_expanded)
        + exception_match_stage(rule_doc, base_table, sources_expanded)
    )


def _has_markers(rule_name: str, rule_key: str) -> bool:
    """Return whether any ledger entry exists for a rule action target."""
    return bool(
        connector.collection(Collections.UNIFIED_MAPPING_MARKERS).count_documents(
            {"rule": rule_name, "ruleKey": rule_key}, limit=1
        )
    )


def _marked_hashes_by_key(rule_name: str, rule_keys: list, hashes: list) -> dict:
    """Return ``{ruleKey: set(rowHash)}`` for the entries that already exist.

    All of a batch's action targets are read in one pass over the unique
    ``(rule, ruleKey, rowHash)`` index instead of one query per action.
    """
    found = {rule_key: set() for rule_key in rule_keys}
    collection = connector.collection(Collections.UNIFIED_MAPPING_MARKERS)
    for chunk in _chunks(hashes):
        for doc in collection.find(
            {
                "rule": rule_name,
                "ruleKey": {"$in": rule_keys},
                "rowHash": {"$in": chunk},
            },
            {"rowHash": 1, "ruleKey": 1, "_id": 0},
        ):
            found[doc["ruleKey"]].add(doc["rowHash"])
    return found


def _write_markers(
    mapping_name: str, rule_name: str, rule_key: str, marks: list
) -> None:
    """Record ledger entries for rows this cycle acted on.

    ``$setOnInsert`` against the unique ``(rule, ruleKey, rowHash)`` index makes
    this idempotent: a retried task, or two workers racing the same row, can only
    ever create one entry, so an action is never performed twice for a row.
    ``ordered=False`` lets the rest of a batch land even when some entries were
    already present.

    Args:
        mapping_name (str): Mapping the rows came from (used by pruning).
        rule_name (str): Rule that matched.
        rule_key (str): ``<configuration>|<action>`` target key.
        marks (list): ``(rowHash, {collection: _id})`` pairs to record.
    """
    if not marks:
        return
    now = datetime.now()
    operations = [
        UpdateOne(
            {"rule": rule_name, "ruleKey": rule_key, "rowHash": hash_value},
            {
                "$setOnInsert": {
                    "mapping": mapping_name,
                    "rule": rule_name,
                    "ruleKey": rule_key,
                    "rowHash": hash_value,
                    "rowIds": identity,
                    "createdAt": now,
                }
            },
            upsert=True,
        )
        for hash_value, identity in marks
    ]
    for chunk in _chunks(operations):
        try:
            connector.collection(Collections.UNIFIED_MAPPING_MARKERS).bulk_write(
                chunk, ordered=False
            )
        except BulkWriteError as error:
            # Duplicate keys are the expected outcome of a concurrent/retried
            # write and mean the row is already marked. Anything else matters.
            if any(
                item.get("code") != 11000
                for item in error.details.get("writeErrors", [])
            ):
                logger.error(
                    f"Error recording unified mapping action markers for rule "
                    f"'{rule_name}' ({rule_key}).",
                    error_code="CRE_1039",
                    details=traceback.format_exc(),
                )


def _drop_markers(rule_name: str, rule_key: str, hashes: list) -> int:
    """Delete ledger entries so their rows can fire again on a future rematch."""
    if not hashes:
        return 0
    deleted = 0
    for chunk in _chunks(hashes):
        result = connector.collection(Collections.UNIFIED_MAPPING_MARKERS).delete_many(
            {"rule": rule_name, "ruleKey": rule_key, "rowHash": {"$in": chunk}}
        )
        deleted += result.deleted_count
    return deleted


def _translate_action_params(action: Action, base_table: str) -> Action:
    """Rewrite an action's ``$<table>.<field>`` params to joined-row paths.

    Action parameters reference the flattened keys the mapping's own field
    picker emits ("table.field"). After the join, base-table fields sit at the
    row's top level while joined-table fields stay nested under the table name —
    the same translation the CTE field mapping applies. Done once per action
    rather than per row.
    """
    prefix = f"${base_table}."
    translated = action.model_copy(deep=True)
    for key, value in translated.parameters.items():
        if isinstance(value, str) and value.startswith(prefix):
            translated.parameters[key] = f"${value[len(prefix):]}"
        elif isinstance(value, list):
            translated.parameters[key] = [
                f"${item[len(prefix):]}"
                if isinstance(item, str) and item.startswith(prefix)
                else item
                for item in value
            ]
    return translated


def _log_safe_row(row: dict) -> dict:
    """Return a copy of a joined row without the joined documents' ``_id``s.

    ``GET /cre/logs`` stringifies the record's top-level ``_id`` only, so an
    ObjectId nested inside a joined sub-document makes the whole Action Logs
    response fail to serialize. The ids are only needed for the marker ledger,
    which reads them from the original row before this copy is taken.
    """
    return {
        key: (
            {k: v for k, v in value.items() if k != "_id"}
            if isinstance(value, dict) and "_id" in value
            else value
        )
        for key, value in row.items()
    }


def _alert_row_fields(row: dict) -> dict:
    """Flatten a joined row into the flat field map an alert's ``rawData`` holds.

    An entity-record alert carries a flat scalar map, so a joined document is
    flattened to its ``table.field`` keys — the same names the mapping's rules
    and action parameters already use — rather than a nested object every alert
    consumer would have to walk, and whose ``_id`` the Alerts page cannot even
    serialize. Identity and bookkeeping fields are dropped, as they are there.
    """
    skip = ("_id", "lastEvals", "lastUpdated")
    fields: dict = {}
    for key, value in row.items():
        if key in skip:
            continue
        if isinstance(value, dict) and "_id" in value:
            for sub_key, sub_value in value.items():
                if sub_key not in skip:
                    fields[f"{key}.{sub_key}"] = sub_value
        else:
            fields[key] = value
    return fields


def _build_row_alert(row: dict, rule_name: str, action: Action, params: Action) -> Alert:
    """Build the CRE alert for one row an action was performed on.

    Same shape as the entity-record alert in ``_evaluate_records``; the joined
    row stands in for the record, flattened to a scalar field map.
    """
    row_fields = _alert_row_fields(row)
    return Alert(
        id="".join(
            random.SystemRandom().choice(string.hexdigits) for _ in range(24)
        ),
        configuration="CRE",
        alertName=rule_name,
        alertType="CRE",
        app="CRE",
        appCategory="CRE",
        type="CRE",
        timestamp=datetime.now(),
        rawData=row_fields
        | {"action": action.label, "businessRule": rule_name}
        | display_action_parameters(params),
    )


def _route_row_action(
    row: dict,
    action: Action,
    mapping_name: str,
    rule_name: str,
    configuration: str,
    settings: SettingsDB,
    actions_batch: dict,
    alerts: list,
    alerts_enabled: bool,
) -> None:
    """Queue, schedule or hold one row's action for approval.

    Mirrors ``_evaluate_records``' routing so unified mapping rows honour the
    same approval and maintenance-window semantics as entity records:
    ``requireApproval`` parks the action as PENDING_APPROVAL, ``performLater``
    outside the action window parks it as SCHEDULED, and everything else is
    batched for immediate execution.

    The action log embeds the row as it looked when the action was created.
    ``cre.perform_action`` and the approve/decline endpoints replay a log purely
    from its own document, so a row that later changes or is deleted cannot
    alter or break an already-queued action.
    """
    # Normalized wrappers are already resolved on the row, so _map_params needs
    # no normalize_fields; source_configuration is a Threat-Indicators concept
    # with no unified-mapping equivalent.
    params = _map_params(action, row, configuration)
    log_action = partial(
        _log_action, mapping_name, _log_safe_row(row), rule_name, configuration
    )
    if action.generateAlert and alerts_enabled:
        alerts.append(_build_row_alert(row, rule_name, action, params))
    if action.requireApproval:
        logger.info(
            f"Scheduling the {action.value} action for unified mapping row with id "
            f"{row.get('_id')} for approval."
        )
        log_action(
            params,
            status=ActionLogStatus.PENDING_APPROVAL,
            performedAt=datetime.now(),
        )
    elif action.performLater and not _is_within_action_window(settings):
        logger.info(
            f"Scheduling the {action.value} action for unified mapping row with id "
            f"{row.get('_id')}, which will be performed during maintenance window."
        )
        log_action(
            params, status=ActionLogStatus.SCHEDULED, performedAt=datetime.now()
        )
    else:
        actions_batch.setdefault((configuration, action.value), []).append(
            {
                "params": params,
                "id": str(ObjectId()),
                "log_func": log_action,
                "before_log_message": (
                    f"Performing the {action.value} action on unified mapping row "
                    f"with id {row.get('_id')}"
                ),
                "after_log_message": (
                    f"Successfully performed the {action.value} action on unified "
                    f"mapping row with id {row.get('_id')}"
                ),
                "error_log_message": (
                    f"Error occurred while performing the {action.value} action on "
                    f"unified mapping row with id {row.get('_id')}."
                ),
            }
        )


def _process_row_batch(
    rows: list,
    mapping_doc: dict,
    rule_doc: dict,
    configuration: str,
    action: Action,
    rule_key: str,
    settings: SettingsDB,
    hashes: list,
    already_marked: set,
    alerts_enabled: bool,
) -> None:
    """Act on one batch of matched rows and record their ledger entries.

    ``rows`` arrive with their normalized values already resolved, and
    ``hashes``/``already_marked`` are computed once per batch by the caller and
    shared across the batch's action targets.
    """
    mapping_name = mapping_doc["name"]
    rule_name = rule_doc["name"]
    actions_batch: dict = {}
    alerts: list = []
    marks = []
    for row, hash_value in zip(rows, hashes):
        if hash_value in already_marked:
            logger.debug(
                f"Unified mapping row {hash_value[:12]} already had the "
                f"{action.value} action performed for rule '{rule_name}'. "
                "Will not be performing it again."
            )
            continue
        try:
            _route_row_action(
                row,
                action,
                mapping_name,
                rule_name,
                configuration,
                settings,
                actions_batch,
                alerts,
                alerts_enabled,
            )
        except Exception:
            logger.error(
                f"Error occurred while preparing the {action.value} action for "
                f"unified mapping row with id {row.get('_id')}.",
                error_code="CRE_1037",
                details=traceback.format_exc(),
            )
            _log_action(
                mapping_name,
                _log_safe_row(row),
                rule_name,
                configuration,
                action,
                status=ActionLogStatus.FAILED,
                performedAt=datetime.now(),
            )
        # Marked whether the action succeeded, failed or was parked for
        # approval — the same "written when routed" rule cre.evaluate_records
        # applies to lastEvals. A permanently failing action therefore reports
        # the failure once instead of retrying it every cycle forever.
        marks.append((hash_value, row_ids(row, mapping_doc)))
    if actions_batch:
        execute_actions_batch(actions_batch)
    if alerts:
        execute_celery_task(
            store_cre_alerts.apply_async, "itsm.store_cre_alerts", args=[alerts]
        )
    # Written on manual runs too: is_manual bypasses the skip above, not the
    # bookkeeping, so a later scheduled run does not repeat what it performed
    # (cre.evaluate_records marks matched records regardless of is_manual).
    _write_markers(mapping_name, rule_name, rule_key, marks)


def _prune_unmatched_markers(
    mapping_doc: dict,
    rule_doc: dict,
    rule_keys: list,
    checkpoint,
    matched_hashes: set,
) -> None:
    """Drop ledger entries for rows that no longer match the rule.

    A row that stopped matching must lose its ledger entry, otherwise a later
    rematch would be silently suppressed. This walks the same candidate set the
    action pass used (a projection of ``_id``s only, a few dozen bytes per row
    however wide the mapping is) and deletes the entries of rows that did not
    survive the rule filter — so a rule edit takes effect on the next cycle,
    not only when the underlying rows happen to change.

    Rows that stopped matching because a joined document was DELETED never reach
    here — deleting a document moves no timestamp, so the row falls out of the
    matched-only join instead. ``cre.um_prune_markers`` collects those. Delaying
    that is safe: identity hashes are derived from never-reused ``_id``s, so a
    leftover entry can never suppress a different row.
    """
    rule_name = rule_doc["name"]
    try:
        # Every key shares one candidate scan — the rule filter, and so the
        # matched set, is the same for all actions of the rule.
        keys = [key for key in rule_keys if _has_markers(rule_name, key)]
        if not keys:
            return
        pipeline = _candidate_pipeline(mapping_doc, checkpoint) + row_identity_stage(
            mapping_doc
        )
        stale: list = []
        dropped = 0
        for row in connector.collection(mapping_doc["baseTable"]).aggregate(
            pipeline, **_agg_options(mapping_doc)
        ):
            hash_value = row_hash(row, mapping_doc)
            if hash_value in matched_hashes:
                continue
            stale.append(hash_value)
            if len(stale) >= _ID_CHUNK_SIZE:
                dropped += sum(_drop_markers(rule_name, key, stale) for key in keys)
                stale = []
        dropped += sum(_drop_markers(rule_name, key, stale) for key in keys)
    except Exception:
        # Housekeeping, not correctness: the next scheduled run repeats this
        # sweep and cre.um_prune_markers clears entries for deleted rows.
        logger.warn(
            f"Could not clear stale action markers for unified mapping rule "
            f"'{rule_name}'; they will be cleaned up by the next scheduled run "
            "or the cre.um_prune_markers task.",
            details=traceback.format_exc(),
        )
        return
    if dropped:
        logger.debug(
            f"Cleared {dropped} unified mapping action marker(s) for rule "
            f"'{rule_name}' ({', '.join(keys)}) because those rows no longer "
            "match. They will be acted on again if they match in future."
        )


def _refresh_action_lock(mapping_name: str) -> None:
    """Keep the beat lock alive while a long scan is in progress.

    Refreshes an already-held lock instead of creating one: a manual run is
    dispatched without lock kwargs, so @track neither acquires nor releases it,
    and stamping it here would block the mapping's schedule until it went stale.
    """
    connector.collection(Collections.UNIFIED_MAPPING).update_one(
        {"name": mapping_name, "lockedAt.actions": {"$ne": None}},
        {"$set": {"lockedAt.actions": datetime.now()}},
    )


def _perform_rule_config_actions(
    mapping_doc: dict,
    rule_doc: dict,
    configuration: str,
    actions: list,
    settings: SettingsDB,
    days: Optional[int],
    is_manual: bool,
) -> bool:
    """Run every action of one (rule, configuration) target over a single scan.

    A scheduled run scans every matching row and lets the ledger decide what is
    new; only a manual run narrows the scan to a days window. Each action keeps
    its own ledger key, so a row still fires each action exactly once.

    Returns whether the target completed without error, so the caller can
    surface a failure on the mapping's ``lastActionRunSuccess`` instead of
    leaving it only in the logs.
    """
    mapping_name = mapping_doc["name"]
    rule_name = rule_doc["name"]
    # Resolved once per target instead of per row, and only reported when an
    # action actually asks for an alert.
    alerts_enabled = cto_alerts_enabled(settings)
    if not alerts_enabled and any(action.generateAlert for action in actions):
        log_cto_alerts_skipped(
            f"unified mapping rule '{rule_name}' on '{mapping_name}'"
        )
    # One scan serves every action here: the pipeline is identical across them
    # and only the ledger key differs.
    targets = [
        (
            action.value,
            um_rule_key(configuration, action.value),
            _translate_action_params(action, mapping_doc["baseTable"]),
        )
        for action in actions
    ]
    checkpoint = datetime.now() - timedelta(days=days) if days is not None else None
    matched_hashes: set = set()
    batch: list = []
    current_action = ""

    def _run_batch(rows: list) -> set:
        """Apply every action to one batch; returns the batch's row hashes."""
        nonlocal current_action
        # Resolved once for this configuration, and the identities and ledger
        # entries once for the batch — every action target then reuses them.
        resolve_normalized_row_values(rows, mapping_doc, configuration)
        hashes = [row_hash(row, mapping_doc) for row in rows]
        # A manual sync deliberately re-performs actions, so it neither consults
        # nor is limited by the ledger — matching cre.evaluate_records' bypass.
        marked = (
            {}
            if is_manual
            else _marked_hashes_by_key(
                rule_name, [key for _, key, _ in targets], hashes
            )
        )
        for action_value, rule_key, translated_action in targets:
            current_action = action_value
            _process_row_batch(
                rows, mapping_doc, rule_doc, configuration,
                translated_action, rule_key, settings,
                hashes, marked.get(rule_key, set()), alerts_enabled,
            )
        return set(hashes)

    try:
        for row in connector.collection(mapping_doc["baseTable"]).aggregate(
            _matched_pipeline(mapping_doc, rule_doc, checkpoint),
            **_agg_options(mapping_doc),
        ):
            batch.append(row)
            if len(batch) >= ROW_BATCH_SIZE:
                _refresh_action_lock(mapping_name)
                matched_hashes |= _run_batch(batch)
                batch = []
        if batch:
            matched_hashes |= _run_batch(batch)
    except Exception:
        # Leaving the checkpoint untouched re-offers this window next cycle;
        # rows already marked are skipped, so only unprocessed rows are retried.
        logger.error(
            f"Error performing the {current_action} action for unified mapping rule "
            f"'{rule_name}' on configuration '{configuration}'.",
            error_code="CRE_1039",
            details=traceback.format_exc(),
        )
        return False
    logger.info(
        f"Evaluated {len(matched_hashes)} unified mapping row(s) from mapping "
        f"'{mapping_name}' for rule '{rule_name}' on configuration "
        f"'{configuration}' ({len(targets)} action(s))."
    )
    _prune_unmatched_markers(
        mapping_doc, rule_doc, [key for _, key, _ in targets], checkpoint, matched_hashes
    )
    return True


def _end_action_life(mapping: str, success: bool) -> None:
    """Record the outcome of a mapping's CRE action run on the mapping doc.

    Deliberately a separate pair of fields from the ``lastRunAt`` /
    ``lastRunSuccess`` that ``cte.um_share_indicators`` writes: the two run on
    independent schedules and either module can be disabled on its own, so
    sharing them would make a green flag ambiguous (whose run succeeded?) and
    let a later successful share mask a failed action run. Mirrors how the same
    document already namespaces its beat locks as ``lockedAt.share`` /
    ``lockedAt.actions``.
    """
    connector.collection(Collections.UNIFIED_MAPPING).update_one(
        {"name": mapping},
        {"$set": {
            "lastActionRunAt": datetime.now(),
            "lastActionRunSuccess": success,
        }},
    )


@APP.task(name="cre.um_evaluate_records", acks_late=False)
@integration("cre")
@track()
def um_evaluate_records(
    mapping: str,
    rule: Optional[str] = None,
    configuration: Optional[str] = None,
    action: Optional[str] = None,
    days: Optional[int] = None,
    is_manual: bool = False,
):
    """Perform each rule's configured CRE actions on a unified mapping's rows.

    Triggered by the per-mapping ``UM Evaluate cre.<mapping>`` schedule created
    with the mapping (interval = the mapping's ``syncInterval``), and directly by the
    manual-sync endpoint. Every unmuted rule on the mapping that declares
    ``creActions`` is processed; each action fires once per distinct joined row.

    It does NOT fetch or store records, does not touch CTE sharing (which runs
    under its own ``cte.um_share_indicators`` schedule so either module can be
    disabled independently), and does not execute actions parked for approval or
    for the maintenance window — ``cre.perform_action`` drains those.

    Args:
        mapping (str): Unified mapping name (also the beat-lock key).
        rule (Optional[str]): Limit the run to a single rule.
        configuration (Optional[str]): Limit the run to one CRE configuration.
        action (Optional[str]): Limit the run to one action value.
        days (Optional[int]): Process rows touched in the last N days instead of
            using each action's checkpoint; leaves checkpoints unchanged.
        is_manual (bool): Re-perform actions regardless of the marker ledger.
    """
    mapping_doc = connector.collection(Collections.UNIFIED_MAPPING).find_one(
        {"name": mapping}
    )
    if mapping_doc is None:
        logger.error(
            f"Could not perform unified mapping actions because unified mapping "
            f"'{mapping}' does not exist.",
            error_code="CRE_1039",
        )
        return {"success": False, "message": f"Unified mapping '{mapping}' not found."}
    # From here the mapping exists, so every exit stamps its action-run outcome.
    success = True
    try:
        settings_doc = connector.collection(Collections.SETTINGS).find_one({})
        if not settings_doc:
            logger.error(
                "Could not perform unified mapping actions; settings are unavailable.",
                error_code="CRE_1039",
            )
            success = False
            return {"success": False, "message": "Settings are unavailable."}
        settings = SettingsDB(**settings_doc)
        rule_query = {"view": mapping, "muted": False, "creActions": {"$ne": {}}}
        if rule:
            rule_query["name"] = rule
        for rule_doc in connector.collection(Collections.UNIFIED_MAPPING_RULES).find(
            rule_query
        ):
            for config_name, actions in (rule_doc.get("creActions") or {}).items():
                if configuration and config_name != configuration:
                    continue
                selected = [
                    Action(**stored_action)
                    for stored_action in (actions or [])
                    if not action or stored_action.get("value") == action
                ]
                if not selected:
                    continue
                # Refresh the beat lock per target so a mapping with many
                # rules/actions cannot exceed the maximum lock wait time.
                _refresh_action_lock(mapping)
                # A failing target must surface on the mapping, not only in
                # the logs; the remaining targets still run.
                if not _perform_rule_config_actions(
                    mapping_doc,
                    rule_doc,
                    config_name,
                    selected,
                    settings,
                    days,
                    is_manual,
                ):
                    success = False
        return {"success": success}
    except Exception:
        # Re-raised so @track still records the task as errored — unlike
        # cte.um_share_indicators, which swallows it and reports "completed".
        success = False
        raise
    finally:
        _end_action_life(mapping, success)


def _delete_orphaned_rule_markers() -> int:
    """Delete ledger entries whose rule or action target no longer exists.

    Covers a deleted rule, a rule that dropped a configuration or action, and a
    rule whose mapping was deleted. Without this, disabling then re-adding an
    action would find stale entries and silently skip rows it should act on.
    """
    collection = connector.collection(Collections.UNIFIED_MAPPING_MARKERS)
    live_keys: dict = {}
    for rule_doc in connector.collection(Collections.UNIFIED_MAPPING_RULES).find(
        {}, {"name": 1, "creActions": 1, "_id": 0}
    ):
        keys = {
            um_rule_key(config_name, stored_action.get("value"))
            for config_name, actions in (rule_doc.get("creActions") or {}).items()
            for stored_action in actions or []
        }
        live_keys[rule_doc["name"]] = keys
    operations = []
    for rule_name in collection.distinct("rule"):
        keys = live_keys.get(rule_name)
        if not keys:
            operations.append(DeleteMany({"rule": rule_name}))
            continue
        stale_keys = [
            key
            for key in collection.distinct("ruleKey", {"rule": rule_name})
            if key not in keys
        ]
        if stale_keys:
            operations.append(
                DeleteMany({"rule": rule_name, "ruleKey": {"$in": stale_keys}})
            )
    if not operations:
        return 0
    result = collection.bulk_write(operations, ordered=False)
    return result.deleted_count


def _mapping_doc_tables(mapping_doc: dict) -> list:
    """Collections a mapping reads: its base table plus every joined table."""
    tables = {mapping_doc["baseTable"]} if mapping_doc.get("baseTable") else set()
    for join in mapping_doc.get("joins") or []:
        if join.get("rightTable"):
            tables.add(join["rightTable"])
    return sorted(tables)


def _lock_mapping_prune(mapping_name: str):
    """Claim a mapping's prune slot, or return None when a run already holds it.

    Namespaced alongside the mapping's existing ``lockedAt.share`` /
    ``lockedAt.actions`` beat locks; a stale lock is reclaimed after the timeout
    so a worker lost mid-sweep does not block the mapping forever.
    """
    now = datetime.now()
    return connector.collection(Collections.UNIFIED_MAPPING).find_one_and_update(
        {
            "name": mapping_name,
            "$or": [
                {"lockedAt.prune": None},
                {"lockedAt.prune": {"$lte": now - _PRUNE_LOCK_TIMEOUT}},
            ],
        },
        {"$set": {"lockedAt.prune": now}},
        return_document=ReturnDocument.AFTER,
    )


def _release_mapping_prune(mapping_name: str, cursors: dict) -> None:
    """Release the mapping's prune slot and persist where its sweep stopped."""
    connector.collection(Collections.UNIFIED_MAPPING).update_one(
        {"name": mapping_name},
        {"$set": {"lockedAt.prune": None, "pruneCursor": cursors}},
    )


def _delete_dangling_row_markers() -> int:
    """Delete ledger entries whose joined row no longer exists.

    The one case recency filtering cannot see: deleting a document moves no
    timestamp, so a row broken by a deletion simply stops appearing in the
    matched-only join.

    Liveness is decided by a ``$lookup`` against each referenced collection's
    ``_id`` index, so only the dangling ids travel back rather than every
    marker. A run examines at most ``_PRUNE_MARKERS_PER_RUN`` markers and stores
    where it stopped on the mapping's ``pruneCursor``; a collection swept to the
    end drops its cursor and starts over on a later run.
    """
    collection = connector.collection(Collections.UNIFIED_MAPPING_MARKERS)
    deleted = 0
    budget = _PRUNE_MARKERS_PER_RUN
    # Materialised up front: the sweep below can outlive an open cursor.
    names = [
        doc["name"]
        for doc in connector.collection(Collections.UNIFIED_MAPPING).find(
            {}, {"name": 1}
        )
    ]
    for mapping_name in names:
        if budget <= 0:
            break
        locked = _lock_mapping_prune(mapping_name)
        if locked is None:
            continue
        name = locked["name"]
        cursors = dict(locked.get("pruneCursor") or {})
        try:
            for table in _mapping_doc_tables(locked):
                field = f"rowIds.{table}"
                while budget > 0:
                    match: dict = {"mapping": name, field: {"$ne": None}}
                    if cursors.get(table) is not None:
                        match["_id"] = {"$gt": cursors[table]}
                    result = list(collection.aggregate(
                        [
                            {"$match": match},
                            {"$sort": {"_id": 1}},
                            {"$limit": min(_MARKER_SCAN_SIZE, budget)},
                            {"$lookup": {
                                "from": table,
                                "localField": field,
                                "foreignField": "_id",
                                "as": "__alive",
                            }},
                            {"$facet": {
                                "dangling": [
                                    {"$match": {"__alive": []}},
                                    {"$project": {"_id": 1}},
                                ],
                                "bound": [{"$group": {
                                    "_id": None,
                                    "lastId": {"$max": "$_id"},
                                    "seen": {"$sum": 1},
                                }}],
                            }},
                        ],
                        allowDiskUse=True,
                    ))
                    facet = result[0] if result else {}
                    bound = (facet.get("bound") or [{}])[0]
                    if not bound.get("seen"):
                        cursors.pop(table, None)
                        break
                    budget -= bound["seen"]
                    cursors[table] = bound["lastId"]
                    dangling = [doc["_id"] for doc in facet.get("dangling") or []]
                    for chunk in _chunks(dangling):
                        deleted += collection.delete_many(
                            {"_id": {"$in": chunk}}
                        ).deleted_count
        finally:
            _release_mapping_prune(name, cursors)
    return deleted


@APP.task(name="cre.um_prune_markers")
@integration("cre")
@track()
def um_prune_markers():
    """Delete unified mapping action markers that can never match a row again.

    Runs on its own low-frequency schedule rather than inside an action cycle
    because it is housekeeping, not correctness: an identity hash is built from
    ``_id``s, which MongoDB never reuses, so a leftover marker can only ever
    refer to the row it was created for and can never suppress a different one.
    Keeping it off the action path is what lets a cycle stay proportional to
    churn instead of to the size of the ledger.

    It does NOT perform or revert any action.
    """
    try:
        orphaned = _delete_orphaned_rule_markers()
        dangling = _delete_dangling_row_markers()
    except Exception:
        logger.error(
            "Error pruning unified mapping action markers.",
            error_code="CRE_1039",
            details=traceback.format_exc(),
        )
        return {"success": False}
    if orphaned or dangling:
        logger.info(
            f"Pruned unified mapping action markers: {orphaned} for removed "
            f"rules/actions, {dangling} for rows that no longer exist."
        )
    return {"success": True}
