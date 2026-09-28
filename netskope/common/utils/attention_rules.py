"""Pure, table-driven rule engine for the proactive "Needs attention" data-plane (plan v5 §6).

NO Mongo, NO LLM, NO I/O. ``evaluate(...)`` takes plain config/settings snapshots + ``now`` +
the prior findings (for streak/hysteresis state) and returns the findings to upsert and the
findingKeys to resolve. The ``attention_scan`` celery task does all the Mongo I/O around it.
Kept pure so the streak/hysteresis logic is unit-testable without a database.

Detection is entirely programmatic — the LLM is only ever called later, on an explicit
[Diagnose] click, via the chat route's ``findingId`` hand-off.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

# --- thresholds (hysteresis: open threshold strictly farther than the close condition) -----
_RUN_FAILURE_OPEN = 3          # consecutive DISTINCT failed runs before a finding opens
_RUN_FAILURE_ERROR = 5         # warn -> error at/after this streak
_STALE_FLOOR = timedelta(minutes=30)
_STALE_POLL_FACTOR = 3         # stale if now-lastRunAt > max(3*pollInterval, 30m)
_STALE_ERROR_FACTOR = 10       # error (vs warn) past 10x the interval
_CERT_WARN_DAYS = 30
_CERT_ERROR_DAYS = 7
# run_cadence_drift: learn each config's ACTUAL run cadence from its last few distinct runs
# (<= _CADENCE_SAMPLES stamps -> <= 5 intervals) and flag when the current gap far exceeds it.
# Hysteresis: open when gap > 2x the learned average; resolve when back within 1x.
_CADENCE_SAMPLES = 6           # stored lastRunAt stamps (yields <= 5 intervals)
_CADENCE_MIN_INTERVALS = 3     # don't judge cadence from fewer than 3 observed intervals
_CADENCE_OPEN_FACTOR = 2.0
_CADENCE_FLOOR = timedelta(minutes=15)  # ignore drift on sub-15-min cadences (jitter)
# plugin_error_logs: recent plugin-raised error/warning log volume (grouped by errorCode).
_ERRLOG_OPEN_COUNT = 3         # >=3 occurrences of one errorCode in the window opens a finding
_ERRLOG_ERROR_COUNT = 10       # error (vs warn) severity at this volume

# The ops whose per-op lastRunSuccess we watch, per module. CTE/CTO write a per-op dict;
# CLS/CRE/EDM/CFC write a bare Optional[bool] that _run_success_map normalizes to {"pull": bool},
# so each of those has the single ("pull",) op.
_MODULE_OPS = {
    "cte": ("pull", "share"),
    "cto": ("pull", "sync", "update"),
    "cls": ("pull",),
    "cre": ("pull",),
    "edm": ("pull",),
    "cfc": ("pull",),
}
_UNIT_MINUTES = {"minutes": 1, "hours": 60, "days": 1440}


@dataclass
class FindingCandidate:
    """One detected condition the scan task will upsert into ``copilot_findings``.

    ``status`` is ``watching`` for a pre-threshold tracker (invisible to the feed, TTL-purged)
    or ``open`` once the threshold trips. ``tracker`` carries the streak/hysteresis state that
    is persisted on the finding doc and fed back in on the next scan.
    """

    findingKey: str
    module: str
    kind: str
    severity: str          # "warn" | "error"
    status: str            # "watching" | "open"
    title: str
    target: dict
    evidence: dict = field(default_factory=dict)
    tracker: dict = field(default_factory=dict)


def aware_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Normalize a datetime to aware UTC (the shared coercion — import this, don't re-inline it).

    CE runs UTC-only (container TZ=UTC), so every datetime.now() and every stored timestamp is
    UTC wall-clock — a naive value is therefore tagged UTC (not converted), and an aware value in
    another zone is converted to UTC. This makes the UTC invariant explicit so all timestamp
    arithmetic is plain aware-aware subtraction (no tz-stripping that is only coincidentally right).
    NOTE: hand-rolled ``if x.tzinfo is None: x.replace(tzinfo=UTC)`` copies handle ONLY the naive
    case and pass a wrongly-zoned aware value through unconverted — call this instead.
    """
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


# Internal alias for the engine's own call sites (kept so their diffs don't churn).
_utc = aware_utc


def _as_dt(value) -> Optional[datetime]:
    """Coerce a stored lastRunAt (datetime or ISO string) to an aware-UTC datetime, else None."""
    if isinstance(value, datetime):
        return _utc(value)
    if isinstance(value, str):
        try:
            return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def _age(now: datetime, then: Optional[datetime]) -> Optional[timedelta]:
    """Return now - then as a timedelta; both normalized to aware UTC (see _utc). None if then is None."""
    if then is None:
        return None
    return _utc(now) - _utc(then)


def _poll_minutes(cfg: dict) -> int:
    """Config poll interval in minutes (defaults to 60 if missing/odd)."""
    every = cfg.get("pollInterval") or 60
    unit = (cfg.get("pollIntervalUnit") or "minutes").lower()
    try:
        return max(1, int(every) * _UNIT_MINUTES.get(unit, 1))
    except (TypeError, ValueError):
        return 60


def _run_success_map(cfg: dict) -> dict:
    """Normalize lastRunSuccess (dict per-op, or a bare bool) to {op: bool}."""
    raw = cfg.get("lastRunSuccess")
    if isinstance(raw, dict):
        return raw
    # A bare bool/None applies to the primary 'pull' op.
    return {"pull": bool(raw)} if raw is not None else {}


def _prior_tracker(prior: dict, key: str) -> dict:
    return (prior.get(key) or {}).get("tracker", {}) if prior else {}


def _eval_run_failures(cfg: dict, module: str, now: datetime, prior: dict,
                       candidates: List[FindingCandidate], resolved: set) -> None:
    """run_failure_streak: open after >=3 DISTINCT failed runs on an op; resolve on 1 healthy run."""
    name = cfg.get("name")
    run_at = _as_dt(cfg.get("lastRunAt"))
    successes = _run_success_map(cfg)
    for op in _MODULE_OPS.get(module, ()):  # only ops meaningful for the module
        if op not in successes:
            continue
        key = f"{module}:{name}:{op}:run_failure"
        if successes[op]:
            resolved.add(key)  # a healthy run closes the streak (hysteresis: close at 1)
            continue
        tracker = dict(_prior_tracker(prior, key))
        streak = int(tracker.get("streak", 0))
        last_counted = tracker.get("lastCountedRunAt")
        stamp = run_at.isoformat() if run_at else None
        # Count only DISTINCT failed runs (lastRunAt advanced) — the 5-min scan sees the same
        # failed run many times between poll cycles; we must not inflate the streak on re-reads.
        # Compare as INSTANTS, not raw strings: a stored lastCountedRunAt may be naive-ISO (older
        # scans / pre-UTC-normalization docs) while `stamp` is now aware-ISO (+00:00) — same moment,
        # different text. Normalizing both through _as_dt makes the re-read correctly non-distinct
        # (else every open streak would inflate by one on the first post-upgrade scan).
        if _as_dt(stamp) != _as_dt(last_counted):
            streak += 1
            last_counted = stamp
        tracker = {"streak": streak, "lastCountedRunAt": last_counted}
        opened = streak >= _RUN_FAILURE_OPEN
        severity = "error" if streak >= _RUN_FAILURE_ERROR else "warn"
        candidates.append(FindingCandidate(
            findingKey=key, module=module, kind="run_failure_streak", severity=severity,
            status="open" if opened else "watching",
            title=f"{name}: {op} has failed {streak} consecutive run{'s' if streak != 1 else ''}",
            target={"type": "config", "name": name, "op": op, "module": module},
            evidence={"streak": streak, "op": op, "lastRunAt": stamp,
                      "errorCode": cfg.get("lastErrorCode")},
            tracker=tracker,
        ))


def _eval_staleness(cfg: dict, module: str, now: datetime,
                    resolved: set) -> Optional[FindingCandidate]:
    """staleness: active, unlocked, now-lastRunAt beyond max(3*poll, 30m). None if healthy.

    Every "healthy" (return None) path also resolves this config's ``:stale`` key, mirroring
    ``_eval_run_failures``/``_eval_cadence`` — the scan closes a finding ONLY when its key is in
    the ``resolved`` set (there is NO absent-from-candidates auto-resolve), so without this a
    recovered stale config's ``open`` finding would linger forever with no TTL.
    """
    name = cfg.get("name")
    key = f"{module}:{name}:stale"
    if not cfg.get("active"):
        resolved.add(key)  # disabled config is not "stale" — clear any prior stale finding
        return None
    # A run in progress is NOT stale. CTE/CTO write a per-op DICT lock (lockedAt.pull/.share/...);
    # CLS/CRE/EDM/CFC write a SCALAR datetime lock (LOCKING_ARGS lock_field="lockedAt"). Recognize
    # both, or a long-but-healthy new-module run would falsely open a staleness finding.
    locked = cfg.get("lockedAt")
    if isinstance(locked, dict):
        if any(locked.values()):
            return None  # a run is in progress — leave a prior finding as-is (don't resolve yet)
    elif locked:
        return None  # scalar datetime lock (cls/cre/edm/cfc) — a run is in progress
    run_at = _as_dt(cfg.get("lastRunAt"))
    age = _age(now, run_at)
    if age is None:
        return None
    poll = timedelta(minutes=_poll_minutes(cfg))
    threshold = max(poll * _STALE_POLL_FACTOR, _STALE_FLOOR)
    if age <= threshold:
        resolved.add(key)  # ran recently enough — a healthy run closes a prior stale finding
        return None
    severity = "error" if age > poll * _STALE_ERROR_FACTOR else "warn"
    return FindingCandidate(
        findingKey=key,
        module=module, kind="staleness", severity=severity, status="open",
        title=f"{name} hasn't run in {int(age.total_seconds() // 60)} min",
        target={"type": "config", "name": name, "module": module},
        evidence={"lastRunAt": run_at.isoformat() if run_at else None,
                  "ageMinutes": int(age.total_seconds() // 60),
                  "pollIntervalMinutes": _poll_minutes(cfg)},
    )


def _eval_cadence(cfg: dict, module: str, now: datetime, prior: dict,
                  candidates: List[FindingCandidate], resolved: set) -> None:
    """run_cadence_drift: current gap since the last run >> the config's own learned cadence.

    The tracker accumulates the last few DISTINCT ``lastRunAt`` stamps across scans (persisted
    on a ``watching`` finding, invisible to the feed) and judges the current gap against the
    average of the <=5 most recent intervals. Complements ``staleness`` (a fixed 3x-poll rule):
    this one learns the ACTUAL cadence, so it catches configs that normally run far more often
    than their configured interval suggests.
    """
    if not cfg.get("active"):
        return
    name = cfg.get("name")
    run_at = _as_dt(cfg.get("lastRunAt"))
    if run_at is None:
        return
    key = f"{module}:{name}:cadence_drift"
    tracker = dict(_prior_tracker(prior, key))
    stamps = list(tracker.get("runStamps") or [])
    stamp = run_at.isoformat()
    # Dedup on the INSTANT, not the raw string: a stored stamp may be naive-ISO (older scans /
    # pre-UTC-normalization docs) while `stamp` is aware-ISO (+00:00) — same run, different text.
    # Comparing parsed datetimes stops the same run being appended twice (a spurious 0s interval).
    if run_at not in [_as_dt(s) for s in stamps]:
        stamps = (stamps + [stamp])[-_CADENCE_SAMPLES:]
    tracker = {"runStamps": stamps}

    times = sorted(t for t in (_as_dt(s) for s in stamps) if t is not None)
    # _as_dt returns aware-UTC, so these are plain aware-aware subtractions.
    intervals = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
    gap = _age(now, run_at)
    if len(intervals) < _CADENCE_MIN_INTERVALS or gap is None:
        # Not enough history to judge — keep accumulating stamps invisibly.
        candidates.append(FindingCandidate(
            findingKey=key, module=module, kind="run_cadence_drift", severity="warn",
            status="watching", title=f"{name}: learning run cadence",
            target={"type": "config", "name": name, "module": module},
            evidence={"samples": len(stamps)}, tracker=tracker,
        ))
        return
    avg = timedelta(seconds=sum(intervals) / len(intervals))
    if avg < _CADENCE_FLOOR:
        avg = _CADENCE_FLOOR
    prior_status = (prior.get(key) or {}).get("status")
    if gap > avg * _CADENCE_OPEN_FACTOR:
        candidates.append(FindingCandidate(
            findingKey=key, module=module, kind="run_cadence_drift", severity="warn",
            status="open",
            title=f"{name}: no run for {int(gap.total_seconds() // 60)} min — "
                  f"it usually runs every ~{int(avg.total_seconds() // 60)} min",
            target={"type": "config", "name": name, "module": module},
            evidence={"avgIntervalMinutes": int(avg.total_seconds() // 60),
                      "currentGapMinutes": int(gap.total_seconds() // 60),
                      "samples": len(intervals)},
            tracker=tracker,
        ))
    elif gap <= avg and prior_status in ("open", "acknowledged"):
        # Back within its learned cadence — close (hysteresis: open at 2x, close at <=1x).
        # Skip the watching re-emit this cycle so the resolve isn't shadowed by a candidate.
        resolved.add(key)
    else:
        candidates.append(FindingCandidate(
            findingKey=key, module=module, kind="run_cadence_drift", severity="warn",
            status="watching", title=f"{name}: run cadence nominal",
            target={"type": "config", "name": name, "module": module},
            evidence={"avgIntervalMinutes": int(avg.total_seconds() // 60)}, tracker=tracker,
        ))


# Same labels as config_tools.MODULE_DISPLAY_LABEL, kept LOCAL ON PURPOSE: this is the pure rule
# engine (Mongo/registry/langchain-free by contract), so it must not import that heavy module. Keep
# the two in sync by eye if the labels change (a coverage test asserts they match).
_MODULE_TITLES = {
    "cte": "Threat Exchange", "cto": "Ticket Orchestrator", "cls": "Log Shipper",
    "cre": "Risk Exchange", "edm": "Exact Data Match", "cfc": "Custom File Classification",
}


# Modules onboarded to module_unconfigured (module enabled but no configs) — all six.
_UNCONFIGURED_MODULES = ("cte", "cto", "cls", "cre", "edm", "cfc")
# Modules with FILTER business rules (no_business_rules + rule_unwired). EDM is excluded: its
# only "rule" is a 1:1 source->dest sharing (the flow itself), never a filter, so it can be
# neither rule-less-with-configs nor unwired in that sense (module_unconfigured still applies).
_RULE_MODULES = ("cte", "cto", "cls", "cre", "cfc")
# Per rule-module: (wiring fieldS ON the rule dict, the "unwired" phrase, the resolution hint).
# The fields are a TUPLE because a module can route through more than ONE target and the rule is
# wired if ANY of them is set. CTE is the case that needs it: a rule on the Threat Indicators
# entity shares via ``sharedWith``, while a rule on a CRE entity shares via ``creShare`` (the two
# are mutually exclusive by construction — cte.models.business_rule.validate_creShare). Checking
# ``sharedWith`` alone reported every fully-configured CRE-entity rule as unwired.
# CFC wiring is CROSS-COLLECTION — load_rules pre-computes a "_sharingWired" bool onto each cfc
# rule (rule referenced by some cfc_sharing.mappings[]), so the pure engine stays Mongo-free.
_WIRING = {
    "cte": (("sharedWith", "creShare"), "matches indicators but shares them to no destination",
            "Open the rule and set its sharing destination"),
    "cto": (("queues",), "matches alerts but routes them to no queue",
            "Open the rule and map it to a queue"),
    "cls": (("siemMappings",), "matches logs but forwards them to no SIEM destination",
            "Open Log Delivery and map the rule to a destination configuration"),
    "cre": (("actions",), "matches records but triggers no action on any configuration",
            "Open the rule and add an action on a configuration"),
    "cfc": (("_sharingWired",), "matches files but is not wired to a classifier under Sharing",
            "Open Sharing and map the rule to a classifier on a destination configuration"),
}
# Cross-module DISABLE LOCK: turning a module off mutes AND locks the other module's rules that
# depend on it — CRE off locks every CTE rule on a CRE entity (``disabledByCre``), CTE off locks
# every CRE rule on the Threat Indicators entity (``disabledByCte``). See
# settings.disable_cre_entity_business_rules / disable_ti_business_rules. While the lock holds the
# API refuses every edit to that rule, so "open the rule and wire it" is an instruction the user
# CANNOT follow; the actionable thing is re-enabling the other module. Skip those rules entirely
# (and flush any prior finding) rather than nagging about something they're forbidden to fix.
_CROSS_MODULE_LOCK = {"cte": "disabledByCre", "cre": "disabledByCte"}
# Literal, not an import: this module is deliberately dependency-free (stdlib only) so the engine
# stays unit-testable without the app. Source of truth: cte.utils.entity.THREAT_INDICATORS_ENTITY.
_THREAT_INDICATORS_ENTITY = "Threat Indicators"
# no_business_rules hint per rule-module (states the intent, never claims breakage).
_NO_RULES_HINT = {
    "cte": "Without a business rule nothing is shared to destinations — fine for a "
           "pull-only/aggregation setup; add a rule when you want to share.",
    "cto": "Without a business rule no alerts are routed to a queue, so no tickets are "
           "created. Add a rule to start ticketing.",
    "cls": "Without a business rule no logs are forwarded to a SIEM destination. Add a rule "
           "and map it under Log Delivery to start shipping.",
    "cre": "Without a business rule no records are scored or actioned. Add a rule to start "
           "evaluating entities and acting on them.",
    "cfc": "Without a business rule no files are classified or shared. Add a rule and wire it "
           "under Sharing to start classifying.",
}


def _unwired_what(module: str, rule: dict, default: str) -> str:
    """Phrase describing what an unwired rule matches, for the finding title.

    Only CTE varies: its rules target EITHER the native Threat Indicators data or a CRE entity,
    so a CRE-entity rule must not be described as matching "indicators" — name the entity it
    actually reads. ``entity`` defaults to Threat Indicators on the stored document, and an
    older document without the field is a Threat Indicators rule too.
    """
    if module != "cte":
        return default
    entity = rule.get("entity")
    if not entity or entity == _THREAT_INDICATORS_ENTITY:
        return default
    return f"matches '{entity}' records but shares them to no destination"


def _eval_wiring(configs: List[dict], rules: Optional[dict], enabled_modules: Optional[set],
                 candidates: List[FindingCandidate], resolved: set) -> None:
    """Flow-wiring rules: module enabled w/o configs; configs w/o rules; rules routed nowhere.

    Deliberately gentle (warn, with the intent stated in the evidence): a pull-only CTE setup
    without sharing rules can be a legitimate aggregation deployment — the finding asks the
    user to confirm intent, it never claims breakage. (True "standalone plugin" detection —
    e.g. a notifier that needs no flow — would need a plugin-type registry; not attempted.)

    Per-module wiring target: CTE sharedWith, CTO queues, CLS siemMappings, CRE actions, CFC a
    cross-collection cfc_sharing mapping (pre-resolved to `_sharingWired` by load_rules). EDM is
    excluded from the filter-rule families (it has no filter rules) but keeps module_unconfigured.
    """
    by_module: dict = {m: [] for m in _UNCONFIGURED_MODULES}
    for c in configs:
        if c.get("module") in by_module:
            by_module[c["module"]].append(c)

    # 1) module enabled but nothing configured — every module.
    for module in _UNCONFIGURED_MODULES:
        label = _MODULE_TITLES[module]
        cfgs = by_module[module]
        key_uncfg = f"{module}:module:unconfigured"
        if enabled_modules is not None and module in enabled_modules and not cfgs:
            candidates.append(FindingCandidate(
                findingKey=key_uncfg, module=module, kind="module_unconfigured",
                severity="warn", status="open",
                title=f"{label} is enabled, Add a plugin configuration, or disable the module",
                target={"type": "module", "name": module},
                evidence={"resolutionHint": "Add a plugin configuration, or disable the module "
                                            "under Settings > General if it's not in use."},
            ))
        else:
            resolved.add(key_uncfg)

    if rules is None:
        return
    # 2 + 3) filter-rule families (skip EDM — no filter rules).
    for module in _RULE_MODULES:
        module_rules = rules.get(module)
        if module_rules is None:
            continue
        # Disabled module — flush its rule-family findings (no flow runs while it's off).
        if enabled_modules is not None and module not in enabled_modules:
            resolved.add(f"{module}:rules:none")
            for rule in module_rules:
                resolved.add(f"{module}:rule:{rule.get('name')}:unwired")
            continue
        label = _MODULE_TITLES[module]
        active = [c for c in by_module[module] if c.get("active")]
        wiring_fields, unwired_what, unwired_hint = _WIRING[module]
        lock_field = _CROSS_MODULE_LOCK.get(module)
        # 2) configs exist but no business rules at all.
        key_norules = f"{module}:rules:none"
        if active and len(module_rules) == 0:
            candidates.append(FindingCandidate(
                findingKey=key_norules, module=module, kind="no_business_rules",
                severity="warn", status="open",
                title=f"{label}: plugins are configured but no business rule exists",
                target={"type": "module", "name": module},
                evidence={"activeConfigs": len(active), "resolutionHint": _NO_RULES_HINT[module]},
            ))
        else:
            resolved.add(key_norules)
        # 3) rules that route nowhere.
        for rule in module_rules:
            rname = rule.get("name")
            key_rule = f"{module}:rule:{rname}:unwired"
            # Locked by the other module's disable switch — the user cannot act on it.
            if lock_field and rule.get(lock_field):
                resolved.add(key_rule)
                continue
            # Wired if ANY of the module's routing targets is set (CTE has two — see _WIRING).
            wired = any(rule.get(f) for f in wiring_fields)
            if not wired:
                candidates.append(FindingCandidate(
                    findingKey=key_rule, module=module, kind="rule_unwired",
                    severity="warn", status="open",
                    title=f"{label} rule '{rname}' {_unwired_what(module, rule, unwired_what)}",
                    target={"type": "rule", "name": rname, "module": module},
                    evidence={"resolutionHint": unwired_hint},
                ))
            else:
                resolved.add(key_rule)


_ERRCODE_MODULE_PREFIX = {
    "CTE": "cte", "CTO": "cto", "ITSM": "cto",
    "CLS": "cls", "CRE": "cre", "EDM": "edm", "CFC": "cfc",
}


def _eval_error_logs(error_logs: Optional[List[dict]], prior: dict,
                     candidates: List[FindingCandidate], resolved: set) -> None:
    """plugin_error_logs: recent error/warning volume per errorCode.

    The errorCode is the plugins' structured error format. ``error_logs`` rows are
    {errorCode, count, message?, resolution?} aggregated by the scan over its recent window;
    module is derived from the code's prefix (CTE_/CTO_/ITSM_ -> cte/cto, else system).
    """
    if error_logs is None:
        return
    seen_keys = set()
    for row in error_logs:
        code = row.get("errorCode")
        count = int(row.get("count") or 0)
        if not code or count < _ERRLOG_OPEN_COUNT:
            continue
        module = _ERRCODE_MODULE_PREFIX.get(str(code).split("_")[0].upper(), "system")
        key = f"{module}:logs:{code}"
        seen_keys.add(key)
        candidates.append(FindingCandidate(
            findingKey=key, module=module, kind="plugin_error_logs",
            severity="error" if count >= _ERRLOG_ERROR_COUNT else "warn", status="open",
            title=f"{count} recent error log(s) with code {code}",
            target={"type": "logs", "name": code, "module": module},
            evidence={"errorCode": code, "count": count,
                      "sampleMessage": (row.get("message") or "")[:300],
                      "resolutionHint": (row.get("resolution") or "")[:500]},
        ))
    # An errorCode that stopped occurring auto-resolves.
    for key, doc in (prior or {}).items():
        if doc.get("kind") == "plugin_error_logs" and key not in seen_keys:
            resolved.add(key)


# An EDM hash still applying on the tenant longer than this never cleared → stuck apply.
_EDM_APPLY_STUCK_MINUTES = 60


def _eval_edm_apply(module_signals: dict, now: datetime, prior: dict,
                    candidates: List[FindingCandidate], resolved: set) -> None:
    """edm_apply_stuck: an EDM hash upload still 'applying' on the tenant well past a normal apply.

    A completed/failed apply deletes its edm_hashes_status doc, so a doc that lingers means the
    tenant keeps returning pending/in-progress. Snapshot-only: opens on doc age, auto-resolves when
    the doc is gone (the apply cleared). Skips entirely if the signal family is absent.
    """
    rows = module_signals.get("edm_apply")
    if rows is None:
        return
    seen: set = set()
    for row in rows:
        src_id = row.get("fileSourceID")
        if not src_id:
            continue
        age = _age(now, _as_dt(row.get("createdAt")))
        if age is None or age.total_seconds() < _EDM_APPLY_STUCK_MINUTES * 60:
            continue
        key = f"edm:{src_id}:apply_stuck"
        seen.add(key)
        candidates.append(FindingCandidate(
            findingKey=key, module="edm", kind="edm_apply_stuck", severity="error", status="open",
            title="An EDM hash upload is stuck applying on the Netskope tenant",
            target={"type": "apply", "name": str(src_id), "module": "edm"},
            evidence={"fileSourceType": row.get("fileSourceType"),
                      "ageMinutes": int(age.total_seconds() // 60),
                      "sampleMessage": (row.get("message") or "")[:300],
                      "resolutionHint": "Check Sharing and Upload Management; if the tenant keeps "
                                        "returning pending, re-trigger the apply or verify the "
                                        "Netskope tenant EDM service."},
        ))
    for key, doc in (prior or {}).items():
        if doc.get("kind") == "edm_apply_stuck" and key not in seen:
            resolved.add(key)


def _eval_cfc_classifiers(module_signals: dict, prior: dict,
                          candidates: List[FindingCandidate], resolved: set) -> None:
    """cfc_deleted_classifier: a CFC sharing mapping whose classifier was deleted/failed on the tenant.

    Signal = a mapping with no classifierID (deleted from the tenant) or a set errorState. Snapshot-
    only: opens per bad mapping, auto-resolves when the mapping regains a classifierID / clears its
    error. Skips entirely if the signal family is absent.
    """
    rows = module_signals.get("cfc_sharing")
    if rows is None:
        return
    seen: set = set()
    for doc in rows:
        source = doc.get("sourceConfiguration")
        dest = doc.get("destinationConfiguration")
        for mapping in (doc.get("mappings") or []):
            if not isinstance(mapping, dict):
                continue
            error_state = mapping.get("errorState")
            bad = (not mapping.get("classifierID")) or bool(error_state)
            if not bad:
                continue
            rule = mapping.get("businessRule")
            key = f"cfc:{source}:{dest}:{rule}:deleted_classifier"
            seen.add(key)
            err_msg = error_state.get("errorMessage") if isinstance(error_state, dict) else error_state
            candidates.append(FindingCandidate(
                findingKey=key, module="cfc", kind="cfc_deleted_classifier", severity="error", status="open",
                title=f"CFC sharing '{source} → {dest}' references a missing classifier",
                target={"type": "sharing", "name": rule, "module": "cfc"},
                evidence={"classifierName": mapping.get("classifierName"), "source": source,
                          "destination": dest, "error": err_msg,
                          "resolutionHint": "Open CFC Sharing and re-select a valid classifier on the "
                                            "tenant for this rule — the mapped classifier was deleted "
                                            "or failed."},
            ))
    for key, doc in (prior or {}).items():
        if doc.get("kind") == "cfc_deleted_classifier" and key not in seen:
            resolved.add(key)


def _eval_settings(settings: dict, now: datetime,
                   candidates: List[FindingCandidate], resolved: set) -> None:
    """queue_backpressure (should_pull=False) + cert_expiry (<30d warn / <7d error)."""
    # queue back-pressure — reuse the platform's should_pull signal (no duplicate RabbitMQ call).
    bp_key = "system:queue:backpressure"
    if settings.get("should_pull") is False:
        candidates.append(FindingCandidate(
            findingKey=bp_key, module="system", kind="queue_backpressure", severity="error",
            status="open", title="Pulling is paused by back-pressure (Available disk space is low)",
            target={"type": "system", "name": "queue"},
            evidence={"lastCheckStatus": "back_pressure_active"},
        ))
    elif settings.get("should_pull") is True:
        resolved.add(bp_key)

    cert = settings.get("certExpiry")
    cert_days = None
    cert_at = _as_dt(cert)
    if cert_at is not None:
        cert_days = _age(cert_at, now)
        cert_days = cert_days.days if cert_days else None
    elif isinstance(cert, (int, float)):
        cert_days = int(cert)
    cert_key = "system:cert:platform"
    if cert_days is not None and cert_days < _CERT_WARN_DAYS:
        candidates.append(FindingCandidate(
            findingKey=cert_key, module="system", kind="cert_expiry",
            severity="error" if cert_days < _CERT_ERROR_DAYS else "warn", status="open",
            title=f"Platform certificate expires in {cert_days} day{'s' if cert_days != 1 else ''}",
            target={"type": "system", "name": "certificate"},
            evidence={"daysToExpiry": cert_days},
        ))
    elif cert_days is not None:
        resolved.add(cert_key)


def evaluate(configs: List[dict], settings: dict, now: datetime,
             prior: Optional[dict] = None, *,
             rules: Optional[dict] = None,
             error_logs: Optional[List[dict]] = None,
             enabled_modules: Optional[set] = None,
             module_signals: Optional[dict] = None) -> Tuple[List[FindingCandidate], set]:
    """Evaluate all rules over the current snapshot. Returns (candidates, resolved_keys).

    ``configs`` are all CE module config docs (CTE/CTO/CLS/CRE/EDM/CFC) each tagged with
    ``module``. ``prior`` maps findingKey -> stored finding (for streak/hysteresis). Every stale
    config surfaces as its OWN staleness finding (no mass-staleness/"storm" summary — see the
    module-level note); a legacy ``system:scheduler:stall`` finding is always resolved so it
    auto-clears after the upgrade.

    Optional (keyword-only; ``None`` skips that rule family, keeping old callers valid):
    ``rules`` — per rule-module wiring docs (cte sharedWith OR creShare / cto queues /
    cls siemMappings / cre actions / cfc _sharingWired) for the wiring rules (EDM omitted — no
    filter rules; a rule locked by the OTHER module's disable switch is skipped, see
    ``_CROSS_MODULE_LOCK``);
    ``error_logs`` — recent {errorCode, count, message?, resolution?} groups for
    plugin_error_logs; ``enabled_modules`` — modules toggled ON;
    ``module_signals`` — per-module signals ({"edm_apply": [...], "cfc_sharing": [...]}) for the
    module-specific rules (edm_apply_stuck, cfc_deleted_classifier).

    Prerequisite gating (each flushes the affected prior findings by adding their keys to
    ``resolved``): a config whose module is NOT in ``enabled_modules`` has EVERY rule skipped
    (a disabled module does not run); a config with ``_runnable is False`` (plugin uninstalled or
    push-only) OR ``_pullWired is False`` (CLS source with no siemMappings — its recurring pull is
    torn down by design) has the RUN-based rules (run_failure / staleness / cadence) skipped, since
    it does not run on a schedule. Both flags are precomputed in attention_data.load_configs
    (from the manifest / the CLS siemMappings join) to keep this engine Mongo-free; ``_pullWired``
    is always True for non-CLS modules (they pull independently of downstream wiring).
    """
    prior = prior or {}
    candidates: List[FindingCandidate] = []
    resolved: set = set()

    # PREREQUISITE GATING (a run-based finding must have something that actually runs behind it):
    # 1. DISABLED module — if a module is toggled OFF (not in enabled_modules), it does not run at
    #    all; skip EVERY per-config rule for its configs and FLUSH any findings they had. (When
    #    enabled_modules is None the family is "skip" per the callers-valid contract — no gating.)
    # 2. NON-RUNNABLE config — plugin uninstalled or push-only (``_runnable`` False, precomputed in
    #    attention_data.load_configs from the manifest): no scheduled run, so the run-based rules
    #    (run_failure / staleness / cadence) can't be valid; skip them and flush their prior findings.
    # Flushing = adding the config's prior finding keys to ``resolved`` (the scan auto-resolves them).
    def _flush_prior(pred) -> None:
        resolved.update(k for k in prior if pred(k))

    stale_candidates: List[FindingCandidate] = []
    for cfg in configs:
        # PER-CONFIG ISOLATION: a single malformed config doc (or a rule helper tripping on one)
        # must never abort the whole scan for every other module/config. The scan's outer handler
        # would otherwise catch it and drop the entire tick (already hand-patched once for a
        # naive-datetime TypeError — this generalizes that). Kept PURE: no logging here (the engine
        # is Mongo/LLM/log-free by contract; the scan caller owns logging). On skip we do NOT add
        # the config's keys to ``resolved`` — a finding we FAILED to evaluate must not be
        # auto-cleared; it simply carries over unchanged to the next tick.
        try:
            module = cfg.get("module")
            name = cfg.get("name")
            # Item 1: disabled module — flush ALL of this config's findings and skip every rule.
            if enabled_modules is not None and module not in enabled_modules:
                _flush_prior(lambda k, _m=module, _n=name: k.startswith(f"{_m}:{_n}:"))
                continue
            # RUN-rule prerequisites — skip (and FLUSH) the run rules when a scheduled run can't/won't
            # legitimately happen, so "no run" is never a false alarm:
            #   * _runnable is False  — plugin uninstalled or push-only (no pull op at all).
            #   * _pullWired is False — CLS ONLY: the source's recurring pull is torn down when it has
            #     no Log Delivery (siemMappings) mapping, so lastRunAt stops advancing BY DESIGN.
            #     Every other module pulls on its own schedule regardless of wiring, so _pullWired is
            #     always True for them (set in attention_data.load_configs).
            # Non-run rules (module_unconfigured, no_business_rules, rule_unwired) still apply below —
            # e.g. the CLS 'rule matches logs but forwards to no SIEM destination' nudge still fires.
            if cfg.get("_runnable") is False or cfg.get("_pullWired") is False:
                _flush_prior(lambda k, _m=module, _n=name: (
                    k.startswith(f"{_m}:{_n}:") and (
                        k.endswith(":run_failure") or k == f"{_m}:{_n}:stale"
                        or k == f"{_m}:{_n}:cadence_drift")
                ))
                continue
            _eval_run_failures(cfg, module, now, prior, candidates, resolved)
            _eval_cadence(cfg, module, now, prior, candidates, resolved)
            stale = _eval_staleness(cfg, module, now, resolved)
            if stale is not None:
                stale_candidates.append(stale)
        except Exception:
            # Skip this one config; every other config + the rule families below still evaluate.
            continue

    # DEDUP staleness vs cadence-drift for the SAME config. Both detect "this config isn't running":
    # staleness = a fixed 3x-poll threshold (opens at 'error'), cadence-drift = the learned-cadence
    # variant (opens at 'warn'). When BOTH fire for one config the user sees the same problem twice
    # (a critical "hasn't run in N min" AND a warning "no run for N min — usually ~15 min", per the
    # duplicate report). Staleness is the stronger, more direct signal, so it WINS: drop the
    # cadence-drift candidate for any config that is also stale, and resolve its cadence_drift key so
    # a previously-open one clears. (Cadence-drift still stands alone for a config that drifted but
    # is NOT yet past the staleness threshold — a genuinely distinct, earlier signal.)
    stale_keys = {sc.findingKey for sc in stale_candidates}

    def _config_of_stale(key: str) -> str:
        return key[: -len(":stale")] if key.endswith(":stale") else key

    stale_configs = {_config_of_stale(k) for k in stale_keys}
    deduped = []
    for c in candidates:
        if c.kind == "run_cadence_drift" and c.status == "open":
            cfg_prefix = c.findingKey[: -len(":cadence_drift")]
            if cfg_prefix in stale_configs:
                resolved.add(c.findingKey)  # clear a previously-open cadence-drift on this config
                continue  # drop the duplicate cadence-drift candidate; staleness covers it
        deduped.append(c)
    candidates = deduped

    # PER-FAMILY ISOLATION: like the per-config loop above, one rule family throwing must not sink
    # the others (or the whole scan). Each is wrapped independently; a family that fails contributes
    # no candidates/resolved this tick (its prior findings carry over untouched). Stays PURE — no
    # logging in the engine; the scan caller logs the aborted tick if evaluate() itself raises.
    def _safe(fn):
        try:
            fn()
        except Exception:
            pass

    _safe(lambda: _eval_wiring(configs, rules, enabled_modules, candidates, resolved))
    _safe(lambda: _eval_error_logs(error_logs, prior, candidates, resolved))
    if module_signals is not None:
        # A disabled module's signals are stale — skip its module-specific rules (item 1). When
        # enabled_modules is None (family-skip contract) both run, preserving old-caller behavior.
        def _module_on(m: str) -> bool:
            return enabled_modules is None or m in enabled_modules
        if _module_on("edm"):
            _safe(lambda: _eval_edm_apply(module_signals, now, prior, candidates, resolved))
        elif enabled_modules is not None:
            # EDM is disabled → its evaluator (which owns the resolve loop) is gated off, so a
            # prior edm_apply_stuck finding would linger open forever. These are keyed by
            # fileSourceID (edm:{src}:apply_stuck), NOT {module}:{name}:, so the per-config
            # disabled-module flush above never matches them — flush by kind here.
            _flush_prior(lambda k: prior.get(k, {}).get("kind") == "edm_apply_stuck")
        if _module_on("cfc"):
            _safe(lambda: _eval_cfc_classifiers(module_signals, prior, candidates, resolved))
        elif enabled_modules is not None:
            # Same for CFC: cfc_deleted_classifier is keyed by source:dest:rule, so flush by kind.
            _flush_prior(lambda k: prior.get(k, {}).get("kind") == "cfc_deleted_classifier")

    candidates.extend(stale_candidates)

    _safe(lambda: _eval_settings(settings or {}, now, candidates, resolved))
    # A key that is both a live candidate and in resolved stays OPEN (candidate wins).
    resolved -= {c.findingKey for c in candidates}
    return candidates, resolved
