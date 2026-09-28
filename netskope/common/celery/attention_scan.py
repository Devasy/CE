"""Proactive "Needs attention" detector (plan v5 §6) — programmatic, no LLM.

Runs every ~5 min (own schedule, own kill switch — not folded into heartbeat). Reads the
per-config run-health CE already writes (``lastRunSuccess``/``lastRunAt``/``lockedAt``) + a
couple of settings signals, runs the pure ``attention_rules.evaluate`` engine, and upserts
``copilot_findings`` with streak/hysteresis + a storm guard. Detection never calls the LLM;
that only happens on an explicit [Diagnose] click via the chat route's ``findingId`` hand-off.

HA/multi-worker safe: it's a SINGLETON periodic task (runs on the beat leader via
``_check_beat_lock``/``is_due`` in celery/scheduler.py), so under normal operation only one
scan runs per tick. There is no additional cross-process scan lock (the per-config
``LOCKING_ARGS`` convention in utils/scheduler.py locks ONE config document for a
pull/share/execute task and doesn't fit a single scan over ALL configs). The real guarantee
is idempotency, not transactional exclusivity: both write phases below are safe to repeat or
interleave — the candidate upsert is keyed by ``findingKey`` (last-write-wins on identical
inputs) and the resolved update is status-guarded (``$in: ["open", "watching",
"acknowledged"]``), so a rare beat-leader handover that double-fires a tick produces a
harmless no-op collision, not corruption. It is NOT full transactional safety: two genuinely
concurrent scans with DIFFERING inputs (e.g. a config change lands mid-handover) could still
interleave their two round-trips into a transient open<->resolve flip; the next tick
self-corrects.
"""

import os
import traceback

from pymongo import UpdateOne
from datetime import datetime, timedelta, UTC

from netskope.common.celery.main import APP
from netskope.common.utils import Collections, DBConnector, Logger, track
from netskope.common.utils import attention_data
from netskope.common.utils.attention_rules import evaluate, aware_utc
from netskope.common.models.ai_copilot.attention import CopilotFinding

connector = DBConnector()
logger = Logger()

# Retention: watching (pre-threshold) findings self-purge quickly (TTL index on expireAt). The
# RESOLVED window is the SHARED attention_data.RESOLVED_TTL, and the auto-resolve `$set` payload is
# built by attention_data.build_auto_resolve_update — so the scan and verify_finding can't drift.
_WATCHING_TTL = timedelta(days=7)

# The Mongo loaders live in utils/attention_data.py, SHARED with the verify endpoint —
# both must evaluate the rules over identical inputs (a verify that skips an input family
# would wrongly auto-resolve still-true findings of that kind).

# The scan runs on the cloudexchange_6 quorum queue (keeps the proactive work off the
# queues that carry plugin pulls). Cadence is operator-tunable via
# CE_ATTENTION_SCAN_INTERVAL_MINUTES (default 5, min 1) — the task self-reconciles its
# schedule doc on each run, so a changed env value takes effect after the NEXT run at the
# old cadence (no migration/manual Mongo edit needed). CE_ATTENTION_SCAN_DISABLED=true
# remains the hard kill switch.
_SCAN_SCHEDULE_NAME = "INTERNAL COPILOT ATTENTION SCAN"
_SCAN_QUEUE = "cloudexchange_6"
_DEFAULT_SCAN_MINUTES = 5


def _configured_minutes() -> int:
    try:
        return max(1, int(os.getenv("CE_ATTENTION_SCAN_INTERVAL_MINUTES", str(_DEFAULT_SCAN_MINUTES))))
    except ValueError:
        return _DEFAULT_SCAN_MINUTES


def _reconcile_schedule() -> None:
    """Align the schedule doc with the configured cadence/queue (best-effort, idempotent)."""
    try:
        minutes = _configured_minutes()
        doc = connector.collection(Collections.SCHEDULES).find_one(
            {"name": _SCAN_SCHEDULE_NAME}, {"_id": 0, "interval": 1, "queue": 1}
        ) or {}
        current = ((doc.get("interval") or {}).get("every"), doc.get("queue"))
        if current != (minutes, _SCAN_QUEUE):
            from netskope.common.utils.scheduler import Scheduler

            Scheduler().upsert(
                name=_SCAN_SCHEDULE_NAME,
                task_name="common.attention_scan",
                poll_interval=minutes,
                poll_interval_unit="minutes",
                args=[],
                queue=_SCAN_QUEUE,
            )
            logger.info(
                f"Copilot attention scan schedule updated: every {minutes} minute(s) on {_SCAN_QUEUE}."
            )
    except Exception:
        logger.warn(
            "Could not reconcile the attention scan schedule; keeping the current one.",
            details=traceback.format_exc(),
        )


def register_attention_scan_schedule() -> None:
    """Register the periodic attention-scan schedule doc if it isn't already registered.

    Called from LLM provider CREATE (on EVERY create, not just the first — the existence check
    below makes repeat calls a cheap no-op, and self-heals if the schedule doc ever goes missing
    while providers still exist) and from the 7.0.0-beta.1 migration (only when a provider
    already exists at upgrade time). Checking existence FIRST (rather than upserting
    unconditionally) also avoids needlessly resetting ``enabled``/``locked_on`` on a schedule
    that's already correctly registered and possibly mid-run. A deployment with zero providers
    never gets this schedule registered at all, so celery beat never fires (and immediately
    early-returns from) a periodic task with nothing to scan yet. Best-effort: never raises into
    its caller (provider CRUD must not fail because of a scheduling hiccup).
    """
    try:
        if connector.collection(Collections.SCHEDULES).find_one(
            {"name": _SCAN_SCHEDULE_NAME}, {"_id": 1}
        ) is not None:
            return

        from netskope.common.utils.scheduler import Scheduler

        Scheduler().upsert(
            name=_SCAN_SCHEDULE_NAME,
            task_name="common.attention_scan",
            poll_interval=_configured_minutes(),
            poll_interval_unit="minutes",
            args=[],
            queue=_SCAN_QUEUE,
        )
        logger.info("Copilot attention scan schedule registered.")
    except Exception:
        logger.warn(
            "Could not register the attention scan schedule.",
            details=traceback.format_exc(),
        )


def unregister_attention_scan_schedule() -> None:
    """Remove the periodic attention-scan schedule doc entirely.

    Called from LLM provider DELETE when that delete left ZERO providers configured — mirrors
    this codebase's existing enable-by-upsert/disable-by-delete_one convention for toggleable
    periodic tasks (see routers/settings.py's ``cre.delete_records`` / ``cte.ioc_retraction``
    schedules). Re-created by the next successful provider create. Best-effort: never raises.
    """
    try:
        from netskope.common.utils.scheduler import Scheduler

        Scheduler().delete(_SCAN_SCHEDULE_NAME)
        logger.info("Copilot attention scan schedule removed (no LLM provider remains).")
    except Exception:
        logger.warn(
            "Could not remove the attention scan schedule.",
            details=traceback.format_exc(),
        )


def purge_attention_findings() -> None:
    """Delete every proactive finding — called when the LAST LLM provider is removed.

    With no provider, findings are unconsumable (Diagnose/Start-fix need one) and the scan is
    unscheduled, so clearing `COPILOT_FINDINGS` (a) stops the feed/summary serving stale
    pre-removal findings and (b) guarantees a clean slate when a provider is later re-added — the
    next scan re-detects from scratch rather than resurrecting month-old `open` docs (which have no
    TTL). The feed also gates on provider existence as a live safety net; this keeps the DB tidy.
    Best-effort: never raises into its caller (provider CRUD must not fail on a cleanup hiccup).
    """
    try:
        connector.collection(Collections.COPILOT_FINDINGS).delete_many({})
        logger.info("Copilot attention findings purged (no LLM provider remains).")
    except Exception:
        logger.warn(
            "Could not purge attention findings.",
            details=traceback.format_exc(),
        )


@APP.task(name="common.attention_scan")
@track()
def attention_scan():
    """Scan CE run-health, upsert proactive findings (idempotent, hysteresis + storm guard)."""
    if os.getenv("CE_ATTENTION_SCAN_DISABLED", "").lower() == "true":
        return
    # No LLM provider CONFIGURED at all → nothing can ever consume findings (Diagnose/Start-fix
    # need one), so the scan would just burn Mongo I/O every 5 min for a feed no one can act on.
    # Deliberately checks EXISTENCE, not `active` — a configured-but-disabled provider can be
    # re-enabled any time without losing scan history, so we keep sensing (and the feed already
    # accumulated) warm for that case; only a fully unconfigured deployment skips. Uses the shared
    # attention_data.llm_provider_configured (existence, not `active`) — the ONE shared gate the
    # feed, this scan, and provider-delete teardown call, so they can't drift. It lives in the light
    # attention_data (NOT langchain-heavy llm_invoke), which this task already imports.
    if not attention_data.llm_provider_configured():
        logger.debug("Skipping the attention scan since no LLM provider is configured.")
        return
    _reconcile_schedule()
    try:
        now = datetime.now(UTC)

        def _safe_load(fn, label, default):
            """Run one loader in isolation — a single failing family must not drop the tick."""
            try:
                return fn()
            except Exception:
                logger.warn(
                    f"Copilot attention scan: loader '{label}' failed; skipping its rule family.",
                    details=traceback.format_exc(),
                )
                return default

        configs = _safe_load(attention_data.load_configs, "configs", [])
        settings = _safe_load(
            lambda: connector.collection(Collections.SETTINGS).find_one(
                {}, attention_data.SETTINGS_PROJECTION
            ) or {},
            "settings", {},
        )
        # Project only the fields the rule engine + the upsert loop read — the evidence/
        # title blobs on month-old resolved docs would otherwise dominate this read. Do NOT
        # filter by status: streak trackers and error-log auto-resolve read resolved priors.
        prior = _safe_load(
            lambda: {
                d["findingKey"]: d
                for d in connector.collection(Collections.COPILOT_FINDINGS).find(
                    {}, {"_id": 0, "findingKey": 1, "kind": 1, "status": 1,
                         "snoozeUntil": 1, "tracker": 1}
                )
            },
            "prior findings", {},
        )
        rules = _safe_load(attention_data.load_rules, "rules", None)
        error_logs = _safe_load(lambda: attention_data.load_error_logs(now), "error_logs", None)
        enabled_modules = _safe_load(
            lambda: attention_data.enabled_modules(settings), "enabled_modules", None
        )
        module_signals = _safe_load(attention_data.load_module_signals, "module_signals", None)

        candidates, resolved = evaluate(
            configs, settings, now, prior,
            rules=rules,
            error_logs=error_logs,
            enabled_modules=enabled_modules,
            module_signals=module_signals,
        )
        findings_coll = connector.collection(Collections.COPILOT_FINDINGS)
        ops = []

        for cand in candidates:
            existing = prior.get(cand.findingKey)
            status = cand.status
            # Feed-only, no auto re-unack (user decision): an acknowledged finding stays muted
            # until it resolves — unless its snooze has elapsed, which re-surfaces it.
            if existing and existing.get("status") == "acknowledged" and status == "open":
                snooze = existing.get("snoozeUntil")
                # Mongo hands back naive datetimes — coerce to aware UTC before comparing with the
                # aware `now` (or the comparison raises TypeError and kills the whole scan). Shared
                # aware_utc (attention_rules) so this coercion isn't re-inlined per site.
                snooze_dt = aware_utc(snooze) if isinstance(snooze, datetime) else None
                if snooze_dt is None or snooze_dt > now:
                    status = "acknowledged"
            # Never downgrade a surfaced finding back to invisible on a re-read.
            if existing and existing.get("status") == "open" and status == "watching":
                status = "open"
            # An acknowledged finding whose condition merely WEAKENED to watching (not
            # resolved) stays acknowledged — a weakened-but-not-resolved condition must not
            # silently un-ack the finding out from under the user's mute decision.
            if existing and existing.get("status") == "acknowledged" and status == "watching":
                status = "acknowledged"

            seed = CopilotFinding(
                findingKey=cand.findingKey, module=cand.module, kind=cand.kind,
                severity=cand.severity, status=status, title=cand.title,
                target=cand.target, evidence=cand.evidence, tracker=cand.tracker,
            )
            expire_at = (now + _WATCHING_TTL) if status == "watching" else None
            ops.append(UpdateOne(
                {"findingKey": cand.findingKey},
                {
                    "$set": {
                        "module": cand.module, "kind": cand.kind, "severity": cand.severity,
                        "status": status, "title": cand.title, "target": cand.target,
                        "evidence": cand.evidence, "tracker": cand.tracker,
                        "lastSeenAt": now, "expireAt": expire_at,
                    },
                    "$setOnInsert": {
                        "findingId": seed.findingId, "firstDetectedAt": now,
                    },
                    # a re-opened condition clears any prior resolution.
                    "$unset": {"resolvedAt": ""},
                },
                upsert=True,
            ))
        if ops:
            # One round-trip for the whole scan instead of one per finding (unordered:
            # findingKeys are distinct, so ops are independent).
            findings_coll.bulk_write(ops, ordered=False)

        if resolved:
            findings_coll.update_many(
                {"findingKey": {"$in": list(resolved)},
                 "status": {"$in": ["open", "watching", "acknowledged"]}},
                {"$set": attention_data.build_auto_resolve_update(now)},
            )

        open_count = sum(1 for c in candidates if c.status == "open")
        logger.info(
            f"Copilot attention scan: {open_count} open finding(s), "
            f"{len(resolved)} resolved, {len(configs)} configs scanned."
        )
    except Exception:
        logger.warn(
            "Copilot attention scan failed.",
            error_code="CE_1052", details=traceback.format_exc(),
        )
