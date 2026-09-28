"""Proactive "Needs attention" feed endpoints (plan v5 §6).

Serves the findings the programmatic ``attention_scan`` task writes. Deliberately NOT gated by
``ai_read`` — programmatic sensing works on AI-disabled deployments; only [Diagnose] (the chat
route's ``findingId`` hand-off) needs ``ai_read``. Access is per-module: a caller sees only the
findings for modules they can read.
"""

import traceback
from datetime import datetime, timedelta, UTC

from fastapi import APIRouter, HTTPException, Security

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.models.ai_copilot.attention import (
    AttentionSummary,
    CopilotFinding,
    FindingAckRequest,
)
from netskope.common.utils import Collections, DBConnector, PrefixedLogger
from netskope.common.utils import attention_data
from netskope.common.utils.attention_rules import evaluate
from netskope.common.utils.config_tools import MODULE_READ_SCOPE, MODULE_WRITE_SCOPE

router = APIRouter(prefix="/copilot")
connector = DBConnector()
logger = PrefixedLogger("[AI-COPILOT]")

# Findings the feed surfaces (watching = pre-threshold/invisible; resolved = historical).
_FEED_STATUSES = ("open", "acknowledged")
# Which security scope a caller needs to ACT on (ack) vs SEE a module's finding —
# the canonical maps live in config_tools (shared with the tool registry + journey routing).
_MODULE_WRITE_SCOPE = MODULE_WRITE_SCOPE
_MODULE_READ_SCOPE = MODULE_READ_SCOPE


def _visible_modules(scopes) -> set:
    """Return the modules the caller may see findings for.

    Derived from ``_MODULE_READ_SCOPE`` so a module added to the map is automatically
    visible/filterable/verifiable consistently.
    """
    s = set(scopes or [])
    return {module for module, scope in _MODULE_READ_SCOPE.items() if scope in s}


@router.get("/attention", tags=["Copilot Attention"], description="Proactive findings feed (per-module).")
def list_attention(
    module: str = None,
    severity: str = None,
    limit: int = 100,
    user: User = Security(get_current_user, scopes=[]),
) -> list:
    """Return the caller's visible open/acknowledged findings, newest first (plan v5 §6)."""
    visible = _visible_modules(user.scopes)
    if not visible:
        raise HTTPException(403, "You don't have read access to any module's findings.")
    if not attention_data.llm_provider_configured():
        # No provider → findings are unconsumable; serve nothing rather than stale pre-removal ones.
        return []
    query = {"status": {"$in": list(_FEED_STATUSES)}, "module": {"$in": list(visible)}}
    if module:
        if module not in visible:
            raise HTTPException(403, f"You don't have read access to '{module}' findings.")
        query["module"] = module
    if severity in ("warn", "error"):
        query["severity"] = severity
    docs = list(
        connector.collection(Collections.COPILOT_FINDINGS)
        .find(query, {"_id": 0})
        .sort("lastSeenAt", -1)
        .limit(max(1, min(limit, 500)))
    )
    return docs


@router.get(
    "/attention/summary",
    tags=["Copilot Attention"],
    description="Counts by module + last scan time for the dashboard 'Needs attention' card.",
)
def attention_summary(user: User = Security(get_current_user, scopes=[])) -> AttentionSummary:
    """Aggregate OPEN findings by module/severity for the dashboard card + rail badge.

    Acknowledged (muted) findings are deliberately excluded here — the user silenced them, so
    they must not inflate "N items need attention" (the feed still returns them, and the Guided
    tab lists them in its Muted section). Keeps this count consistent with the tab badge and
    the per-tab status pills, which also count only unmuted findings.
    """
    visible = _visible_modules(user.scopes)
    if not visible:
        return AttentionSummary()
    if not attention_data.llm_provider_configured():
        # No provider → nothing to surface on the dashboard card / rail badge.
        return AttentionSummary()
    docs = list(
        connector.collection(Collections.COPILOT_FINDINGS).find(
            {"status": "open", "module": {"$in": list(visible)}},
            {"_id": 0, "module": 1, "severity": 1, "lastSeenAt": 1},
        )
    )
    by_module: dict = {}
    last_scan = None
    for d in docs:
        m = by_module.setdefault(d.get("module"), {"warn": 0, "error": 0})
        m[d.get("severity", "warn")] = m.get(d.get("severity", "warn"), 0) + 1
        seen = d.get("lastSeenAt")
        if isinstance(seen, datetime) and (last_scan is None or seen > last_scan):
            last_scan = seen
    return AttentionSummary(total=len(docs), byModule=by_module, lastScanAt=last_scan,
                            visibleModules=sorted(visible))


def _load_finding(finding_id: str) -> dict:
    doc = connector.collection(Collections.COPILOT_FINDINGS).find_one({"findingId": finding_id}, {"_id": 0})
    if not doc:
        raise HTTPException(404, "Finding not found.")
    return doc


@router.patch(
    "/attention/{finding_id}/ack",
    tags=["Copilot Attention"],
    description="Acknowledge/snooze (or un-acknowledge) a finding.",
)
def ack_finding(
    finding_id: str,
    payload: FindingAckRequest,
    user: User = Security(get_current_user, scopes=[]),
) -> CopilotFinding:
    """Mute or un-mute a finding (needs the module's *_write scope, or admin)."""
    doc = _load_finding(finding_id)
    module = doc.get("module")
    write_scope = _MODULE_WRITE_SCOPE.get(module)
    if "admin" not in set(user.scopes or []) and write_scope not in set(user.scopes or []):
        raise HTTPException(403, f"Acknowledging a '{module}' finding requires '{write_scope}' or 'admin'.")
    now = datetime.now(UTC)
    if payload.acknowledged:
        update = {"status": "acknowledged", "ackBy": user.username, "ackAt": now}
        if payload.snoozeMinutes:
            update["snoozeUntil"] = now + timedelta(minutes=payload.snoozeMinutes)
    else:
        update = {"status": "open", "ackBy": None, "ackAt": None, "snoozeUntil": None}
    connector.collection(Collections.COPILOT_FINDINGS).update_one(
        {"findingId": finding_id}, {"$set": update}
    )
    return CopilotFinding(**{**doc, **update})


@router.post(
    "/attention/{finding_id}/verify",
    tags=["Copilot Attention"],
    description="Re-run this finding's rule now against fresh reads; auto-resolve if it cleared.",
)
def verify_finding(
    finding_id: str,
    user: User = Security(get_current_user, scopes=[]),
) -> CopilotFinding:
    """Re-evaluate ONE finding synchronously (powers the guided-fix Verify step)."""
    doc = _load_finding(finding_id)
    module = doc.get("module")
    if module in _MODULE_READ_SCOPE and _MODULE_READ_SCOPE[module] not in set(user.scopes or []):
        raise HTTPException(403, f"Verifying a '{module}' finding requires '{_MODULE_READ_SCOPE[module]}'.")
    now = datetime.now(UTC)
    try:
        # Re-evaluate over the SAME complete inputs the periodic scan uses (shared loaders in
        # attention_data). This must never pass a partial input set: evaluate() treats a missing
        # family (rules/error_logs/enabled_modules=None) as "skip those rules", so a partial
        # verify would silently auto-resolve still-true module-level findings (no_business_rules,
        # module_unconfigured, rule_unwired, plugin_error_logs) — seen in review, do not "optimize"
        # this back to a single-config read.
        configs = attention_data.load_configs()
        settings = connector.collection(Collections.SETTINGS).find_one(
            {}, attention_data.SETTINGS_PROJECTION
        ) or {}
        # THIS finding rides as prior (so streak/hysteresis state carries), then check if its
        # key is still an open condition.
        candidates, resolved = evaluate(
            configs, settings, now, {doc["findingKey"]: doc},
            rules=attention_data.load_rules(),
            error_logs=attention_data.load_error_logs(now),
            enabled_modules=attention_data.enabled_modules(settings),
            module_signals=attention_data.load_module_signals(),
        )
        # A candidate for this key that is EITHER "open" OR "watching" means the condition has
        # NOT recovered — mirror the scan's open->watching guard (attention_scan.py: "never
        # downgrade a surfaced finding back to invisible"). Treating a "watching" candidate as
        # resolved would false-resolve a still-drifting finding (e.g. run_cadence_drift in the
        # hysteresis band: avg < gap <= 2*avg — drifted, not recovered) with a 30-day TTL, telling
        # the operator "resolved" while the config is still off. Only ABSENCE from candidates (the
        # rule no longer fires at all) is a genuine recovery.
        still_open = any(
            c.findingKey == doc["findingKey"] and c.status in ("open", "watching")
            for c in candidates
        )
    except Exception:
        logger.warn("Finding verify re-evaluation failed.", details=traceback.format_exc())
        # On any read error, don't falsely resolve — report the finding unchanged.
        return CopilotFinding(**doc)

    if not still_open:
        # Shared builder (attention_data) — SAME auto-resolve payload + 30-day TTL the scan writes,
        # so a Verify-resolved and a scan-resolved finding prune on the same schedule (no drift).
        update = attention_data.build_auto_resolve_update(now)
        connector.collection(Collections.COPILOT_FINDINGS).update_one(
            {"findingId": finding_id}, {"$set": update}
        )
        return CopilotFinding(**{**doc, **update})
    connector.collection(Collections.COPILOT_FINDINGS).update_one(
        {"findingId": finding_id}, {"$set": {"lastSeenAt": now}}
    )
    return CopilotFinding(**{**doc, "lastSeenAt": now})
