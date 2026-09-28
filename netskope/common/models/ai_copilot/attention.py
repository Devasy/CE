"""Models for the proactive "Needs attention" data-plane (plan v5 §6).

Findings are produced programmatically by the ``attention_scan`` celery task (no LLM) and
served from ``GET /copilot/attention``. The LLM is only involved later, on an explicit
[Diagnose] click, via the chat route's ``findingId`` hand-off.
"""

from datetime import datetime, UTC
from typing import Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

FindingSeverity = Literal["warn", "error"]
# watching = pre-threshold tracker (invisible to the feed); open = surfaced; acknowledged =
# user-muted; resolved = fixed by the user; auto_resolved = cleared by a later scan / verify.
FindingStatus = Literal["watching", "open", "acknowledged", "resolved", "auto_resolved"]


class CopilotFinding(BaseModel):
    """One proactive finding (stored in COPILOT_FINDINGS, keyed uniquely by ``findingKey``)."""

    findingId: str = Field(default_factory=lambda: str(uuid4()))
    findingKey: str  # stable dedup key (module:target:op:kind) — one doc per condition
    module: str  # cte | cto | cls | cre | edm | cfc | system  (§18 all-module expansion)
    kind: str  # run_failure_streak | staleness | queue_backpressure | cert_expiry | plugin_error_logs | …
    severity: FindingSeverity = "warn"
    status: FindingStatus = "open"
    title: str
    target: dict = Field(default_factory=dict)  # {type, name, op?, module?}
    evidence: dict = Field(default_factory=dict)  # NON-SECRET whitelist (streak, ages, codes, hints)
    tracker: dict = Field(default_factory=dict)  # streak/hysteresis state (engine-owned)
    firstDetectedAt: datetime = Field(default_factory=lambda: datetime.now(UTC))
    lastSeenAt: datetime = Field(default_factory=lambda: datetime.now(UTC))
    resolvedAt: Optional[datetime] = None
    ackBy: Optional[str] = None
    ackAt: Optional[datetime] = None
    snoozeUntil: Optional[datetime] = None


class FindingAckRequest(BaseModel):
    """PATCH body to acknowledge/snooze (or un-ack) a finding."""

    acknowledged: bool = Field(default=True, description="True to acknowledge/mute; False to un-ack.")
    snoozeMinutes: Optional[int] = Field(
        default=None,
        ge=1,
        le=525600,  # bound 1 min … 1 year — no negative / absurd far-future snooze
        description="Optional: re-surface the finding after this many minutes (1 … 525600).",
    )


class AttentionSummary(BaseModel):
    """Dashboard 'Needs attention' card payload: counts by module + last scan time."""

    total: int = 0
    byModule: dict = Field(default_factory=dict)  # {module: {warn, error}}
    lastScanAt: Optional[datetime] = None
    # Modules the CALLER may read. Lets per-module UI (the Home status pills) distinguish
    # "no findings" from "not allowed to see findings" without a per-module request.
    visibleModules: list = Field(default_factory=list)
