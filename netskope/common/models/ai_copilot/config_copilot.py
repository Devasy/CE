"""Models for the Configuration Copilot endpoints.

The Configuration Copilot is a conversational, human-in-the-loop assistant for
configuring and tuning Cloud Exchange modules — CTE, CTO/ITSM, CLS, CRE, EDM and
CFC — plus system/general settings, dashboards, the Plugin Store and (admin) user
scopes. It reuses the LLM gateway, SSE streaming, citation grounding and usage
telemetry built for the Posture assessment.

A turn's structured response (``CopilotTurnResponse``) carries a prose ``answer``
with ``[N]`` citations, typed insight cards, and — for a multi-step setup or fix —
an interactive ``journey`` (a step checklist the Guided tab renders). The journey
is the actionable artifact; the copilot never writes config itself (the real form's
Save is the only writer).
"""

import json
import re
from datetime import datetime, UTC
from typing import List, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

from netskope.common.models.ai_copilot.analyze import Citation

# Model-layer size ceiling for the untrusted, client-captured dicts below (pageState /
# DashboardSnapshot's open dict fields). Mirrors copilot_agents._SNAPSHOT_CAP (8 KiB) — kept as
# its own constant (not imported) so this models module doesn't reach into utils. This is
# belt-and-suspenders: copilot_agents.page_state_block/dashboard_snapshot_block ALWAYS
# byte-cap + strip secrets before either dict reaches the model on the /chat path (see
# _bounded_block), so this validator's job is to keep an oversized payload from being carried
# around uncapped elsewhere (persisted session docs, other future callers) rather than to be the
# only guard. TRUNCATE (not reject) — an oversized form/dashboard snapshot must never 422 a chat
# request; the UI already caps client-side, so this only catches a caller that didn't.
_CLIENT_DICT_CAP = 8_192


def _truncate_oversized_dict(value):
    """Cap a client-supplied dict's JSON-serialized size, truncating (never rejecting) it.

    Returns ``value`` unchanged if it isn't a dict or fits the cap. Otherwise returns a small
    marker dict flagging the drop — the original keys are NOT partially kept (a half-truncated
    JSON blob would be invalid/misleading), matching the "degrade, don't revert" pattern used
    elsewhere in the copilot (e.g. _bounded_block's "…[truncated]" suffix).
    """
    if not isinstance(value, dict):
        return value
    try:
        size = len(json.dumps(value, default=str))
    except (TypeError, ValueError):
        return value
    if size <= _CLIENT_DICT_CAP:
        return value
    return {"_truncated": True, "_originalSizeBytes": size}


# Markers that indicate the model NARRATED its structured-output tool-call wire format into the
# `answer` string instead of only filling the field (seen in the field: an answer ending with
# `...</answer> <parameter name="citations"> [{"title":...}]`). ToolStrategy accepts it — `answer`
# is just a string — so it isn't a parse failure/degrade (no gateway warn fires); the pollution
# reaches the UI AND normalize_citations scrapes the narrated JSON into junk Sources. Cut the
# answer at the first such marker (the real prose always precedes it).
_SCAFFOLD_MARKERS = ("</answer>", "<parameter name=", "<parameter", "<answer>")
# [^<>]* (not [^>]*): a model-narrated answer with many unclosed "<parameter " runs would
# otherwise make re.search retry an unbounded scan-to-end at every one of those start
# positions (O(n^2)) — excluding "<" too lets each attempt fail fast at the next tag open.
_SCAFFOLD_TAG_RE = re.compile(r"</?answer>|<parameter\b[^<>]*>")


def scrub_answer_scaffolding(text: str) -> str:
    """Strip leaked structured-output tool-call scaffolding from an ``answer`` string.

    Best-effort + conservative: if no scaffold marker is present the text returns unchanged; if
    cutting would leave nothing usable, the stripped-tag original is kept (a messy answer beats an
    empty one).
    """
    if not isinstance(text, str) or not text:
        return text
    cut = len(text)
    for marker in _SCAFFOLD_MARKERS:
        idx = text.find(marker)
        if idx != -1:
            cut = min(cut, idx)
    trimmed = _SCAFFOLD_TAG_RE.sub("", text[:cut]).strip()
    return trimmed or _SCAFFOLD_TAG_RE.sub("", text).strip() or text


# CE modules the copilot can introspect (all six as of the all-module expansion). Used for
# SERVER-constructed values (e.g. an insight/journey deep-link target) where we control the value.
# NOTE: PageContext below deliberately does NOT use this Literal for its client-supplied hint —
# see the comment there. Adding a new module means adding it here too.
CopilotModule = Literal["cte", "cto", "cls", "cre", "edm", "cfc"]
CopilotSurface = Literal["plugin", "settings", "system", "store", "users", "dashboard", "health"]


class DashboardSnapshot(BaseModel):
    """A bounded, client-captured snapshot of the dashboard the user is viewing.

    Sent so the copilot reasons over EXACTLY what's on screen (the same counts and
    recent lists the user sees), rather than only what backend tools re-fetch. It is
    treated as UNTRUSTED data server-side: wrapped in ``<dashboard_data>`` and
    stripped of any secret-bearing keys before it reaches the model. The UI builds
    these via per-surface whitelist builders and size-caps them; the server
    defensively re-strips. Each section is an open dict so the UI can evolve without
    a model change — but it must NEVER include plugin ``parameters`` or credentials.
    """

    surface: Optional[str] = None
    systemHealth: Optional[dict] = None
    cteDashboard: Optional[dict] = None
    ctoDashboard: Optional[dict] = None
    pluginCards: Optional[List[dict]] = None
    plugin: Optional[dict] = Field(default=None, description="Single failed-plugin context for a tile handoff.")
    truncated: bool = Field(default=False, description="True if the UI dropped data to fit the size budget.")

    @field_validator("systemHealth", "cteDashboard", "ctoDashboard", "plugin", mode="before")
    @classmethod
    def _cap_section_size(cls, value):
        """Model-layer size ceiling (see ``_CLIENT_DICT_CAP``) — truncate, never reject."""
        return _truncate_oversized_dict(value)


class PageContext(BaseModel):
    """Where the user is in the CE UI when they ask the copilot something.

    Sent by the UI on every turn so the copilot can tailor guidance (and pick the
    fresh-config vs optimize-existing mode). All fields optional — the copilot
    degrades to module-agnostic help when context is absent.
    """

    # Client-supplied page hints — kept LENIENT (plain str, not the CopilotSurface/CopilotModule
    # Literals) on purpose: an unrecognized surface/module must NEVER 422 the chat request. The
    # router (select_specialists) degrades an unknown value to the caller's accessible general set,
    # and RBAC is enforced by the scope-gated tools — not by trusting this hint. (A strict Literal
    # here previously 422'd every CLS/CRE/EDM/CFC page: "Input should be 'cte' or 'cto'".)
    surface: Optional[str] = None
    module: Optional[str] = None
    screen: Optional[str] = Field(default=None, description="UI screen/tab, e.g. 'plugins', 'businessRules'.")
    fields: Optional[List[str]] = Field(
        default=None,
        description="Specific field(s) the user asked about, if any. A list so co-related multi-field "
        "questions (e.g. thread count and batch size together) carry full context.",
    )
    quickActionId: Optional[str] = Field(
        default=None,
        description="Id of the Rovo-style quick-action chip that seeded this turn, if any (telemetry/UX).",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of the current page, from the UI route registry.",
    )
    dashboardSnapshot: Optional[DashboardSnapshot] = Field(
        default=None,
        description="Bounded snapshot of the dashboard the user is viewing, fed as untrusted grounding context.",
    )
    pageState: Optional[dict] = Field(
        default=None,
        description="Live form/rule/editor state the user is editing on this page — untrusted, secret-stripped; "
        "lets the copilot help with the actual values in front of them.",
    )

    @field_validator("pageState", mode="before")
    @classmethod
    def _cap_page_state_size(cls, value):
        """Model-layer size ceiling (see ``_CLIENT_DICT_CAP``) — truncate, never reject.

        Belt-and-suspenders: ``page_state_block`` already byte-caps + strips secrets on the
        /chat path (see module docstring above), but this keeps an oversized ``pageState`` from
        being carried around uncapped before it gets there (e.g. if persisted on the session
        doc as-is).
        """
        return _truncate_oversized_dict(value)


class ConfigChatRequest(BaseModel):
    """Incoming chat turn for the Configuration Copilot."""

    sessionId: Optional[str] = Field(default=None, description="Resume an existing session; omit to start a new one.")
    # Bounded server-side (the UI also caps at MAX_PROMPT_CHARS=4000 — keep the two in sync). This is
    # the REAL guard: a huge message can't reach the model (token-cost/DoS surface) and an empty one is
    # rejected up front. Over-limit → 422 at request validation, surfaced as the SSE error.
    message: str = Field(
        min_length=1,
        max_length=4000,
        description="The user's message for this turn (1–4000 chars).",
    )
    pageContext: Optional[PageContext] = None
    findingId: Optional[str] = Field(
        default=None,
        description="Set when the turn is diagnosing a proactive finding ([Diagnose]); the router grounds the turn "
        "in that finding's evidence.",
    )

    @model_validator(mode="after")
    def _validate_message(self):
        if self.message and self.message.strip():
            return self
        raise ValueError("message must not be empty.")


# --- guided journeys (the Guided tab's interactive, completable step checklist) ----------
_JOURNEY_KIND = Literal["setup", "fix"]
_STEP_STATE = Literal["pending", "done", "skipped", "blocked"]
_STEP_PHASE = Literal["detect", "rootcause", "apply", "verify"]


class JourneyStepDraft(BaseModel):
    """One journey step AS THE LLM EMITS IT (the router assigns ids + resolves route/citations)."""

    title: str = Field(description="Short imperative step title, e.g. 'Configure the CrowdStrike plugin'.")
    detail: str = Field(default="", description="What to do in this step (markdown, 1-3 sentences).")
    routeId: Optional[str] = Field(
        default=None,
        description="A route CATALOG KEY this step acts on (NOT a URL path). Use ONLY the keys listed in the prompt.",
    )
    actions: List[str] = Field(
        default_factory=list,
        description="Concrete do/update/add items for this step (each a short line; <=5).",
    )
    phase: Optional[_STEP_PHASE] = Field(
        default=None, description="For a 'fix' journey only: detect | rootcause | apply | verify."
    )
    citationRefs: List[int] = Field(
        default_factory=list,
        description="Indices into THIS turn's citations[] that ground the step (<=3).",
    )


class JourneyDraft(BaseModel):
    """A guided journey AS THE LLM EMITS IT (validated + persisted into ``CopilotJourney``)."""

    title: str = Field(description="The journey's goal as a short title.")
    goal: str = Field(default="", description="One line describing the end state the user reaches.")
    kind: _JOURNEY_KIND = Field(
        default="setup", description="'setup' to configure something; 'fix' to resolve a problem."
    )
    playbookId: Optional[str] = Field(
        default=None,
        description="When the ask matches one of the pre-declared playbooks listed in the prompt, "
                    "its id (follow that playbook's skeleton exactly). Leave null for a free-form "
                    "guide — the UI tells the user it is free-form.",
    )
    steps: List[JourneyStepDraft] = Field(default_factory=list, description="Ordered steps (<=10).")


class JourneyStep(BaseModel):
    """A PERSISTED journey step (server-owned ids/route/citations/state)."""

    stepId: str
    title: str
    detail: str = ""
    routeId: Optional[str] = None
    path: Optional[str] = None  # resolved from the route catalog (server)
    actions: List[str] = Field(default_factory=list)
    phase: Optional[_STEP_PHASE] = None
    citations: List[Citation] = Field(default_factory=list)  # resolved snapshots (survive resume)
    state: _STEP_STATE = "pending"
    blockedReason: Optional[str] = None  # set when RBAC blocks the step (kept, never silently dropped)


class CopilotJourney(BaseModel):
    """A persisted, resumable guided journey — one ACTIVE per session (plan v5 §7).

    A new journey that ALIGNS with the active one (same target) is merged into it
    (progress preserved); a genuinely different one PAUSES the active journey instead of
    discarding it — the user resumes or dismisses paused guides from the Guided panel.
    """

    journeyId: str = Field(default_factory=lambda: str(uuid4()))
    title: str
    goal: str = ""
    kind: _JOURNEY_KIND = "setup"
    module: Optional[str] = None
    status: Literal["active", "paused", "dismissed"] = "active"
    playbookId: Optional[str] = Field(
        default=None,
        description="The pre-declared playbook this journey follows (validated against the "
                    "catalog); None = a free-form guide, which the UI labels as such.",
    )
    steps: List[JourneyStep] = Field(default_factory=list)
    # Set when the caller can VIEW/navigate every step (has the *_read scopes) but lacks a WRITE
    # scope needed to actually apply the fix (e.g. cte_write). The steps are still generated and
    # usable as guidance; this advisory tells the user which role they'll need to save the change
    # (or to ask an admin). None when the caller can act on every step. Distinct from a step's
    # `blocked` state, which is for steps the user cannot even VIEW.
    writeNotice: Optional[str] = None
    createdAt: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updatedAt: datetime = Field(default_factory=lambda: datetime.now(UTC))


class JourneyStepUpdateRequest(BaseModel):
    """PATCH body for a journey: change one step's state, dismiss, or resume a paused journey."""

    stepId: Optional[str] = Field(default=None, description="Step to update (with ``status``).")
    status: Optional[Literal["done", "skipped", "pending"]] = Field(
        default=None, description="New state for ``stepId`` (blocked steps accept only 'skipped')."
    )
    action: Optional[Literal["dismiss", "resume"]] = Field(
        default=None,
        description="'dismiss' ends a journey; 'resume' swaps a PAUSED journey (by ``journeyId``) back "
                    "to active (the currently active one is paused, not lost). Ignores stepId/status.",
    )
    journeyId: Optional[str] = Field(
        default=None,
        description="Target journey for 'resume'/'dismiss'. Defaults to the active journey for 'dismiss'.",
    )


class JourneyStateResponse(BaseModel):
    """Full journey state after a PATCH — the active journey plus any paused ones."""

    journey: Optional[CopilotJourney] = None
    pausedJourneys: List[CopilotJourney] = Field(default_factory=list)


# --- typed insight cards (the Conversational tab's labeled answer cards, plan v5 §7) ------
_INSIGHT_TYPE = Literal[
    "ROOT_CAUSE", "LOG_DIAGNOSIS", "EXPLAINER", "HOW_TO", "GUIDED_SETUP", "PROACTIVE_ALERT",
    # An unused-but-available capability framed as ADDED ADVANTAGE over a working setup (e.g.
    # enabling the other sync direction) — positive framing, never "your setup is wrong".
    "OPPORTUNITY",
]


class InsightDraft(BaseModel):
    """A typed insight card AS THE LLM EMITS IT (router validates route + resolves citations)."""

    type: _INSIGHT_TYPE = Field(description="The card kind — pick the one that fits the answer.")
    title: str = Field(description="Short card title.")
    summary: str = Field(
        default="",
        description=(
            "ONE plain sentence a busy admin can act on without reading further — the takeaway + the single "
            "next action (e.g. 'CrowdStrike is pull-only by design; confirm that's intended.'). This is always "
            "shown; `body` is hidden behind a 'Details' expander. Never leave empty for a substantive card."
        ),
    )
    body: str = Field(
        default="",
        description="The full reasoning/detail for a user who wants to understand it (markdown). Hidden by default.",
    )
    routeId: Optional[str] = Field(
        default=None, description="Optional deep-link — a route CATALOG KEY (not a path), from the allowed list."
    )
    citationRefs: List[int] = Field(default_factory=list, description="1-based indices into this turn's citations.")


class CopilotInsight(BaseModel):
    """A PERSISTED typed insight card (route resolved + RBAC-checked + citations snapshotted)."""

    type: _INSIGHT_TYPE
    title: str
    summary: str = ""
    body: str = ""
    routeId: Optional[str] = None
    path: Optional[str] = None
    citations: List[Citation] = Field(default_factory=list)


class CopilotTurnResponse(BaseModel):
    """Structured result for a single Configuration Copilot turn.

    ``answer`` is the prose/markdown reply (citations anchored with ``[N]`` by
    ``normalize_citations``). Token/tool counts are backfilled by the gateway.
    """

    answer: str = Field(description="Markdown reply to the user. Anchor grounded claims with [N] markers.")
    writeNotice: Optional[str] = Field(default=None)
    citations: list[Citation] = Field(
        default_factory=list,
        description="Documentation sources grounding this turn. Do NOT inline these URLs in 'answer'.",
    )
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description="How confident you are in this turn's guidance (never 0 — that implies no due diligence).",
    )
    webSearchEnriched: bool = Field(default=False, description="True if web search/fetch grounding was used.")
    inputTokens: int = Field(default=0, description="Total input tokens consumed (set by the gateway).")
    outputTokens: int = Field(default=0, description="Total output tokens consumed (set by the gateway).")
    toolCallsUsed: int = Field(default=0, description="Tool calls used this turn (set by the gateway).")
    messageId: Optional[str] = Field(
        default=None,
        description="Stable id of this assistant turn (set by the router); the UI keys copy/feedback off it.",
    )
    roundTripMs: Optional[int] = Field(
        default=None, description="End-to-end request→response time in ms (set by the router)."
    )
    agentsInvoked: List[str] = Field(
        default_factory=list,
        description="Specialist agents that ran this turn (e.g. ['cte_config'] or ['supervisor','log_analyzer']).",
    )
    journey: Optional[CopilotJourney] = Field(
        default=None,
        description="A guided journey the turn started/updated (validated + persisted by the router).",
    )
    journeyAction: Optional[Literal["started", "updated", "paused_previous"]] = Field(
        default=None,
        description="What happened to journey state this turn: a fresh guide started, the active guide "
                    "was updated in place (aligned — progress preserved), or a new guide started and the "
                    "previous one was paused (resumable from the Guided panel).",
    )
    pausedJourneys: List[CopilotJourney] = Field(
        default_factory=list,
        description="Paused guides on the session after this turn (the UI syncs its Guided panel from this).",
    )
    insights: List[CopilotInsight] = Field(
        default_factory=list,
        description="Typed insight cards (ROOT_CAUSE/HOW_TO/EXPLAINER/...) rendered in the Conversational tab.",
    )


class CopilotAgentTurn(BaseModel):
    """Lean structured output the LLM actually produces for a turn.

    Deliberately flat (close to AnalyzeResponse) so the provider's structured-output schema
    compiler accepts it. The actionable setup/fix artifact is the ``journey`` (an interactive
    step checklist), which the router validates against the route catalog + RBAC and returns
    on the ``CopilotTurnResponse``.
    """

    answer: str = Field(description="Markdown reply to the user. Anchor grounded claims with [N] markers.")

    @field_validator("answer", mode="before")
    @classmethod
    def _scrub_answer(cls, v):
        """Strip narrated tool-call scaffolding the model sometimes leaks into `answer`.

        Runs at the model boundary so EVERY path that builds a CopilotAgentTurn (structured
        success, the repair reformat, the plain-text fallback) gets a clean answer — and the
        narrated citations JSON never reaches normalize_citations to become junk Sources.
        """
        return scrub_answer_scaffolding(v) if isinstance(v, str) else v
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description="How confident you are in this turn's guidance (never 0 — that implies no due diligence).",
    )
    citations: list[Citation] = Field(
        default_factory=list,
        description="Documentation sources grounding this turn. Do NOT inline these URLs in 'answer'.",
    )
    journey: Optional[JourneyDraft] = Field(
        default=None,
        description=(
            "When the user asks to be walked through a multi-step setup or fix, emit a guided journey here "
            "(title, goal, kind, ordered steps with concrete actions + routeId from the allowed catalog keys + "
            "citationRefs). Emit ONE journey per request; do not re-emit an unchanged journey on follow-ups. "
            "Leave null for plain answers."
        ),
    )
    insights: List[InsightDraft] = Field(
        default_factory=list,
        description=(
            "OPTIONAL typed insight cards for a substantive answer — e.g. a ROOT_CAUSE for a diagnosis, HOW_TO for "
            "a procedure, EXPLAINER for a concept, LOG_DIAGNOSIS for a log finding. Keep `answer` as the prose "
            "summary; put the structured card(s) here. Omit for trivial replies/clarifying questions."
        ),
    )
    webSearchEnriched: bool = Field(default=False, description="True if web search/fetch grounding was used.")
    inputTokens: int = Field(default=0, description="Total input tokens consumed (set by the gateway).")
    outputTokens: int = Field(default=0, description="Total output tokens consumed (set by the gateway).")
    toolCallsUsed: int = Field(default=0, description="Tool calls used this turn (set by the gateway).")


class CopilotMessage(BaseModel):
    """One stored message in a persisted copilot session."""

    role: Literal["user", "assistant", "system"]
    content: str
    citations: list[Citation] = Field(default_factory=list)
    insights: List[CopilotInsight] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    messageId: Optional[str] = Field(
        default=None,
        description="Assistant-turn id, mirrored onto the AIUsageRecord so feedback/timing correlate on resume.",
    )
    # Persisted so a RESUMED session restores the same assistant-turn view the live stream showed
    # (all Optional/defaulted — legacy messages without them still parse):
    # the streamed thinking-steps (the "Thought for Ns" summary), the journey-started note
    # (journeyId + journeyAction), and the round-trip timing badge.
    progress: List[dict] = Field(
        default_factory=list,
        description="Streamed step frames for the collapsed 'Thought for Ns' summary on resume.",
    )
    journeyId: Optional[str] = Field(
        default=None, description="The journey this turn started/updated (for the resumed 'journey started' note)."
    )
    journeyAction: Optional[Literal["started", "updated", "paused_previous"]] = Field(
        default=None, description="What happened to journey state this turn (drives the resumed note's wording)."
    )
    roundTripMs: Optional[int] = Field(
        default=None, description="End-to-end turn time so the resumed message keeps its timing badge."
    )
    writeNotice: Optional[str] = Field(
        default=None, description="Write-role advisory (journey the caller can see but not save) — resumed too."
    )


class CopilotSession(BaseModel):
    """A persisted, resumable copilot conversation (stored in COPILOT_SESSIONS).

    Sessions are always scoped to their owner; every read/write filters on
    ``username`` so a user can never load another user's session.
    """

    sessionId: str = Field(default_factory=lambda: str(uuid4()))
    username: str
    title: Optional[str] = None
    pageContext: Optional[PageContext] = None
    messages: List[CopilotMessage] = Field(default_factory=list)
    journey: Optional[CopilotJourney] = None  # the ONE active guided journey
    # Guides displaced by a different new journey — paused (progress kept), resumable from the
    # Guided panel. Capped at the 3 most recent; never silently discarded.
    pausedJourneys: List[CopilotJourney] = Field(default_factory=list)
    createdAt: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updatedAt: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CopilotSessionSummary(BaseModel):
    """Lightweight session row for the session list (no message bodies)."""

    sessionId: str
    title: Optional[str] = None
    pageContext: Optional[PageContext] = None
    createdAt: datetime
    updatedAt: datetime
    messageCount: int = 0


class CopilotTurnRecord(BaseModel):
    """Per-turn outcome record (stored in COPILOT_TURNS) for SSE-stream recovery (plan v5 §8).

    A copilot turn keeps running server-side even if the client's SSE stream drops (the agent
    is a detached task — the disconnect shield). This record captures the turn's status and,
    once finished, its full result, so the UI can recover a dropped turn by polling
    ``GET /copilot/config/sessions/{sessionId}/turns/{messageId}`` instead of losing a turn it
    was already charged for. A ``running`` record whose ``updatedAt`` is stale (process died
    mid-turn) is reported as ``interrupted`` on read.
    """

    messageId: str
    sessionId: str
    username: str
    status: Literal["running", "ok", "error", "interrupted"] = "running"
    result: Optional[CopilotTurnResponse] = None
    error: Optional[str] = None
    createdAt: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updatedAt: datetime = Field(default_factory=lambda: datetime.now(UTC))


class FeedbackRequest(BaseModel):
    """Thumbs-up/down feedback on one assistant turn (``POST /copilot/config/feedback``)."""

    messageId: str = Field(description="The assistant turn's messageId, as returned in the chat result.")
    rating: Literal["up", "down"] = Field(description="Thumbs up or down.")
    comment: Optional[str] = Field(
        default=None,
        max_length=2000,  # bound the persisted free-text (no unbounded user write)
        description="Optional free-text or a tiered down-vote reason.",
    )


class FeedbackResponse(BaseModel):
    """Result of a feedback upsert."""

    success: bool = True
