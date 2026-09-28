"""Models for platform log triage (posture assessment)."""

import re
from datetime import UTC, datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

from netskope.common.models import LogType
from netskope.common.models.ai_copilot.analyze import Citation


class Posture(str, Enum):
    """Overall posture verdict — a str-Enum so it serializes to its plain string value.

    Self-coercing: the model occasionally emits a malformed value (wrong case, a stray internal
    space like 'De graded', or extra words like 'Posture: critical'). ``_missing_`` maps any such
    raw value to the right member by matching a canonical token as a WHOLE WORD (case-insensitive)
    — so a token appearing only as a substring of another word does NOT match (critically,
    'unhealthy' must NOT become HEALTHY, it's the opposite). CRITICAL is checked before DEGRADED so
    the more severe verdict wins when both appear; anything unrecognizable falls back to DEGRADED (a
    neutral "look at this" default, never a false 'healthy'/'critical'). This replaces the old
    _normalize_posture field validator — the enum owns its own coercion now.
    """

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"

    @classmethod
    def _missing_(cls, value):
        if not isinstance(value, str):
            return cls.DEGRADED
        lowered = value.lower()
        for member in (cls.CRITICAL, cls.DEGRADED, cls.HEALTHY):
            if re.search(rf"\b{member.value}\b", lowered):
                return member
        # Whole-word match can't catch an internal-space typo ('De graded'); squash whitespace
        # and retry exact membership before giving up.
        squashed = "".join(lowered.split())
        for member in (cls.HEALTHY, cls.DEGRADED, cls.CRITICAL):
            if squashed == member.value:
                return member
        return cls.DEGRADED


class TriageRequest(BaseModel):
    """Incoming request for log triage / posture assessment."""

    filters: Optional[dict] = Field(
        None,
        description="MongoDB filter dict in the same format accepted by GET /logs/. Pass null to analyze all logs.",
    )


class TimelineEvent(BaseModel):
    """A synthesized moment in the incident timeline."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    severity: LogType = Field(description="The Severity of the event observed")
    description: str = Field(
        description=(
            "LLM-synthesized narrative for this moment, "
            "correlate with other events around this time to craft an engaging description."
        )
    )
    logCount: int


class ActionItem(BaseModel):
    """A single prioritized remediation action."""

    priority: int = Field(ge=1, le=5, description="1 = highest priority")
    title: str = Field(description="Easy to understand title for user about the action item.")
    steps: List[str] = Field(
        min_length=1,
        description="Steps which user can take keeping in mind the services are deployed on docker, "
        "with user not having access to internal code, Steps should not involve any type of suggestions "
        "which requires code changes. The steps be actionable rather than only descriptive, for example, "
        "If the steps points to wards the raising the support ticket then it should also mention "
        "from where user can raise it. Can only be empty when there are no action items!",
    )
    estimatedImpact: str = Field(description="e.g. 'Resolves 47 AUTH_FAIL errors'")
    affectedLogCount: int = Field(
        description="Total logs which points towards the requirement of the suggested action."
    )


class HealthCategory(Enum):
    """CE Platform Logs analysis Categories."""

    DATA_INGESTION_HEALTH = "Data ingestion health"
    NETSKOPE_TENANT_HEALTH = "Netskope tenant health"
    SYSTEM_HEALTH = "System health"
    CONTAINER_HEALTH = "Container health"
    PLATFORM_HEALTH = "Platform health"


HEALTH_CATEGORY_DESCRIPTIONS: dict[HealthCategory, str] = {
    HealthCategory.DATA_INGESTION_HEALTH:
        "Logs/events flowing from configured plugins into or out of CE — pull/poll/push success, "
        "ingestion lag, empty or dropped batches, skipped pulls/pushes, errored pulls/pushes, "
        "mapping/transform failures.",
    HealthCategory.NETSKOPE_TENANT_HEALTH:
        "Connection to the Netskope tenant — auth/token validity, API reachability, "
        "rate limiting, and tenant-side configuration issues. Do not include analytics sharing information."
        "Also skip the management token generation info, since management is internal to CE.",
    HealthCategory.SYSTEM_HEALTH:
        "Host/runtime resource health — CPU, memory, disk, and OS-level errors "
        "affecting the CE deployment.",
    HealthCategory.CONTAINER_HEALTH:
        "Per-container/service health — restarts, crashes, OOM kills, unhealthy or "
        "stopped containers, and Mongo/RabbitMQ connectivity.",
    HealthCategory.PLATFORM_HEALTH:
        "Overall CE core/platform health not covered above — core engine, "
        "scheduler/Celery tasks, migrations, git interactions, plugin updates and cross-cutting errors.",
}

# Rendered into CategoryScore.category's description so the model receives each
# category's scope — plain Enum values carry no per-value description into the
# structured-output schema.
_CATEGORY_GUIDANCE = "\n".join(
    f"- {category.value}: {description}"
    for category, description in HEALTH_CATEGORY_DESCRIPTIONS.items()
)


class CategoryScore(BaseModel):
    """Health score for one CE platform category.

    Modelled as a list item (category embedded) rather than a dict keyed by
    HealthCategory: Anthropic's strict structured output cannot express an
    open enum-keyed map — `transform_schema` collapses Dict[Enum, Model] to an
    object with empty `properties` + `additionalProperties: false`, whose only
    valid value is `{}`, so the model could never emit any scores. A list of
    objects transforms to a normal array schema the model fills reliably.
    """

    category: HealthCategory = Field(
        description=(
            "The platform health category being scored. Score each using this scope:\n"
            + _CATEGORY_GUIDANCE
        )
    )
    score: int = Field(description="Health score of the category.", ge=10, le=100)
    details: str = Field(
        description="Relevant details about the assigned score, and reasons and brief information around it."
    )


class TriageResponse(BaseModel):
    """Consolidated posture-assessment response from the triage agent."""

    # Option 2 — scorecard. Posture is a self-coercing str-Enum (see Posture._missing_): a
    # malformed model value maps to the right member instead of raising, and it serializes to its
    # plain string value ('healthy'/'degraded'/'critical'), so the SSE/report wire shape is
    # unchanged. Enum default 'critical'-before-'degraded' keeps the more severe verdict on a tie.
    overallPosture: Posture = Field(description="'healthy', 'degraded', or 'critical'")
    summary: str = Field(
        description=(
            "<Must be available in response>Thorough Platform log AI analysis in Markdown format. "
            "Focus on what happened, why, and the impact — do NOT include operational metadata "
            "such as total logs analyzed, tool calls used, token counts;"
            "those are tracked and displayed separately by the UI. do not include words like executive. "
            "Write COMPLETE prose — never elide with '...'/'…' or trail off; the UI and the downloaded "
            "report show this verbatim, so an abbreviated summary reads as broken."
        )
    )
    categoryScores: List[CategoryScore] = Field(
        description="<Must be available in response>. One entry per platform health "
        "category, each with its score and rationale — cover every relevant category. "
        "LLM-identified health information about the Cloud Exchange platform.",
    )

    # Option 1 — incident timeline
    timeline: List[TimelineEvent] = Field(default_factory=list)

    @field_validator("timeline")
    @classmethod
    def sort_timeline_descending(cls, v: List[TimelineEvent]) -> List[TimelineEvent]:
        """Sort the events according to timestamp.

        Args:
            v (List[TimelineEvent]): timeline list

        Returns:
            List[TimelineEvent]: sorted timeline list in desc order.
        """
        return sorted(v, key=lambda x: x.timestamp, reverse=True)

    # Option 3 — action board
    actionItems: List[ActionItem] = Field(
        default_factory=list,
        description="LLM-suggested prioritized remediation actions which are most relevant to the identified issues"
        ", with clear and easy to understand steps for the user to take."
        "<Important Notes>"
        "Action Items: "
        "1. If the action item mentions about contacting support team, "
        "mention to attach the diagnose file generated by visiting Settings > General > Run Diagnose, "
        "this would help the Support team with initial discovery."
        "2. Action items should never mention the exact Shell or terminal commands, we don't want the users to break "
        "their already working CE deployment."
        "3. Since the CE source code is dockerize, you must never suggest any code changes for any of the scenarios, "
        "if need be you can only redirect user to valid citation link only if available."
        "</Important Notes> ",
    )

    citations: List[Citation] = Field(
        default_factory=list,
        description="Documentation sources grounding this assessment. Do NOT inline "
        "these URLs in the summary or action-item prose — surface them only here.",
    )

    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description="<Must be available in response>This should never be 0, "
        "since this indicates how confident are you with the analysis of the observed data. "
        "if its 0, then it implies you have not done your due-diligence.",
    )
    logsAnalyzed: int = Field(default=0, description="Total logs fetched by the agent")
    toolCallsUsed: int = Field(default=0, description="Number of tool-call iterations used")
    inputTokens: int = Field(default=0, description="Total input tokens consumed across all LLM calls")
    outputTokens: int = Field(default=0, description="Total output tokens consumed across all LLM calls")
