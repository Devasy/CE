"""Models for AI feature usage telemetry."""

from datetime import datetime, UTC
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field


class AIFeature(str, Enum):
    """Supported AI features for telemetry grouping."""

    POSTURE_ASSESSMENT = "posture_assessment"
    COPILOT = "copilot"
    CONFIGURATION_COPILOT = "configuration_copilot"
    CRE_AUTO_MAPPER = "cre_auto_mapper"


class AIProvider(str, Enum):
    """Known LLM provider types, extracted from plugin module paths."""

    ANTHROPIC = "anthropic"
    CUSTOM = "custom"


class AIUsageStatus(str, Enum):
    """Outcome of an AI invocation."""

    SUCCESS = "success"
    ERROR = "error"


class FeedbackInfo(BaseModel):
    """Per-message thumbs feedback, attached to a usage record after the turn.

    Defaults keep the parent record valid when no feedback was given. The
    Configuration Copilot's ``POST /copilot/config/feedback`` endpoint upserts
    this subdocument by ``messageId``; it never touches token/timing fields.
    """

    rating: Optional[Literal["up", "down"]] = None
    comment: Optional[str] = None
    submittedAt: Optional[datetime] = None


class AIUsageRecord(BaseModel):
    """Single AI invocation record stored in ai_usage_metrics collection.

    The ``messageId``/timing/``feedback`` fields are additive and default to
    ``None`` so existing records (and the usage-metrics ``$facet`` aggregations
    that ignore them) keep working unchanged. ``durationMs`` is model time;
    ``roundTripMs`` is end-to-end request→response time (a superset).
    """

    username: str
    feature: AIFeature
    provider: AIProvider
    providerConfig: str
    model: Optional[str] = None
    inputTokens: int = 0
    outputTokens: int = 0
    totalTokens: int = 0
    durationMs: Optional[int] = None
    status: AIUsageStatus = AIUsageStatus.SUCCESS
    errorType: Optional[str] = None
    metadata: dict = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    # Additive telemetry for the Configuration Copilot (all optional/back-compat).
    messageId: Optional[str] = Field(default=None, description="Stable id of the assistant turn this record backs.")
    requestSentAt: Optional[datetime] = None
    responseReceivedAt: Optional[datetime] = None
    roundTripMs: Optional[int] = Field(default=None, description="End-to-end request→response ms (>= durationMs).")
    feedback: Optional[FeedbackInfo] = None
