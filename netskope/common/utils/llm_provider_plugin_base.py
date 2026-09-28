"""Provides base classes for LLM provider plugins."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from netskope.common.utils import PluginBase as CommonPluginBase
from netskope.common.utils.provider_plugin_base import ValidationResult


class StopReasonKind(str, Enum):
    """Provider-NEUTRAL classification of a turn's stop reason.

    The gateway reasons in these kinds; each provider plugin maps its own raw
    ``stop_reason`` wire values to them in ``classify_stop_reason`` (the same
    provider-vocabulary-lives-in-the-plugin pattern as ``classify_error``). Kept
    deliberately small — only the reasons the gateway reacts to:

    - ``TRUNCATED`` — the response was cut off because the per-response token
      budget was exhausted (Anthropic ``max_tokens``, OpenAI ``length``, …).
    - ``REFUSAL`` — the model declined on policy grounds (Anthropic ``refusal``,
      OpenAI ``content_filter``, …).
    - ``CONTEXT_EXCEEDED`` — the input + output overflowed the context window
      (Anthropic ``model_context_window_exceeded``, …).

    Any other stop reason (``end_turn``/``stop``/``tool_use``/…) maps to None —
    a normal turn the gateway needs no special reaction for.
    """

    TRUNCATED = "truncated"
    REFUSAL = "refusal"
    CONTEXT_EXCEEDED = "context_exceeded"


class LLMErrorType(str, Enum):
    """Canonical error types returned by PluginBase.classify_error.

    Extends str so values work as dict keys and compare equal to their string
    equivalents — LLMErrorType.RATE_LIMIT == "rate_limit" is True.
    """

    PARSE_ERROR = "parse_error"
    RATE_LIMIT = "rate_limit"
    AUTH_ERROR = "auth_error"
    DEPRECATED_MODEL = "deprecated_model"
    CONTEXT_LIMIT_EXCEEDED = "context_limit_exceeded"
    TIMEOUT = "timeout"
    SERVER_ERROR = "server_error"
    ITERATION_LIMIT = "iteration_limit"  # agent exceeded its reasoning-step budget
    # The provider rejected OUR request shape: structured-output/tool-schema grammar
    # compilation failed ("compiled grammar too large" / "grammar compilation timed
    # out"). Deterministic and an ENGINEERING bug (too many/too-heavy schemas), not a
    # user error — telemetry should alarm on it. Previously fell through to
    # CONTEXT_LIMIT_EXCEEDED, hiding the real cause.
    SCHEMA_COMPILATION = "schema_compilation"

    @property
    def is_retryable(self) -> bool:
        """Whether this error class is worth retrying.

        Transient/server-side errors (rate limit, timeout, server error — including Anthropic's 529
        overloaded_error) are retryable; auth, parse, deprecated-model,
        context-limit and iteration-limit are deterministic and are not.

        Retryability is intrinsic to the classified type, so the agent layer
        derives it straight from classify_error: ``classify_error(exc).is_retryable``.
        """
        return self in _RETRYABLE_ERROR_TYPES


@dataclass(frozen=True)
class LLMErrorResponse:
    """HTTP response shape paired with a classified LLM error."""

    http_status: int
    message: str


class LLMProviderError(Exception):
    """A provider/model-originated failure (rate limit, auth, timeout, parse, etc.).

    Raised by the LLM invocation gateway instead of a transport-specific exception,
    so the gateway stays decoupled from the web framework. The API layer translates
    it: unary endpoints via a global FastAPI exception handler, and the triage
    stream by surfacing ``message`` in an SSE error event.

    Attributes:
        error_type: the classified LLMErrorType (also recorded in usage metrics).
        http_status: the HTTP status the API should return for this failure.
        message: the user-facing message.
    """

    def __init__(self, error_type: LLMErrorType, http_status: int, message: str):
        """Exception type to be raised for all the llm provider related exceptions.

        Args:
            error_type (LLMErrorType): any of the LLMErrorType.
            http_status (int): status code.
            message (str): message to transmit.
        """
        self.error_type = error_type
        self.http_status = http_status
        self.message = message
        super().__init__(message)


# Built once at import time; not re-created on every classify_error call.
_HTTP_STATUS_MAP: dict[int, LLMErrorType] = {
    400: LLMErrorType.CONTEXT_LIMIT_EXCEEDED,
    401: LLMErrorType.AUTH_ERROR,
    403: LLMErrorType.AUTH_ERROR,
    404: LLMErrorType.DEPRECATED_MODEL,
    422: LLMErrorType.CONTEXT_LIMIT_EXCEEDED,
    429: LLMErrorType.RATE_LIMIT,
    500: LLMErrorType.SERVER_ERROR,
    502: LLMErrorType.SERVER_ERROR,
    503: LLMErrorType.SERVER_ERROR,
    504: LLMErrorType.TIMEOUT,
    529: LLMErrorType.SERVER_ERROR,  # Anthropic overloaded_error
}

# Error classes worth retrying a model/LLM call for — transient or server-side.
# Auth, parse, deprecated-model and context-limit errors are deterministic: a
# retry would fail identically, so they are excluded.
_RETRYABLE_ERROR_TYPES: frozenset[LLMErrorType] = frozenset(
    {
        LLMErrorType.RATE_LIMIT,
        LLMErrorType.TIMEOUT,
        LLMErrorType.SERVER_ERROR,  # includes Anthropic's 529 overloaded_error
    }
)


class PluginBase(CommonPluginBase):
    """Base contract for LLM provider plugins."""

    integration = "llm_provider"

    def validate(self, configuration: dict) -> ValidationResult:
        """Validate provider configuration."""
        raise NotImplementedError()

    def get_runnable(self, runtime_config: Optional[dict] = None):
        """Return a runnable model instance implementing invoke/ainvoke."""
        raise NotImplementedError()

    def cleanup(self, configuration: Optional[dict] = None) -> None:
        """Cleanup plugin dependencies on configuration deletion."""
        raise NotImplementedError()

    def get_web_search_tool(self):
        """Return a LangChain BaseTool for web search enrichment, or None if unsupported.

        Override in your plugin to enable web-enriched triage analysis.
        The tool should query docs.netskope.com and the open web.
        Returning None (default) skips web enrichment — backward compatible.
        """
        return None

    def get_citation_allowed_domains(self) -> list:
        """Return extra hostname a citation URL may live on, beyond Core's base allowlist.

        SECURITY NOTE: a returned host is trusted for any URL on it, including a redirector that can
        302 anywhere — so return ONLY hosts your provider controls and only when the alternative is
        losing genuine grounded citations. Keep it minimal and provider-specific.
        """
        return []

    def get_agent_model_settings(self) -> dict:
        """Return provider-specific model_settings the AGENT must apply at bind time, or {}.

        The gateway runs turns through ``langchain.agents.create_agent``, which binds the model via
        ``model.bind_tools(final_tools, **{**kwargs, **request.model_settings})`` — so any bind-level
        config (e.g. Gemini's ``tool_config``) MUST be threaded through ``request.model_settings`` to
        survive; a config bound in ``get_runnable`` is discarded when create_agent rebinds. The gateway
        applies this dict onto ``request.model_settings`` via middleware. Keys are the model's
        ``bind_tools`` kwargs (e.g. ``{"tool_config": {...}}``). Keep it pure/side-effect-free.

        Default returns ``{}`` (no extra settings — backward compatible). Override only when the
        provider needs a bind-level runtime config the tool list alone can't carry.
        """
        return {}

    def extract_token_usage(self, message) -> dict:
        """Extract normalised token counts from a LangChain response MESSAGE.

        Receives the AI message object (not a raw ``llm_output`` dict): LangChain 1.x carries
        token counts on ``message.usage_metadata`` — the canonical, provider-agnostic
        ``UsageMetadata`` (``input_tokens`` / ``output_tokens`` / ``total_tokens`` +
        ``output_token_details.reasoning``) — for every provider integration. Provider-specific
        raw counts, if a plugin wants them, live in ``message.response_metadata`` (a dict).

        The default reads ``usage_metadata`` and covers every provider. Override in your plugin
        ONLY to pull a provider-specific breakdown from ``response_metadata``. Returns a dict with
        at least ``input_tokens`` / ``output_tokens`` (ints); extra numeric keys (e.g.
        ``thinking_tokens``) are persisted generically by the gateway.
        """
        um = getattr(message, "usage_metadata", None) or {}
        usage = {
            "input_tokens": um.get("input_tokens") or 0,
            "output_tokens": um.get("output_tokens") or 0,
        }
        reasoning = (um.get("output_token_details") or {}).get("reasoning")
        if reasoning:
            usage["thinking_tokens"] = reasoning
        return usage

    def classify_error(self, exc: Exception) -> LLMErrorType:
        """Classify an LLM invocation error into an LLMErrorType.

        Called by invoke_with_tracking to decide what HTTP response to return
        to the caller.

        The default implementation works for any HTTP-based LLM SDK that sets
        .status_code on API errors (Anthropic, OpenAI, etc.) and raises
        langchain_core.exceptions.OutputParserException on parse failures.
        Override in your plugin for provider-specific exception types or
        non-standard status codes.
        """
        from langchain_core.exceptions import OutputParserException

        # Parse failures are non-retryable: the same prompt against the same
        # schema will fail identically. OutputParserException has no .status_code.
        if isinstance(exc, OutputParserException):
            return LLMErrorType.PARSE_ERROR

        # HTTP-based SDKs (Anthropic, OpenAI) set .status_code on API errors. Google's
        # google.genai.errors.APIError instead exposes the int HTTP status as `.code`
        # (not `.status_code`), so that is checked too — but only trusted as an HTTP
        # status when it is an int in the valid HTTP range: some SDKs overload `.code`
        # for a string error enum, which must NOT be mistaken for a status code.
        status_code = (
            getattr(exc, "status_code", None)
            or getattr(exc, "code", None)
            or getattr(getattr(exc, "response", None), "status_code", None)
        )
        if not isinstance(status_code, int) or not (100 <= status_code <= 599):
            status_code = None
        if status_code is not None:
            return _HTTP_STATUS_MAP.get(status_code, LLMErrorType.SERVER_ERROR)

        # No status code — connection/timeout errors (e.g. anthropic.APIConnectionError,
        # APITimeoutError). Classify by exception class name as a last resort.
        cls_name = type(exc).__name__.lower()
        if "ratelimit" in cls_name:
            return LLMErrorType.RATE_LIMIT
        if "authentication" in cls_name or "unauthorized" in cls_name:
            return LLMErrorType.AUTH_ERROR
        if "timeout" in cls_name or "connection" in cls_name:
            return LLMErrorType.TIMEOUT
        return LLMErrorType.SERVER_ERROR

    def extract_citations(self, message) -> list:
        """Return Core Citations from a LangChain message's standard `Citation` annotations.

        LangChain 1.x normalizes every provider's web-search sources into
        `langchain_core.messages.Citation` blocks (url+title) on the message content blocks —
        identical across langchain-anthropic/-openai/-google-genai. Reading them here means Core
        gets citations from ANY provider with no per-plugin code. Domain allowlisting is applied
        downstream by normalize_citations; this only extracts. Never raises — returns [] on any shape.
        """
        from netskope.common.models.ai_copilot.analyze import Citation

        out = []
        try:
            blocks = getattr(message, "content_blocks", None)
            if not isinstance(blocks, list):
                return out
            seen = set()
            for block in blocks:
                if not isinstance(block, dict):
                    continue
                for ann in block.get("annotations") or []:
                    if not isinstance(ann, dict) or ann.get("type") != "citation":
                        continue
                    url = ann.get("url")
                    if not url or url in seen:
                        continue
                    seen.add(url)
                    out.append(Citation(title=ann.get("title") or url, url=url))
        except Exception:
            return []
        return out

    def classify_stop_reason(self, stop_reason: Optional[str]) -> Optional[StopReasonKind]:
        """Map a provider ``stop_reason`` wire value to a neutral ``StopReasonKind``.

        The gateway calls this when a turn produced no structured response, to decide
        whether the turn was truncated / refused / context-exceeded (and log an
        actionable message + tag telemetry) versus an ordinary short turn. Same
        provider-vocabulary boundary as ``classify_error``: the raw strings
        (``max_tokens``, ``refusal``, …) are Anthropic/OpenAI-specific and belong in the
        plugin, never in the gateway.

        The default returns None for everything (a provider-neutral base can't know any
        provider's stop-reason strings). Override in the plugin. Returning None means "no
        special reaction" — the gateway falls back to its generic degrade path.
        """
        return None
