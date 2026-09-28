"""Central LLM invocation gateway with token tracking, error handling, and retries."""

import asyncio
import os
import time
import traceback
from datetime import datetime, UTC
from typing import Any, Awaitable, Callable, Optional, Type

from langchain.agents import create_agent
from langchain.agents.middleware import ModelRetryMiddleware
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.exceptions import OutputParserException  # fallback gate in structured-output path
from langchain_core.messages import convert_to_messages
from langchain_core.outputs import LLMResult
from pydantic import ValidationError  # structured-output tool-call schema-validation failures
from langgraph.errors import GraphRecursionError
from fastapi import HTTPException

from netskope.common.models.ai_copilot.ai_usage import (
    AIFeature,
    AIProvider,
    AIUsageRecord,
    AIUsageStatus,
)
from netskope.common.utils.db_connector import Collections, DBConnector
from netskope.common.utils.llm_provider_plugin_base import (
    LLMErrorResponse,
    LLMErrorType,
    LLMProviderError,
    StopReasonKind,
)
from netskope.common.utils.langfuse_tracing import trace_config
from netskope.common.utils.logger import PrefixedLogger
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.common.utils.secrets_manager import SecretDict

connector = DBConnector()
logger = PrefixedLogger("[AI-COPILOT]")
# resolve_active_llm_plugin uses this. It is NOT constructed at module top: this module is imported
# during ``netskope.common.utils.__init__`` bootstrap (unlike config_tools, which loads later), so a
# top-level ``PluginHelper()`` re-enters the still-initializing utils package and NameErrors on
# ``PluginHelper``. Lazily initialized on first use instead (``_get_plugin_helper``); tests patch
# this attribute directly, so the lazy init only fires when it is still None.
plugin_helper = None


def _get_plugin_helper():
    """Return the module's PluginHelper singleton, constructing it lazily on first use."""
    global plugin_helper
    if plugin_helper is None:
        plugin_helper = PluginHelper()
    return plugin_helper


# Gateway v2 (plan v5 Phase 1) is opt-in per deployment: AI_COPILOT_GATEWAY=v2 upgrades the
# middleware stack (fewer, visible retries + tool retry + tool-output context editing) while
# the event loop stays on the production-proven async astream_events(v2). v1 remains the
# default until v2 soaks; flipping back is an env change, no deploy.
_GATEWAY_V2 = os.getenv("AI_COPILOT_GATEWAY", "").lower() == "v2"

# Shared reasoning-step budget for every AI Copilot agent (the Configuration Copilot turn agent
# and its log_analyzer sub-agent, the single-log Analyze agent, and the Posture Assessment/Triage
# agent) — one env knob, same cap everywhere, no deploy needed to retune it.
_DEFAULT_TURN_MAX_ITERATIONS = 30


def _configured_turn_max_iterations() -> int:
    """Resolve AI_COPILOT_TURN_MAX_ITERATIONS, tolerating unset/empty/non-numeric/non-positive values.

    This module is imported during utils bootstrap, so a bad env value must not raise at import
    time and take down the whole AI Copilot gateway — fall back to the default instead (same
    guard pattern as attention_scan._configured_minutes).
    """
    try:
        return max(1, int(os.getenv("AI_COPILOT_TURN_MAX_ITERATIONS", str(_DEFAULT_TURN_MAX_ITERATIONS))))
    except ValueError:
        return _DEFAULT_TURN_MAX_ITERATIONS


AI_COPILOT_TURN_MAX_ITERATIONS = _configured_turn_max_iterations()

# Tools that ground an answer in CE's OWN curated knowledge packs, as opposed to web_search.
# Tracked separately because KB grounding produces NO citations — an answer grounded purely in
# the knowledge packs is indistinguishable from an ungrounded one if you only count citations.
# NOT a member: get_ce_docs_keywords, which returns CE vocabulary to aim a web_search and is
# therefore web-side, not a grounding source of its own.
_KB_GROUNDING_TOOLS = frozenset({"get_ce_knowledge"})


def _model_settings_middleware(plugin):
    """Middleware that applies a plugin's get_agent_model_settings() onto every model call.

    create_agent binds the model via ``model.bind_tools(final_tools, **{**kwargs,
    **request.model_settings})`` — so bind-level runtime config a provider needs (e.g. Gemini's
    ``tool_config={"include_server_side_tool_invocations": True}``, required to mix the built-in
    google_search tool with function tools) MUST land in ``request.model_settings``; anything a plugin
    pre-binds in get_runnable is discarded by that rebind. This reads the plugin's declared settings
    (provider vocabulary stays in the plugin) and merges them into model_settings before the call.
    Both sync + async hooks are implemented since the gateway drives via astream_events. No-op when
    the plugin declares no settings (the base default {}), so it's safe on the shared stack.
    """
    from langchain.agents.middleware.types import AgentMiddleware

    try:
        settings = plugin.get_agent_model_settings() or {}
    except Exception:
        logger.warn("plugin.get_agent_model_settings failed; proceeding without extra model settings.")
        settings = {}

    class _ModelSettingsMiddleware(AgentMiddleware):
        def _apply(self, request):
            # request.override(...) returns a NEW request (direct attribute assignment to
            # model_settings is deprecated); merge our settings over any already present.
            if settings:
                return request.override(model_settings={**(request.model_settings or {}), **settings})
            return request

        def wrap_model_call(self, request, handler):
            return handler(self._apply(request))

        async def awrap_model_call(self, request, handler):
            return await handler(self._apply(request))

    return _ModelSettingsMiddleware()


def _build_agent_middleware(plugin):
    """Middleware stack for create_agent — v1 (legacy) or v2 per the gateway flag.

    v2 changes vs v1: retries drop 4→3 with the middleware as the single retry owner
    (the SDK's own retries are a transport safety net; see plan v5 §4), transient tool
    failures get 2 retries (web/Mongo blips), and older tool outputs are cleared from
    context once they stop being referenced (keep the 3 most recent) so long tool loops
    don't blow the context window.

    The plugin model-settings middleware is prepended on BOTH stacks (it's a no-op unless the
    plugin declares settings) so provider bind-level config (e.g. Gemini tool_config) is applied
    regardless of the gateway flag.
    """
    settings_mw = _model_settings_middleware(plugin)
    retry = ModelRetryMiddleware(
        max_retries=3 if _GATEWAY_V2 else 4,
        retry_on=lambda exc: plugin.classify_error(exc).is_retryable,
        on_failure="error",
        initial_delay=1.0,
        backoff_factor=2.0,
        max_delay=20.0,
        jitter=True,
    )
    if not _GATEWAY_V2:
        return [settings_mw, retry]
    from langchain.agents.middleware import ContextEditingMiddleware, ToolRetryMiddleware
    from langchain.agents.middleware.context_editing import ClearToolUsesEdit

    return [
        settings_mw,
        ToolRetryMiddleware(max_retries=2, backoff_factor=2.0, initial_delay=1.0, jitter=True),
        retry,
        ContextEditingMiddleware(edits=[ClearToolUsesEdit(keep=3)]),
    ]


# User-facing messages returned to the API caller on failure.
_ERROR_RESPONSES: dict[LLMErrorType, LLMErrorResponse] = {
    LLMErrorType.DEPRECATED_MODEL: LLMErrorResponse(
        400, "Model is deprecated. Update your LLM provider configuration."
    ),
    LLMErrorType.CONTEXT_LIMIT_EXCEEDED: LLMErrorResponse(
        400, "LLM provider rejected the request (context too long or invalid request). Check CE logs for details."
    ),
    # 400 (not 401): the frontend axios interceptor treats 401 as a CE session
    # expiry and redirects to the login page, which is wrong for a provider
    # credential failure. Use 400 so the detail string passes through unchanged.
    LLMErrorType.AUTH_ERROR: LLMErrorResponse(400, "LLM provider authentication failed. Check logs for details."),
    LLMErrorType.RATE_LIMIT: LLMErrorResponse(
        429, "LLM provider rate limit exceeded. Try again later. Check logs for details."
    ),
    LLMErrorType.TIMEOUT: LLMErrorResponse(504, "LLM provider request timed out. Check logs for details."),
    LLMErrorType.SERVER_ERROR: LLMErrorResponse(
        503, "LLM provider returned a server error (may be temporarily overloaded). Check logs for details."
    ),
    LLMErrorType.PARSE_ERROR: LLMErrorResponse(
        400, "LLM response could not be parsed. Check your prompt or schema configuration. Check logs for details."
    ),
    # Engineering alarm, not a user error: our tool/response schemas overflowed the
    # provider's constrained-decoding grammar. Non-retryable; fix is to shrink the
    # schema surface (see plan v5 topology notes).
    LLMErrorType.SCHEMA_COMPILATION: LLMErrorResponse(
        422, "The assistant's request schema was rejected by the LLM provider (grammar compilation). "
        "This is an internal configuration issue — please report it; check CE logs for details."
    ),
}


def plugin_id_to_provider(plugin_id: str) -> AIProvider:
    """Map a fully-qualified plugin module path to an AIProvider enum value.

    Example: "netskope.plugins.Default.anthropic.main" → AIProvider.ANTHROPIC
    """
    segment = (plugin_id.split(".")[-2].lower() if plugin_id else "").split("_")[
        0
    ]  # splits <provider>_llm to -> provider.
    try:
        return AIProvider(segment)
    except ValueError:
        return AIProvider.CUSTOM


def get_active_llm_provider(log=None) -> dict:
    """Return the active LLM provider configuration document.

    Args:
        log: PrefixedLogger of the calling subsystem, so the failure is tagged
            with that subsystem's prefix. Defaults to this module's logger.

    Raises:
        HTTPException: 400 when no LLM provider configuration is active.
    """
    log = log or logger
    provider = connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find_one({"active": True})
    if provider is None:
        log.error(
            "No active LLM provider is configured.",
            resolution="Enable an LLM provider under Settings → LLM Provider to use AI features.",
            error_code="CE_1320",
        )
        raise HTTPException(
            status_code=400,
            detail="No active LLM provider is configured. Enable an LLM provider to use this feature.",
        )
    return provider


def resolve_active_llm_plugin(log=None) -> tuple[dict, Any]:
    """Resolve the active LLM provider config and instantiate its plugin.

    Shared by every AI feature (AI Copilot analyze/triage, CRE Auto-Mapper, ...)
    so they all fail the same way with the same error codes.

    Args:
        log: PrefixedLogger of the calling subsystem. It tags the failure logs and
            is handed to the plugin instance, so the plugin's own logs carry the
            calling feature's prefix too. Defaults to this module's logger.

    Returns:
        tuple[dict, Any]: the provider configuration document and a ready-to-use
        provider plugin instance.

    Raises:
        HTTPException: 400 when no provider is active, its plugin cannot be
            loaded, the configured plugin is not an LLM provider integration, or
            the plugin fails to initialize.
    """
    log = log or logger
    provider_doc = get_active_llm_provider(log)
    plugin_id = provider_doc.get("plugin", "")
    helper = _get_plugin_helper()
    plugin_class = helper.find_by_id(plugin_id)
    if plugin_class is None:
        log.error(
            "Configured LLM provider plugin could not be loaded.",
            details=f"Plugin ID: {plugin_id}. The plugin may be missing or corrupt.",
            error_code="CE_1321",
        )
        raise HTTPException(400, "Configured LLM provider plugin could not be loaded.")
    if helper.find_integration_by_id(plugin_id) != "llm_provider":
        log.error(
            "Configured plugin is not a valid LLM provider plugin.",
            details=f"Plugin ID: {plugin_id} resolved to a non-LLM integration type.",
            error_code="CE_1322",
        )
        raise HTTPException(400, "Configured plugin is not a valid LLM provider plugin.")
    try:
        plugin = plugin_class(
            provider_doc["name"],
            SecretDict(provider_doc.get("parameters", {})),
            provider_doc.get("storage", {}),
            None,
            log,
            ssl_validation=provider_doc.get("sslValidation", True),
        )
    except Exception:
        log.error(
            "Error initializing LLM provider plugin",
            details=traceback.format_exc(),
        )
        raise HTTPException(400, "Error initializing LLM provider plugin.")
    return provider_doc, plugin


class TokenUsageCallbackHandler(BaseCallbackHandler):
    """Captures the final AI message from LangChain's on_llm_end hook.

    Captures ``_last_message`` — the response message carrying both ``usage_metadata`` (token
    counts, read by ``plugin.extract_token_usage``) and ``content_blocks``/annotations (read by
    ``plugin.extract_citations``). Both hooks take the message object, so a single capture serves
    both; ``on_llm_end`` fires on the underlying chat-model call even when the runnable used
    ``with_structured_output`` (which otherwise returns only the parsed object, not the message).
    """

    def __init__(self):
        """Init: capture the final response message."""
        super().__init__()
        self._last_message = None

    def on_llm_end(self, response: LLMResult, **kwargs) -> None:
        """Capture the final AI message.

        Args:
            response (LLMResult): Result generated from llm.
        """
        generations = response.generations or []
        if generations and generations[-1]:
            message = getattr(generations[-1][-1], "message", None)
            if message is not None:
                self._last_message = message


def _usage_from_message(message) -> dict:
    """Read token usage from a response message's canonical ``usage_metadata``.

    The LangChain-standard ``UsageMetadata`` (``input_tokens``/``output_tokens`` +
    ``output_token_details.reasoning``) rides on the AI message, NOT the provider
    ``llm_output`` dict. Several integrations populate ONLY the message: e.g.
    langchain-google-genai's ``llm_output`` is just ``{"prompt_feedback": ...}`` / ``{}``
    (no tokens), so ``plugin.extract_token_usage(llm_output)`` returns 0/0 for Gemini.
    This is the provider-agnostic fallback the single-shot path uses when the plugin's
    ``llm_output``-based extraction comes back empty — mirroring the agent path, which
    already reads ``output.usage_metadata`` directly. Returns {} when unavailable.
    """
    um = getattr(message, "usage_metadata", None) or {}
    if not um:
        return {}
    usage = {
        "input_tokens": um.get("input_tokens", 0) or 0,
        "output_tokens": um.get("output_tokens", 0) or 0,
    }
    # Carry any extra numeric usage fields generically (e.g. output_token_details.reasoning →
    # thinking_tokens) so callers persist them without provider-specific code.
    reasoning = (um.get("output_token_details") or {}).get("reasoning")
    if reasoning:
        usage["thinking_tokens"] = reasoning
    return usage


def _plugin_usage(plugin, message) -> dict:
    """Token usage via the plugin hook, with a provider-agnostic message fallback.

    The SINGLE source of truth for all three gateway paths. ``plugin.extract_token_usage`` now
    takes the response MESSAGE (LangChain 1.x carries token counts on ``message.usage_metadata``);
    the plugin reads canonical usage and/or its own ``response_metadata`` breakdown. Try the hook
    FIRST (a plugin's provider-specific extraction always wins), then fall back to the canonical
    ``usage_metadata`` here when the hook returns nothing — so a plugin whose override only reads a
    provider raw block still gets counted, and a broken/throwing hook degrades instead of failing
    the turn. Never raises.
    """
    usage: dict = {}
    try:
        usage = plugin.extract_token_usage(message) or {}
    except Exception:
        logger.warn("plugin.extract_token_usage failed; falling back to message usage_metadata.")
        usage = {}
    if not (usage.get("input_tokens") or usage.get("output_tokens")):
        fallback = _usage_from_message(message)
        if fallback:
            usage = fallback
    return usage


def _add_message_usage(plugin, total_input: int, total_output: int, extra_totals: dict, message) -> tuple:
    """Fold a message's token usage into running input/output scalars + extras.

    Shared by both agent event loops. Resolves usage via the plugin hook (message-based) with the
    canonical ``usage_metadata`` fallback — the same precedence the single-shot path uses. Returns
    the updated ``(total_input, total_output)`` scalars and mutates ``extra_totals`` in place; a
    no-usage message is a no-op.
    """
    usage = _plugin_usage(plugin, message)
    if not usage:
        return total_input, total_output
    total_input += usage.get("input_tokens", 0)
    total_output += usage.get("output_tokens", 0)
    for k, v in usage.items():
        if k not in ("input_tokens", "output_tokens") and isinstance(v, (int, float)):
            extra_totals[k] = extra_totals.get(k, 0) + v
    return total_input, total_output


def _persist_usage(record: AIUsageRecord) -> None:
    """Write a usage record to the DB; never raises so the caller is not blocked."""
    try:
        connector.collection(Collections.AI_USAGE_METRICS).insert_one(record.model_dump())
    except Exception:
        logger.error(
            "Failed to persist AI usage record.",
            details=traceback.format_exc(),
        )


def stamp_usage_metadata(message_id: Optional[str], username: str, fields: dict) -> None:
    """Merge late-known telemetry onto an already-persisted usage record.

    The gateway writes the record as soon as the model call settles, but a few
    telemetry values are only final AFTER the route post-processes the response —
    notably ``citationCount``, which depends on ``normalize_citations()`` promoting
    inline prose URLs into the numbered Sources list. A model that emits no
    ``citations`` but inlines a docs.netskope.com URL still ends up grounded, so
    counting before normalisation would understate grounding.

    Keyed by ``messageId`` + ``username`` — the same pair ``submit_feedback`` upserts
    on — so a caller can only touch its own turn. Feedback-only stubs are excluded:
    they are not turns and carry no telemetry (see ``_EXCLUDE_STUBS`` in the usage
    router). Never upserts, so a missing record is simply a no-op.

    Best-effort and never raises: telemetry must not fail a delivered answer.
    """
    if not message_id or not fields:
        return
    try:
        connector.collection(Collections.AI_USAGE_METRICS).update_one(
            {
                "messageId": message_id,
                "username": username,
                "feedbackStub": {"$ne": True},
            },
            {"$set": {f"metadata.{key}": value for key, value in fields.items()}},
        )
    except Exception:
        logger.error(
            "Failed to stamp AI usage metadata.",
            details=traceback.format_exc(),
        )


async def _invoke_once(
    runnable,
    messages: list,
    cb: "TokenUsageCallbackHandler",
    trace: Optional[dict] = None,
) -> Any:
    """Single LLM call; retries are handled by the model's max_retries setting.

    ``trace`` is an optional ``langfuse_tracing.trace_config`` dict ({} / callbacks+metadata);
    tracing rides the same config as the token callback and is a no-op when disabled.
    """
    config: dict = {"callbacks": [cb]}
    if trace:
        config["callbacks"] = [cb, *trace.get("callbacks", ())]
        config["metadata"] = trace.get("metadata", {})
    return await runnable.ainvoke(messages, config=config)


def _start_label(ev_name: str, tool_input) -> str:
    """Non-empty label for a tool's START row.

    Some tools derive their label from OUTPUT (empty at start); an empty label renders as
    an icon-only row in the Steps panel, so fall back to a generic "Running ..." line.
    Used by BOTH the main-agent and sub-agent event loops — keep them identical.
    """
    return (
        _progress_message(ev_name, tool_input if isinstance(tool_input, dict) else {}, "")
        or f"Running {ev_name.replace('_', ' ')}…"
    )


# Anthropic server-side web_search never fires on_tool_end (it runs inside the API call, not in
# LangGraph's tool node), so both the main loop AND the sub-agent loop must count it from the
# response content blocks. The accepted block-type set + the "web_search" name MUST stay lockstep
# across both, or the two loops under/over-count web calls in the shared per-turn record — so the
# scan lives here, ONCE.
_WEB_SEARCH_BLOCK_TYPES = ("server_tool_use", "tool_use")  # native built-in / standard tool_use


def _block_field(block, key):
    """Read a field from a response content block, dict-or-object.

    A block is a plain dict in the Anthropic response and an object elsewhere, so this hides the
    repeated ``dict? .get() : getattr()`` accessor.
    """
    return block.get(key) if isinstance(block, dict) else getattr(block, key, None)


def _iter_web_search_blocks(output):
    """Yield ``(name, input)`` for each server-side web_search block in a message's content.

    One definition of the web_search detection (block-type set + name) shared by the main and
    sub-agent streaming loops so their toolCallsUsed accounting can't drift. Never raises.
    """
    content = getattr(output, "content", None) or []
    for block in content if isinstance(content, list) else []:
        if _block_field(block, "name") == "web_search" \
                and _block_field(block, "type") in _WEB_SEARCH_BLOCK_TYPES:
            yield "web_search", (_block_field(block, "input") or {})


def _progress_message(tool_name: str, args: dict, result_content: str) -> str:
    """Generate a human-readable progress message for a completed tool call."""
    if tool_name == "count_logs":
        return result_content
    if tool_name == "get_error_summary":
        lines = result_content.splitlines()
        return lines[0] if lines else "Analyzed error distribution"
    if tool_name == "get_logs_in_window":
        start = args.get("start_time", "")
        end = args.get("end_time", "")
        fetched = len([ln for ln in result_content.splitlines() if ln.strip()])
        return f"Fetched {fetched} logs from window {start} – {end}"
    if tool_name == "get_log_details":
        return f"Inspected log details (id: {args.get('log_id', '?')})"
    if tool_name == "web_search":
        return f"Searched: {args.get('query', '')}"
    # Configuration Copilot tools (config_tools.build_config_tool_registry). These fire
    # on_tool_end like any client-side tool; give them readable progress labels.
    _module = args.get("module", "")
    if tool_name == "list_configurations":
        return f"Listed {_module} configurations".strip()
    if tool_name == "get_configuration_details":
        return f"Inspected {_module} configuration: {args.get('name', '')}".strip()
    if tool_name == "list_available_plugins":
        return f"Listed available {_module} plugins".strip()
    if tool_name == "get_plugin_capabilities":
        return f"Read plugin capabilities: {', '.join(args.get('plugin_ids') or [])}"
    if tool_name == "get_plugin_schema":
        return f"Read plugin schema: {args.get('plugin_id', '')}"
    if tool_name == "get_business_rules":
        return f"Read {_module} business rules".strip()
    if tool_name == "get_business_rule_format":
        return f"Looked up {_module} business-rule format".strip()
    if tool_name == "validate_draft":
        return f"Validated draft for {args.get('plugin_id', '')}"
    if tool_name == "get_plugin_walkthrough":
        return f"Mapped {_module} plugin configuration steps".strip()
    if tool_name == "get_plugin_guide":
        return f"Located plugin guide for {args.get('plugin_id', '')}"
    if tool_name == "get_plugin_prefilters":
        return f"Checked {_module} plugin pull pre-filters".strip()
    if tool_name == "get_settings":
        return f"Read {args.get('section', '')} settings".strip()
    if tool_name == "get_dashboard_data":
        return f"Read {args.get('surface', '')} dashboard".strip()
    if tool_name == "get_system_health":
        return "Checked system health"
    if tool_name == "get_plugin_run_status":
        return "Checked plugin run status"
    if tool_name == "analyze_cte_config":
        return "Analyzed Threat Exchange configuration"
    if tool_name == "analyze_cto_config":
        return "Analyzed Ticket Orchestrator configuration"
    if tool_name == "list_users_and_scopes":
        return "Reviewed users and scopes"
    if tool_name == "get_security_scopes_reference":
        return "Looked up the scope reference"
    if tool_name == "get_ce_knowledge":
        return f"Read the CE knowledge pack: {args.get('area', '')}".strip()
    if tool_name == "get_ce_docs_keywords":
        return "Looked up the Cloud Exchange keywords"
    # Per-module inspection tools (CLS/CRE/EDM/CFC leaf reads — config_tools).
    if tool_name == "get_cls_mappings":
        return "Read Log Shipper SIEM mappings"
    if tool_name == "get_cre_entities":
        return "Read Risk Exchange entities"
    if tool_name == "get_cre_actions":
        return "Read Risk Exchange actions"
    if tool_name == "get_unified_mappings":
        return "Read Unified Schema mappings"
    if tool_name == "get_edm_sharing":
        return "Read Exact Data Match sharing"
    if tool_name == "get_edm_hash_status":
        return "Checked Exact Data Match hash status"
    if tool_name == "get_cfc_sharing":
        return "Read Custom File Classification sharing"
    if tool_name == "get_cfc_classifiers":
        return "Read Custom File Classification classifiers"
    # The one sub-agent delegation tool (copilot_agents): ask_log_analyzer.
    if tool_name.startswith("ask_"):
        return f"Consulting the {tool_name[4:].replace('_', ' ')} specialist"
    return f"Called {tool_name}"


async def invoke_with_tracking(
    plugin,
    runnable,
    messages: list,
    *,
    feature: AIFeature,
    username: str,
    provider_config: str,
    provider: AIProvider,
    model: Optional[str] = None,
    response_model: Optional[Type] = None,
    feature_metadata: Optional[dict] = None,
    message_id: Optional[str] = None,
) -> Any:
    """Invoke an LLM runnable.

    Invokes with token tracking, native structured output, and status-code-based
    error classification. Always persists an AIUsageRecord regardless of outcome.

    ``message_id`` stamps the record with this turn's id so the caller can enrich it
    afterwards via ``stamp_usage_metadata`` (analyze routes both paths through the same
    kwargs, so this mirrors ``invoke_agent_with_tracking``).
    Raises LLMProviderError (classified error_type + http_status + message) on
    failure; a structured-output parse failure surfaces as a 400 PARSE_ERROR. The
    API layer translates LLMProviderError into the HTTP/SSE response.
    """
    start_ms = int(time.monotonic() * 1000)
    cb = TokenUsageCallbackHandler()
    # Optional Langfuse trace (opt-in via env; {} when off). Named by feature so single-shot
    # analyze calls are filterable; user for per-admin attribution.
    trace = trace_config(
        trace_name=f"ce-{feature.value.replace('_', '-')}",
        session_id=(feature_metadata or {}).get("sessionId"),
        user_id=username,
        tags=[provider.value, *([model] if model else [])],
    )

    try:
        if response_model is not None and hasattr(runnable, "with_structured_output"):
            try:
                result = await _invoke_once(
                    runnable.with_structured_output(response_model, method="json_schema"),
                    messages,
                    cb,
                    trace,
                )
            except (OutputParserException, ValidationError) as exc:
                # ValidationError arrives when the model's tool call is well-formed JSON
                # but violates a bounded field (e.g. an ActionItem.priority outside its
                # ge/le range) — schema-level, not JSON-parse-level, but the same "no
                # valid structured result for this prompt" outcome as OutputParserException.
                # Re-raise as OutputParserException so classify_error's existing isinstance
                # check routes it to PARSE_ERROR (400) instead of falling through to the
                # generic Exception→SERVER_ERROR (503) branch below.
                logger.error(
                    "Structured output parsing failed for response_model "
                    f"{getattr(response_model, '__name__', response_model)}.",
                    details=traceback.format_exc(),
                )
                if isinstance(exc, ValidationError):
                    raise OutputParserException(str(exc)) from exc
                raise
        else:
            result = await _invoke_once(runnable, messages, cb, trace)

        # A structured-output call can return None without raising (e.g. the
        # function_calling parser's first_tool_only when the model emits no tool
        # call, or an empty/refusal turn). Returning None to the endpoint makes
        # FastAPI reject it against response_model with an opaque 500
        # (ResponseValidationError). Treat it as a parse failure so the handler
        # below classifies it as a clean 400 PARSE_ERROR and records ERROR usage.
        if response_model is not None and result is None:
            raise OutputParserException(
                "Structured output returned no parseable result for "
                f"{getattr(response_model, '__name__', response_model)}."
            )

        usage = _plugin_usage(plugin, cb._last_message)
        input_tokens = usage.get("input_tokens", 0)
        output_tokens = usage.get("output_tokens", 0)
        # Any extra keys (e.g. thinking_tokens from Anthropic, reasoning_tokens from
        # future providers) are persisted generically without provider-specific code here.
        extra_usage = {k: v for k, v in usage.items() if k not in ("input_tokens", "output_tokens")}

        _persist_usage(
            AIUsageRecord(
                username=username,
                feature=feature,
                provider=provider,
                providerConfig=provider_config,
                model=model,
                inputTokens=input_tokens,
                outputTokens=output_tokens,
                totalTokens=input_tokens + output_tokens,
                durationMs=int(time.monotonic() * 1000) - start_ms,
                status=AIUsageStatus.SUCCESS,
                # webSearchEnriched is structurally False here: the single-shot path uses
                # with_structured_output(), which forces tool_choice onto the output schema
                # and therefore blocks web_search. Grounding only happens on the agent path.
                # Recorded explicitly (not omitted) so the analytics rollup can aggregate the
                # field without a missing-value branch.
                metadata={
                    **(feature_metadata or {}),
                    **extra_usage,
                    "webSearchEnriched": False,
                    # No tools at all on this path, so no knowledge lookups either.
                    "kbGrounded": False,
                },
                messageId=message_id,
            )
        )
        # Surface token usage on the structured response when the model exposes
        # the fields (mirrors invoke_agent_with_tracking), so the UI can show it.
        if result is not None and hasattr(result, "inputTokens"):
            result.inputTokens = input_tokens
        if result is not None and hasattr(result, "outputTokens"):
            result.outputTokens = output_tokens
        _merge_extracted_citations(plugin, result, cb._last_message)
        return result

    except LLMProviderError:
        # Already classified — pass through without re-wrapping.
        raise
    except Exception as exc:
        error_type = plugin.classify_error(exc)
        _persist_usage(
            AIUsageRecord(
                username=username,
                feature=feature,
                provider=provider,
                providerConfig=provider_config,
                model=model,
                durationMs=int(time.monotonic() * 1000) - start_ms,
                status=AIUsageStatus.ERROR,
                errorType=error_type,
                metadata=feature_metadata or {},
                messageId=message_id,
            )
        )
        logger.error(
            "LLM invocation failed.",
            details=traceback.format_exc(),
        )
        response = _ERROR_RESPONSES.get(
            error_type,
            LLMErrorResponse(503, "Error occurred while invoking LLM provider."),
        )
        raise LLMProviderError(error_type, response.http_status, response.message)


def _last_ai_text(messages) -> Optional[str]:
    """Extract the final assistant text from a graph output's message list.

    Used to DEGRADE gracefully when a structured-output run ends without emitting the
    structured tool call: ToolStrategy (unlike a provider grammar) does not force the model
    to call the synthetic output tool, so the model can end a turn with a plain-text answer
    and no ``structured_response``. Rather than error, we can surface that text as the answer.
    """
    for m in reversed(messages or []):
        mtype = m.get("type") if isinstance(m, dict) else getattr(m, "type", None)
        content = m.get("content") if isinstance(m, dict) else getattr(m, "content", None)
        if mtype != "ai" or not content:
            continue
        if isinstance(content, str):
            if content.strip():
                return content.strip()
        elif isinstance(content, list):
            parts = [
                (b.get("text") if isinstance(b, dict) else getattr(b, "text", None)) for b in content
            ]
            text = "".join(p for p in parts if p).strip()
            if text:
                return text
    return None


def _last_ai_message(messages):
    """Return the last AI message OBJECT (not just its text) from a graph output's messages.

    Used to pass the final assistant turn — the one carrying ``content_blocks``/annotations —
    to ``plugin.extract_citations`` so LangChain-standard `Citation` annotations are merged
    into the result regardless of provider. Returns None if no AI message is found (e.g. an
    empty/odd message list); callers must treat that as "nothing to extract from".
    """
    for m in reversed(messages or []):
        mtype = m.get("type") if isinstance(m, dict) else getattr(m, "type", None)
        if mtype == "ai":
            return m
    return None


def _last_ai_stop_reason(messages) -> Optional[str]:
    """Extract the provider ``stop_reason`` from the final assistant message.

    Under ``disable_streaming=True`` the model's ``stop_reason`` rides on the last AI
    message's ``response_metadata`` (LangChain) or ``additional_kwargs``. We read it to
    tell WHY a turn produced no structured response — a ``max_tokens`` truncation is
    actionable (raise the budget / lower effort) and must not look like an ordinary
    plain-text turn; ``refusal`` is a policy decline; ``model_context_window_exceeded``
    means the input+output overflowed the window. Returns None when unavailable (older
    cores, streaming, or a provider that doesn't surface it) — callers must treat a
    missing stop_reason as "unknown", never as a specific reason.
    """
    for m in reversed(messages or []):
        mtype = m.get("type") if isinstance(m, dict) else getattr(m, "type", None)
        if mtype != "ai":
            continue
        for attr in ("response_metadata", "additional_kwargs"):
            meta = m.get(attr) if isinstance(m, dict) else getattr(m, attr, None)
            if isinstance(meta, dict) and meta.get("stop_reason"):
                return meta["stop_reason"]
        return None  # found the last AI msg but it carried no stop_reason
    return None


def _merge_extracted_citations(plugin, result: Any, final_message) -> None:
    """Append-only merge of SDK-extracted citations onto ``result.citations``.

    Shared by both gateway functions (invoke_with_tracking / invoke_agent_with_tracking).
    Calls ``plugin.extract_citations(final_message)`` (LangChain-standard `Citation`
    annotations, provider-neutral) and appends only the NEW (url not already present)
    ones to the END of ``result.citations`` — never inserts at the front, reorders, or
    renumbers existing entries. Downstream, journey/insight ``citationRefs`` are 1-based
    indices into the model's original ``citations`` order; reordering would corrupt them.

    Guarded by ``hasattr(result, "citations")`` because some result types (e.g. the
    plain-text degrade) may lack the field. Never raises.

    Also OR-in's ``result.webSearchEnriched`` when at least one extracted citation is
    present (whether newly appended or already known) — standard `Citation` annotations
    are a provider-neutral signal that the model grounded against the web, unlike the
    Anthropic-only web_search content-block detection this flag was originally set from.
    Only ever set to True here; never cleared/lowered, and untouched when there are no
    extracted citations so the existing block-count path still governs those cases.
    """
    if not hasattr(result, "citations") or final_message is None:
        return
    try:
        extracted = plugin.extract_citations(final_message)
        have = {c.url.rstrip("/") for c in result.citations}
        for c in extracted:
            if c.url.rstrip("/") not in have:
                result.citations.append(c)
                have.add(c.url.rstrip("/"))
        if extracted and hasattr(result, "webSearchEnriched"):
            result.webSearchEnriched = True  # standard citations => model web-grounded (provider-agnostic)
    except Exception:
        logger.warn("extract_citations merge failed; continuing without SDK citations.")


async def invoke_agent_with_tracking(
    plugin,
    runnable,
    tools: list,
    messages: list,
    *,
    feature: AIFeature,
    username: str,
    provider_config: str,
    provider: AIProvider,
    model: Optional[str] = None,
    response_model: Optional[Type] = None,
    feature_metadata: Optional[dict] = None,
    max_iterations: int = 5,
    on_progress: Optional[Callable[..., Awaitable[None]]] = None,
    agent_label: Optional[str] = None,
    extra_aggregate: Optional[dict] = None,
    message_id: Optional[str] = None,
    request_sent_at: Optional[datetime] = None,
    fallback_from_text: Optional[Callable[[str], Any]] = None,
    structured_repair: Optional[Callable[[str], Awaitable[Any]]] = None,
) -> Any:
    """Run a multi-turn LangGraph react agent with tool-use, token tracking, and SSE progress.

    Uses langchain.agents.create_agent which natively handles both client-side LangChain
    tools and Anthropic server-side built-ins (e.g. web_search_20260209) without any
    client-side execution loop.

    Token usage is accumulated across all iterations via astream_events and persisted as a
    single AIUsageRecord. Structured output is extracted inside the graph via response_format.

    For the multi-agent Configuration Copilot, this is the SOLE writer of the per-turn usage
    record even when specialists run as sub-agents: their usage is summed in via
    ``extra_aggregate`` (a dict the caller mutates as children complete), so exactly one record
    is persisted per turn with the combined totals.

    Args:
        on_progress: Optional async callback(step, message, meta) invoked per tool/step for SSE
            progress streaming (``meta`` is {kind, agent, tool, status}). Pass None to run silently.
        agent_label: Name of the agent running (e.g. 'supervisor' or a specialist key); stamped
            onto emitted step meta and into the record's ``agentsInvoked``.
        extra_aggregate: Optional {input, output, extra, tool_calls, agents} accumulated from
            sub-agents; folded into the single persisted record + the returned toolCallsUsed.
        message_id / request_sent_at: assistant-turn id + when the request was received; written
            onto the record (with responseReceivedAt/roundTripMs) for feedback + timing.
    """
    start_ms = int(time.monotonic() * 1000)
    total_input = total_output = 0
    extra_totals: dict = {}
    iterations_used = 0
    tool_calls_count = 0
    # Server-side web_search invocations this turn (incl. any inside a sub-agent, folded via
    # extra_aggregate). Counted independently of `on_progress` so grounding telemetry is
    # correct even when step-streaming is off (posture/analyze, AI_COPILOT_STREAM_STEPS=false)
    # — the routes' own web_search flags only exist when they pass a progress callback.
    web_search_calls = 0
    # Knowledge-pack grounding this turn (incl. any inside a sub-agent, folded the same way).
    # The second, CITATION-LESS grounding source — see _KB_GROUNDING_TOOLS.
    kb_tool_calls = 0
    # Depth of the supervisor's currently-executing delegate (ask_*) tools. astream_events
    # surfaces a delegated CHILD specialist's own tool/model events into THIS (parent) stream
    # too — so without a guard every inner step shows up twice (once labelled 'supervisor',
    # once labelled by the child) and the child's tokens/tool-calls get counted twice (here AND
    # via extra_aggregate). While depth>0 we skip those leaked child events; the child's own
    # invoke_subagent_no_record loop emits them with the correct specialist label and its usage
    # is folded back through extra_aggregate. ask_* boundaries themselves are always emitted.
    delegate_depth = 0
    agg = extra_aggregate or {}

    async def _emit(
        step: str, message: str, *, kind: str, tool: Optional[str] = None, status: str = "end", run_id=None
    ) -> None:
        if on_progress:
            await on_progress(
                step, message,
                {"kind": kind, "agent": agent_label, "tool": tool, "status": status, "runId": run_id},
            )

    def _record(status: AIUsageStatus, *, error_type=None, extra_meta=None) -> AIUsageRecord:
        """Build the single per-turn usage record, folding in sub-agent totals + timing."""
        in_tok = total_input + agg.get("input", 0)
        out_tok = total_output + agg.get("output", 0)
        merged_extra = dict(extra_totals)
        for k, v in (agg.get("extra") or {}).items():
            merged_extra[k] = merged_extra.get(k, 0) + v
        # Grounding: True when web_search ran anywhere in the turn, including inside the
        # ask_log_analyzer sub-agent (whose count arrives through extra_aggregate).
        web_used = web_search_calls + agg.get("web_search_calls", 0) > 0
        # Collapsed to a boolean like web: the metrics only ever ask "was this answer
        # grounded", never "how many lookups". The per-turn counters stay in the gateway
        # if volume is ever wanted.
        kb_used = kb_tool_calls + agg.get("kb_tool_calls", 0) > 0
        agents = (agg.get("agents") or []) + ([agent_label] if agent_label else [])
        now = datetime.now(UTC)
        # roundTripMs: prefer the client-supplied request_sent_at (true end-to-end, incl. network)
        # when a caller provides it (the copilot chat does). Callers that DON'T (posture assessment
        # + single-log analyze) still get a latency value — fall back to the server-side elapsed
        # timer (same source as durationMs). Without this fallback their roundTripMs stayed null and
        # the dashboard's Reliability/Latency chart (which filters out null-latency features) showed
        # ONLY the copilot — hiding posture/analyze latency. Every feature now reports latency.
        rtt = (
            int((now - request_sent_at).total_seconds() * 1000)
            if request_sent_at
            else int(time.monotonic() * 1000) - start_ms
        )
        return AIUsageRecord(
            username=username,
            feature=feature,
            provider=provider,
            providerConfig=provider_config,
            model=model,
            inputTokens=in_tok,
            outputTokens=out_tok,
            totalTokens=in_tok + out_tok,
            durationMs=int(time.monotonic() * 1000) - start_ms,
            status=status,
            errorType=error_type,
            metadata={
                **(feature_metadata or {}),
                "iterationsUsed": iterations_used,
                "webSearchEnriched": web_used,
                "kbGrounded": kb_used,
                **({"agentsInvoked": agents} if agents else {}),
                **merged_extra,
                **(extra_meta or {}),
            },
            messageId=message_id,
            requestSentAt=request_sent_at,
            responseReceivedAt=now,
            roundTripMs=rtt,
        )

    # Normalise tuple messages; separate system prompt for create_agent's system_prompt param.
    lc_messages = convert_to_messages(messages)
    system_msgs = [m for m in lc_messages if m.type == "system"]
    input_msgs = [m for m in lc_messages if m.type != "system"]
    system_prompt = "\n\n".join(m.content for m in system_msgs) or None

    try:
        graph = create_agent(
            runnable,
            tools=tools,
            system_prompt=system_prompt,
            response_format=response_model,
            middleware=_build_agent_middleware(plugin),
            debug=os.getenv("AI_COPILOT_AGENT_DEBUG", "").lower() == "true",
        )

        result = None
        fallback_text = None  # last assistant text, for graceful degrade if no structured response
        stop_reason = None  # last AI stop_reason, to explain WHY a turn had no structured response
        final_message = None  # last AI message OBJECT, for plugin.extract_citations

        # Optional Langfuse trace for the whole agent turn (opt-in; {} when off). The
        # LangChain integration captures model/tokens/tool spans; sessionId groups the
        # conversation's turns in the Sessions view, username enables per-admin filtering.
        trace = trace_config(
            trace_name=f"ce-{feature.value.replace('_', '-')}-turn",
            session_id=(feature_metadata or {}).get("sessionId"),
            user_id=username,
            tags=[provider.value, *([model] if model else []), *([agent_label] if agent_label else [])],
        )
        try:
            async for event in graph.astream_events(
                {"messages": input_msgs},
                config={"recursion_limit": (max_iterations * 2 + 1), **trace},
                version="v2",
            ):
                ev_type = event["event"]
                ev_name = event.get("name", "")
                metadata = event.get("metadata", {})
                if ev_type == "on_chat_model_end" and metadata.get("langgraph_node") == "model":
                    if delegate_depth > 0:
                        # A delegated child specialist's model step leaked into this stream.
                        # Its tokens are counted by the child loop and folded back via
                        # extra_aggregate — skip here so they aren't double-counted.
                        continue
                    iterations_used += 1
                    output = event["data"].get("output")
                    if output is not None:
                        total_input, total_output = _add_message_usage(
                            plugin, total_input, total_output, extra_totals, output
                        )
                        # Server-side web_search never fires on_tool_end, so count it from the
                        # content blocks here (shared _iter_web_search_blocks — same scan the
                        # sub-agent loop uses, so counts can't drift). COUNT unconditionally — a
                        # web_search still ran and must land in toolCallsUsed.
                        for block_name, block_input in _iter_web_search_blocks(output):
                            tool_calls_count += 1
                            # Grounding telemetry (analytics L2/C2): the record's
                            # webSearchEnriched is counted HERE, not from on_progress, so it
                            # is correct with step-streaming off. Must be incremented beside
                            # tool_calls_count — the two count the same blocks.
                            web_search_calls += 1
                            if on_progress:
                                await _emit(
                                    block_name,
                                    _progress_message(block_name, block_input, ""),
                                    kind="web",
                                    tool=block_name,
                                    run_id=event.get("run_id"),
                                )

                elif ev_type == "on_tool_start":
                    # Live "step started" so the UI shows an in-flight step + can time it.
                    # No count here — on_tool_end is the single counting point.
                    is_delegate = ev_name.startswith("ask_")
                    if not is_delegate and delegate_depth > 0:
                        continue  # leaked child tool-start; the child loop emits it
                    if is_delegate:
                        delegate_depth += 1
                    if on_progress:
                        tool_input = event["data"].get("input") or {}
                        await _emit(
                            ev_name,
                            _start_label(ev_name, tool_input),
                            kind="tool",
                            tool=ev_name,
                            status="start",
                            run_id=event.get("run_id"),
                        )

                elif ev_type == "on_tool_end":
                    # Count only tools surfaced to the user. get_ce_docs_keywords is an
                    # internal grounding lookup hidden from progress (see the router's
                    # on_progress), so excluding it keeps toolCallsUsed equal to the
                    # number of analysis steps shown in the UI.
                    is_delegate = ev_name.startswith("ask_")
                    if not is_delegate and delegate_depth > 0:
                        continue  # leaked child tool-end; the child loop counts + emits it
                    if is_delegate:
                        delegate_depth = max(0, delegate_depth - 1)
                    if ev_name != "get_ce_docs_keywords":
                        tool_calls_count += 1
                    if ev_name in _KB_GROUNDING_TOOLS:
                        kb_tool_calls += 1
                    if on_progress:
                        tool_input = event["data"].get("input") or {}
                        tool_output = event["data"].get("output", "")
                        output_str = getattr(tool_output, "content", str(tool_output))
                        msg = _progress_message(
                            ev_name,
                            tool_input if isinstance(tool_input, dict) else {},
                            output_str,
                        )
                        await _emit(ev_name, msg, kind="tool", tool=ev_name, status="end", run_id=event.get("run_id"))

                elif ev_type == "on_chain_end" and ev_name == "LangGraph":
                    # A delegated child IS also a LangGraph; its completion leaks here with
                    # no structured_response. Ignore it (depth>0) so it can't clobber the
                    # supervisor's own structured result with None.
                    if delegate_depth > 0:
                        continue
                    output = event["data"].get("output") or {}
                    if isinstance(output, dict):
                        result = output.get("structured_response")
                        # Capture the final AI message OBJECT regardless of whether a
                        # structured response was produced — plugin.extract_citations needs
                        # it either way (content_blocks/annotations ride on the raw message,
                        # not on the parsed structured_response).
                        msgs = output.get("messages")
                        final_message = _last_ai_message(msgs) or final_message
                        if result is None:
                            # No structured tool call this run — remember the plain-text
                            # answer so we can degrade to it instead of erroring (below), and
                            # the model's stop_reason so we can explain WHY (max_tokens
                            # truncation vs refusal vs an ordinary short turn).
                            fallback_text = _last_ai_text(msgs) or fallback_text
                            stop_reason = _last_ai_stop_reason(msgs) or stop_reason

        except GraphRecursionError:
            logger.error(
                message=f"Agent '{agent_label or feature.value}' exceeded the maximum of {max_iterations} iterations.",
                details=traceback.format_exc(),
            )
            _persist_usage(_record(AIUsageStatus.ERROR, error_type=LLMErrorType.ITERATION_LIMIT))
            # Already classified — the outer handler passes LLMProviderError through
            # without re-persisting or re-wrapping. Message is feature-agnostic (shared by
            # the log-triage and config-copilot paths).
            raise LLMProviderError(
                LLMErrorType.ITERATION_LIMIT,
                400,
                f"Analysis exceeded the maximum of {max_iterations} reasoning iterations. "
                "Try narrowing the log filter to reduce the scope, then retry.",
            )
        except asyncio.CancelledError:
            logger.warn(
                f"Agent '{agent_label or feature.value}' cancelled by client disconnect after "
                f"{iterations_used} iteration(s), persisting partial usage.",
                details=traceback.format_exc(),
            )
            _persist_usage(
                _record(AIUsageStatus.ERROR, error_type="client_disconnected", extra_meta={"cancelled": True})
            )
            raise

        # The agent finished but produced no structured response. With ToolStrategy this is a
        # real (if uncommon) outcome — it is NOT grammar-forced, so the model can end a turn
        # with a plain-text answer and never call the synthetic output tool (more likely on
        # long tool-heavy turns). If the caller gave a `fallback_from_text` builder and we have
        # the model's text, DEGRADE to a plain answer (usable, just no structured insights/
        # journey) instead of erroring. Otherwise raise so the outer handler classifies it.
        degraded = None
        stop_meta = None  # folded into the usage record so telemetry shows WHY a turn degraded
        if response_model is not None and result is None:
            # WHY did the structured turn not materialize? Classify the raw provider stop_reason
            # into a NEUTRAL kind via the plugin.getattr-guarded so a third-party plugin predating classify_stop_reason
            # simply yields None (no special reaction). We still run the SAME degrade ladder below
            # regardless — this only adds an actionable log line + a telemetry tag so a
            # truncated/refused turn is diagnosable instead of looking like a generic no-op.
            _classify_sr = getattr(plugin, "classify_stop_reason", None)
            stop_kind = _classify_sr(stop_reason) if callable(_classify_sr) else None
            if stop_kind == StopReasonKind.TRUNCATED:
                stop_meta = {"stopReason": stop_reason, "stopKind": "truncated", "truncated": True}
                # error_code CE_1331 = per-response TOKEN-BUDGET truncation. ACTION FOR THE
                # OPERATOR: raise the per-response output budget by setting the environment variable
                # AI_COPILOT_MAX_TOKENS to a higher value (it takes precedence over the per-model
                # default, clamped to the model ceiling), OR lower the configured effort/thinking
                # level so the answer isn't crowded out by reasoning. Then re-run the turn.
                logger.warn(
                    f"Agent '{agent_label or feature.value}' hit the token budget — the turn was TRUNCATED "
                    "(thinking + answer exceeded the per-response output budget) before it could emit "
                    f"the generated response. Provider stop_reason: {stop_reason!r}. Degrading to the "
                    "partial answer for now.",
                    error_code="CE_1331",
                    resolution=(
                        "The model's per-response output budget was exhausted before it finished. "
                        "Do ONE of the following, then re-run the request: "
                        "1) Raise the per-response output budget — set the environment variable "
                        "AI_COPILOT_MAX_TOKENS to a higher value on the Core container (it overrides the "
                        "per-model default and is clamped to the model's ceiling), then restart Core. "
                        "2) Lower the reasoning/effort level on the LLM Provider configuration "
                        "(Settings > LLM Provider) so the answer isn't crowded out by thinking tokens. "
                        "3) Ask a narrower question so the response is shorter. "
                    ),
                )
            elif stop_kind == StopReasonKind.REFUSAL:
                stop_meta = {"stopReason": stop_reason, "stopKind": "refusal"}
                logger.warn(
                    f"Agent '{agent_label or feature.value}' declined the request (a policy refusal by the "
                    f"model). Provider stop_reason: {stop_reason!r}. Surfacing the model's own refusal text.",
                    error_code="CE_1332",
                    resolution=(
                        "The LLM provider's own safety/policy layer declined this request — this is the "
                        "provider's decision, not a Cloud Exchange error, so no CE-side configuration "
                        "change will fix it. Rephrase the request, or omit the content the provider "
                        "objected to, and try again."
                    ),
                )
            elif stop_kind == StopReasonKind.CONTEXT_EXCEEDED:
                # Belt-and-suspenders: the input+output overflowed the context window. This usually
                # arrives as a BadRequest classified CONTEXT_LIMIT_EXCEEDED, but if the core surfaced
                # it as a stop_reason on an otherwise-200 turn, catch it here too. error_code CE_1333
                # = context-WINDOW overflow (distinct from CE_1331 per-response truncation): the fix is
                # to shorten the conversation/data, NOT to raise AI_COPILOT_MAX_TOKENS.
                stop_meta = {"stopReason": stop_reason, "stopKind": "context_exceeded", "truncated": True}
                logger.warn(
                    f"Agent '{agent_label or feature.value}' hit the context-window limit — the input plus "
                    f"output overflowed the model's context window. Provider stop_reason: {stop_reason!r}.",
                    error_code="CE_1333",
                    resolution=(
                        "The conversation plus the current turn's data exceeded the model's total "
                        "context window. Do ONE of the following, then "
                        "retry: 1) Start a new chat to drop the accumulated history. 2) Narrow the "
                        "question so fewer/smaller tool results are pulled in (e.g. a tighter log "
                        "filter or a single configuration). 3) Configure a model with a larger context "
                        "window on the LLM Provider configuration."
                    ),
                )
            # Recovery ladder: (1) structured REPAIR — one cheap forced-tool-call turn that
            # reformats the model's own final text into the schema (recovers insights/journey
            # too, unlike the plain-text fallback); (2) plain-text degrade; (3) raise.
            if structured_repair and fallback_text:
                try:
                    result = await structured_repair(fallback_text)
                    degraded = "repaired_structured"
                    logger.warn(
                        f"Agent '{agent_label or feature.value}' skipped the structured-output tool; "
                        "recovered via a forced-tool-call reformat of its final text.",
                    )
                except Exception:
                    logger.warn(
                        f"Structured repair failed for agent '{agent_label or feature.value}'.",
                        details=traceback.format_exc(),
                    )
            if result is None and fallback_from_text and fallback_text:
                logger.warn(
                    f"Agent '{agent_label or feature.value}' produced no structured response for "
                    f"{getattr(response_model, '__name__', response_model)}; degrading to the "
                    "model's plain-text answer (ToolStrategy is not grammar-forced).",
                )
                result = fallback_from_text(fallback_text)
                degraded = "no_structured_response"
            if result is None:
                raise OutputParserException(
                    f"Agent produced no structured response for "
                    f"{getattr(response_model, '__name__', response_model)}."
                )

        # Tag the record with the degrade reason AND the stop_reason diagnosis (max_tokens
        # truncation / refusal / context-window) so the usage dashboard can distinguish a
        # budget-truncated turn from an ordinary plain-text degrade.
        _usage_meta = {**(stop_meta or {}), **({"degraded": degraded} if degraded else {})}
        _persist_usage(_record(
            AIUsageStatus.SUCCESS,
            extra_meta=_usage_meta or None,
        ))
        if result is not None and hasattr(result, "toolCallsUsed"):
            result.toolCallsUsed = tool_calls_count + agg.get("tool_calls", 0)
        if result is not None and hasattr(result, "inputTokens"):
            result.inputTokens = total_input + agg.get("input", 0)
        if result is not None and hasattr(result, "outputTokens"):
            result.outputTokens = total_output + agg.get("output", 0)
        _merge_extracted_citations(plugin, result, final_message)
        return result

    except LLMProviderError:
        # Already classified (e.g. the GraphRecursionError branch) — pass through
        # without re-persisting or re-wrapping.
        raise
    except Exception as exc:
        logger.error(
            "LLM agent invocation failed.",
            details=traceback.format_exc(),
        )
        error_type = plugin.classify_error(exc)
        _persist_usage(_record(AIUsageStatus.ERROR, error_type=error_type))
        response = _ERROR_RESPONSES.get(
            error_type,
            LLMErrorResponse(503, "Error occurred while invoking LLM provider."),
        )
        raise LLMProviderError(error_type, response.http_status, response.message)


async def invoke_subagent_no_record(
    plugin,
    runnable,
    tools: list,
    messages: list,
    *,
    agent_label: str,
    max_iterations: int = 8,
    on_progress: Optional[Callable[..., Awaitable[None]]] = None,
) -> tuple:
    """Run a specialist sub-agent WITHOUT persisting a usage record.

    Used by the multi-agent Configuration Copilot supervisor: each specialist runs
    here as a child of the supervisor turn and returns ``(text, delta)`` where
    ``delta`` = {input, output, extra, tool_calls}. The caller folds ``delta`` into
    the single per-turn record that the outer ``invoke_agent_with_tracking`` writes
    (via its ``extra_aggregate``), so exactly one usage record is persisted per turn.

    Progress is threaded through ``on_progress`` (stamped with ``agent_label``) so
    the specialist's inner tool steps surface live instead of collapsing into a
    single opaque tool-end at the supervisor level. The child has NO structured
    output and NO server (web) tools — it returns plain text the supervisor cites.

    A mid-stream provider failure is CAUGHT (not propagated): the tokens already consumed
    are returned in ``delta`` alongside an error marker in ``text``, so the caller folds the
    partial usage into the single per-turn record (billing/telemetry never undercounts the
    sub-agent's spend) and the supervisor gets a graceful "(sub-agent failed)" tool result
    instead of an exception. Matches the gateway's degrade-don't-crash posture.
    """
    total_input = total_output = 0
    extra_totals: dict = {}
    tool_calls_count = 0
    subagent_error: Optional[str] = None
    # Reported to the parent in `delta` so a sub-agent's grounding counts toward the
    # turn's webSearchEnriched / kbGrounded (the parent owns the single usage record).
    web_search_calls = 0
    kb_tool_calls = 0

    lc_messages = convert_to_messages(messages)
    system_msgs = [m for m in lc_messages if m.type == "system"]
    input_msgs = [m for m in lc_messages if m.type != "system"]
    system_prompt = "\n\n".join(m.content for m in system_msgs) or None

    async def _emit(
        step: str, message: str, *, kind: str, tool: Optional[str] = None, status: str = "end", run_id=None
    ) -> None:
        if on_progress:
            await on_progress(
                step, message,
                {"kind": kind, "agent": agent_label, "tool": tool, "status": status, "runId": run_id},
            )

    graph = create_agent(
        runnable,
        tools=tools,
        system_prompt=system_prompt,
        middleware=_build_agent_middleware(plugin),
        debug=os.getenv("AI_COPILOT_AGENT_DEBUG", "").lower() == "true",
    )

    final_output = None
    # Sub-agent runs inside the parent turn's OTel context, so its spans nest under the
    # parent trace when tracing is on (falls back to its own trace if context didn't
    # propagate — still attributed via the agent tag).
    trace = trace_config(
        trace_name=f"ce-subagent-{agent_label}",
        tags=[f"subagent:{agent_label}"],
    )
    try:
        async for event in graph.astream_events(
            {"messages": input_msgs},
            config={"recursion_limit": (max_iterations * 2 + 1), **trace},
            version="v2",
        ):
            ev_type = event["event"]
            ev_name = event.get("name", "")
            metadata = event.get("metadata", {})
            if ev_type == "on_chat_model_end" and metadata.get("langgraph_node") == "model":
                output = event["data"].get("output")
                if output is not None:
                    total_input, total_output = _add_message_usage(
                        plugin, total_input, total_output, extra_totals, output
                    )
                    # A sub-agent (e.g. log_analyzer) can also invoke the server-side web_search,
                    # which never fires on_tool_end — count it via the SAME shared
                    # _iter_web_search_blocks the main loop uses, so the sub-agent's web_search folds
                    # into the turn's toolCallsUsed and the two loops can't drift.
                    for block_name, block_input in _iter_web_search_blocks(output):
                        tool_calls_count += 1
                        # Reaches the parent's record through delta["web_search_calls"] ->
                        # turn_totals -> extra_aggregate, so grounding that happened inside
                        # ask_log_analyzer counts toward the turn that owns the record.
                        web_search_calls += 1
                        if on_progress:
                            await _emit(
                                block_name,
                                _progress_message(block_name, block_input, ""),
                                kind="web",
                                tool=block_name,
                                run_id=event.get("run_id"),
                            )
            elif ev_type == "on_tool_start":
                if on_progress:
                    tool_input = event["data"].get("input") or {}
                    await _emit(
                        ev_name,
                        _start_label(ev_name, tool_input),
                        kind="tool",
                        tool=ev_name,
                        status="start",
                        run_id=event.get("run_id"),
                    )
            elif ev_type == "on_tool_end":
                if ev_name != "get_ce_docs_keywords":
                    tool_calls_count += 1
                if ev_name in _KB_GROUNDING_TOOLS:
                    kb_tool_calls += 1
                if on_progress:
                    tool_input = event["data"].get("input") or {}
                    tool_output = event["data"].get("output", "")
                    output_str = getattr(tool_output, "content", str(tool_output))
                    await _emit(
                        ev_name,
                        _progress_message(ev_name, tool_input if isinstance(tool_input, dict) else {}, output_str),
                        kind="tool",
                        tool=ev_name,
                        status="end",
                        run_id=event.get("run_id"),
                    )
            elif ev_type == "on_chain_end" and ev_name == "LangGraph":
                out = event["data"].get("output") or {}
                if isinstance(out, dict):
                    final_output = out
    except Exception as sub_exc:
        # Mid-stream sub-agent failure: keep the usage accumulated so far (folded by the
        # caller into the single per-turn record) and surface a graceful marker instead of
        # propagating — the supervisor degrades rather than crashing the whole turn.
        subagent_error = str(sub_exc) or sub_exc.__class__.__name__
        logger.warn(
            f"Sub-agent '{agent_label}' failed mid-stream after "
            f"{total_input + total_output} token(s), returning partial usage.",
            details=traceback.format_exc(),
        )

    text = ""
    if isinstance(final_output, dict):
        msgs = final_output.get("messages") or []
        if msgs:
            content = getattr(msgs[-1], "content", "") or ""
            # Anthropic content can be a list of blocks; flatten to text.
            if isinstance(content, list):
                text = " ".join(
                    b.get("text", "") if isinstance(b, dict) else str(b) for b in content
                ).strip()
            else:
                text = content
    if subagent_error and not text:
        # Failed before producing any text — hand the caller a graceful marker (the
        # supervisor's prompt already tolerates a non-conclusive tool result) rather than
        # an empty string that reads as "the sub-agent had nothing to say".
        text = f"(the {agent_label} sub-agent could not complete: {subagent_error})"
    delta = {
        "input": total_input,
        "output": total_output,
        "extra": extra_totals,
        "tool_calls": tool_calls_count,
        "web_search_calls": web_search_calls,
        "kb_tool_calls": kb_tool_calls,
    }
    return text, delta
