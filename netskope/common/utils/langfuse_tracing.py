"""Optional Langfuse tracing for the AI Copilot LLM gateway (opt-in, fail-open).

Every LLM call in CE flows through the ``llm_invoke`` gateway, so tracing attaches there —
one seam, all three paths (single-shot analyze, the copilot agent turn, the log-analyzer
sub-agent). The LangChain integration is used (not manual spans): it captures model name,
token usage, tool calls, and the span hierarchy automatically.

Operator contract (1300+ on-prem deployments — tracing must never affect a turn):
- **Opt-in via environment only.** Enabled iff ``LANGFUSE_PUBLIC_KEY`` + ``LANGFUSE_SECRET_KEY``
  are set (and ``LANGFUSE_TRACING_ENABLED`` is not ``false``). ``LANGFUSE_BASE_URL`` points at
  the operator's self-hosted/cloud instance. The SDK also honors ``LANGFUSE_SAMPLE_RATE``,
  ``LANGFUSE_TRACING_ENVIRONMENT``, ``LANGFUSE_RELEASE``, ``LANGFUSE_FLUSH_*`` from env.
- **Fail-open.** Any import/init failure logs once and disables tracing for the process;
  the turn proceeds untraced. The exporter is async/batched (OTel), so a slow or down
  Langfuse never blocks an LLM call.
- **Data boundary.** Traces contain the prompts/completions/tool IO the model actually saw.
  CE already redacts secrets and PII-strips client context BEFORE the LLM (tool layer +
  ``_strip_secrets``/``_PII_KEY_RE``), so the trace payload is bounded by the same policy —
  but trace storage is the operator's Langfuse instance; enabling tracing is their call.

Trace attributes ride the LangChain config ``metadata`` (read by the SDK's CallbackHandler):
``langfuse_trace_name`` / ``langfuse_session_id`` / ``langfuse_user_id`` / ``langfuse_tags``.
"""

import os
import threading
import traceback

from netskope.common.utils.logger import PrefixedLogger

logger = PrefixedLogger("[AI-COPILOT]")

_lock = threading.Lock()
_handler = None
_state = "unset"  # unset -> enabled | disabled (resolved once per process, lazily)


def _enabled_by_env() -> bool:
    """Tracing is opt-in: both keys present and not explicitly disabled."""
    if os.getenv("LANGFUSE_TRACING_ENABLED", "").lower() in ("false", "0"):
        return False
    return bool(os.getenv("LANGFUSE_PUBLIC_KEY") and os.getenv("LANGFUSE_SECRET_KEY"))


def _get_handler():
    """Lazily build the process-wide CallbackHandler (or None when tracing is off).

    Lazy so importing this module costs nothing on untraced deployments, and so env is
    fully loaded before the SDK reads it. One shared handler is safe: the LangChain
    integration keys its state by run_id, and the underlying client is a singleton.
    """
    global _handler, _state
    if _state != "unset":
        return _handler
    with _lock:
        if _state != "unset":
            return _handler
        if not _enabled_by_env():
            _state = "disabled"
            return None
        try:
            from langfuse.langchain import CallbackHandler

            _handler = CallbackHandler()  # binds the env-configured singleton client
            _state = "enabled"
            logger.info(
                "Langfuse tracing enabled for AI Copilot LLM calls.",
                details=f"base_url={os.getenv('LANGFUSE_BASE_URL') or os.getenv('LANGFUSE_HOST')}",
            )
        except Exception:
            _handler = None
            _state = "disabled"
            logger.warn(
                "Langfuse tracing is configured but could not initialize — continuing WITHOUT "
                "tracing (turns are unaffected). Check the langfuse package and LANGFUSE_* env.",
                details=traceback.format_exc(),
            )
    return _handler


def trace_config(*, trace_name, session_id=None, user_id=None, tags=None) -> dict:
    """Return ``{"callbacks": [...], "metadata": {...}}`` to merge into a runnable config.

    Returns ``{}`` when tracing is disabled, so call sites can splat it unconditionally:
    ``config={"recursion_limit": n, **trace_config(...)}``. Never raises.
    """
    handler = _get_handler()
    if handler is None:
        return {}
    metadata = {"langfuse_trace_name": str(trace_name)}
    if session_id:
        metadata["langfuse_session_id"] = str(session_id)
    if user_id:
        metadata["langfuse_user_id"] = str(user_id)
    if tags:
        metadata["langfuse_tags"] = " ".join([str(t) for t in tags])
    return {"callbacks": [handler], "metadata": metadata}
