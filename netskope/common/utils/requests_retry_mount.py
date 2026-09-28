"""Override session init."""
import os
import subprocess
from contextlib import contextmanager
from contextvars import ContextVar

import requests.sessions
from requests.adapters import HTTPAdapter
from collections import OrderedDict
from .const import (
    MAX_RETRY_COUNT,
    DEFAULT_TIMEOUT,
    MAX_RESPONSE_BYTES,
    MAX_DECOMPRESS_RATIO,
    DECOMPRESS_RATIO_FLOOR_BYTES,
    MAX_CONTENT_ENCODINGS,
    MAX_REDIRECTS,
)

try:
    TIME_OUT = int(os.environ.get("POPEN_TIMEOUT", DEFAULT_TIMEOUT))
    if TIME_OUT < 1:
        TIME_OUT = DEFAULT_TIMEOUT
except Exception:
    pass


class MaxRetryExceededException(Exception):
    """Max Retry Exceeded Exception."""

    pass


class ResponseTooLargeException(Exception):
    """Raised when a response body exceeds the decompression guards.

    urllib3 1.26.x places no bound on decompression, so a hostile or
    compromised upstream can return a small compressed body that expands
    without limit. The guards below fail closed rather than let the worker
    exhaust memory.
    """

    pass


def _content_encoding_chain_length(headers) -> int:
    """Count the non-identity encodings stacked in Content-Encoding."""
    raw_value = headers.get("Content-Encoding") or ""
    return len(
        [
            part
            for part in raw_value.split(",")
            if part.strip() and part.strip().lower() != "identity"
        ]
    )


def _compressed_size_hint(headers):
    """Return Content-Length as an int, or None when absent/unparsable.

    Chunked responses omit Content-Length; ratio enforcement is skipped for
    those and only the absolute byte cap applies.
    """
    try:
        length = int(headers.get("Content-Length"))
    except (TypeError, ValueError):
        return None
    return length if length > 0 else None


def _install_body_guard(response):
    """Bound the decompressed bytes readable from ``response``.

    Wraps the urllib3 response's ``stream``/``read`` so the running total is
    checked as chunks arrive. This covers plain ``response.content`` /
    ``.json()`` too: requests reads even non-streaming bodies through
    ``raw.stream(chunk, decode_content=True)``.
    """
    raw = getattr(response, "raw", None)
    if raw is None or getattr(raw, "_ce_body_guard", False):
        return

    compressed_hint = _compressed_size_hint(response.headers)
    ratio_limit = (
        compressed_hint * MAX_DECOMPRESS_RATIO if compressed_hint else None
    )
    total = 0

    def _account(chunk):
        nonlocal total
        if not chunk:
            return
        total += len(chunk)
        if total > MAX_RESPONSE_BYTES:
            raise ResponseTooLargeException(
                f"Response body exceeded {MAX_RESPONSE_BYTES} bytes "
                f"after decompression; aborting read."
            )
        if (
            ratio_limit is not None
            and total > DECOMPRESS_RATIO_FLOOR_BYTES
            and total > ratio_limit
        ):
            raise ResponseTooLargeException(
                f"Response expanded {total} bytes from {compressed_hint} "
                f"compressed bytes, exceeding the {MAX_DECOMPRESS_RATIO}:1 "
                f"decompression ratio limit; aborting read."
            )

    original_stream = getattr(raw, "stream", None)
    original_read = getattr(raw, "read", None)

    if original_stream is not None:
        def guarded_stream(*args, **kwargs):
            for chunk in original_stream(*args, **kwargs):
                _account(chunk)
                yield chunk

        raw.stream = guarded_stream

    if original_read is not None:
        def guarded_read(*args, **kwargs):
            chunk = original_read(*args, **kwargs)
            _account(chunk)
            return chunk

        raw.read = guarded_read

    raw._ce_body_guard = True


class _BodyGuardMixin:
    """Applies the decompression guards to every response an adapter returns."""

    def send(self, request, **kwargs):
        response = super().send(request, **kwargs)
        chain_length = _content_encoding_chain_length(response.headers)
        if chain_length > MAX_CONTENT_ENCODINGS:
            response.close()
            raise ResponseTooLargeException(
                f"Response stacks {chain_length} Content-Encoding values "
                f"(limit {MAX_CONTENT_ENCODINGS}); refusing to decompress."
            )
        _install_body_guard(response)
        return response


# When False (default), urllib3 may retry on Retry-After for 429/503/413 so
# simple plugins still get automatic backoff. When True, each response is
# returned to the caller immediately so plugin-level retry loops can log and
# sleep (see ``yield_retry_after_to_plugin``).
_yield_retry_after_to_plugin: ContextVar[bool] = ContextVar(
    "yield_retry_after_to_plugin", default=False
)


@contextmanager
def yield_retry_after_to_plugin():
    """Let plugin code observe each HTTP response during rate-limit/retry loops.

    Inside this context, urllib3 does not consume ``Retry-After`` (429/503/413);
    transport-level retries (connections, reads, redirects) stay enabled.
    Outside this context, behavior matches plain ``HTTPAdapter(max_retries=3)``:
    naive callers still benefit from urllib3's Retry-After handling.

    Usage::

        with yield_retry_after_to_plugin():
            response = requests.request(...)
    """
    token = _yield_retry_after_to_plugin.set(True)
    try:
        yield
    finally:
        _yield_retry_after_to_plugin.reset(token)


class _TimeoutHTTPAdapter(_BodyGuardMixin, HTTPAdapter):
    """HTTPAdapter that injects a default timeout and honours yield_retry_after_to_plugin."""

    def __init__(self, *args, **kwargs):
        from urllib3.util.retry import Retry
        self._retry_with_retry_after = Retry.from_int(3)
        self._retry_without_retry_after = self._retry_with_retry_after.new(
            respect_retry_after_header=False
        )
        super().__init__(max_retries=self._retry_with_retry_after, *args, **kwargs)

    def send(self, request, **kwargs):
        if kwargs.get("timeout") is None:
            DEFAULT_REQUESTS_TIMEOUT = 300
            try:
                timeout = int(os.environ.get("REQUESTS_TIMEOUT", DEFAULT_REQUESTS_TIMEOUT))
                if timeout < 1:
                    timeout = DEFAULT_REQUESTS_TIMEOUT
                kwargs["timeout"] = timeout
            except Exception:
                kwargs["timeout"] = DEFAULT_REQUESTS_TIMEOUT
        # Read the ContextVar at send-time so reused sessions also honour the flag.
        # self.max_retries mutation is safe because requests.request() creates a
        # new Session (and adapter) per call; a shared session across threads
        # would require a lock here.
        old_retries = self.max_retries
        try:
            self.max_retries = (
                self._retry_without_retry_after
                if _yield_retry_after_to_plugin.get()
                else self._retry_with_retry_after
            )
            return super().send(request, **kwargs)
        finally:
            self.max_retries = old_retries


# Captured once at import time so that assigning _patched_session_init to
# Session.__init__ on every task run is idempotent — we always wrap the same
# original, never a previously-patched version.
_original_session_init = requests.sessions.Session.__init__


class _GuardOnlyHTTPAdapter(_BodyGuardMixin, HTTPAdapter):
    """Applies the decompression guards without changing timeout/retry behaviour.

    Used in the API process, where injecting the worker's 300s default timeout
    would alter existing request behaviour.
    """

    pass


def _patched_session_init(self, *args, **kwargs):
    """Session.__init__ replacement: installs _TimeoutHTTPAdapter after original init."""
    _original_session_init(self, *args, **kwargs)
    # Replace only the adapters; original init already configured everything else.
    self.adapters = OrderedDict()
    self.mount('https://', _TimeoutHTTPAdapter())
    self.mount('http://', _TimeoutHTTPAdapter())
    # Bound redirect chains (hardening for GHSA-pq67-6m6q-mj2v).
    self.max_redirects = MAX_REDIRECTS


def _guard_only_session_init(self, *args, **kwargs):
    """Session.__init__ replacement for the API process: guards, no timeout change."""
    _original_session_init(self, *args, **kwargs)
    self.adapters = OrderedDict()
    self.mount('https://', _GuardOnlyHTTPAdapter())
    self.mount('http://', _GuardOnlyHTTPAdapter())
    self.max_redirects = MAX_REDIRECTS


def install_api_response_guards():
    """Enable the decompression guards for the API process.

    Idempotent: always wraps the original ``Session.__init__`` captured at
    import time, never a previously patched version. Call once at API startup.
    """
    requests.sessions.Session.__init__ = _guard_only_session_init


def install_worker_response_guards():
    """Enable the decompression guards (with the worker default timeout) for worker processes.

    Task-level installation via task_decorator.track() ties guard presence to
    every HTTP-calling task remembering to stack that decorator. Call this once
    per forked worker process (celery's ``worker_process_init`` signal) so the
    guards are unconditional at process boot, matching install_api_response_guards.
    Idempotent for the same reason as install_api_response_guards.
    """
    requests.sessions.Session.__init__ = _patched_session_init


def popen_retry_mount(process, is_wait: bool):
    """Retry mechanism in wait and communicates calls.

    Args:
        process (subprocess): Popen object.
        is_wait (bool): use wait method if is_wait is True else use communicate.
    Raises:
        MaxRetryExceededException: Exception when all retires completed

    Returns:
        tuple : output and error if any
    """
    retry_count = 0
    while retry_count < MAX_RETRY_COUNT:
        try:
            if is_wait:
                process.wait(timeout=TIME_OUT)
                return
            else:
                return process.communicate(timeout=TIME_OUT)
        except subprocess.TimeoutExpired:
            continue
        finally:
            retry_count += 1
    raise MaxRetryExceededException
