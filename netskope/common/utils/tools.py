"""Tools for CE AI Agents."""

import json
import re
import traceback
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse

from bson import ObjectId
from bson.errors import InvalidId
from langchain_core.tools import tool

from netskope.common.models.ai_copilot.analyze import Citation
from netskope.common.models.log import Log
from netskope.common.utils import (
    Collections,
    DBConnector,
    PluginHelper,
    PrefixedLogger,
)

connector = DBConnector()
logger = PrefixedLogger("[AI-COPILOT]")
plugin_helper = PluginHelper()
_TRIAGE_MAX_LOGS_PER_CALL = 200
_DEFAULT_TRIAGE_CUMULATIVE_LOG_CAP = 500


def _configured_triage_log_cap() -> int:
    """Resolve AI_COPILOT_MAX_LOG_CAP, tolerating unset/empty/non-numeric/non-positive values.

    This module is imported during utils bootstrap, so a bad env value must not raise at import
    time and take down the whole triage-tools module (same guard pattern as
    attention_scan._configured_minutes / llm_invoke._configured_turn_max_iterations).
    """
    try:
        return max(1, int(os.getenv("AI_COPILOT_MAX_LOG_CAP", str(_DEFAULT_TRIAGE_CUMULATIVE_LOG_CAP))))
    except ValueError:
        return _DEFAULT_TRIAGE_CUMULATIVE_LOG_CAP


TRIAGE_CUMULATIVE_LOG_CAP = _configured_triage_log_cap()
TRIAGE_MAX_WINDOW_CALLS = max(1, TRIAGE_CUMULATIVE_LOG_CAP // 100)  # max get_logs_in_window calls per run
TRIAGE_MAX_DETAIL_LOOKUPS = TRIAGE_MAX_WINDOW_CALLS  # max get_log_details calls per run
_MAX_FIELD_BYTES = 16_384  # 16 KB per log field — prevents context-window stuffing


def truncate(value: str) -> str:
    """Truncate a log field to _MAX_FIELD_BYTES bytes to cap prompt size."""
    encoded = value.encode("utf-8")
    if len(encoded) <= _MAX_FIELD_BYTES:
        return value
    return encoded[:_MAX_FIELD_BYTES].decode("utf-8", errors="ignore") + " [truncated]"


# --- Citation normalization ------------------------------------------------
# Anchors AI Copilot prose to its sources. The model is told to keep prose clean
# and reference sources with inline [N] markers (N = 1-based index into the
# `citations` array). This is the deterministic backstop: any URL the model still
# inlines is pulled out, registered in `citations`, and replaced in the prose with
# its [N] marker — so the reader always has a prose->source link, and the markers
# line up with the numbered Sources list the UI renders. Shared by analyze + triage.

# Markdown link [label](url) and bare http(s) URL.
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^\s)]+)\)")
_BARE_URL_RE = re.compile(r"https?://[^\s)\]]+")
# Anthropic web_search sometimes wraps cited text in a <cite index="3-3,3-4">…</cite> tag.
# That markup is NOT part of our citation contract (we use [N] markers + a Sources list) and
# renders as literal "<cite index=…>" in the chat/assessment prose. UNWRAP it — keep the cited
# text, drop the tag — so it reads as normal prose (the [N]/Sources pill carry provenance).
# Non-greedy body; DOTALL so a tag spanning a line break is still caught. Also mop up a stray
# closing </cite> with no opener.
_CITE_TAG_RE = re.compile(r"<cite\b[^>]*>(.*?)</cite>", re.IGNORECASE | re.DOTALL)
_CITE_STRAY_RE = re.compile(r"</?cite\b[^>]*>", re.IGNORECASE)
# Empty "(source: )" / "(see )" parentheticals left behind once a URL is removed.
# The label+separator+trailing-space is one alternative (not three independently
# optional \s*/label?/[:\-]? slots) so a long run of whitespace with no closing ")"
# can't be re-split across those slots in O(n^3) ways
_EMPTY_REF_RE = re.compile(
    r"\(\s*(?:(?:see|sources?|refs?|references?|docs?|available at)\s*[:\-]?\s*|[:\-]\s*)?\)",
    re.IGNORECASE,
)
# A connector word left dangling right before a marker once its URL was replaced
# (e.g. "contact support at [6]" -> "contact support [6]").
_CONNECTOR_BEFORE_MARKER_RE = re.compile(
    r"\b(?:at|see|refer(?:\s+to)?|available\s+at|sources?)\s+(\[\d+\])",
    re.IGNORECASE,
)


def _title_from_url(url: str) -> str:
    """Derive a human-ish title from a URL's last path segment (fallback: host)."""
    try:
        parsed = urlparse(url)
        segments = [s for s in parsed.path.split("/") if s]
        if segments:
            return segments[-1].replace("-", " ").replace("_", " ").strip().title()
        return parsed.netloc or url
    except Exception:
        return url


def _iter_prose_urls(text: str):
    """Yield (label, url) for each markdown link, then each bare URL, in `text`.

    A URL inside a markdown link is not also yielded as a bare URL.
    """
    if not text:
        return
    md_spans = []
    for m in _MD_LINK_RE.finditer(text):
        md_spans.append((m.start(), m.end()))
        yield m.group(1).strip(), m.group(2).rstrip(".,;")
    for m in _BARE_URL_RE.finditer(text):
        if any(start <= m.start() < end for start, end in md_spans):
            continue
        yield "", m.group(0).rstrip(".,;")


def _replace_urls_with_markers(text: str, marker_for: dict) -> str:
    """Replace inline URLs with their citation marker (e.g. "[3]").

    Markdown links keep their visible label ("guide [3]"); bare URLs become just
    the marker. `marker_for` maps a normalised URL (rstrip "/") to its "[N]" string.
    Leftover artifacts (dangling connectors, empty parens, doubled spaces) are tidied.
    """
    if not text:
        return text

    # URLs NOT in marker_for (non-allowlisted domains, see CITATION_ALLOWED_DOMAINS) are
    # left untouched — a markdown link stays a link, a bare URL stays text. Deleting them
    # (the old "" default) silently dropped e.g. a tenant hostname the user needed to see.
    def _md_sub(m: "re.Match") -> str:
        marker = marker_for.get(m.group(2).rstrip(".,;").rstrip("/"))
        if marker is None:
            return m.group(0)
        return f"{m.group(1).strip()} {marker}".strip()

    cleaned = _MD_LINK_RE.sub(_md_sub, text)

    def _bare_sub(m: "re.Match") -> str:
        return marker_for.get(m.group(0).rstrip(".,;").rstrip("/"), m.group(0))

    cleaned = _BARE_URL_RE.sub(_bare_sub, cleaned)

    cleaned = _CONNECTOR_BEFORE_MARKER_RE.sub(r"\1", cleaned)
    cleaned = _EMPTY_REF_RE.sub("", cleaned)
    cleaned = re.sub(r"\(\s*\)", "", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    # \s{1,50} (not \s+): a long unterminated whitespace run makes re.search retry the
    # backtrack at every start position inside it (O(n^2)); no real prose has 50+
    # consecutive whitespace chars before punctuation, so the cap is unobservable here.
    cleaned = re.sub(r"\s{1,50}([.,;])", r"\1", cleaned)
    return cleaned.strip()


def _iter_prose_fields(result):
    """Yield every prose string on an Analyze/Triage/Copilot response (read-only).

    Covers AnalyzeResponse/TriageResponse (summary/root-cause/remediation +
    actionItems/categoryScores/timeline) and CopilotTurnResponse (``answer``). All
    access is via getattr so each shape only contributes the fields it actually has.
    """
    # Guard every yield with isinstance(str): the current typed models make these all str/List[str],
    # but this walker is duck-typed across response shapes — a loosely-typed future field must not
    # feed a non-string into the URL regex downstream. Symmetric with the top-level fields below.
    for field in ("summary", "probableRootCause", "suggestedRemediation", "answer"):
        value = getattr(result, field, None)
        if isinstance(value, str):
            yield value
    for item in getattr(result, "actionItems", None) or []:
        if isinstance(item.estimatedImpact, str):
            yield item.estimatedImpact
        yield from (s for s in item.steps if isinstance(s, str))
    for score in getattr(result, "categoryScores", None) or []:
        if isinstance(score.details, str):
            yield score.details
    for event in getattr(result, "timeline", None) or []:
        if isinstance(event.description, str):
            yield event.description


# Only URLs on these domains are PROMOTED from inline prose into numbered [N] citations.
# Grounding sources are the Netskope docs; anything else the model happens to inline —
# a tenant hostname ("crestdata-team-....goskope.com"), a support/portal link, a vendor
# site — stays as plain text instead of polluting the Sources list with junk titles.
# Model-emitted `citations` entries ARE gated by this allowlist too (see normalize_citations'
# seed loop): a hallucinated/injection-planted URL must never render as a trusted [N] Source.
# Safe to drop here — journey/insight citationRefs are resolved in the router against the model's
# original citations (into {title,url} snapshots) BEFORE normalize_citations runs, so re-indexing
# the Sources list here cannot dangle those refs.
CITATION_ALLOWED_DOMAINS = ("docs.netskope.com",)


def _citable_url(url: str, extra_domains: tuple = ()) -> bool:
    """Return True when the URL's host is on (or under) an allowed citation domain.

    Allowlist = the base ``CITATION_ALLOWED_DOMAINS`` plus ``extra_domains`` — hosts the active
    provider plugin contributes via ``PluginBase.get_citation_allowed_domains()`` (e.g. Gemini's
    grounding redirector, whose citation URLs would otherwise be dropped). There is only ever ONE
    active LLM provider, so the extra hosts are unambiguously that provider's. ``extra_domains``
    defaults to () — the strict base allowlist — for every other caller.
    """
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return False
    allowed = CITATION_ALLOWED_DOMAINS + tuple(extra_domains or ())
    return any(host == d or host.endswith("." + d) for d in allowed)


def normalize_citations(result, extra_domains: tuple = ()):
    """Anchor prose to citations: pull inline URLs out and replace them with [N].

    Two passes over the response's prose fields:
      1. Discover every inlined URL and register it in `citations` — model-provided
         entries keep their order (and thus their index); newly-found URLs are
         appended — so every URL has a stable 1-based index.
      2. Replace each inlined URL with its "[N]" marker (N = that index), matching
         the numbered Sources list the UI renders.

    Gives the reader a real prose->source link even when the model inlines a URL
    instead of emitting its own [N]. Deduped by URL. Existing [N] markers the model
    wrote are left untouched (they are not URLs). Handles AnalyzeResponse and
    TriageResponse shapes.

    ``extra_domains`` extends the model-citation allowlist with the active provider plugin's
    ``get_citation_allowed_domains()`` (e.g. Gemini's grounding-redirector host) — applied ONLY to
    the model-emitted `citations` seed (where a provider's redirector URLs arrive), never to the
    prose-URL harvest (prose only ever contains real URLs, never a provider redirector). Defaults
    to () → strict base allowlist.
    """
    if result is None:
        return result

    # The `citations` attribute gates only citation PROMOTION (the seed + prose-URL harvest below).
    # Prose SANITIZATION (Pass 2 — <cite> unwrap + inline-URL tidy) runs regardless, so a response
    # shape that carries prose but no citations list (a degrade/plain-text turn, or a future shape)
    # still gets its prose cleaned instead of leaking a raw <cite index=…> tag or bare URL. When
    # there's no citations list, marker_for stays empty: URLs aren't promoted to [N], but <cite>
    # tags are still unwrapped and dangling refs tidied.
    has_citations = hasattr(result, "citations")

    by_url: dict = {}
    if has_citations:
        # Seed with model-provided citations, preserving their order (= their indices).
        # SECURITY: a model-supplied citation URL must clear the docs.netskope.com allowlist (plus
        # the active provider's extra_domains) — a hallucinated/injection-planted URL must never
        # render as a trusted [N] Source. Non-allowlisted model URLs are dropped here.
        for c in result.citations:
            if not _citable_url(c.url, extra_domains):
                continue
            by_url.setdefault(c.url.rstrip("/"), c)
        result.citations = list(by_url.values())

        # Pass 1 — register every URL found in prose; append new ones to citations.
        # Only allowlisted-domain URLs are promoted (CITATION_ALLOWED_DOMAINS) — other inlined
        # URLs (tenant hosts, support links) are left as plain text, never made a Source.
        for text in _iter_prose_fields(result):
            for label, url in _iter_prose_urls(text):
                key = url.rstrip("/")
                existing = by_url.get(key)
                if existing is None:
                    if not _citable_url(url):
                        continue
                    citation = Citation(title=label or _title_from_url(url), url=url)
                    by_url[key] = citation
                    result.citations.append(citation)
                elif not existing.title and label:
                    existing.title = label

    # Stable 1-based marker per URL, aligned with the UI's ordered Sources list.
    # Empty when the result has no citations list (prose still gets <cite>-unwrapped in Pass 2).
    marker_for = (
        {c.url.rstrip("/"): f"[{i}]" for i, c in enumerate(result.citations, start=1)}
        if has_citations else {}
    )

    def process(text):
        # normalize_citations is duck-typed (it walks several response shapes via getattr), so
        # guard non-strings here rather than trusting every traversed field to be a str — a
        # non-string would otherwise reach _replace_urls_with_markers -> re.sub and raise.
        if not isinstance(text, str):
            return text
        if "<cite" in text.lower():
            text = _CITE_TAG_RE.sub(r"\1", text)  # unwrap <cite …>body</cite> -> body
            text = _CITE_STRAY_RE.sub("", text)  # drop any unmatched opener/closer
        return _replace_urls_with_markers(text, marker_for)

    # Pass 2 — rewrite prose with markers in place of URLs.
    for field in ("summary", "probableRootCause", "suggestedRemediation", "answer"):
        value = getattr(result, field, None)
        if isinstance(value, str):
            setattr(result, field, process(value))
    for item in getattr(result, "actionItems", None) or []:
        item.estimatedImpact = process(item.estimatedImpact)
        item.steps = [process(s) for s in item.steps]
    for score in getattr(result, "categoryScores", None) or []:
        score.details = process(score.details)
    for event in getattr(result, "timeline", None) or []:
        event.description = process(event.description)
    # Typed insight cards (copilot turn) carry their own prose — normalize them too so a
    # <cite> tag or inline URL in a card body doesn't leak raw.
    for insight in getattr(result, "insights", None) or []:
        for f in ("title", "summary", "body"):
            v = getattr(insight, f, None)
            if isinstance(v, str):
                setattr(insight, f, process(v))

    # A non-empty, ALLOWLISTED citations list is itself a provider-agnostic signal that the
    # answer is doc-grounded — regardless of HOW the sources arrived. The gateway's
    # _merge_extracted_citations only trues webSearchEnriched from annotation-based citations
    # (grounding_metadata), which misses the case where the model reports its docs sources
    # directly in the structured `citations` field (observed with Gemini: real
    # docs.netskope.com URLs present, but grounding_metadata absent -> flag left False despite
    # cited docs). By the time we're here, the allowlist filter above has run, so any surviving
    # citation is a trusted docs.netskope.com source. OR it in (never lower a True the gateway set).
    if has_citations and result.citations and hasattr(result, "webSearchEnriched"):
        result.webSearchEnriched = True

    # Warn when citations are populated but no [N] markers ended up in the prose.
    # normalize_citations can only promote inline URLs to markers — it cannot inject
    # markers for citations the model emitted in the array but never referenced in
    # the prose text. If this fires, the model did not follow CITATION_FORMAT_GUIDANCE.
    if has_citations and result.citations:
        _marker_re = re.compile(r"\[\d+\]")
        all_prose = " ".join(v for v in _iter_prose_fields(result) if isinstance(v, str) and v)
        if not _marker_re.search(all_prose):
            logger.warn(
                "Citations array is populated but no [N] markers found in prose — "
                "model did not follow citation format guidance.",
            )

    return result


# --- CE documentation vocabulary for grounding web_search ------------------
# Official Cloud Exchange terminology + searchable topics, keyed by descriptive
# area. Returned on demand by the get_ce_docs_keywords tool so the agent can
# frame accurate docs.netskope.com queries — kept OUT of the system prompt so it
# isn't re-sent on every agent iteration.
_CE_GENERAL_AREA = "Cloud Exchange platform (general)"

CE_DOCS_KEYWORDS: dict = {
    _CE_GENERAL_AREA: (
        "Cloud Exchange (CE) is a platform that runs functional Modules, each driving "
        "third-party Plugins.\n"
        "- Core: the CE core engine; manages plugins and their lifecycle methods and "
        "exposes API endpoints for interacting with the platform.\n"
        "- Module: a functional area (CLS, CTE, CRE, CTO, EDM, CFC) that invokes "
        "module-specific plugins to accomplish a workflow.\n"
        "- Plugin: a Python package with logic to pull/push/transform data to or from a "
        "third-party platform.\n"
        "- Plugin Configuration: a configured plugin instance, scheduled by Core to run.\n"
        "- Business Rules / Sharing Configurations: rules that decide what data is shared "
        "with which destination.\n"
        "- Mapping: the format/shape of data sent to a destination platform.\n"
        "Common doc topics: Troubleshooting, FAQs, Cloud Exchange System Requirements, "
        "Cloud Exchange Hardening, Backup/Restore Cloud Exchange, SSO with "
        "Okta/Entra/Netskope, Dashboards, Cloud Exchange Feature Lists."
    ),
    "Cloud Log Shipper (CLS)": (
        "Cloud Log Shipper (CLS) ingests, transforms, and pushes Netskope tenant "
        "logs/events to third-party SIEMs and destinations. Key terms: Mapping, Log "
        "Delivery, Business Rules. Plugin: Log Shipper Plugin."
    ),
    "Cloud Threat Exchange (CTE)": (
        "Cloud Threat Exchange (CTE) pulls and pushes Threat IoCs (malware hashes, "
        "malsite URLs) to/from third-party Threat Intel systems. Key terms: Indicators "
        "(Threat IoCs), Business Rules, Sharing Configurations. Plugin: Threat Exchange "
        "Plugin."
    ),
    "Cloud Risk Exchange (CRE)": (
        "Cloud Risk Exchange (CRE) fetches users, devices, and applications plus their "
        "risk scores from third parties and can act on them (e.g. add/remove from group). "
        "Key terms: Entity, Records, Schema Editor. Plugin: Risk Exchange Plugin."
    ),
    "Cloud Ticket Orchestrator (CTO)": (
        "Cloud Ticket Orchestrator (CTO, also called ITSM) creates or updates tasks and "
        "alerts in platforms such as Jira or ServiceNow. Key terms: Tasks, Alerts, "
        "Queues. Plugin: Ticket Orchestrator Plugin."
    ),
    "Exact Data Match (EDM)": (
        "Exact Data Match (EDM) supports exact-data-match hashing and sharing. Key terms: "
        "Sharing Configurations, Manual Upload, sent/received hashes. Plugin: Exact Data "
        "Match Plugin."
    ),
    "Custom File Classification (CFC)": (
        "Custom File Classification (CFC) shares classified file/image data. Key terms: "
        "Sharing, Business Rules, Manual Upload, sent images. Plugin: Custom File "
        "Classification Plugin."
    ),
    "Deployment and Installation": (
        "CE deployment & installation: Standalone vs HA Deployment; CE as a Virtual "
        "Machine (Ubuntu, Red Hat Enterprise Linux); Docker / Podman runtimes; Install on "
        "Cloud Platform; Backup and Restore Cloud Exchange; Cloud Exchange System "
        "Requirements; Cloud Exchange Hardening."
    ),
    "Plugins and Repositories": (
        "Plugins & repositories: Netskope Tenant Plugin, Netskope Borderless WAN Tenant "
        "Plugin, Beta Plugins; updating/managing/configuring <Module> plugins; plugin "
        "repositories (Default, Beta, Custom)."
    ),
}


def _ce_docs_area_index() -> str:
    """Return available areas plus the general glossary (the no-arg fallback)."""
    areas = "\n".join(f"- {name}" for name in CE_DOCS_KEYWORDS)
    return (
        "Available CE areas (pass one as `area`):\n"
        f"{areas}\n\n{_CE_GENERAL_AREA}: \n{CE_DOCS_KEYWORDS[_CE_GENERAL_AREA]}"
    )


@tool
def get_ce_docs_keywords(area: Optional[str] = None) -> str:
    """Return official Netskope Cloud Exchange (CE) terminology and search topics.

    Call this BEFORE web_search to ground your query in correct CE vocabulary
    (module / plugin / feature names), so searches against docs.netskope.com are
    accurate. This is free and does not count against the web_search budget.

    Args:
        area: The CE area to fetch vocabulary for. One of:
            "Cloud Exchange platform (general)", "Cloud Log Shipper (CLS)",
            "Cloud Threat Exchange (CTE)", "Cloud Risk Exchange (CRE)",
            "Cloud Ticket Orchestrator (CTO)", "Exact Data Match (EDM)",
            "Custom File Classification (CFC)", "Deployment and Installation",
            "Plugins and Repositories". Matched loosely (case-insensitive /
            substring, so "cls" or "log shipper" also work). Omit to get the
            list of areas plus the general glossary.
    """
    try:
        query = (area or "").strip().lower()
        if not query:
            return _ce_docs_area_index()
        for name, text in CE_DOCS_KEYWORDS.items():
            name_lc = name.lower()
            abbr = name_lc[name_lc.find("(") + 1: name_lc.find(")")] if "(" in name_lc else ""
            if query in name_lc or name_lc in query or (abbr and (query in abbr or abbr in query)):
                return f"{name}: \n{text}"
        return f"No CE area matched '{area}'.\n\n{_ce_docs_area_index()}"
    except Exception as exc:
        logger.error(
            "could not able to list the CE keywords.",
            details=traceback.format_exc(),
            error_code="CE_1330",
        )
        return f"Error getting the CE keywords: {exc}"


# Shared web-search TOOL guidance — governs how/when to use the search tools only.
# Single source of truth so analyze and triage stay consistent. Response-formatting
# (how to surface sources) lives separately in CITATION_FORMAT_GUIDANCE below.
WEB_SEARCH_TOOL_GUIDANCE = (
    "\n\nYou have two grounding tools: get_ce_docs_keywords (official CE vocabulary, "
    "free) and web_search (scoped to Netskope documentation). For EVERY important "
    "finding and EVERY remediation or recommendation you make, you MUST first call "
    "get_ce_docs_keywords for the relevant area, then web_search Netskope CE "
    "documentation to confirm it. Do not state a finding or suggest a "
    "remediation/command that you have not grounded in a CE doc — ungrounded "
    "commands can cause downtime. You have at most 5 web searches: prioritize the "
    "most important findings and batch related concepts into focused queries "
    "(module/plugin/feature + symptom) rather than searching one keyword at a time."
)


# Shared citation-FORMATTING contract — governs only how grounded sources are
# surfaced in the structured response, independent of how/when searching happens.
# Appended after WEB_SEARCH_TOOL_GUIDANCE in both addenda. The [N] markers line up
# with the numbered Sources list the UI renders; normalize_citations() is the
# deterministic backstop that converts any inlined URL into its matching [N].
CITATION_FORMAT_GUIDANCE = (
    "\n\nReturn every documentation source you relied on in the `citations` array "
    "(title + url), in the order you first reference them. Do NOT paste raw URLs "
    "into any prose field. Instead, anchor every grounded claim with an inline "
    "marker [N], where N is the 1-based index of the matching entry in the "
    "`citations` array (the first source is [1], the second [2], and so on). Every "
    "grounded claim MUST carry a marker, and every citation MUST be referenced by "
    "at least one marker in the prose."
)


def build_triage_tools(filters: dict, web_tool=None) -> tuple:
    """
    Build LangChain tools for the triage agent.

    All tools close over `filters` (MongoDB filter dict) and `connector`.

    The LLM only sees typed parameters — session filters cannot be altered.

    Args:
        filters (dict): filters configured by user.
        web_tool (dict, optional): web tool configs if supported. Defaults to None.

    Returns:
        tuple: tools, _logs_fetched, _web_search_called
    """
    from netskope.common.utils import parse_dates

    base_filter = json.loads(json.dumps(filters or {}), object_hook=lambda pair: parse_dates(pair))
    # Distinct logs examined — reported to the user as logsAnalyzed. Tracked as a
    # set of ids so overlapping/re-fetched windows are not double-counted (which
    # previously let logsAnalyzed exceed the total matching count).
    _logs_fetched = [0]
    _fetched_ids: set = set()
    # Cumulative log LINES pulled into the model context across all windows
    # (gross, including duplicates) — this is what the budget cap guards, since
    # every re-fetched line still consumes context/tokens.
    _context_lines = [0]
    _window_calls = [0]
    _detail_calls = [0]
    _web_search_called = [False]

    @tool
    def count_logs() -> str:
        """Return the total number of platform logs matching the current filter.

        Call this first to understand the scale before fetching logs.
        """
        try:
            result = connector.collection(Collections.LOGS).aggregate(
                [
                    {"$match": base_filter},
                    {"$count": "count"},
                ]
            )
            doc = next(result, None)
            count = doc["count"] if doc else 0
            return f"Total matching logs: {count}"
        except Exception as exc:
            logger.error(
                "could not count the logs available for filter via tool.",
                details=traceback.format_exc(),
                error_code="CE_1330",
            )
            return f"Error getting the logs count: {exc}"

    @tool
    def get_error_summary() -> str:
        """Return a lightweight heatmap of error distribution across time.

        Call this after count_logs() to identify WHICH time windows have the
        highest error concentration — then use get_logs_in_window() to fetch
        only those windows directly, without scanning through earlier pages.

        Returns:
            - Top time windows by error count (auto-bucketed)
            - Most frequent error codes with NEW flag for codes that emerged
              partway through the analyzed window (not present from the start)
            - Per-source/plugin breakdown by errorCode prefix
            - Overall log type breakdown (error/warning/info/debug counts)
        """
        try:
            # Determine time span of matching logs to pick bucket size
            bounds_pipeline = [
                {"$match": base_filter},
                {
                    "$group": {
                        "_id": None,
                        "min_ts": {"$min": "$createdAt"},
                        "max_ts": {"$max": "$createdAt"},
                    }
                },
            ]
            bounds_doc = next(connector.collection(Collections.LOGS).aggregate(bounds_pipeline), None)
            if bounds_doc is None:
                return "No logs found matching the current filter."

            min_ts: datetime = bounds_doc["min_ts"]
            max_ts: datetime = bounds_doc["max_ts"]
            span_ms = max(int((max_ts - min_ts).total_seconds() * 1000), 1)
            # Target ~20 buckets regardless of time span
            bucket_ms = max(span_ms // 20, 60_000)  # minimum 1-minute buckets

            # Time-bucketed error distribution
            bucket_pipeline = [
                {"$match": base_filter},
                {
                    "$group": {
                        "_id": {
                            "bucket": {
                                "$subtract": [
                                    {"$toLong": "$createdAt"},
                                    {"$mod": [{"$toLong": "$createdAt"}, bucket_ms]},
                                ]
                            },
                            "errorCode": "$errorCode",
                        },
                        "count": {"$sum": 1},
                    }
                },
                {
                    "$group": {
                        "_id": "$_id.bucket",
                        "total": {"$sum": "$count"},
                        "codes": {"$push": {"code": "$_id.errorCode", "count": "$count"}},
                    }
                },
                {"$sort": {"total": -1}},
                {"$limit": 10},
            ]
            buckets = list(connector.collection(Collections.LOGS).aggregate(bucket_pipeline))

            # Top error codes across all matching logs
            code_pipeline = [
                {"$match": base_filter},
                {
                    "$group": {
                        "_id": "$errorCode",
                        "count": {"$sum": 1},
                        "first": {"$min": "$createdAt"},
                    }
                },
                {"$sort": {"count": -1}},
                {"$limit": 15},
            ]
            top_codes = list(connector.collection(Collections.LOGS).aggregate(code_pipeline))

            # Determine NEW error codes — first ever occurrence within filter window
            lines = []
            # Only emit the "Error distribution" header when there ARE buckets — otherwise it was a
            # dangling header line with no rows (and a trailing space), which surfaced as an empty
            # "Error distribution (…bucket=Ns):" analysis step in the report.
            if buckets:
                lines.append(f"Error distribution (top windows, bucket={bucket_ms // 1000}s):")
            for b in buckets:
                ts = datetime.fromtimestamp(b["_id"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                codes_str = ", ".join(
                    f"{c['code'] or 'unknown'}×{c['count']}" for c in sorted(b["codes"], key=lambda x: -x["count"])[:5]
                )
                lines.append(f"  {ts} → {b['total']} logs [{codes_str}]")

            lines.append("")
            lines.append("Top error codes:")
            # An errorCode is "NEW" if it emerged partway through the analyzed
            # window — i.e. its first occurrence falls past the opening bucket
            # rather than being present from the start. Computed from `first`
            # already returned by code_pipeline (no extra query), and well-defined
            # whether or not the filter pins a time range. This is a window-relative
            # "regression appeared during this period" signal, not a claim about
            # deep history before the window.
            emergence_cutoff = min_ts + timedelta(milliseconds=bucket_ms)
            for tc in top_codes:
                code = tc["_id"] or "unknown"
                first_seen = tc.get("first")
                new_flag = " [NEW]" if first_seen and first_seen >= emergence_cutoff else ""
                lines.append(f"  {code}: {tc['count']} occurrences{new_flag}")

            # Source breakdown by errorCode prefix
            prefix_counts: dict = {}
            for tc in top_codes:
                code = tc["_id"] or "unknown"
                prefix = code.split("_")[0] if "_" in code else code[:3]
                prefix_counts[prefix] = prefix_counts.get(prefix, 0) + tc["count"]
            if len(prefix_counts) > 1:
                lines.append("")
                lines.append("Source breakdown (by errorCode prefix):")
                for prefix, cnt in sorted(prefix_counts.items(), key=lambda x: -x[1]):
                    lines.append(f"  {prefix}_* → {cnt} errors")

            # Log level breakdown
            level_pipeline = [
                {"$match": base_filter},
                {
                    "$group": {
                        "_id": {"$ifNull": ["$type", "info"]},
                        "count": {"$sum": 1},
                    }
                },
            ]
            level_counts = {
                d["_id"]: d["count"] for d in connector.collection(Collections.LOGS).aggregate(level_pipeline)
            }
            level_str = ", ".join(f"{k}={v}" for k, v in sorted(level_counts.items()))
            lines.append("")
            lines.append(f"Log type breakdown: {level_str}")
            return_str = "\n".join(lines)
            return return_str

        except Exception as exc:
            logger.error(
                "Error encountered while generating the error summary for provided filter.",
                details=traceback.format_exc(),
                error_code="CE_1330",
            )
            return f"Error generating summary: {exc}"

    @tool
    def get_logs_in_window(start_time: datetime, end_time: datetime, limit: int = 100) -> str:
        """Fetch logs within a specific time window in chronological order.

        Args:
            start_time: Window start as a datetime (ISO-8601 string accepted).
            end_time:   Window end as a datetime (ISO-8601 string accepted).
            limit:      Max logs to return. Capped at 200 server-side, enforced limit >= 1.

        Use timestamps from get_error_summary() to jump directly to error-dense
        windows. You can widen the window slightly to capture the lead-up to a spike.

        Returns compact lines:
            [timestamp] [id:...] LEVEL  errorCode  message  [+details?]

        Logs marked [+details] have additional context — call get_log_details()
        for pivotal logs only.
        """
        try:
            if _window_calls[0] >= TRIAGE_MAX_WINDOW_CALLS:
                return (
                    f"Window call budget exhausted ({TRIAGE_MAX_WINDOW_CALLS} windows fetched). "
                    "Proceed with analysis of fetched data."
                )
            if _context_lines[0] >= TRIAGE_CUMULATIVE_LOG_CAP:
                return (
                    f"Log fetch budget exhausted ({TRIAGE_CUMULATIVE_LOG_CAP} logs retrieved). "
                    "Proceed with analysis of fetched data."
                )

            _window_calls[0] += 1
            limit = max(1, limit)
            limit = min(limit, _TRIAGE_MAX_LOGS_PER_CALL)
            remaining = TRIAGE_CUMULATIVE_LOG_CAP - _context_lines[0]
            limit = min(limit, remaining)

            window_filter = {
                **base_filter,
                "createdAt": {"$gte": start_time, "$lte": end_time},
            }
            pipeline = [
                {"$match": window_filter},
                {"$sort": {"createdAt": 1}},
                {"$limit": limit},
            ]
            docs = list(connector.collection(Collections.LOGS).aggregate(pipeline))

            if not docs:
                return f"No logs found in window {start_time} – {end_time}."

            _context_lines[0] += len(docs)
            _fetched_ids.update(str(doc["_id"]) for doc in docs)
            _logs_fetched[0] = len(_fetched_ids)
            lines = []
            for doc in docs:
                log = Log(**doc)
                ts = log.createdAt.strftime("%Y-%m-%d %H:%M:%S") if log.createdAt else "?"
                level = str(log.ce_log_type or "?").upper()
                code = log.errorCode or "-"
                msg = truncate(log.message or "")[:120]
                has_details = " [+details]" if log.details else ""
                lines.append(f"[{ts}] [id:{doc['_id']}] {level}  {code}  {msg}{has_details}")

            return "\n".join(lines)
        except Exception as exc:
            logger.error(
                "Error encountered while querying for the logs in provided window.",
                details=traceback.format_exc(),
            )
            return f"Error getting logs with given parameters: {exc}"

    @tool
    def get_log_details(log_id: str) -> str:
        """Fetch the full details and resolution text for a single log entry.

        Args:
            log_id: The MongoDB ObjectId from a log line (the id:... value).

        Use this only for logs marked [+details] that appear significant.
        Do not call this for every log — only when the detail field would
        materially improve your root cause assessment.
        """
        try:
            if _detail_calls[0] >= TRIAGE_MAX_DETAIL_LOOKUPS:
                return (
                    f"Detail lookup budget exhausted ({TRIAGE_MAX_DETAIL_LOOKUPS} lookups used). "
                    "Proceed with analysis of fetched data."
                )
            _detail_calls[0] += 1
            try:
                doc = connector.collection(Collections.LOGS).find_one({"_id": ObjectId(log_id)})
            except InvalidId:
                return f"Invalid log ID: {log_id}"
            if doc is None:
                return f"Log not found: {log_id}"
            log = Log(**doc)
            # details/resolution are free-text pulled from Mongo and may contain
            # attacker-influenced content. Frame them in <log_data> — the same
            # structural barrier the analyze path uses — so the agent treats them
            # strictly as data, not instructions (see triage system prompt).
            return (
                "<log_data>\n"
                f"Details: {log.details or 'None'}\n"
                f"Resolution: {log.resolution or 'None'}\n"
                "</log_data>"
            )
        except Exception as exc:
            logger.error(
                "Error encountered while getting the logs details.",
                details=traceback.format_exc(),
                error_code="CE_1330",
            )
            return f"Error getting the log details: {exc}"

    tools = [count_logs, get_error_summary, get_logs_in_window, get_log_details]

    if web_tool is not None:
        if hasattr(web_tool, "invoke"):
            # LangChain BaseTool — wrap invoke to track usage client-side.
            original_invoke = web_tool.invoke

            def _tracked_invoke(input, **kwargs):
                _web_search_called[0] = True
                return original_invoke(input, **kwargs)

            web_tool.invoke = _tracked_invoke
        # Plain dicts (Anthropic native server-side tools) are tracked via
        # on_progress("web_search", ...) in the caller.
        tools.append(web_tool)
        # Vocabulary lookup to ground web_search queries (only useful alongside it).
        tools.append(get_ce_docs_keywords)

    return tools, _logs_fetched, _web_search_called
