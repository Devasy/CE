"""Agent tools for the Configuration Copilot.

``build_config_tool_registry`` returns the scope-permitted LangChain tools the
configuration copilot agents may call (name -> tool), mirroring ``build_triage_tools``:
each tool is a ``@tool``-decorated closure over budget cells, and the whole set is
scope-gated so the model only ever sees data the caller is entitled to. The multi-agent
router (``copilot_agents``) builds each specialist's tool subset by selecting names from
this registry.

Scope: all six CE modules (CTE, CTO/ITSM, CLS, CRE, EDM, CFC), system/general
settings, dashboards/health, read-only correlation tools (optimize mode), the
curated knowledge pack, the deployment topology, and admin-only user/scope reads.
Everything here is **read-only** — the copilot never writes config; drafts are
prefilled into the existing forms whose Save validates and persists.

Security:
- module data is gated per call by the relevant ``*_read`` scope (and ``admin``
  for users); an out-of-scope call returns an access-denied string, never data.
- plugin ``parameters`` are redacted by manifest ``type == "password"`` *before*
  returning — secrets never reach the model (we never resolve ``secret:`` refs).
- tool outputs that echo stored config/log content are wrapped in a
  ``CONFIG_DATA`` delimiter so the prompt can treat them strictly as data.
"""

import contextlib
import contextvars
import json
import re
import traceback
from dataclasses import dataclass, field as dc_field
from typing import List, Optional

from langchain_core.tools import tool

from netskope.common.utils import (
    Collections,
    DBConnector,
    PluginHelper,
    PrefixedLogger,
    get_dynamic_fields_from_plugin,
)
from netskope.common.utils.copilot_knowledge import get_knowledge, list_areas
from netskope.common.utils.tools import _MAX_FIELD_BYTES, truncate
from netskope.common.utils.deployment import (
    DEPLOYMENT_OPTIONS,
    PLATFORM_PROVIDER_LABELS,
    deployment_details,
)

connector = DBConnector()
logger = PrefixedLogger("[AI-COPILOT][Config-Copilot]")
plugin_helper = PluginHelper()


# --- module wiring ---------------------------------------------------------
# CTE configs live in the generic CONFIGURATIONS collection; every other module in its own.
# Canonical module -> scope maps (single source of truth): the attention router,
# copilot_agents, and the tools below all import/read these. A module onboarded here is
# consistently gated across the tool registry, the findings feed, and journey routing.
# NOTE: the module id (cte/cto/cls/cre/edm/cfc) is the copilot's own key; it does NOT always
# equal the plugin-integration key (cto->itsm) nor the collection prefix (cre->crev2), so the
# three maps below are kept explicit rather than derived. cre -> CREV2_* (the live Risk
# Exchange); legacy CRE_* is migration-only. edm's "business rules" collection stores 1:1
# sharing (source->dest via sharedWith), not filter rules — EDM has no filter rules, so it is
# excluded from the no_business_rules / rule_unwired attention families (see attention_rules).
MODULE_READ_SCOPE = {
    "cte": "cte_read",
    "cto": "cto_read",
    "cls": "cls_read",
    "cre": "cre_read",
    "edm": "edm_read",
    "cfc": "cfc_read",
    "system": "settings_read",
}
MODULE_WRITE_SCOPE = {
    "cte": "cte_write",
    "cto": "cto_write",
    "cls": "cls_write",
    "cre": "cre_write",
    "edm": "edm_write",
    "cfc": "cfc_write",
    "system": "settings_write",
}
_MODULE_INTEGRATION = {
    "cte": "cte",
    "cto": "itsm",
    "cls": "cls",
    "cre": "cre",
    "edm": "edm",
    "cfc": "cfc",
}
# Available-plugin listing spans the six CE modules AND the two non-module plugin CLASSES:
# `provider` (Netskope Tenant connectivity plugins) and `llm_provider` (AI providers, e.g.
# Anthropic). Maps a copilot key -> (plugin_helper.plugins bucket key, required read scope).
# Kept SEPARATE from _MODULE_INTEGRATION/MODULE_READ_SCOPE on purpose: provider/llm_provider
# have no module data collection, business rules, or dashboard, so they must NOT leak into the
# module-data tools' validation or the _allowed()/MODULE_READ_SCOPE semantics. Scopes mirror the
# real routes: provider -> settings_read (as tenants), llm_provider -> ai_read (as the copilot's
# own llm_provider status read + the whole config_copilot router).
_AVAILABLE_PLUGIN_CLASSES = {
    "cte": ("cte", "cte_read"),
    "cto": ("itsm", "cto_read"),
    "cls": ("cls", "cls_read"),
    "cre": ("cre", "cre_read"),
    "edm": ("edm", "edm_read"),
    "cfc": ("cfc", "cfc_read"),
    "provider": ("provider", "settings_read"),
    "llm_provider": ("llm_provider", "ai_read"),
}
# The two non-module plugin classes: capability flags (push/pull/receiving) don't exist in their
# manifests, so get_plugin_capabilities refuses them rather than returning nulls.
_NON_MODULE_PLUGIN_CLASSES = frozenset({"provider", "llm_provider"})
_MODULE_COLLECTION = {
    "cte": Collections.CONFIGURATIONS,
    "cto": Collections.ITSM_CONFIGURATIONS,
    "cls": Collections.CLS_CONFIGURATIONS,
    "cre": Collections.CREV2_CONFIGURATIONS,
    "edm": Collections.EDM_CONFIGURATIONS,
    "cfc": Collections.CFC_CONFIGURATIONS,
}
_MODULE_BUSINESS_RULES = {
    "cte": Collections.CTE_BUSINESS_RULES,
    "cto": Collections.ITSM_BUSINESS_RULES,
    "cls": Collections.CLS_BUSINESS_RULES,
    "cre": Collections.CREV2_BUSINESS_RULES,
    # EDM's rules collection is its 1:1 sharing store (sharedWith), not filter rules.
    "edm": Collections.EDM_BUSINESS_RULES,
    "cfc": Collections.CFC_BUSINESS_RULES,
}
# Human module labels used to build the docs.netskope.com plugin-guide search query
# ("<Module label> <Plugin name> plugin guide"). Covers all six CE modules the copilot configures.
# Canonical module id -> human display label (single source of truth, next to MODULE_READ_SCOPE).
# copilot_agents imports this. NOTE: attention_rules._MODULE_TITLES intentionally keeps its OWN copy
# — that module is the PURE rule engine (Mongo/registry-free by contract) and must not import this
# langchain-heavy module; keep the two in sync by eye if the labels ever change.
MODULE_DISPLAY_LABEL = {
    "cte": "Threat Exchange",
    "cto": "Ticket Orchestrator",
    "cls": "Log Shipper",
    "cre": "Risk Exchange",
    "edm": "Exact Data Match",
    "cfc": "Custom File Classification",
}
# Manifest field keys/labels that typically act as pull-time pre-filters (the data a
# plugin pulls), used to reconcile a business rule's scope with what the plugin fetches.
_PREFILTER_KEY_RE = re.compile(
    r"type|indicator|ioc|query|filter|severity|reputation|category|threat", re.IGNORECASE
)

# Per-tool call budgets (per chat turn). Mirrors the triage cap discipline so a
# single turn can't stuff the context window or hammer Mongo.
_CAP_LIST = 8
_CAP_DETAIL = 8
_CAP_PLUGINS = 4
_CAP_PLUGIN_CAPS = 8  # charged once PER plugin_id, so a batch of 4 costs 4 — total lookups per turn stays 8
_CAP_SCHEMA = 6
_CAP_WALKTHROUGH = 4
_CAP_RULES = 4
_CAP_VALIDATE = 4
_CAP_SETTINGS = 6
_CAP_DASHBOARD = 4
_CAP_HEALTH = 2
_CAP_RUN_STATUS = 4
_CAP_ANALYZE = 3
_CAP_USERS = 2
_CAP_MODULE_TOOL = 3  # per-turn cap for the per-module inspection tools (cls/cre/edm/cfc leaf reads)
_MAX_PLUGIN_CAPS_BATCH = 4
_CAP_DEPLOYMENT = 2  # the deployment topology can't change mid-turn; one read is enough

# Row caps for the unbounded list/collection reads. These are DOCUMENT-count limits on the
# Mongo query itself (distinct from the _CAP_* per-turn CALL budgets above): _wrap()/truncate
# only shrinks the serialized JSON AFTER the full result set is loaded, so a deployment with
# hundreds of configs/rules/users would still stream them all into the process without a
# .limit(). When a read hits the cap we tell the model it's a partial view (see _capped_note).
_MAX_LIST_DOCS = 50
_MAX_HASH_DOCS = 25

# Non-secret settings keys get_settings may return per section (everything NOT listed here is
# withheld). CTE/CLS/CRE/EDM/CFC each store their module settings under a single top-level sub-doc
# keyed by the module name (SettingsOut.<module>: <Module>Settings — retention/interval/strategy
# fields, no credentials), so each is whitelisted as its own nested key. CTO/ITSM is the exception:
# its cleanup knobs are flat top-level keys. System is an explicit allow-list of non-secret keys.
_SETTINGS_WHITELIST = {
    "system": [
        "logLevel", "logsCleanup", "dataBatchCleanup", "tasksCleanup", "sslValidation",
        "emailAddress", "platforms", "certExpiry", "passwordPolicy", "databaseVersion",
        "enableUpdateChecking", "disk_alarm", "ssoEnable", "version", "aiDataCleanup",
        "aiStatsCleanup", "pluginsUpdatedAt", "proxy"
    ],
    "cte": ["cte"],
    "cto": [
        "alertCleanup", "eventCleanup", "ticketsCleanup", "ticketsCleanupQuery",
        "notificationsCleanup", "notificationsCleanupUnit", "itsm"
    ],
    "cls": ["cls"],
    "cre": ["cre"],
    "edm": ["edm"],
    "cfc": ["cfc"],
}


def _wrap(label: str, payload) -> str:
    """Frame tool output as untrusted data the model must not treat as instructions."""
    body = payload if isinstance(payload, str) else json.dumps(payload, default=str, indent=2)
    return f"<config_data source={label!r}>\n{truncate(body)}\n</config_data>"


def _capped(rows, cap):
    """Attach a partial-view note when a list read hit its row cap.

    The list reads apply a Mongo ``.limit(cap)``; when exactly ``cap`` rows come back the
    result is (almost certainly) truncated, so return ``{"_note": ..., "items": rows}`` to
    tell the model the view is partial and it should NOT assert completeness. Below the cap
    the rows pass through unchanged.
    """
    if len(rows) >= cap:
        return {
            "_note": f"Showing the first {cap} — there may be more. This is a partial view.",
            "items": rows,
        }
    return rows


def _capped_by_bytes(rows, budget=_MAX_FIELD_BYTES):
    """Cap ``rows`` to fit ``budget`` bytes when serialized, degrading the overflow to stubs.

    Row-count caps (``_capped``) assume roughly uniform row size, which holds for compact Mongo
    documents but NOT for free-text-heavy rows like plugin catalog entries (vendor-authored
    descriptions vary from ~200 to 800+ bytes with no upper bound enforced at the source). Left
    unchecked, the full serialized list can overflow ``_wrap()``'s downstream ``truncate()`` byte
    cut — which slices the ALREADY-SERIALIZED JSON string mid-object with no partial-view signal,
    handing the model invalid JSON.

    Rather than dropping the overflow plugins outright, once the budget is spent every remaining
    row is degraded to an ``{"id": ..., "name": ...}`` stub (no ``description``/``version``) — so
    the model still knows the plugin exists and can name it, and can call
    ``get_plugin_capabilities``/``get_plugin_schema`` on it directly in a follow-up turn instead of
    concluding (per the §32/§38 "never assume a plugin exists" rule) that it isn't installed.
    """
    def _note(n_full):
        return (
            f"Showing {n_full} of {len(rows)} in full — the rest didn't fit this response's size "
            "budget, so only id/name are listed for them. Call get_plugin_capabilities/"
            "get_plugin_schema directly on a stubbed plugin_id if needed."
        )

    def _fits(n_full):
        stubs = [{"id": r.get("id"), "name": r.get("name")} for r in rows[n_full:]]
        payload = rows if n_full == len(rows) else {"_note": _note(n_full), "items": rows[:n_full] + stubs}
        return len(json.dumps(payload, default=str, indent=2).encode("utf-8")) <= budget

    if _fits(len(rows)):
        return rows
    # Binary search the largest prefix of FULL rows (+ stubs for the rest) that fits — measuring
    # the actual final serialized structure, not a per-row estimate, so it can't drift off the
    # real byte count the way summing isolated row sizes did.
    lo, hi = 0, len(rows)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _fits(mid):
            lo = mid
        else:
            hi = mid - 1
    stubs = [{"id": r.get("id"), "name": r.get("name")} for r in rows[lo:]]
    return {"_note": _note(lo), "items": rows[:lo] + stubs}


def _iter_manifest_fields(configuration):
    """Yield (field, step_label, is_dynamic_step) for a plugin manifest's fields.

    Handles flat fields and multi-step wizards: ``step`` entries nest their fields
    under ``fields``; ``dynamic_step`` entries declare no static fields (their
    options are fetched live) — surfaced with is_dynamic_step=True so callers can
    explain "this step populates after you enter credentials".
    """
    for entry in configuration or []:
        if not isinstance(entry, dict):
            continue
        etype = entry.get("type")
        if etype == "step":
            label = entry.get("label") or entry.get("name") or "Step"
            for field in entry.get("fields", []) or []:
                if isinstance(field, dict):
                    yield field, label, False
        elif etype == "dynamic_step":
            label = entry.get("label") or entry.get("name") or "Step"
            yield {"label": label, "key": entry.get("name"), "type": "dynamic_step"}, label, True
        else:
            yield entry, "", False


def _secret_keys(plugin_id: str) -> set:
    """Return the manifest field keys to redact.

    Mirrors CE's canonical config redactor ``collect_diagnose.collect_plugin_parameters`` field
    by field — 'password'-type, 'textarea'-type, or a key matching its ``exclude_key_regex`` (minus
    the ``authentication_method22`` carve-out) — so the copilot's fallback redaction never DIVERGES
    from the diagnose-bundle redaction (and any future hardening there benefits both). This is only
    the FALLBACK; ``_redacted_plugin_doc`` calls ``collect_plugin_parameters`` directly on the hot
    path. Do NOT add copilot-specific narrowing here — reuse the canonical rule as-is.
    """
    plugin_cls = plugin_helper.find_by_id(plugin_id)
    if plugin_cls is None:
        return set()
    configuration = (getattr(plugin_cls, "metadata", {}) or {}).get("configuration", [])
    exclude_key_regex = (
        r"^(id_|api_|server_|key_|auth_|host|api|ip_|"
        r".*(_id|_arn|_key|_assigne|_url|_email|_username|_host|_address|_server|_file|_uri|_auth|_ip)$|"
        r".*(hostname|address|servername|uri|server|tenantName|arn|email|id|file|key|auth|username|url|host).*)"
    )

    def _is_secret(field) -> bool:
        ftype = field.get("type")
        key = field.get("key", "")
        # Same predicate as collect_diagnose: password OR textarea OR regex-key (except the carve-out).
        if ftype in ("password", "textarea"):
            return True
        return bool(re.match(exclude_key_regex, key, re.IGNORECASE)) and key != "authentication_method22"

    return {f.get("key") for f, _step, _dyn in _iter_manifest_fields(configuration) if _is_secret(f)}


def _redact_parameters(plugin_id: str, params: dict) -> dict:
    """Blank password-type fields by manifest before returning params to the model.

    Never wraps params in SecretDict (which would resolve ``secret:`` references);
    redaction is purely structural, by manifest field type.
    """
    secrets = _secret_keys(plugin_id)
    return {k: ("***REDACTED***" if k in secrets else v) for k, v in (params or {}).items()}


def _redacted_plugin_doc(doc: dict) -> dict:
    """Return a deep-copied plugin config with secrets stripped, for the model.

    Reuses ``collect_diagnose.collect_plugin_parameters`` — CE's canonical config
    redactor used for support-diagnose bundles. It strips password/textarea and
    regex-matched sensitive keys, recurses into nested values, and (for
    non-receiver plugins) resolves dynamic-step / has_api_call fields to catch
    dynamically-added secret keys — so the copilot redacts exactly like diagnose,
    and any future hardening there benefits the copilot too. ``_secret_keys``
    already shares that utility's ``exclude_key_regex``.

    Caveat: that path may make a live third-party call to resolve dynamic fields.
    On any failure (live fetch error, or a doc without ``_id``/``storage``) we
    fall back to manifest-level blanking via ``_redact_parameters``. The import is
    lazy to keep collect_diagnose's router import chain out of module load.
    """
    from copy import deepcopy

    try:
        from netskope.common.utils.collect_diagnose import collect_plugin_parameters

        result = collect_plugin_parameters(deepcopy(doc))
        if isinstance(result, dict):
            return result
        # A non-dict result is the utility's "plugin unavailable" message string.
    except Exception:
        logger.debug("Encountered error while redacting plugin parameters, using fallback path",
                     details=traceback.format_exc())

    safe = deepcopy(doc)
    safe.pop("_id", None)
    safe.pop("storage", None)
    safe["parameters"] = _redact_parameters(doc.get("plugin", ""), doc.get("parameters", {}))
    return safe


# Business rules (filters/sharing/queues/mappings) have no plugin manifest to redact by field
# TYPE the way ``_redact_parameters``/``_secret_keys`` do — they're operator-authored documents,
# not plugin config. So rule redaction is a plain key-name heuristic: any dict key that looks
# like it holds a credential is blanked wherever it appears, however deeply nested.
_RULE_SECRET_KEY_RE = re.compile(
    r"token|secret|password|api[_-]?key|credential|authorization|webhook.*url", re.IGNORECASE
)
# A rule's human-readable filter ``query`` is a free-text STRING (e.g. "field = value AND ..."),
# not a nested dict, so a key-based walk alone can't catch a secret typed INTO that text (an
# operator embedding a bearer token or webhook URL in a filter value). Scrub matching
# substrings within string values too: "<key>=<value up to a delimiter>" and bare
# http(s) webhook URLs, wherever they occur in a string.
# A labelled secret (`token: …`, `Authorization: Bearer …`) — capture the WHOLE value after the
# label to end-of-line, not just the first whitespace-delimited token, or a `Authorization: Bearer
# <jwt>` shape would leave the jwt in clear text (only "Bearer" scrubbed). Also webhook URLs, incl.
# Slack incoming-webhooks (hooks.slack.com/services/…) which carry no literal "webhook" substring.
_RULE_SECRET_VALUE_RE = re.compile(
    r"(?i)\b(?:token|secret|password|api[_-]?key|credential|authorization)\s*[=:]\s*.+"
    r"|https?://\S*webhook\S*"
    r"|https?://hooks\.slack\.com/\S+"
)


def _redact_rule_secrets(value):
    """Recursively redact secret-looking content from a business-rule document (or list of them).

    Two passes, applied uniformly whether ``value`` is the whole rule dict or a bare string
    pulled out of one (e.g. a ``filters.query`` display string):
    - dict keys matching ``_RULE_SECRET_KEY_RE`` (token/secret/password/api_key/credential/
      authorization/webhook*url) have their value replaced wholesale;
    - string values (dict values, list items, or a standalone string) additionally get any
      ``key=value``/webhook-URL SUBSTRING scrubbed via ``_RULE_SECRET_VALUE_RE``, since a filter
      query is free text where a secret can be embedded without a matching dict key.
    Structure (dict/list shape, non-secret keys/values) is left intact.
    """
    if isinstance(value, dict):
        redacted = {}
        for k, v in value.items():
            if isinstance(k, str) and _RULE_SECRET_KEY_RE.search(k):
                redacted[k] = "***REDACTED***"
            else:
                redacted[k] = _redact_rule_secrets(v)
        return redacted
    if isinstance(value, list):
        return [_redact_rule_secrets(v) for v in value]
    if isinstance(value, str):
        return _RULE_SECRET_VALUE_RE.sub("***REDACTED***", value)
    return value


# Mirrors cte.utils.entity.THREAT_INDICATORS_ENTITY; a CTE rule's `entity` defaults to it and an
# older document omits the field entirely.
_THREAT_INDICATORS_ENTITY = "Threat Indicators"


def _rule_share_destinations(rule: dict) -> set:
    """Destination configurations a CTE rule shares to, across BOTH rule shapes.

    A Threat Indicators rule nests destinations under each source
    (``sharedWith`` = {source: {destination: [actions]}}); a CRE-entity rule has no source
    layer (``creShare`` = {destination: [actions]}) because its source is a CRE entity, not a
    CTE plugin config. The two are mutually exclusive per rule. Counting only ``sharedWith``
    made every fully-configured CRE-entity rule read as sharing to nothing — the same false
    "unwired" reading the attention scan's rule_unwired had.
    """
    destinations = set()
    for dest_map in (rule.get("sharedWith") or {}).values():
        destinations.update(dest_map or {})
    destinations.update(rule.get("creShare") or {})
    return destinations


def _display_value(field: dict, value):
    """Map a stored value to its user-facing display (choice label), else pass through.

    CE manifest choices are ``{"key": "<label shown in the form>", "value": "<stored>"}``;
    we present the label the user actually sees rather than the stored code.
    """
    if not isinstance(field, dict) or field.get("type") not in ("choice", "multichoice"):
        return value
    labels = {c.get("value"): (c.get("key") or c.get("value")) for c in (field.get("choices") or [])}
    if isinstance(value, list):
        return [labels.get(v, v) for v in value]
    return labels.get(value, value)


def _humanize_parameters(plugin_id: str, params: dict) -> list:
    """Render stored parameters as a user-facing ``[{label, value}]`` view (req#7).

    Presents the manifest field LABEL instead of the internal storage key, and the choice
    DISPLAY label instead of the stored choice code — so the copilot echoes what the user
    sees in the form, not backend storage keys/codes (clearer for users, and it avoids
    exposing the storage-layer schema). Secret fields are already redacted/removed upstream.
    Used by the config-read tools; ``get_plugin_schema`` still returns keys (drafts need them).
    """
    plugin_cls = plugin_helper.find_by_id(plugin_id)
    configuration = (getattr(plugin_cls, "metadata", {}) or {}).get("configuration", []) if plugin_cls else []
    field_by_key = {}
    for field, step, _is_dyn in _iter_manifest_fields(configuration):
        key = field.get("key")
        if key:
            field_by_key[key] = (field, step)
    rows = []
    for key, value in (params or {}).items():
        field, step = field_by_key.get(key, ({}, ""))
        row = {"label": field.get("label") or key, "value": _display_value(field, value)}
        if step:
            row["step"] = step
        rows.append(row)
    return rows


# --- business-rule query vocabulary (sourced from the SAME schema the UI uses) ----------
# Operator vocabulary per JSON-schema filter type (the canonical CE filter defs). Keys match
# the "#/definitions/<name>Filters" $refs in INDICATOR_QUERY_SCHEMA (CTE) and the schema
# returned by alert_event_query_schema() (CTO). Logical operators ($and/$or/$nor) wrap clauses.
_FILTER_OPERATORS = {
    "stringFilters": ["$eq", "$ne", "$regex", "$in", "$nin", "$not"],
    "numberFilters": ["$gt", "$gte", "$lt", "$lte", "$eq", "$ne"],
    "dateFilters": ["$gt", "$gte", "$lt", "$lte", "$ne", "$not"],
    "booleanFilters": ["$eq", "$ne"],
    "arrayFilters": ["$in", "$nin"],
}


def _ref_filter_name(spec) -> str:
    """Return the filter-type name from a {'$ref': '#/definitions/<name>'} schema node."""
    ref = spec.get("$ref", "") if isinstance(spec, dict) else ""
    return ref.rsplit("/", 1)[-1] if ref else ""


# filter-type -> the VALUE type the field holds, so the model drafts correctly-TYPED conditions
# (e.g. reputation is a number, firstSeen a date, tags an array) instead of guessing from the name.
# NOTE: the query schemas constrain operator shape + value TYPE only — they carry NO per-field
# value enums (verified across the CTE/CTO/CLS/CFC schemas), so there is no reliable "allowed
# values" list to surface here. CRE is the exception (user-defined entity fields), handled in
# get_cre_entities where an enum/choices, if the field declares one, is passed through.
_FILTER_TYPE_TO_VALUE = {
    "stringFilters": "string",
    "numberFilters": "number",
    "dateFilters": "date",
    "booleanFilters": "boolean",
    "arrayFilters": "array",
}


# CRE entity fields are user-defined, typed by EntityFieldType (crev2/models/entities.py). Map each
# type to the CE FILTER_TYPES family whose Mongo operators ($eq/$gt/$in/...) the copilot uses when
# drafting a rule condition. Keys MUST stay in sync with the EntityFieldType enum — a coverage test
# (test_config_tools) asserts every enum member is mapped, so a NEW type can't silently fall through
# to the string default.
#
# SIBLING (deliberately NOT merged): crev2/routers/records.py::_map_type maps the SAME enum to the
# UI query-builder WIDGET type ("number"/"select"/"datetime"/...) — a different vocabulary (widgets,
# not Mongo operators), so the two can't share a table. They DO encode the same type-grouping
# decision, so keep them consistent by eye (e.g. both treat CALCULATED as numeric); if _map_type
# regroups a type, revisit the row here.
_CRE_TYPE_TO_FILTER = {
    "string": "stringFilters", "email": "stringFilters", "ipv4": "stringFilters",
    "ipv6": "stringFilters", "reference": "stringFilters", "value_map_string": "stringFilters",
    "number": "numberFilters", "value_map_number": "numberFilters", "range_map": "numberFilters",
    "calculated": "numberFilters",
    "datetime": "dateFilters",
    "boolean": "booleanFilters",
    "list": "arrayFilters",
}


def _cre_field_operators(field_type) -> list:
    """Map a CRE EntityFieldType (string) to its valid rule operators (defaults to string ops)."""
    return _FILTER_OPERATORS.get(_CRE_TYPE_TO_FILTER.get(field_type, "stringFilters"),
                                 _FILTER_OPERATORS["stringFilters"])


def _fields_from_props(props: dict, prefix: str = "") -> list:
    """Extract [{field, type, operators}] from a schema 'properties' map (skips logical $-keys).

    ``type`` is the field's VALUE type (string/number/date/boolean/array), derived from the filter
    ``$ref`` — so a drafted condition uses the right value shape. The query schemas hold no per-field
    value enums, so none is emitted (see ``_FILTER_TYPE_TO_VALUE``).

    Flat view (dotted ``prefix`` for one nesting level). Kept for the fallback path and any caller
    that wants the simple list; the STRUCTURAL view below (``_schema_field_tree``) is what the rule
    tools now feed the model so nested/array fields keep their shape.
    """
    rows = []
    for key, spec in (props or {}).items():
        if key.startswith("$"):  # $and/$or/$nor/$expr — logical, not a field
            continue
        filter_name = _ref_filter_name(spec)
        ops = _FILTER_OPERATORS.get(filter_name)
        if ops:
            rows.append({
                "field": f"{prefix}{key}",
                "type": _FILTER_TYPE_TO_VALUE.get(filter_name, "string"),
                "operators": ops,
            })
    return rows


# Bound the structural walk so a self-referential definition (e.g. CTE's ``retractionResult`` refs
# itself through $and/$or) can't recurse forever, and the emitted tree can't balloon the prompt.
_SCHEMA_TREE_MAX_DEPTH = 3


def _resolve_ref(spec: dict, defs: dict) -> dict:
    """Resolve a ``{'$ref': '#/definitions/<name>'}`` node to its definition dict (else the node)."""
    if not isinstance(spec, dict):
        return {}
    name = _ref_filter_name(spec)
    if name and name in (defs or {}):
        return defs[name]
    return spec


def _schema_field_tree(props: dict, defs: dict, depth: int = 0) -> list:
    """Build a COMPACT, STRUCTURAL field tree from a schema 'properties' map.

    Unlike ``_fields_from_props`` (which flattens nested objects to a dotted name and drops their
    shape), this keeps the schema's structure so the agent sees fields, operators AND nesting:
      - a leaf filter field  -> {"field", "type", "operators"}
      - a nested object field -> {"field", "kind": "object", "fields": [...children...]}
      - an array-of-object    -> {"field", "kind": "array", "itemFields": [...children...]}
    ``$ref`` is resolved against ``defs``; logical $-keys ($and/$or/$nor/$expr) are omitted (they are
    documented once as ``logicalOperators`` on the spec, not per field). Bounded by
    ``_SCHEMA_TREE_MAX_DEPTH`` so self-referential defs terminate and the payload stays small.
    """
    rows = []
    if depth > _SCHEMA_TREE_MAX_DEPTH:
        return rows
    for key, spec in (props or {}).items():
        if key.startswith("$"):  # logical operator, not a field
            continue
        filter_name = _ref_filter_name(spec)
        ops = _FILTER_OPERATORS.get(filter_name)
        if ops:  # a leaf filter field (string/number/date/boolean/array)
            rows.append({
                "field": key,
                "type": _FILTER_TYPE_TO_VALUE.get(filter_name, "string"),
                "operators": ops,
            })
            continue
        # Not a leaf filter — resolve it and see whether it is a nested object or array-of-object.
        target = _resolve_ref(spec, defs)
        # array of objects: {"type": "array", "items": {"$ref": ...}}
        if isinstance(spec, dict) and spec.get("type") == "array" and isinstance(spec.get("items"), dict):
            item = _resolve_ref(spec["items"], defs)
            children = _schema_field_tree(item.get("properties", {}), defs, depth + 1)
            if children:
                rows.append({"field": key, "kind": "array", "itemFields": children})
            continue
        nested_props = target.get("properties") if isinstance(target, dict) else None
        # $elemMatch wrapper (e.g. CTE ``sources`` -> sourcesFilters {$elemMatch: sourceFilters}):
        # the field is an ARRAY of objects; descend through $elemMatch into the element schema so
        # its sub-fields (reputation/severity/tags/...) are surfaced, not dropped as a $-key.
        if isinstance(nested_props, dict) and "$elemMatch" in nested_props:
            item = _resolve_ref(nested_props["$elemMatch"], defs)
            children = _schema_field_tree(item.get("properties", {}), defs, depth + 1)
            if children:
                rows.append({"field": key, "kind": "array", "itemFields": children})
            continue
        # plain nested object with its own properties (directly or via $ref)
        if nested_props:
            children = _schema_field_tree(nested_props, defs, depth + 1)
            if children:
                rows.append({"field": key, "kind": "object", "fields": children})
    return rows


# The rule-query schema constants live under netskope.integrations.<mod>.utils, which in turn
# import from netskope.common.utils — so a MODULE-TOP import here would risk a circular import at
# load (this file is itself imported lazily by the copilot router). We therefore import each schema
# lazily but ONCE, memoized in ``_SCHEMA_CACHE`` keyed by module, so ``_rule_fields_for`` does not
# re-run the import on every call (the "redundant imports" cleanup) while staying cycle-safe.
_SCHEMA_CACHE: dict = {}


def _load_rule_schema(module: str):
    """Lazily import + cache the raw query schema for a module (cycle-safe, imported once).

    Returns the FULL assembled JSON-schema dict (``definitions`` + ``searchRoot``) for cte/cto/cfc,
    so all three go through the same ``_schema_field_tree`` structural walk. For cto the third
    element of ``alert_event_query_schema()`` IS that assembled schema (same shape as CTE's
    INDICATOR_QUERY_SCHEMA — we use it rather than re-flattening the static/raw prop dicts). cls
    returns the deployment-learned properties map (flat, dynamic — NOT cached, re-read each call).
    """
    if module == "cls":
        from netskope.common.utils import get_database_fields_schema
        return get_database_fields_schema()
    if module in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[module]
    if module == "cte":
        from netskope.integrations.cte.utils.schema import INDICATOR_QUERY_SCHEMA
        _SCHEMA_CACHE[module] = INDICATOR_QUERY_SCHEMA
    elif module == "cto":
        from netskope.integrations.itsm.utils.schemas import alert_event_query_schema
        # Use the assembled query schema (3rd return) — it carries definitions + searchRoot exactly
        # like CTE, so CTO reuses the same structural walk instead of a flat static/raw merge.
        _SCHEMA_CACHE[module] = alert_event_query_schema()[2]
    elif module == "cfc":
        from netskope.integrations.cfc.utils.schema import IMAGE_METADATA_QUERY_SCHEMA
        _SCHEMA_CACHE[module] = IMAGE_METADATA_QUERY_SCHEMA
    else:
        _SCHEMA_CACHE[module] = None
    return _SCHEMA_CACHE[module]


def _rule_fields_for(module: str) -> list:
    """Return the REAL, STRUCTURAL field vocabulary for a module's rule query.

    Feeds the agent the FULL schema shape — fields, per-field value ``type`` + ``operators``, AND
    nesting (nested objects and arrays-of-objects keep their child fields) — sourced from the SAME
    schema the UI query builder validates against, so a drafted rule uses correct, correctly-typed,
    correctly-nested fields. Uses ``_schema_field_tree`` (compact structural tree, depth-bounded so
    self-referential defs terminate and the payload can't balloon) rather than flattening. Per module:
    - CTE: ``INDICATOR_QUERY_SCHEMA`` searchRoot (incl. the nested ``sources`` array of source fields).
    - CTO: ``alert_event_query_schema()`` (static + deployment-dynamic ``rawData_*`` — flat by nature).
    - CLS: ``get_database_fields_schema()`` (learned Netskope fields — deployment-dynamic, flat; empty
      on a fresh install until fields are learned).
    - CFC: ``IMAGE_METADATA_QUERY_SCHEMA`` searchRoot (file/path/extension/sourceType/... fields).
    - CRE: returns [] — rule fields are DYNAMIC per-entity (Schema Editor); the format tool points
      the caller at get_cre_entities to resolve the chosen entity's fields instead.
    - EDM: returns [] — EDM has no filter rules (its "rule" is 1:1 sharing).
    Falls back to a minimal set on any import/DB failure so the tool never breaks the turn.
    """
    try:
        schema = _load_rule_schema(module)
        # cte/cto/cfc all expose the SAME assembled-schema shape (definitions + searchRoot), so they
        # share one structural walk — CTO's fields are flat today, but routing it through the tree
        # keeps one code path and auto-handles any future nesting.
        if module in ("cte", "cto", "cfc"):
            defs = (schema or {}).get("definitions", {})
            root = defs.get("searchRoot", {}).get("properties", {})
            return _schema_field_tree(root, defs)
        if module == "cls":
            # CLS is the deployment-learned flat properties map (no definitions/searchRoot wrapper).
            return _fields_from_props(schema or {})
        # cre (dynamic per-entity) / edm (no filter rules): no static vocabulary here.
        return []
    except Exception:
        logger.debug("Rule-field schema lookup fell back.", details=traceback.format_exc())
        return [{"field": "type", "type": "string", "operators": _FILTER_OPERATORS["stringFilters"]}]


# --- guided plugin-walkthrough helpers (grounded in the real manifest taxonomy) ----------
# The synthetic "Basic Information" step the FORM injects (the manifest does not contain it) is
# built PER-PLUGIN from manifest capability flags, gated EXACTLY as the UI's BasicInformation
# form gates each field — so a third-party plugin never has a Netskope-only knob suggested for
# it, and the Sync Interval label matches the form. Sources of truth (keep labels in sync):
#   CTE  src/components/Plugins/CteStepperForm/BasicInformation.jsx
#   CTO  src/components/Integrations/Itsm/.../WizardPluginForm/BasicInformation.jsx
# Manifest gates (the same flags the plugin-list API maps to camelCase for the UI):
#   `netskope`          -> the Tenant selector (both modules); "Sharing " label prefix (CTE)
#   `sharing_supported` -> the CTO "Update Incidents back to the Netskope Tenant" toggle +
#                          the "Sharing Sync Interval" label (CTO)
# Only Netskope-family plugins (currently just netskope_itsm / the Netskope CTE source) set
# these, so third-party plugins get just Configuration Name + Sync Interval (+ CTE's core knobs).
def _basic_info_fields(module: str, meta: dict) -> list:
    """Build the Basic Information fields for a plugin, gated by its manifest capability flags.

    Each module's BasicInformation wizard step gates its optional fields on the SAME manifest
    flags the UI form does (verified per module, 2026-07-22):
      CTE  netskope -> "Sharing " label + Tenant; core knobs always.
      CTO  sharing_supported -> "Update Incidents" toggle + "Sharing Sync Interval"; netskope -> Tenant.
      CLS  pull_supported&&!netskope -> Pull Interval; !netskope&&push_supported -> inline Mapping+Format;
           netskope -> Tenant.
      CRE  !netskope -> Sync Interval (netskope plugins are fixed to a 1h pull); netskope -> Tenant.
      EDM  !netskope -> Sync Interval; netskope -> Plugin Type (forwarder/receiver) + SSL + Tenant
           (Tenant only when the manifest also has provider_id).
      CFC  !netskope -> Sync Interval; netskope -> SSL + Tenant.
    Third-party (non-netskope) plugins therefore get Configuration Name + Sync/Pull Interval only
    (plus CTE's core knobs / CLS's mapping) — a Netskope-only knob is never suggested for them.
    """
    netskope = bool(meta.get("netskope"))
    sharing = bool(meta.get("sharing_supported"))
    pull_supported = bool(meta.get("pull_supported"))
    push_supported = bool(meta.get("push_supported"))
    fields = [{"label": "Configuration Name", "key": "name", "type": "text", "mandatory": True}]
    if module == "cte":
        # if netskope then the tenant selection as well and skip ssl validation toggle for netskope,
        fields.append({
            "label": "Sharing Sync Interval" if netskope else "Sync Interval",
            "key": "pollInterval", "type": "number", "mandatory": True,
            "description": ("Netskope also pulls IoCs from malware/malsite alerts every 30 seconds "
                            "regardless of this interval.") if netskope else None,
        })
        fields += [
            {"label": "Indicator Aging Criteria", "key": "ageAfterDays", "type": "number", "mandatory": False},
            {"label": "Enable SSL verification", "key": "sslValidation", "type": "boolean", "mandatory": False},
            {"label": "Override Reputation", "key": "defaultReputation", "type": "number", "mandatory": False},
            {"label": "Tags Aggregate Strategy", "key": "tagsAggregateStrategy", "type": "choice",
             "mandatory": False, "choices": ["Append", "Overwrite"]},
        ]
    elif module == "cto":
        # if netskope then the tenant selection as well
        fields.append({
            "label": "Sharing Sync Interval" if sharing else "Sync Interval",
            "key": "pollInterval", "type": "number", "mandatory": True,
        })
        if sharing:
            # UI: WizardPluginForm/BasicInformation.jsx renders this toggle only when
            # manifest.sharingSupported — third-party ticketing plugins never see it.
            fields.append({
                "label": "Update Incidents back to the Netskope Tenant", "key": "updateIncidents",
                "type": "boolean", "mandatory": False,
                "description": "Sharing-capable plugins only: syncs ticket status/assignee/severity "
                               "back to the source incident on the Netskope tenant.",
            })
    elif module == "cls":
        if pull_supported and not netskope:
            fields.append({"label": "Pull Interval", "key": "pollInterval", "type": "number", "mandatory": True})
        if not netskope and push_supported:
            # Inline on the Basic Information step (NOT a separate wizard step): the mapping file
            # + the CEF/JSON transform that shape the forwarded log.
            fields += [
                {"label": "Mapping", "key": "attributeMapping", "type": "choice", "mandatory": True,
                 "description": "The attribute-mapping file that transforms CE fields to the SIEM's "
                                "format. Author custom mappings under Settings > Log Shipper > Mapping."},
                {"label": "Format", "key": "transformData", "type": "choice", "mandatory": True,
                 "choices": ["CEF", "JSON"], "description": "The wire format for the forwarded log."},
            ]
    elif module == "cre":
        if not netskope:
            fields.append({"label": "Sync Interval", "key": "pollInterval", "type": "number", "mandatory": True,
                           "description": "Netskope-tenant CRE plugins pull on a fixed 1-hour interval "
                                          "(no interval field); third-party plugins set it here."})
    elif module == "edm":
        if not netskope:
            fields.append({"label": "Sync Interval", "key": "time", "type": "number", "mandatory": True})
        if netskope:
            fields.append({
                "label": "Plugin Type", "key": "pluginType", "type": "choice", "mandatory": True,
                "choices": ["forwarder", "receiver"],
                "description": "Netskope EDM forwarder/receiver only. A 'receiver' has NO downstream "
                               "configuration steps (it ingests pre-generated hashes over CE-to-CE).",
            })
            fields.append({"label": "Enable SSL verification", "key": "sslValidation",
                           "type": "boolean", "mandatory": False})
    elif module == "cfc":
        if not netskope:
            fields.append({"label": "Sync Interval", "key": "time", "type": "number", "mandatory": True})
        if netskope:
            fields.append({"label": "Enable SSL verification", "key": "sslValidation",
                           "type": "boolean", "mandatory": False})
    # Tenant selector: Netskope-tenant plugins only (EDM additionally requires a manifest provider_id).
    if netskope and (module != "edm" or bool(meta.get("provider_id"))):
        fields.append({
            "label": "Tenant", "key": "tenant", "type": "choice", "mandatory": True,
            "description": "Netskope-family plugins only: select the configured Netskope tenant.",
        })
    return fields


# Modules whose UI wizard renders manifest config as ONE flattened PluginDynamicForm step
# ("Configuration Parameters"), vs per-manifest-step. get_plugin_walkthrough mirrors this so its
# step NAMES align with the form's currentStep/pageState.stepName. (CTE=CtePluginForm flatten;
# CLS/CRE=WizardPluginForm flatten; CRE also appends "Entity Sources". CTO/EDM/CFC=per step.)
_FLATTEN_MODULES = {"cte", "cls", "cre"}

_AUTH_LABEL_RE = re.compile(r"auth|login|credential|connect|token|api key", re.IGNORECASE)
_MAPPING_NAME_RE = re.compile(r"mapping|incident_update", re.IGNORECASE)
# Static per-kind guidance note (deterministic + offline-testable; vendor specifics come from
# get_plugin_guide + web_search, not baked here).
_STEP_KIND_NOTE = {
    "basics": "Name the configuration and set the Sync Interval / Indicator Aging Criteria. For "
              "Netskope-tenant plugins, select the Tenant first.",
    "auth": "Enter the credentials/connection details. You fill secrets — the copilot never sees them. "
            "These are verified live when you proceed (CTO) or Save (CTE); if it fails, ask to troubleshoot.",
    "params": "Tune the pull parameters (data types, filters, batch size) per best practice; narrow the pull "
              "to only what your sharing/ticket rules need.",
    "dynamic": "This step's options load from the third party only AFTER the previous step's credentials "
               "validate — fill credentials first, then choose here. The copilot can't pre-list these options.",
    "mapping": "Map Cloud Exchange fields to the destination's fields. Unmapped required destination fields "
               "will block ticket creation.",
    "entity": "Map each plugin field to a Cloud Risk Exchange ENTITY field (e.g. Users, Applications). "
              "Leaving an entity unmapped pulls nothing for it but still lets actions run.",
}


def _classify_step_kind(name: str, label: str, fields: list, is_dynamic: bool) -> str:
    """Classify a manifest step as basics/auth/params/dynamic/mapping (grounded taxonomy)."""
    if is_dynamic:
        return "dynamic"
    text = f"{name or ''} {label or ''}"
    if _MAPPING_NAME_RE.search(text):
        return "mapping"
    secret_count = sum(1 for f in (fields or []) if isinstance(f, dict) and f.get("type") == "password")
    if (name or "").lower() == "auth" or _AUTH_LABEL_RE.search(text) or (
        fields and secret_count >= max(1, len(fields) // 2)
    ):
        return "auth"
    return "params"


def _walkthrough_field(field: dict) -> dict:
    """Project a manifest field to the walkthrough's flagged shape (no secret values)."""
    ftype = field.get("type")
    return {
        "label": field.get("label") or field.get("key"),
        "key": field.get("key"),
        "type": ftype,
        "mandatory": field.get("mandatory", False),
        "default": field.get("default"),
        "choices": field.get("choices"),
        "secret": ftype == "password",
        "dynamic": ftype == "dynamic_step",
        "hasApiCall": bool(field.get("has_api_call")),
        "description": field.get("description"),
    }


# --- per-turn context (ContextVar) --------------------------------------------
# The tools are module-level SINGLETONS (so agent graphs can be cached and are not rebuilt
# per turn). Per-turn state — the caller's scopes, per-tool budget counters, and the page
# being viewed — rides on a ContextVar set once per turn by ``copilot_turn_context`` (in
# ``run_copilot_turn``). Verified to propagate through ``astream_events`` tool execution and
# to stay isolated across concurrent turns (asyncio tasks each carry their own context), so
# no per-call context threading is needed. Replaces the old closure-captured ``state``.
@dataclass
class CopilotTurnContext:
    """State for one copilot turn, shared across every tool call in that turn."""

    scopes: frozenset = dc_field(default_factory=frozenset)
    calls: dict = dc_field(default_factory=dict)  # per-tool call counters (budget caps)


_TURN: "contextvars.ContextVar[Optional[CopilotTurnContext]]" = contextvars.ContextVar(
    "copilot_turn", default=None
)


def _current() -> CopilotTurnContext:
    """Return the active turn context, or a deny-everything empty one if none is set.

    A missing context means a tool ran outside ``copilot_turn_context`` (shouldn't happen in
    production): empty scopes make every scope check fail closed, so we never leak data.
    """
    ctx = _TURN.get()
    return ctx if ctx is not None else CopilotTurnContext()


@contextlib.contextmanager
def copilot_turn_context(scopes=None):
    """Bind a fresh ``CopilotTurnContext`` for the duration of one turn (set + reset)."""
    ctx = CopilotTurnContext(scopes=frozenset(scopes or ()))
    token = _TURN.set(ctx)
    try:
        yield ctx
    finally:
        _TURN.reset(token)


def _scopes() -> frozenset:
    """Return the caller's security scopes for the active turn."""
    return _current().scopes


def _allowed(module: str) -> bool:
    """Return whether the caller may read ``module`` data (any CE module), by scope."""
    return MODULE_READ_SCOPE.get(module) in _current().scopes


def _budget(name: str, cap: int) -> bool:
    """Consume one call from ``name``'s per-turn budget; False once the cap is hit."""
    calls = _current().calls
    used = calls.get(name, 0)
    if used >= cap:
        return False
    calls[name] = used + 1
    return True


def _deny(module: str) -> str:
    """Build the standard access-denied string (never returns data)."""
    return f"Access denied: you don't have read access to the '{module}' module."


# --- health/dashboard helpers (module-level; used by the tools below) ----------
def _monitoring_status() -> dict:
    """Return the platform monitoring payload (RabbitMQ queues + MongoDB/HA status).

    Reuses the SAME handler that powers the System Health page's /api/monitoring endpoint
    (``dashboard.get_status``) instead of re-issuing the RabbitMQ call by hand — so the copilot
    interprets exactly what the monitoring page shows (queue depth/bytes AND mongo cluster/HA
    node status), and there's one source of that logic. ``get_status`` is a SYNC handler that
    returns a ``JSONResponse``; decode its body to the dict. Same route-reuse pattern the
    migrations (and ``collect_diagnose``) use to call route logic outside FastAPI.
    """
    import json

    from netskope.common.api.routers.dashboard import get_status

    resp = get_status(user=_turn_user())
    return json.loads(resp.body)


def _system_health_payload() -> dict:
    """Build the system-health summary (cert expiry + queue/mongo status); no gating.

    certExpiry stays a precise single-field projection: reusing ``read_settings`` here would
    build the entire (scope-filtered) settings payload just to pull one field — a wasteful read
    for no gain, since certExpiry is a plain top-level Settings field.
    """
    doc = connector.collection(Collections.SETTINGS).find_one({}, {"certExpiry": 1, "_id": 0}) or {}
    out = {"certExpiry": doc.get("certExpiry")}
    try:
        status = _monitoring_status()
        out["queues"] = (status.get("rabbitmq") or {}).get("queues", [])
        out["mongodb"] = status.get("mongodb")
    except Exception:
        out["queues"] = "unavailable"
    return out


# --- tools (module-level singletons) ------------------------------------------
# --- config introspection ---------------------------------------------
@tool
def list_configurations(module: str) -> str:
    """List configured plugins for a module (no parameter values).

    Args:
        module: one of 'cte' (Threat Exchange), 'cto' (Ticket Orchestrator), 'cls' (Log Shipper),
            'cre' (Risk Exchange), 'edm' (Exact Data Match), 'cfc' (Custom File Classification).
    """
    if module not in _MODULE_COLLECTION:
        return f"Unknown module '{module}'. Use one of: cte, cto, cls, cre, edm, cfc."
    if not _allowed(module):
        return _deny(module)
    if not _budget("list_configurations", _CAP_LIST):
        return "Budget for listing configurations is exhausted for this turn."
    try:
        docs = connector.collection(_MODULE_COLLECTION[module]).find(
            {}, {"name": 1, "plugin": 1, "active": 1, "pollInterval": 1, "pollIntervalUnit": 1, "_id": 0}
        ).limit(_MAX_LIST_DOCS)
        rows = list(docs)
        if not rows:
            return f"No {module} configurations found."
        return _wrap(f"{module}_configurations", _capped(rows, _MAX_LIST_DOCS))
    except Exception:
        logger.error(f"Error listing {module} configurations.", details=traceback.format_exc())
        return f"Error listing {module} configurations. See CE logs for details."


@tool
def get_configuration_details(module: str, name: str) -> str:
    """Get one configuration's details with secret parameters redacted.

    Args:
        module: one of 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc'.
        name: the configuration name.
    """
    if module not in _MODULE_COLLECTION:
        return f"Unknown module '{module}'. Use one of: cte, cto, cls, cre, edm, cfc."
    if not _allowed(module):
        return _deny(module)
    if not _budget("get_configuration_details", _CAP_DETAIL):
        return "Budget for configuration detail lookups is exhausted for this turn."
    try:
        doc = connector.collection(_MODULE_COLLECTION[module]).find_one({"name": name})
        if not doc:
            return f"No {module} configuration named '{name}'."
        plugin_id = doc.get("plugin", "")
        redacted = _redacted_plugin_doc(doc)
        summary = {
            "name": redacted.get("name"),
            "plugin": plugin_id,
            "pluginName": redacted.get("pluginName"),
            "active": redacted.get("active"),
            "pollInterval": redacted.get("pollInterval"),
            "pollIntervalUnit": redacted.get("pollIntervalUnit"),
            "checkpoint": redacted.get("checkpoint"),
            "lastRunAt": redacted.get("lastRunAt"),
            "lastRunSuccess": redacted.get("lastRunSuccess"),
            # User-facing {label, value} view (form labels + choice display labels),
            # not internal storage keys/codes (req#7).
            "settings": _humanize_parameters(plugin_id, redacted.get("parameters", {})),
        }
        return _wrap(f"{module}_configuration", summary)
    except Exception:
        logger.error(f"Error getting {module} configuration {name}", details=traceback.format_exc())
        return f"Error getting {module} configuration '{name}'. See CE logs for details."


@tool
def list_available_plugins(module: str) -> str:
    """List plugins available to configure for a module or plugin class (id, name, version, description).

    Does NOT include push/pull/receiving capabilities — call get_plugin_capabilities(module,
    plugin_ids) for that, once specific plugin(s) are being considered.

    Args:
        module: one of the six CE modules — 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc' — OR a
            non-module plugin class: 'provider' (Netskope Tenant connectivity plugins) or
            'llm_provider' (AI providers such as Anthropic).
    """
    entry = _AVAILABLE_PLUGIN_CLASSES.get(module)
    if entry is None:
        return ("Unknown value '{}'. Use one of: cte, cto, cls, cre, edm, cfc, provider, "
                "llm_provider.").format(module)
    bucket, scope = entry
    if scope not in _scopes():
        return f"Access denied: you don't have '{scope}' to list available '{module}' plugins."
    if not _budget("list_available_plugins", _CAP_PLUGINS):
        return "Budget for listing available plugins is exhausted for this turn."
    try:
        classes = plugin_helper.plugins.get(bucket, [])
        rows = []
        for cls in classes:
            meta = getattr(cls, "metadata", {}) or {}
            rows.append({
                "id": getattr(cls, "__module__", meta.get("id")),
                "name": meta.get("name"),
                "version": meta.get("version"),
                "description": meta.get("description"),
            })
        if not rows:
            return f"No available plugins found for {module}."
        # Plugin descriptions are vendor-authored free text with no length bound, so a plugin
        # catalog can exceed _wrap()'s downstream byte truncation — which cuts the ALREADY-
        # SERIALIZED JSON string mid-object with no partial-view signal. Cap by cumulative byte
        # size on a row boundary instead, with headroom reserved for the <config_data> tags and
        # (if triggered) the _note/items wrapper, so the emitted JSON is always valid.
        return _wrap(f"{module}_available_plugins", _capped_by_bytes(rows, budget=_MAX_FIELD_BYTES - 512))
    except Exception:
        logger.error(f"Error listing available {module} plugins", details=traceback.format_exc())
        return f"Error listing available {module} plugins. See CE logs for details."


@tool
def get_plugin_capabilities(module: str, plugin_ids: List[str]) -> str:
    """Get push/pull/receiving capabilities (plus name/description) for up to 4 plugins per call.

    Args:
        module: one of the six CE modules — 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc' (from
            list_available_plugins). Not valid for 'provider'/'llm_provider' — those manifests
            carry no capability flags.
        plugin_ids: up to 4 plugin fully-qualified ids (from list_available_plugins).
    """
    if module in _NON_MODULE_PLUGIN_CLASSES:
        return f"Capabilities are not applicable for '{module}' plugins."
    entry = _AVAILABLE_PLUGIN_CLASSES.get(module)
    if entry is None:
        return f"Unknown module '{module}'. Use one of: cte, cto, cls, cre, edm, cfc."
    _bucket, scope = entry
    if scope not in _scopes():
        return f"Access denied: you don't have '{scope}' to read '{module}' plugin capabilities."
    if not plugin_ids:
        return "No plugin_ids provided."
    if len(plugin_ids) > _MAX_PLUGIN_CAPS_BATCH:
        return f"Too many plugin_ids — pass at most {_MAX_PLUGIN_CAPS_BATCH} per call."
    # Charge the budget once PER plugin_id (not once per call) so a batched call can't buy more
    # find_by_id lookups per turn than the cap allows — a miss triggers a full plugin-registry
    # refresh(), so the per-lookup cost is the same whether it arrives batched or one at a time.
    for _ in plugin_ids:
        if not _budget("get_plugin_capabilities", _CAP_PLUGIN_CAPS):
            return "Budget for plugin capability lookups is exhausted for this turn."
    results = []
    for plugin_id in plugin_ids:
        try:
            plugin_cls = plugin_helper.find_by_id(plugin_id)
            if plugin_cls is None:
                results.append({"id": plugin_id, "error": f"Plugin '{plugin_id}' not found."})
                continue
            meta = getattr(plugin_cls, "metadata", {}) or {}
            results.append({
                "id": plugin_id,
                "name": meta.get("name"),
                "description": meta.get("description"),
                "pushSupported": meta.get("push_supported"),
                "pullSupported": meta.get("pull_supported"),
                "receivingSupported": meta.get("receiving_supported"),
            })
        except Exception:
            logger.error(f"Error getting capabilities for plugin {plugin_id}.", details=traceback.format_exc())
            results.append({"id": plugin_id, "error": "Error getting capabilities. See CE logs for details."})
    return _wrap(f"{module}_plugin_capabilities", results)


@tool
def get_plugin_schema(module: str, plugin_id: str, current: Optional[dict] = None) -> str:
    """Get a plugin's configuration fields, grouped by step, with dynamic/secret flags.

    Static fields can be prefilled. Fields flagged dynamic populate live from
    the third party only after credentials are entered (so they can't be
    prefilled). Secret fields are filled by the user, never the copilot.

    Args:
        module: one of 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc'.
        plugin_id: the plugin's fully-qualified id (from list_available_plugins).
        current: optional partial config; if given (and it includes credentials),
            live dynamic-field options are resolved too.
    """
    if module not in _MODULE_INTEGRATION:
        return f"Unknown module '{module}'. Use one of: cte, cto, cls, cre, edm, cfc."
    if not _allowed(module):
        return _deny(module)
    if not _budget("get_plugin_schema", _CAP_SCHEMA):
        return "Budget for plugin schema lookups is exhausted for this turn."
    try:
        plugin_cls = plugin_helper.find_by_id(plugin_id)
        if plugin_cls is None:
            return f"Plugin '{plugin_id}' not found."
        configuration = (getattr(plugin_cls, "metadata", {}) or {}).get("configuration", [])
        fields = []
        for field, step, is_dynamic_step in _iter_manifest_fields(configuration):
            fields.append({
                "step": step,
                "key": field.get("key"),
                "label": field.get("label"),
                "type": field.get("type"),
                "mandatory": field.get("mandatory", False),
                "default": field.get("default"),
                "choices": field.get("choices"),
                "description": field.get("description"),
                "secret": field.get("type") == "password",
                "dynamic": is_dynamic_step,
            })
        result = {"plugin_id": plugin_id, "fields": fields}
        if current:
            try:
                result["dynamic_fields"] = get_dynamic_fields_from_plugin(plugin_id, current)
            except Exception:
                result["dynamic_fields_note"] = (
                    "Could not resolve live dynamic fields — they populate in the form once "
                    "valid credentials are entered."
                )
        return _wrap(f"{module}_plugin_schema", result)
    except Exception:
        logger.error(f"Error getting plugin schema for {plugin_id}.", details=traceback.format_exc())
        return f"Error getting plugin schema for '{plugin_id}'. See CE logs for details."


@tool
def get_business_rules(module: str) -> str:
    """List a module's business rules (names, filters, targets).

    Args:
        module: one of 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc'.
    """
    if module not in _MODULE_BUSINESS_RULES:
        return f"Unknown module '{module}'. Use one of: cte, cto, cls, cre, edm, cfc."
    if not _allowed(module):
        return _deny(module)
    if not _budget("get_business_rules", _CAP_RULES):
        return "Budget for business-rule lookups is exhausted for this turn."
    try:
        docs = list(
            connector.collection(_MODULE_BUSINESS_RULES[module]).find({}, {"_id": 0}).limit(_MAX_LIST_DOCS)
        )
        if not docs:
            return f"No {module} business rules found."
        # Operator-authored rule documents (filters/sharing/queues) may embed a token or
        # webhook URL in free text — redact before these reach the model.
        docs = [_redact_rule_secrets(doc) for doc in docs]
        return _wrap(f"{module}_business_rules", _capped(docs, _MAX_LIST_DOCS))
    except Exception:
        logger.error(f"Error getting {module} business rules.", details=traceback.format_exc())
        return f"Error getting {module} business rules. See CE logs for details."


@tool
def get_business_rule_format(module: str) -> str:
    """Return the canonical business-rule format for a module, to draft a correctly-shaped rule.

    Use before suggesting/drafting a business rule so the suggestion matches the VISUAL query
    builder the form actually uses: the available filter FIELDS and, per field, its value ``type``
    (string/number/date/boolean/array — so a condition's value is correctly typed) and valid
    OPERATORS, a field+operator+value ``conditionsExample``, and the module's routing/targeting
    (all on SEPARATE screens — CTE Sharing / CTO Queues / CLS Log Delivery / CRE Actions / CFC
    Sharing). The rule form has NO query-string or MongoDB input — never suggest one; build the
    filter only from the returned fields/operators, honoring each field's ``type``. The query
    schemas carry no fixed value lists, so match a value to the field's ``type`` (and to the
    knowledge pack when a field has conventional values). Pair with get_plugin_prefilters to keep
    the rule's scope aligned with what the plugin pulls. NOTE: EDM has no filter rules (1:1 sharing)
    and CRE rule fields are per-entity/dynamic (call get_cre_entities first — its fields carry
    ``type`` and, when the entity defines one, ``values``).

    Args:
        module: one of 'cte', 'cto', 'cls', 'cre', 'cfc' (EDM has no filter rules — returns a note).
    """
    if module not in _MODULE_BUSINESS_RULES:
        return f"Unknown module '{module}'. Use one of: {', '.join(sorted(_MODULE_BUSINESS_RULES))}."
    if not _allowed(module):
        return _deny(module)
    if not _budget("get_business_rule_format", _CAP_RULES):
        return "Budget for business-rule-format lookups is exhausted for this turn."
    if module == "edm":
        # EDM has no filter business rules — its "rule" is a 1:1 source->dest sharing.
        return _wrap("edm_business_rule_format", {
            "module": "edm",
            "note": "EDM has NO filter business rules. Its 'rule' is a 1:1 source->destination "
                    "SHARING created on the Sharing screen (one source config shares its EDM "
                    "hashes to one destination config). There is no filter query to draft — use "
                    "get_edm_sharing to inspect the configured sharing pairs.",
        })
    # Real applicable fields + per-field operators, sourced from the same query schema the
    # UI builder validates against (req: suggested rules use correct field names/operators).
    rule_fields = _rule_fields_for(module)
    logical_operators = ["$and", "$or", "$nor", "$not"]
    if module == "cte":
        spec = {
            "module": "cte",
            "requiredKeys": ["name", "filters", "sharedWith"],
            "optionalKeys": ["exceptions", "muted", "unmuteAt"],
            "fields": rule_fields,
            "fieldsSchema": (
                "'fields' is a STRUCTURAL tree. A leaf field is {field, type, operators}. A field "
                "with 'kind':'object' has nested 'fields'; 'kind':'array' has 'itemFields' (a "
                "per-item object, e.g. 'sources' — the per-source-feed fields like reputation, "
                "severity, tags). Reference a nested field by its path (e.g. a source's reputation "
                "is 'sources.reputation'); use the LEAF's own type + operators for the condition."
            ),
            "logicalOperators": logical_operators,
            "targeting": {
                "sharedWith": "Which source feeds share to which destination, and the share action. "
                "This is NOT edited on the Business Rules page — the Business Rules page has ONLY the "
                "filter. The source->destination wiring is created on a SEPARATE 'Sharing' screen "
                "(Threat Exchange > Sharing). To check whether a rule delivers anywhere, send the user "
                "to the Sharing screen (or use get_business_rules, whose sharesToConfigs count reflects "
                "it) — never tell them to inspect a sharing field on the rule.",
                "creShare": "A CTE rule targets EITHER the native 'Threat Indicators' data or a CRE "
                "ENTITY (its `entity` field says which; absent/'Threat Indicators' means the former). "
                "A CRE-entity rule reads that entity's records instead of indicators, so it has no "
                "source feed and its sharing lives in `creShare` = {destination: [actions]} with NO "
                "source layer. The two fields are mutually exclusive on a rule. A CRE-entity rule with "
                "`creShare` set IS fully wired — never call it unshared just because `sharedWith` is "
                "empty. Both are configured on the same Threat Exchange > Sharing screen.",
            },
            # A CONDITIONS example (what the visual builder actually holds) — NOT a query/mongo string.
            # Each condition is {field, operator, value}; combine rows with the logical operators.
            "conditionsExample": {
                "name": "Share high-confidence URLs to firewall",
                "match": "all",  # all = AND, any = OR
                "conditions": [
                    {"field": "type", "operator": "$in", "value": ["url"]},
                    {"field": "externalHits", "operator": "$gte", "value": 5},
                ],
            },
        }
    elif module == "cto":
        spec = {
            "module": "cto",
            "requiredKeys": ["name", "filters", "queues"],
            "optionalKeys": ["dedupeRules", "muteRules", "muted", "unmuteAt"],
            "fields": rule_fields,
            "fieldsSchema": (
                "'fields' is a STRUCTURAL tree: a leaf is {field, type, operators}; a "
                "'kind':'object'/'array' node carries nested 'fields'/'itemFields'. CTO fields are "
                "flat today (static alert/event fields + deployment-dynamic rawData_* fields), so "
                "expect leaves — but reference any nested field by its path and use the leaf's own "
                "type + operators."
            ),
            "logicalOperators": logical_operators,
            "targeting": {
                "queues": "Which ticketing config/queue receives a ticket, and the field mappings — set "
                "on the SEPARATE Queues screen, not the rule form.",
            },
            "conditionsExample": {
                "name": "Open tickets for malware alerts",
                "match": "all",
                "conditions": [
                    {"field": "alertType", "operator": "$eq", "value": "Malware"},
                    {"field": "rawData_aggregateScore", "operator": "$gte", "value": 80},
                ],
            },
        }
    elif module == "cls":
        spec = {
            "module": "cls",
            "requiredKeys": ["name", "filters", "siemMappings"],
            "optionalKeys": ["muteRules", "muted", "unmuteAt", "isDefault"],
            "fields": rule_fields,
            "logicalOperators": logical_operators,
            "targeting": {
                "siemMappings": "Which log-source configuration forwards matching logs to which SIEM "
                "destination configuration — set on the Log Delivery screen, not the rule form.",
            },
            # CLS rule fields are the deployment's LEARNED fields (rule_fields above). Only offer a
            # concrete conditions example when fields have actually been learned — otherwise tell the
            # caller the field list is empty and to guide from real fields once logs are ingested,
            # NEVER invent a field like 'severity' that may not exist on this deployment.
            "conditionsExample": (
                {
                    "name": "Forward selected events",
                    "match": "all",
                    "conditions": [
                        {"field": rule_fields[0]["field"],
                         "operator": rule_fields[0]["operators"][0], "value": "<value>"},
                    ],
                }
                if rule_fields else None
            ),
            "notes": "The default 'All' rule (isDefault) is undeletable. Rule fields are the "
                     "deployment's LEARNED alert/event/webtx/log fields — build conditions ONLY from "
                     "the 'fields' list above. If it is EMPTY, tell the user the filterable fields "
                     "populate after logs are ingested; do not invent field names.",
        }
    elif module == "cre":
        spec = {
            "module": "cre",
            "requiredKeys": ["name", "entity", "entityFilters"],
            "optionalKeys": ["actions"],
            "entityModel": "A CRE rule targets ONE entity (e.g. Users, Applications), immutable "
                           "after creation. Its filter FIELDS are DYNAMIC per entity — call "
                           "get_cre_entities to list entities and each entity's fields, then build "
                           "conditions (field + operator + value) over the chosen entity's fields.",
            "fields": rule_fields,  # [] — resolve per-entity via get_cre_entities
            "logicalOperators": logical_operators,
            "targeting": {
                "actions": "{<configName>: [{label, value, parameters, generateAlert, performAction, "
                "requireApproval}]} — the actions run on each configuration when a record matches (this is the "
                "rule's wiring; an empty actions dict means the rule scores/evaluates but triggers nothing). Each "
                "action carries three toggles (all default TRUE): generateAlert (raise an alert on the Netskope "
                "tenant/CTO when the action runs), performAction (run during the Maintenance Window rather than "
                "instantly), requireApproval (wait for manual approval before running).",
            },
            "notes": "Fields are per-entity (get_cre_entities). The 'Threat Indicators' entity is "
                     "read-only (bridged from CTE) and requires a sourceConfiguration. The Action itself + its "
                     "generateAlert/performAction/requireApproval toggles are set on the SEPARATE Actions screen, "
                     "not the rule form.",
        }
    elif module == "cfc":
        spec = {
            "module": "cfc",
            "requiredKeys": ["name", "filters"],
            "optionalKeys": ["exceptions", "muted", "unmuteAt"],
            "fields": rule_fields,
            "fieldsSchema": (
                "'fields' is a STRUCTURAL tree: a leaf is {field, type, operators}; 'kind':'object' "
                "has nested 'fields' and 'kind':'array' has 'itemFields'. Reference a nested field "
                "by its path and use the leaf's own type + operators."
            ),
            "logicalOperators": logical_operators,
            "targeting": {
                "note": "CFC rules are FILTER ONLY. Routing is SEPARATE: on the Sharing screen a "
                "rule is mapped to a classifier (+ training type) on a destination configuration. "
                "A rule referenced by no Sharing mapping is 'unwired'. Use get_cfc_sharing to see "
                "the rule->classifier mappings.",
            },
            "conditionsExample": {
                "name": "Classify large PDFs",
                "match": "all",
                "conditions": [
                    {"field": "extension", "operator": "$eq", "value": "PDF"},
                    {"field": "fileSize", "operator": "$gte", "value": 1000000},
                ],
            },
        }
    spec["guidance"] = (
        "The rule form is a VISUAL query builder: the user picks a FIELD, an OPERATOR and a VALUE per condition "
        "row, and combines rows with Add rule / Add group and the AND/OR/NOT toggles. There is NO text box for a "
        "query string and NO MongoDB field. So:\n"
        "1. Guide the user to build CONDITIONS — each as field + operator + value — and how to group them. Use the "
        "'conditionsExample' above as the shape of your suggestion.\n"
        "2. Use ONLY fields from the 'fields' list above and, for each field, ONLY an operator listed for THAT "
        "field. Do NOT use any field or operator that is not in that list — a field/operator the builder does not "
        "offer is invalid and cannot be entered. If a field the user wants is not listed, say it is not a "
        "filterable field for this module rather than inventing one.\n"
        "3. If the 'fields' list is EMPTY (fields are learned/dynamic for this module and none are available yet), "
        "do NOT propose any condition — tell the user which action populates the fields first.\n"
        "4. NEVER write, show, or ask the user to paste a MongoDB query or a query string — the builder derives "
        "those itself and the user never sees or enters them. Talk only in fields, operators and values.\n"
        "Keep the rule's scope consistent with what the source plugin actually pulls (check get_plugin_prefilters). "
        "This is a set of conditions for the user to build in the visual builder and Save."
    )
    return _wrap(f"{module}_business_rule_format", spec)


@tool
def get_plugin_walkthrough(module: str, plugin_id: str) -> str:
    """Return the ordered, step-by-step configuration plan for a plugin, to guide the user.

    Use this to walk the user through configuring a plugin ONE step at a time (don't dump the
    whole schema). Returns the form's real step layout keyed by step NAME, matching each module's
    UI wizard: a synthetic 'basic_info' step, then EITHER one flattened 'configuration_parameters'
    step (CTE/CLS/CRE — a single PluginDynamicForm) OR one step per manifest step/dynamic_step
    (CTO/EDM/CFC), plus any form-injected trailing step (CTO 'Mapping Configuration' / CRE 'Entity
    Sources'), classified basics/auth/params/dynamic/mapping/entity.
    Each field is flagged required/secret/dynamic/hasApiCall so you can tell the user what to fill
    (secrets + dynamic options are filled by the user — never enumerate dynamic options, they load
    after credentials validate). Match the current step to pageState.stepName. Validation runs in
    the form (stepped modules: each Next; CTE: on Save) — interpret its outcome, don't call it here.

    Args:
        module: one of 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc'.
        plugin_id: the plugin's fully-qualified id (from list_available_plugins / the page).
    """
    if module not in _MODULE_INTEGRATION:
        return f"Unknown module '{module}'. Use one of: {', '.join(sorted(_MODULE_INTEGRATION))}."
    if not _allowed(module):
        return _deny(module)
    if not _budget("get_plugin_walkthrough", _CAP_WALKTHROUGH):
        return "Budget for plugin-walkthrough lookups is exhausted for this turn."
    try:
        plugin_cls = plugin_helper.find_by_id(plugin_id)
        if plugin_cls is None:
            return f"Plugin '{plugin_id}' not found."
        meta = getattr(plugin_cls, "metadata", {}) or {}
        plugin_name = meta.get("name") or plugin_id
        module_label = MODULE_DISPLAY_LABEL.get(module, module)
        configuration = meta.get("configuration", []) or []

        steps = []

        def _add_step(name, label, kind, fields):
            steps.append({
                "index": len(steps),
                "name": name,
                "label": label,
                "kind": kind,
                "fields": [_walkthrough_field(f) for f in fields if isinstance(f, dict)],
                "note": _STEP_KIND_NOTE.get(kind, ""),
            })

        # Synthetic Basic Information step (the form injects it; the manifest does not) so the
        # plan's step NAMES align with the form's currentStep/stepName. Its fields are gated by
        # the manifest's capability flags, exactly as the UI's BasicInformation form gates them.
        _add_step("basic_info", "Basic Information", "basics", _basic_info_fields(module, meta))

        if module in _FLATTEN_MODULES:
            # CTE/CLS/CRE render the manifest config as ONE flattened "Configuration Parameters"
            # step (a single PluginDynamicForm), matching their UI wizard. CRE then appends its
            # form-injected "Entity Sources" step (map plugin fields -> CRE entity fields).
            flat = [f for f, _step, _dyn in _iter_manifest_fields(configuration)]
            _add_step("configuration_parameters", "Configuration Parameters", "params", flat)
            if module == "cre":
                _add_step("entity_sources", "Entity Sources", "entity", [])
            # CTE validates the whole config on Save; CLS/CRE use the stepped WizardPluginForm
            # (each step validates on Next).
            validation_model = "whole_config_on_save" if module == "cte" else "per_step_on_next"
        else:
            # CTO/EDM/CFC render one form step per manifest step/dynamic_step (keyed by name),
            # each validated on Next. (EDM's Sanitization + CFC's Directory/Preview steps ARE in
            # the manifest — picked up by the loop; CLS's Mapping/Format are inline on Basic
            # Information, handled by _basic_info_fields, so CLS is in _FLATTEN_MODULES above.)
            for entry in configuration:
                if not isinstance(entry, dict):
                    continue
                etype = entry.get("type")
                name = entry.get("name") or entry.get("key") or f"step{len(steps)}"
                label = entry.get("label") or name
                if etype == "dynamic_step":
                    _add_step(name, label, "dynamic", [])
                elif etype == "step":
                    _fields = entry.get("fields", []) or []
                    _add_step(name, label, _classify_step_kind(name, label, _fields, False), _fields)
                else:
                    # Stray top-level field (uncommon) — fold into a params step.
                    _add_step(name, label, "params", [entry])
            # CTO appends "Mapping Configuration" for third-party ITSM plugins
            # (WizardPluginForm.checkForOptionalStep, shown when the manifest has NO
            # incident_update_config step). netskope_itsm carries its own (classified 'mapping'
            # above), so gate on that to avoid a duplicate.
            if module == "cto" and not any(s["kind"] == "mapping" for s in steps):
                _add_step("mapping_configuration", "Mapping Configuration", "mapping", [])
            validation_model = "per_step_on_next"

        # validatesHere: CTO validates every step on Next; CTE validates the whole config on Save.
        for i, st in enumerate(steps):
            st["validatesHere"] = (
                True if validation_model == "per_step_on_next" else (i == len(steps) - 1)
            )

        result = {
            "plugin_id": plugin_id,
            "plugin_name": plugin_name,
            "module": module_label,
            "validationModel": validation_model,
            "docSearchQuery": f"{module_label} {plugin_name} plugin guide",
            "stepCount": len(steps),
            "stepNames": [s["name"] for s in steps],
            "steps": steps,
            "guidance": (
                "Guide ONE step at a time, matching the current step to pageState.stepName. For the current "
                "step, cover EVERY field in that step's `fields` list — do not skip any and do not summarise to "
                "just a couple. For each field give: its form LABEL, what it is / what it controls (use its "
                "`description`), whether it is required, secret (the user enters it — never a value), or dynamic "
                "(options load after credentials validate), and a grounded recommended value or a sensible "
                "default where one applies (`default`/`choices` are provided). A field with `hasApiCall` loads "
                "more fields when set — say so. Then give a single next action. Don't preview LATER steps or "
                "enumerate dynamic options. The step plan is COMPLETE — walk every step in stepNames, including "
                "any final form-injected step (CTO 'Mapping Configuration' maps CE fields to the destination's "
                "fields; CRE 'Entity Sources' maps plugin fields to CRE entity fields); their options load after "
                "credentials validate, so guide them once the user is on them. (EDM 'receiver' plugins have no "
                "downstream steps.) Completeness of the CURRENT step's fields takes priority over brevity."
            ),
        }
        return _wrap(f"{module}_plugin_walkthrough", result)
    except Exception:
        logger.error(f"Error building walkthrough for {plugin_id}.", details=traceback.format_exc())
        return f"Error building walkthrough for '{plugin_id}'. See CE logs for details."


@tool
def get_plugin_guide(module: str, plugin_id: str) -> str:
    """Find a plugin's docs.netskope.com guide to cite best practices + troubleshooting.

    Reads the plugin manifest (name, capabilities) and builds the canonical doc-search
    query for its guide — "<Module label> <Plugin name> plugin guide". Then USE the
    web_search tool with the returned ``docSearchQuery`` to open the guide and cite its
    best-practices / troubleshooting sections with [N]. This tool does not search the web
    itself; it primes the search so the guide is found reliably.

    Args:
        module: one of 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc'.
        plugin_id: the plugin's fully-qualified id (from list_available_plugins / the page).
    """
    if module not in _MODULE_INTEGRATION:
        return f"Unknown module '{module}'. Use one of: cte, cto, cls, cre, edm, cfc."
    if not _allowed(module):
        return _deny(module)
    if not _budget("get_plugin_guide", _CAP_SCHEMA):
        return "Budget for plugin-guide lookups is exhausted for this turn."
    try:
        plugin_cls = plugin_helper.find_by_id(plugin_id)
        if plugin_cls is None:
            return f"Plugin '{plugin_id}' not found."
        meta = getattr(plugin_cls, "metadata", {}) or {}
        plugin_name = meta.get("name") or plugin_id
        module_label = MODULE_DISPLAY_LABEL.get(module, module)
        result = {
            "plugin": plugin_name,
            "module": module_label,
            "version": meta.get("version"),
            "description": meta.get("description"),
            "capabilities": {
                # The manifest capability flags that determine what the plugin can DO in a flow —
                # surface them all so guidance/journeys reason over the plugin's real role instead of
                # its name (e.g. only a sharing/patch-capable plugin can push updates back).
                "pull": meta.get("pull_supported"),          # can fetch data from the source
                "push": meta.get("push_supported"),          # can send data to a destination
                "receiving": meta.get("receiving_supported"),  # CE-to-CE receiver (ingests pre-shared data)
                "sharing": meta.get("sharing_supported"),    # bi-directional: syncs updates back to the tenant
                "patch": meta.get("patch_supported"),        # supports incremental/patch updates (not full re-push)
            },
            "docSearchQuery": f"{module_label} {plugin_name} plugin guide",
            "guidance": (
                "Use the web_search tool with docSearchQuery to open this plugin's guide on "
                "docs.netskope.com, then cite its best-practices and troubleshooting sections with [N]. "
                "If the exact guide isn't found, broaden to '<Module label> plugin guide'. Use "
                "'capabilities' to ground the plugin's ROLE (pull=source, push=destination, "
                "receiving=CE-to-CE receiver, sharing=bi-directional update-back, patch=incremental "
                "updates) rather than inferring it from the name — and frame an unused-but-supported "
                "capability (e.g. sharing off on a sharing-capable plugin) as an OPPORTUNITY, not a fault."
            ),
        }
        return _wrap(f"{module}_plugin_guide", result)
    except Exception:
        logger.error(f"Error building plugin-guide pointer for {plugin_id}.", details=traceback.format_exc())
        return f"Error building plugin-guide pointer for '{plugin_id}'. See CE logs for details."


@tool
def get_plugin_prefilters(module: str, name: Optional[str] = None) -> str:
    """Show configured plugins' pull-time pre-filters, to reconcile a business rule's scope.

    Use on the business-rules page when creating/reviewing a rule. Some plugins pre-filter
    at PULL time (e.g. only pull IPv4, or a query), so a rule must target only data the
    plugin actually pulls. Conversely a narrow rule (e.g. MD5-only) wastes effort unless the
    plugin is also narrowed — when it supports pull-time filtering. Returns each configured
    plugin's pull capability and the manifest fields that act as pre-filters, with their
    current (secret-redacted) values, so you can flag mismatches and recommend aligning them.

    Args:
        module: one of 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc'.
        name: optional single configuration name; omit to list all configured plugins.
    """
    if module not in _MODULE_COLLECTION:
        return f"Unknown module '{module}'. Use one of: cte, cto, cls, cre, edm, cfc."
    if not _allowed(module):
        return _deny(module)
    if not _budget("get_plugin_prefilters", _CAP_DETAIL):
        return "Budget for plugin pre-filter lookups is exhausted for this turn."
    try:
        query = {"name": name} if name else {}
        docs = list(connector.collection(_MODULE_COLLECTION[module]).find(query))
        if not docs:
            suffix = f" named '{name}'" if name else ""
            return f"No {module} configurations found{suffix}."
        rows = []
        for doc in docs[:_CAP_DETAIL]:
            plugin_id = doc.get("plugin", "")
            plugin_cls = plugin_helper.find_by_id(plugin_id)
            meta = (getattr(plugin_cls, "metadata", {}) or {}) if plugin_cls else {}
            # Redact via the canonical path (collect_plugin_parameters, with the manifest-level
            # fallback) — same single reduction every copilot tool uses, never a divergent copy.
            redacted = _redacted_plugin_doc(doc).get("parameters", {})
            prefilter_fields = []
            for field, _step, _is_dyn in _iter_manifest_fields(meta.get("configuration", [])):
                key = field.get("key")
                label = field.get("label") or ""
                if not key:
                    continue
                if _PREFILTER_KEY_RE.search(key) or _PREFILTER_KEY_RE.search(label):
                    prefilter_fields.append({
                        "label": field.get("label") or key,
                        # Choice display label, not the stored code (req#7).
                        "value": _display_value(field, redacted.get(key)),
                    })
            rows.append({
                "name": doc.get("name"),
                "plugin": plugin_id,
                "pluginName": meta.get("name"),
                "pullSupported": meta.get("pull_supported"),
                "supportsPreFiltering": bool(prefilter_fields),
                "pullFilterFields": prefilter_fields,
            })
        return _wrap(f"{module}_plugin_prefilters", rows)
    except Exception:
        logger.error(f"Error getting {module} plugin pre-filters.", details=traceback.format_exc())
        return f"Error getting {module} plugin pre-filters. See CE logs for details."


@tool
def validate_draft(module: str, plugin_id: str, parameters: dict) -> str:
    """Manifest-level dry-run of draft plugin parameters (no live API call, no write).

    Checks mandatory fields are present, choice values are valid, and number
    fields are numeric — deterministic and side-effect-free. The live
    connectivity/credential check happens when the admin Saves the form.

    Args:
        module: one of 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc'.
        plugin_id: the plugin's fully-qualified id.
        parameters: the draft parameter values to check.
    """
    if module not in _MODULE_INTEGRATION:
        return f"Unknown module '{module}'. Use one of: cte, cto, cls, cre, edm, cfc."
    if not _allowed(module):
        return _deny(module)
    if not _budget("validate_draft", _CAP_VALIDATE):
        return "Budget for draft validation is exhausted for this turn."
    try:
        plugin_cls = plugin_helper.find_by_id(plugin_id)
        if plugin_cls is None:
            return f"Plugin '{plugin_id}' not found."
        configuration = (getattr(plugin_cls, "metadata", {}) or {}).get("configuration", [])
        params = parameters or {}
        issues = []
        for field, _step, is_dynamic_step in _iter_manifest_fields(configuration):
            if is_dynamic_step:
                continue
            key, ftype = field.get("key"), field.get("type")
            present = key in params and params.get(key) not in (None, "")
            if field.get("mandatory") and not present and ftype != "password":
                issues.append(f"Missing mandatory field '{field.get('label') or key}'.")
            if present and ftype == "choice":
                valid = {c.get("value") for c in (field.get("choices") or [])}
                if valid and params[key] not in valid:
                    issues.append(f"'{params[key]}' is not a valid choice for '{key}'.")
            if present and ftype == "number":
                try:
                    float(params[key])
                except (TypeError, ValueError):
                    issues.append(f"Field '{key}' expects a number.")
        if issues:
            return "Draft has issues:\n- " + "\n- ".join(issues) + (
                "\n(Note: credential/connectivity validation runs when you Save the form.)"
            )
        return (
            "Draft passes manifest checks (mandatory present, choices/types valid). "
            "Credential/connectivity validation runs when you Save the form."
        )
    except Exception:
        logger.error(f"Error validating draft for {plugin_id}.", details=traceback.format_exc())
        return f"Error validating draft for '{plugin_id}'. See CE logs for details."


def _tenants_presence() -> dict:
    """Read-only Netskope-tenant PRESENCE — names/count only, NEVER tokens.

    Many CE plugins are tenant-based (no URL/token of their own), so a setup journey's first step
    confirms a tenant exists. This lets the copilot CONFIRM that instead of only telling the user to
    go look. Only the tenant NAME (top-level ``name`` + ``parameters.tenantName`` display identity)
    is surfaced; ``token``/``v2token`` and everything else are dropped.
    """
    docs = list(connector.collection(Collections.NETSKOPE_TENANTS).find(
        {}, {"_id": 0, "name": 1, "parameters.tenantName": 1}
    ).limit(_MAX_LIST_DOCS))
    tenants = [
        {"name": d.get("name"), "tenantName": (d.get("parameters") or {}).get("tenantName")}
        for d in docs
    ]
    return {"tenantCount": len(tenants), "tenants": tenants,
            "note": "Presence only — tokens are never exposed. A plugin's tenant is chosen in its form."}


def _llm_provider_status() -> dict:
    """Read-only LLM-provider status — provider/model/active, NEVER the API key.

    Reuses ``LLMProviderOut`` (the canonical secret-redacted model — it drops password-type
    parameters by manifest), so the copilot can say e.g. 'you're on the Anthropic provider, model
    claude-opus-…, web search on' without ever seeing credentials.
    """
    from netskope.common.models.llm_provider import LLMProviderOut

    out = []
    for doc in connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find({}).limit(_MAX_LIST_DOCS):
        try:
            safe = LLMProviderOut(**doc)  # strips password params by manifest
            out.append({
                "name": safe.name, "plugin": safe.plugin, "active": safe.active,
                "parameters": safe.parameters,  # non-secret only (model/effort/etc.)
            })
        except Exception:
            # A malformed/legacy doc — surface only its non-secret identity, never raw parameters.
            out.append({"name": doc.get("name"), "plugin": doc.get("plugin"),
                        "active": doc.get("active")})
    return {"providerCount": len(out), "providers": out,
            "note": "API keys are never exposed. Only one provider is enabled (active) at a time."}


# Read-only settings sections that live OUTSIDE the SETTINGS doc (their own collections) and have
# their own scope gate — handled before the SETTINGS-doc whitelist path. Both surface NON-SECRET
# fields only (see the helpers). tenants → settings_read; llm_provider → ai_read.
_SPECIAL_SETTINGS = {
    "tenants": ("settings_read", _tenants_presence),
    "llm_provider": ("ai_read", _llm_provider_status),
}


# --- settings introspection -------------------------------------------
@tool
def get_settings(section: str) -> str:
    """Read a settings section (secrets excluded), scope-gated.

    Args:
        section: 'system' (settings_read); a module — 'cte'/'cto'/'cls'/'cre'/'edm'/'cfc' (each
            gated on that module's read scope); 'tenants' (settings_read — Netskope tenant PRESENCE,
            names only, no tokens); or 'llm_provider' (ai_read — the active AI provider + model, no
            API key).
    """
    section = (section or "").lower()
    if not _budget("get_settings", _CAP_SETTINGS):
        return "Budget for settings reads is exhausted for this turn."
    # Special read-only sections backed by their own collections (tenants / llm_provider).
    if section in _SPECIAL_SETTINGS:
        required, reader = _SPECIAL_SETTINGS[section]
        if required not in _scopes():
            return f"Access denied: you don't have '{required}' to read {section}."
        try:
            return _wrap(f"settings_{section}", reader())
        except Exception:
            logger.error("Failed to mark the settings details as un-safe for Agent.", details=traceback.format_exc())
            return f"Error reading {section}. See CE logs for details."
    if section not in _SETTINGS_WHITELIST:
        valid = sorted(list(_SETTINGS_WHITELIST) + list(_SPECIAL_SETTINGS))
        return f"Unknown section. Use one of: {', '.join(valid)}."
    required = MODULE_READ_SCOPE[section]
    if required not in _scopes():
        return f"Access denied: you don't have '{required}' to read {section} settings."
    try:
        doc = connector.collection(Collections.SETTINGS).find_one({}) or {}
        out = {}
        for key in _SETTINGS_WHITELIST[section]:
            if key not in doc:
                continue
            value = doc[key]
            if section == "system" and key == "emailAddress" and value:
                # PII masking: the configured email is a person/inbox identifier the copilot never
                # needs — its settings guidance only cares THAT an email is configured, not what it
                # is. Report presence, never the address.
                out[key] = "***configured (redacted)***"
                continue
            if section == "system" and key == "proxy" and isinstance(value, dict):
                out[key] = {
                    "scheme": value.get("scheme"),
                    "configured": bool(value.get("server")),
                    "authenticated": bool(value.get("username")),
                }
                continue
            if section == "system" and key == "platforms" and isinstance(value, dict):
                # "grc" is a decommissioned module (no integration package, no route, no
                # UI page — see routers/settings.py's own "GRC"->"ARE" display patch) that
                # can still linger in older SETTINGS.platforms docs. Never surface it to
                # the model: it isn't a real module and confuses users if named in an
                # answer (e.g. listed as "disabled").
                out[key] = {k: v for k, v in value.items() if k != "grc"}
                continue
            out[key] = value
        # System section also surfaces non-secret secrets-manager status.
        if section == "system":
            sms = doc.get("secretsManagerSettings") or {}
            params = sms.get("params") or {}
            out["secretsManagerSettings"] = {
                "enabled": sms.get("enabled"),
                "provider": params.get("provider"),
            }
        return _wrap(f"settings_{section}", out)
    except Exception:
        logger.error(f"Error reading {section} settings.", details=traceback.format_exc())
        return f"Error reading {section} settings. See CE logs for details."


# --- dashboards / health ----------------------------------------------
@tool
def get_system_health() -> str:
    """Summarise system health: queue depth + certificate expiry. Needs settings_read."""
    if "settings_read" not in _scopes():
        return "Access denied: you don't have 'settings_read' to view system health."
    if not _budget("get_system_health", _CAP_HEALTH):
        return "Budget for system-health reads is exhausted for this turn."
    try:
        return _wrap("system_health", _system_health_payload())
    except Exception:
        logger.error("System health unavailable.", details=traceback.format_exc())
        return "System health unavailable. See CE logs for details."


@tool
def get_deployment_details() -> str:
    """Report HOW this Cloud Exchange is deployed, plus every option the four axes can take.

    The axes are: platform provider (gcp / aws / azure / vmware / microsoft = Hyper-V /
    custom), host OS (Ubuntu or RHEL, with the major version), deployment type
    (Standalone (SA) or HA) and flavour (Container or CE as VM — the pre-built
    OVA/VHDX/AMI/Azure/GCP appliance image).

    Call this whenever the answer depends on the deployment shape — "how is my CE
    deployed", "am I on HA", "is this the appliance/OVA", "which cloud is this on" — and
    BEFORE giving upgrade, HA-node, storage, backup or OS-level steps, which differ per
    axis. Reads the deployment environment directly (the same values the System Health
    page's System Specifications panel shows), so it is free, needs no scope, and works
    from ANY page. Pair with get_ce_knowledge('deployment') for what each value means and
    which combinations exist.
    """
    if not _budget("get_deployment_details", _CAP_DEPLOYMENT):
        return "Budget for deployment reads is exhausted for this turn."
    try:
        details = deployment_details()
        provider = details["Platform Provider"]
        return _wrap(
            "deployment_details",
            {
                "current": {
                    "platformProvider": provider,
                    # "microsoft" means Hyper-V and "custom" means "nothing recognised" —
                    # neither reads as itself, so hand the model the label too.
                    "platformProviderLabel": PLATFORM_PROVIDER_LABELS.get(
                        provider, "Unrecognised provider value"
                    ),
                    "hostOS": details["Host OS"] or "Unknown",
                    "deploymentType": details["Deployment Type"],
                    "flavor": details["Flavor"],
                },
                "availableOptions": DEPLOYMENT_OPTIONS,
            },
        )
    except Exception:
        logger.error("Deployment details unavailable.", details=traceback.format_exc())
        return "Deployment details unavailable. See CE logs for details."


def _run_coro(coro):
    """Run an async coroutine to completion from this SYNC tool.

    The config tools are sync ``@tool``s; when the agent drives them via ``astream_events`` they
    execute in a threadpool worker with NO running event loop, so ``asyncio.run`` works. But if a
    loop IS already running in this thread (e.g. a caller invoking the tool on the async path),
    ``asyncio.run`` would raise — so fall back to running the coroutine on a dedicated thread with
    its own loop. Used to call the modules' own async dashboard route handlers directly (the same
    pattern the migrations use to reuse route logic outside FastAPI), so the copilot returns the
    SAME data the module's dashboard route serves without duplicating each aggregation.
    """
    import asyncio

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)  # no running loop (the normal tool path)
    # A loop is already running in this thread — execute the coroutine on a separate thread.
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(coro)).result()


def _turn_user():
    """Build a ``User`` carrying the current turn's scopes, to pass to a module route handler.

    The dashboard handlers declare ``user: User = Security(...)``; calling them outside FastAPI we
    supply that arg ourselves. Scope is still enforced by get_dashboard_data's own ``_allowed``
    gate before we get here, so this only satisfies the handler's signature.
    """
    from netskope.common.models.user import User

    return User(username="ai-copilot", scopes=sorted(_scopes()))


# Each new-module surface reuses its module's OWN dashboard route logic (async), so the copilot
# interprets exactly what the module's dashboard shows. Kept as thin adapters: import lazily (keep
# the integration import graph out of module load), pass the turn user, translate an HTTPException
# to a readable string. CTE/CTO stay inline below (small, indicator/task aggregations).
def _module_dashboard_payload(surface: str) -> dict:
    """Return the module's own dashboard summary payload for cls/cre/edm/cfc (raises on failure)."""
    user = _turn_user()
    if surface == "cls":
        from netskope.integrations.cls.routers.dashboard import get_count_of_logs_and_bytes

        return _run_coro(get_count_of_logs_and_bytes(user=user))
    if surface == "cre":
        # TimeRange lives in the router module (not a models module); the user arg is named `user`.
        from netskope.integrations.crev2.routers.dashboard import get_pulled_data_by_entity, TimeRange

        return _run_coro(get_pulled_data_by_entity(
            entity="All", time_range=TimeRange.ALL_TIME, start_date=None, end_date=None, user=user,
        ))
    if surface == "edm":
        from netskope.integrations.edm.routers.dashboard import get_summary
        from netskope.integrations.edm.models.statistics import EDMActions

        # EDM's summary is per-action; return both sent + received so the copilot sees the picture.
        return {
            action.value: _run_coro(get_summary(action_name=action, _=user))
            for action in EDMActions
        }
    if surface == "cfc":
        from netskope.integrations.cfc.routers.dashboard import get_summary

        return _run_coro(get_summary(_=user))
    raise ValueError(surface)


@tool
def get_dashboard_data(surface: str) -> str:
    """Read the live data behind a dashboard, for interpretation.

    Returns the same summary a module's dashboard shows, so you can interpret what the user sees.

    Args:
        surface: 'system_health', or a module dashboard — 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc'
            (each gated on that module's read scope; 'system_health' needs settings_read).
    """
    surface = (surface or "").lower()
    if not _budget("get_dashboard_data", _CAP_DASHBOARD):
        return "Budget for dashboard reads is exhausted for this turn."
    try:
        if surface == "system_health":
            if "settings_read" not in _scopes():
                return "Access denied: you don't have 'settings_read' for the system dashboard."
            return _wrap("system_health", _system_health_payload())
        if surface == "cte":
            if not _allowed("cte"):
                return _deny("cte")
            total = connector.collection(Collections.INDICATORS).count_documents({"active": True})
            pull = list(connector.collection(Collections.INDICATORS).aggregate([
                {"$unwind": "$sources"},
                {"$group": {
                    "_id": "$sources.source",
                    "retracted": {"$sum": {"$cond": ["$sources.retracted", 1, 0]}},
                    "total": {"$sum": 1},
                }},
                {"$limit": 50},
            ], allowDiskUse=True))
            return _wrap("cte_dashboard", {"activeIndicators": total, "perSource": pull})
        if surface == "cto":
            if not _allowed("cto"):
                return _deny("cto")
            by_status = list(connector.collection(Collections.ITSM_TASKS).aggregate([
                {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            ]))
            dedupe = list(connector.collection(Collections.ITSM_TASKS).aggregate([
                {"$group": {"_id": None, "dedupeTotal": {"$sum": {"$ifNull": ["$dedupeCount", 0]}}}},
            ]))
            return _wrap("cto_dashboard", {
                "ticketsByStatus": by_status,
                "totalDuplicatesMerged": (dedupe[0]["dedupeTotal"] if dedupe else 0),
            })
        if surface in ("cls", "cre", "edm", "cfc"):
            if not _allowed(surface):
                return _deny(surface)
            return _wrap(f"{surface}_dashboard", _module_dashboard_payload(surface))
        return "Unknown surface. Use 'system_health', 'cte', 'cto', 'cls', 'cre', 'edm', or 'cfc'."
    except Exception:
        logger.error(f"Error reading {surface} dashboard..", details=traceback.format_exc())
        return f"Error reading {surface} dashboard. See CE logs for details."


@tool
def get_plugin_run_status(module: str, name: Optional[str] = None) -> str:
    """Report run health (last run, success, lock, checkpoint) for a module's configs.

    Args:
        module: one of 'cte', 'cto', 'cls', 'cre', 'edm', 'cfc'.
        name: optional single configuration name.
    """
    if module not in _MODULE_COLLECTION:
        return f"Unknown module '{module}'. Use one of: cte, cto, cls, cre, edm, cfc."
    if not _allowed(module):
        return _deny(module)
    if not _budget("get_plugin_run_status", _CAP_RUN_STATUS):
        return "Budget for run-status reads is exhausted for this turn."
    try:
        query = {"name": name} if name else {}
        projection = {
            "_id": 0, "name": 1, "active": 1, "lastRunAt": 1,
            "lastRunSuccess": 1, "lockedAt": 1, "checkpoint": 1,
        }
        rows = list(connector.collection(_MODULE_COLLECTION[module]).find(query, projection))
        if not rows:
            return f"No {module} run status found{f' for {name}' if name else ''}."
        return _wrap(f"{module}_run_status", rows)
    except Exception:
        logger.error(f"Error reading {module} run status.", details=traceback.format_exc())
        return f"Error reading {module} run status. See CE logs for details."


# --- optimize / correlation (read-only) -------------------------------
@tool
def analyze_cte_config(name: Optional[str] = None) -> str:
    """Correlate CTE config + indicators + rules to surface optimization observations.

    Flags: feeds pulled but shared by no rule; high retracted-ratio while
    retraction is off; rules referencing a disabled/missing source; and a tagging
    cross-dependency view (tag volume, which tags rules consume, which configs have
    tagging enabled) so a recommendation to change tagging can check its impact first.
    Also surfaces the live module-settings values (Settings → Threat Exchange):
    IoC(s) Retraction + its interval, Reconciliation Criteria, Delete Inactive
    IoC(s) Indicators, and Generate Alerts — so a "guide me through this screen"
    walkthrough states actual configured values instead of guessing defaults.
    Current-state observations only (no trends).

    Args:
        name: optional single source-configuration name to focus on.
    """
    if not _allowed("cte"):
        return _deny("cte")
    if not _budget("analyze_cte_config", _CAP_ANALYZE):
        return "Budget for CTE analysis is exhausted for this turn."
    try:
        configs = list(connector.collection(Collections.CONFIGURATIONS).find(
            {} if not name else {"name": name}, {"_id": 0, "name": 1, "active": 1}
        ))
        active_names = {c["name"] for c in configs if c.get("active")}
        all_names = {c["name"] for c in configs}
        # SOURCE-side analysis, so `sharedWith` alone is correct here: its keys are the CTE source
        # configurations a rule pulls from. A CRE-entity rule (`creShare`) reads a CRE entity, not
        # a CTE plugin config, so it contributes no source — do NOT add creShare to these sets or
        # every CRE-entity destination would be miscounted as a "shared source".
        rules = list(connector.collection(Collections.CTE_BUSINESS_RULES).find({}, {"_id": 0, "sharedWith": 1}))
        shared_sources = set()
        referenced_sources = set()
        for r in rules:
            for src in (r.get("sharedWith") or {}):
                referenced_sources.add(src)
                shared_sources.add(src)
        unshared = sorted(active_names - shared_sources)
        rules_referencing_missing = sorted(referenced_sources - all_names)
        settings = connector.collection(Collections.SETTINGS).find_one({}, {"_id": 0, "cte": 1}) or {}
        cte_settings = settings.get("cte") or {}
        retraction_on = bool(cte_settings.get("iocRetraction"))
        retraction_interval_days = cte_settings.get("iocRetractionInterval")
        reconciliation_criteria = cte_settings.get("criteria")
        delete_inactive_indicators = bool(cte_settings.get("deleteInactiveIndicators"))
        generate_alerts_enabled = bool(cte_settings.get("generateAlerts"))
        match = {"sources.source": name} if name else {}
        retract = list(connector.collection(Collections.INDICATORS).aggregate([
            {"$unwind": "$sources"},
            *([{"$match": match}] if match else []),
            {"$group": {
                "_id": "$sources.source",
                "retracted": {"$sum": {"$cond": ["$sources.retracted", 1, 0]}},
                "total": {"$sum": 1},
            }},
            {"$limit": 50},
        ], allowDiskUse=True))
        # Tagging cross-dependency: tag volume + who CONSUMES tags, so a "disable tagging"
        # suggestion can be checked for impact (rules that mute/filter on tags; push plugins
        # attach indicator tags to their payload). Tagging is a per-plugin `enable_tagging`
        # config field — there is no global auto-tagging setting.
        tag_volume = list(connector.collection(Collections.INDICATORS).aggregate([
            {"$unwind": "$sources"},
            {"$unwind": "$sources.tags"},
            {"$group": {"_id": "$sources.tags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 25},
        ], allowDiskUse=True))
        rules_full = list(connector.collection(Collections.CTE_BUSINESS_RULES).find(
            {}, {"_id": 0, "name": 1, "filters": 1, "exceptions": 1, "muted": 1, "sharedWith": 1,
                 "creShare": 1, "entity": 1}
        ))
        # Compact rule inventory so a health/posture scan sees each rule's SCOPE inline (its
        # human-readable filter), not just its name — a rule name conveys nothing about breadth.
        # Only the human `query` (never the mongo JSON) so the model can describe it to the user.
        rules_inventory = [
            {
                "name": r.get("name"),
                "entity": r.get("entity") or _THREAT_INDICATORS_ENTITY,
                "filter": _redact_rule_secrets(
                    (r.get("filters") or {}).get("query") or "(none — matches ALL indicators)"
                ),
                "muted": bool(r.get("muted")),
                "sharesToConfigs": len(_rule_share_destinations(r)),
            }
            for r in rules_full
        ]
        tags_used_in_rules = set()
        for r in rules_full:
            for exc in (r.get("exceptions") or []):
                for t in (exc.get("tags") or []):
                    tags_used_in_rules.add(t)
            mongo = (r.get("filters") or {}).get("mongo")
            if isinstance(mongo, str) and '"tags"' in mongo:
                tags_used_in_rules.add("<referenced in a rule filter>")
        configs_full = list(connector.collection(Collections.CONFIGURATIONS).find(
            {} if not name else {"name": name}, {"_id": 0, "name": 1, "parameters.enable_tagging": 1}
        ))
        tagging_enabled_configs = [
            c["name"] for c in configs_full
            if str((c.get("parameters") or {}).get("enable_tagging", "")).lower() in ("yes", "true", "1")
        ]
        findings = {
            "rulesInventory": rules_inventory,
            "rulesNote": "Describe a rule by its 'filter' (its actual scope), not its name. A "
                         "'(none — matches ALL indicators)' filter is a catch-all worth flagging.",
            "feedsPulledButNeverShared": unshared,
            "rulesReferencingMissingSource": rules_referencing_missing,
            "iocRetractionEnabled": retraction_on,
            "iocRetractionIntervalDays": retraction_interval_days,
            "reconciliationCriteria": reconciliation_criteria,
            "deleteInactiveIndicatorsEnabled": delete_inactive_indicators,
            "generateAlertsEnabled": generate_alerts_enabled,
            "perSourceRetractedVsTotal": retract,
            "tagging": {
                "topTagsByVolume": tag_volume,
                "tagsConsumedByRules": sorted(tags_used_in_rules),
                "configsWithTaggingEnabled": tagging_enabled_configs,
                "note": "Before recommending a config's tagging be turned off, check tagsConsumedByRules and "
                        "any push plugin that attaches indicator tags — disabling tagging affects those "
                        "consumers (e.g. a rule that mutes/filters on tags, or Netskope push-to-private-app).",
            },
            "note": "Observations are current-state, not trends. Suggestions are advisory.",
        }
        return _wrap("cte_analysis", findings)
    except Exception:
        logger.error("Error analyzing CTE config.", details=traceback.format_exc())
        return "Error analyzing CTE config. See CE logs for details."


@tool
def analyze_cto_config(name: Optional[str] = None) -> str:
    """Correlate CTO tasks + rules to surface optimization observations.

    Flags: rules with high dedupe; rules that never created a ticket; tickets
    stuck in failed sync; rules targeting a disabled/missing config. Current-
    state observations only; true false-positive detection is out of scope.

    Args:
        name: optional business-rule name to focus on.
    """
    if not _allowed("cto"):
        return _deny("cto")
    if not _budget("analyze_cto_config", _CAP_ANALYZE):
        return "Budget for CTO analysis is exhausted for this turn."
    try:
        dedupe_by_rule = list(connector.collection(Collections.ITSM_TASKS).aggregate([
            *([{"$match": {"businessRule": name}}] if name else []),
            {"$group": {
                "_id": "$businessRule",
                "tickets": {"$sum": 1},
                "dedupeTotal": {"$sum": {"$ifNull": ["$dedupeCount", 0]}},
            }},
            {"$sort": {"dedupeTotal": -1}},
            {"$limit": 25},
        ]))
        ruled_with_tickets = {row["_id"] for row in dedupe_by_rule}
        rules = list(connector.collection(Collections.ITSM_BUSINESS_RULES).find(
            {} if not name else {"name": name}, {"_id": 0, "name": 1, "queues": 1, "filters": 1}
        ))
        rules_inventory = [
            {
                "name": r.get("name"),
                "filter": _redact_rule_secrets(
                    (r.get("filters") or {}).get("query") or "(none — matches ALL alerts)"
                ),
                "targetsQueues": len(r.get("queues") or {}),
            }
            for r in rules
        ]
        configs = {c["name"] for c in connector.collection(Collections.ITSM_CONFIGURATIONS).find(
            {"active": True}, {"_id": 0, "name": 1}
        )}
        never_match = [r["name"] for r in rules if r.get("name") not in ruled_with_tickets]
        disabled_target = [
            r["name"] for r in rules
            if any(cfg not in configs for cfg in (r.get("queues") or {}))
        ]
        stuck = connector.collection(Collections.ITSM_TASKS).count_documents({"syncStatus": "failed"})
        findings = {
            "rulesInventory": rules_inventory,
            "rulesNote": "Describe a rule by its 'filter' (its actual scope), not its name.",
            "dedupeByRule": dedupe_by_rule,
            "rulesThatNeverCreatedTickets": never_match,
            "rulesTargetingDisabledOrMissingConfig": disabled_target,
            "ticketsStuckInFailedSync": stuck,
            "note": "Observations are current-state, not trends. Suggestions are advisory.",
        }
        return _wrap("cto_analysis", findings)
    except Exception:
        logger.error("Error analyzing CTO config.", details=traceback.format_exc())
        return "Error analyzing CTO config. See CE logs for details."


# --- per-module inspection tools (CLS / CRE / EDM / CFC) ------------------------------
# Small, module-specific reads that surface each module's flow-wiring (the analogue of CTE
# sharing / CTO queues), so the copilot can inspect + guide the parts that are unique to the
# module. Each is scope-gated to its own module_read and budget-capped like the others.
@tool
def get_cls_mappings() -> str:
    """List CLS Log-Delivery wiring: the available configs (with their REAL role) + per-rule siemMappings.

    Use to see or guide CLS log-delivery wiring — a rule's siemMappings routes matching logs from a
    SOURCE config to SIEM DESTINATION config(s) (set on the Log Delivery screen). A rule with empty
    siemMappings forwards nothing. Requires 'cls_read'.

    Returns two sections:
    - ``configs``: every CLS configuration with its plugin's real capability — ``pullSupported`` /
      ``pushSupported`` — and a derived ``role`` (source = pull-capable / brings logs IN; destination
      = push-capable / sends logs to a SIEM; both = can do either). GROUND source-vs-destination on
      THIS, never on the config's NAME (a config called 'syslog desti' is not a destination unless
      its plugin is push-capable). When guiding a Log Delivery entry, the Source Configuration must be
      a pull-capable config and the Destination a push-capable one.
    - ``rules``: each business rule's siemMappings ({source config -> [destination configs]}).
    """
    if not _allowed("cls"):
        return _deny("cls")
    if not _budget("get_cls_mappings", _CAP_MODULE_TOOL):
        return "Budget for CLS mapping lookups is exhausted for this turn."
    try:
        docs = list(connector.collection(Collections.CLS_BUSINESS_RULES).find(
            {}, {"_id": 0, "name": 1, "siemMappings": 1, "isDefault": 1}).limit(_MAX_LIST_DOCS))
        rules = [{"rule": d.get("name"), "isDefault": bool(d.get("isDefault")),
                  "siemMappings": d.get("siemMappings") or {}} for d in docs]
        # Enumerate the CLS configs with their REAL capability so the copilot grounds the
        # source/destination role on the plugin manifest, NOT on the config name (Ledger: judge a
        # thing by what it IS, not its name — the flagged 'syslog desti'/'netskope source' roles must
        # come from pull/push capability, not the label). Capped: this loop does a per-doc
        # plugin_helper.find_by_id, so an unbounded config set would be the worst offender.
        cfgs = []
        for c in connector.collection(Collections.CLS_CONFIGURATIONS).find(
                {}, {"_id": 0, "name": 1, "plugin": 1, "active": 1}).limit(_MAX_LIST_DOCS):
            meta = {}
            plugin_cls = plugin_helper.find_by_id(c.get("plugin"))
            if plugin_cls is not None:
                meta = getattr(plugin_cls, "metadata", {}) or {}
            pull = bool(meta.get("pull_supported"))
            push = bool(meta.get("push_supported"))
            role = ("both" if pull and push else "source" if pull
                    else "destination" if push else "unknown")
            cfgs.append({"name": c.get("name"), "active": bool(c.get("active")),
                         "pullSupported": pull, "pushSupported": push, "role": role})
        if not rules and not cfgs:
            return "No CLS business rules or configurations found."
        payload = {"configs": cfgs, "rules": rules}
        if len(cfgs) >= _MAX_LIST_DOCS or len(rules) >= _MAX_LIST_DOCS:
            payload["_note"] = (
                f"Showing at most {_MAX_LIST_DOCS} configs and {_MAX_LIST_DOCS} rules — "
                "there may be more. This is a partial view."
            )
        return _wrap("cls_log_delivery_wiring", payload)
    except Exception:
        logger.error("Error getting CLS mappings.", details=traceback.format_exc())
        return "Error getting CLS mappings. See CE logs for details."


@tool
def get_cre_entities() -> str:
    """List Cloud Risk Exchange ENTITIES and each entity's queryable fields (the dynamic rule vocabulary).

    CRE business-rule fields are per-entity (not a static list), so call this before drafting a CRE
    rule to see which fields are available on the entity the rule targets. Requires 'cre_read'.
    """
    if not _allowed("cre"):
        return _deny("cre")
    if not _budget("get_cre_entities", _CAP_MODULE_TOOL):
        return "Budget for CRE entity lookups is exhausted for this turn."
    try:
        docs = list(connector.collection(Collections.CREV2_ENTITIES).find(
            {}, {"_id": 0, "name": 1, "fields": 1}).limit(_MAX_LIST_DOCS))
        if not docs:
            return "No CRE entities found."
        rows = []
        for d in docs:
            fields = []
            for f in (d.get("fields") or []):
                if not isinstance(f, dict):
                    continue
                ftype = f.get("type")
                field = {"label": f.get("label"), "name": f.get("name"), "type": ftype,
                         # Feed the valid OPERATORS per field (mapped from its type) so a drafted CRE
                         # condition uses an operator the builder actually offers — the other modules'
                         # rule-format tool already carries operators; CRE's dynamic fields must too.
                         "operators": _cre_field_operators(ftype)}
                # CRE entity fields are USER-DEFINED (Schema Editor), so unlike the static query
                # schemas a field CAN declare an allowed-value set — surface it (capped) so a drafted
                # rule condition uses a real value. Tolerant of the key name the editor stores it under.
                values = f.get("enum") or f.get("values") or f.get("choices")
                if isinstance(values, list) and values:
                    field["values"] = values[:20]
                fields.append(field)
            rows.append({"entity": d.get("name"), "fields": fields})
        return _wrap("cre_entities", _capped(rows, _MAX_LIST_DOCS))
    except Exception:
        logger.error("Error getting CRE entities.", details=traceback.format_exc())
        return "Error getting CRE entities. See CE logs for details."


@tool
def get_cre_actions(name: Optional[str] = None) -> str:
    """Show CRE rule wiring (which configs each rule acts on) + recent action-log status counts.

    A CRE rule's `actions` map (configName -> actions) is its wiring; an empty actions map means the
    rule scores/evaluates records but triggers nothing. The action-log status counts show how recent
    actions performed (success/failed/declined/pending_approval/scheduled). Requires 'cre_read'.

    Args:
        name: optional business-rule name to focus on.
    """
    if not _allowed("cre"):
        return _deny("cre")
    if not _budget("get_cre_actions", _CAP_MODULE_TOOL):
        return "Budget for CRE action lookups is exhausted for this turn."
    try:
        query = {"name": name} if name else {}
        rules = list(connector.collection(Collections.CREV2_BUSINESS_RULES).find(
            query, {"_id": 0, "name": 1, "entity": 1, "actions": 1}).limit(_MAX_LIST_DOCS))
        wiring = [{"rule": r.get("name"), "entity": r.get("entity"),
                   "actionsByConfig": {cfg: len(acts or [])
                                       for cfg, acts in (r.get("actions") or {}).items()}}
                  for r in rules]
        status_counts = list(connector.collection(Collections.CREV2_ACTION_LOGS).aggregate([
            *([{"$match": {"rule": name}}] if name else []),
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            {"$limit": 20},
        ]))
        result = {"wiring": wiring,
                  "recentActionStatusCounts": {s["_id"]: s["count"]
                                               for s in status_counts if s.get("_id")}}
        return _wrap("cre_actions", result)
    except Exception:
        logger.error("Error getting CRE actions.", details=traceback.format_exc())
        return "Error getting CRE actions. See CE logs for details."


@tool
def get_unified_mappings(name: Optional[str] = None) -> str:
    """List saved Unified Schema mappings (USB) and any business rules built on them.

    A Unified Schema mapping is a live, saved JOIN across CTE indicators and/or CRE entity
    collections (Universal Schema Builder > Unified Mapping). A Unified Mapping Business Rule
    filters one mapping's joined rows and wires them to CTE sharing (`cteShare`) and/or CRE
    actions (`creActions`) — this is how a joined view actually DOES something, mirroring how a
    CRE business rule needs an Actions-screen wiring to matter. Suggest creating a mapping when
    the user wants to correlate/join CTE and CRE data, and a rule on top when they want that
    joined data to trigger sharing or a CRE action. Requires 'cre_read'.

    Args:
        name: optional mapping name to focus on (also filters rules to that mapping).
    """
    if not _allowed("cre"):
        return _deny("cre")
    if not _budget("get_unified_mappings", _CAP_MODULE_TOOL):
        return "Budget for Unified Schema mapping lookups is exhausted for this turn."
    try:
        mapping_query = {"name": name} if name else {}
        mappings = list(connector.collection(Collections.UNIFIED_MAPPING).find(
            mapping_query, {"_id": 0, "name": 1, "baseTable": 1, "joins": 1, "matchMode": 1}
        ).limit(_MAX_LIST_DOCS))
        if name and not mappings:
            return f"No Unified Schema mapping named '{name}' found."
        rule_query = {"view": name} if name else {}
        rules = list(connector.collection(Collections.UNIFIED_MAPPING_RULES).find(
            rule_query, {"_id": 0, "name": 1, "view": 1, "muted": 1, "cteShare": 1, "creActions": 1}
        ).limit(_MAX_LIST_DOCS))
        mapping_rows = [{
            "mapping": m.get("name"),
            "baseTable": m.get("baseTable"),
            "joinedTables": [j.get("rightTable") for j in (m.get("joins") or [])],
            "matchMode": m.get("matchMode"),
        } for m in mappings]
        rule_rows = [{
            "rule": r.get("name"),
            "mapping": r.get("view"),
            "muted": r.get("muted", False),
            "cteShareDestinations": list((r.get("cteShare") or {}).keys()),
            "creActionConfigs": list((r.get("creActions") or {}).keys()),
        } for r in rules]
        if not mapping_rows and not rule_rows:
            return "No Unified Schema mappings or business rules found."
        return _wrap("unified_mappings", {"mappings": mapping_rows, "rules": rule_rows})
    except Exception:
        logger.error("Error getting Unified Schema mappings.", details=traceback.format_exc())
        return "Error getting Unified Schema mappings. See CE logs for details."


@tool
def get_edm_sharing() -> str:
    """List EDM sharing pairs (source config -> destination config + share action).

    EDM has NO filter business rules — this 1:1 source->destination sharing IS its flow (created on the
    Sharing screen). Use to see what EDM hashes are shared where. Requires 'edm_read'.
    """
    if not _allowed("edm"):
        return _deny("edm")
    if not _budget("get_edm_sharing", _CAP_MODULE_TOOL):
        return "Budget for EDM sharing lookups is exhausted for this turn."
    try:
        docs = list(connector.collection(Collections.EDM_BUSINESS_RULES).find(
            {}, {"_id": 0, "sharedWith": 1}).limit(_MAX_LIST_DOCS))
        pairs = []
        for d in docs:
            for source, dests in (d.get("sharedWith") or {}).items():
                for dest, actions in (dests or {}).items():
                    pairs.append({
                        "source": source, "destination": dest,
                        "actions": [a.get("label") if isinstance(a, dict) else a
                                    for a in (actions or [])],
                    })
        if not pairs:
            return "No EDM sharing configured (no source->destination hash sharing set up yet)."
        if len(docs) >= _MAX_LIST_DOCS:
            return _wrap(
                "edm_sharing",
                {
                    "_note": (
                        f"Showing sharing from the first {_MAX_LIST_DOCS} rules — "
                        "there may be more. This is a partial view."
                    ),
                    "items": pairs,
                },
            )
        return _wrap("edm_sharing", pairs)
    except Exception:
        logger.error("Error getting EDM sharing", details=traceback.format_exc())
        return "Error getting EDM sharing. See CE logs for details."


@tool
def get_edm_hash_status() -> str:
    """Show EDM hash apply-status: hash files still being applied on the Netskope tenant (in-flight).

    Reads the in-flight apply-tracking store. A row whose createdAt is old and not clearing indicates a
    STUCK apply (the tenant keeps returning pending/in-progress) — the signal for an EDM apply problem.
    Requires 'edm_read'.
    """
    if not _allowed("edm"):
        return _deny("edm")
    if not _budget("get_edm_hash_status", _CAP_MODULE_TOOL):
        return "Budget for EDM hash-status lookups is exhausted for this turn."
    try:
        docs = list(connector.collection(Collections.EDM_HASHES_STATUS).find(
            {}, {"_id": 0, "fileSourceType": 1, "message": 1, "createdAt": 1, "updatedAt": 1}
        ).limit(_MAX_HASH_DOCS))
        if not docs:
            return "No EDM hash uploads are pending tenant apply (nothing in-flight)."
        rows = [{"fileSourceType": d.get("fileSourceType"), "message": d.get("message"),
                 "createdAt": str(d.get("createdAt")), "updatedAt": str(d.get("updatedAt"))}
                for d in docs]
        note = ("Each row is a hash file still being applied on the Netskope tenant. A row whose "
                "createdAt is old (not clearing) indicates a STUCK apply.")
        if len(rows) >= _MAX_HASH_DOCS:
            note += f" (Showing the first {_MAX_HASH_DOCS} in-flight rows — this is a partial view.)"
        return _wrap("edm_hash_status", {"inFlight": rows, "note": note})
    except Exception:
        logger.error("Error getting EDM hash status.", details=traceback.format_exc())
        return "Error getting EDM hash status. See CE logs for details."


@tool
def get_cfc_sharing() -> str:
    """List CFC sharing: per source->destination, the rule -> classifier mappings (+ any errorState).

    CFC splits filtering (business rules) from ROUTING: on the Sharing screen a rule is mapped to a
    classifier (+ training type) on a destination config. A mapping with a missing classifierID or an
    errorState points at a deleted/failed classifier. Use to see how CFC rules are wired. Requires 'cfc_read'.
    """
    if not _allowed("cfc"):
        return _deny("cfc")
    if not _budget("get_cfc_sharing", _CAP_MODULE_TOOL):
        return "Budget for CFC sharing lookups is exhausted for this turn."
    try:
        docs = list(connector.collection(Collections.CFC_SHARING).find(
            {}, {"_id": 0, "sourceConfiguration": 1, "destinationConfiguration": 1,
                 "status": 1, "errorState": 1, "mappings": 1}).limit(_MAX_LIST_DOCS))
        if not docs:
            return "No CFC sharing configured."
        rows = []
        for d in docs:
            mappings = [{"rule": m.get("businessRule"), "classifierName": m.get("classifierName"),
                         "classifierID": m.get("classifierID"), "trainingType": m.get("trainingType"),
                         "errorState": m.get("errorState")}
                        for m in (d.get("mappings") or []) if isinstance(m, dict)]
            rows.append({"source": d.get("sourceConfiguration"),
                         "destination": d.get("destinationConfiguration"),
                         "status": d.get("status"), "errorState": d.get("errorState"),
                         "mappings": mappings})
        return _wrap("cfc_sharing", _capped(rows, _MAX_LIST_DOCS))
    except Exception:
        logger.error("Error getting CFC sharing.", details=traceback.format_exc())
        return "Error getting CFC sharing. See CE logs for details."


@tool
def get_cfc_classifiers() -> str:
    """List the CFC classifiers already IN USE across configured sharing mappings.

    Does NOT call the Netskope tenant — the complete live classifier list is fetched by the Sharing
    screen. This shows the classifiers referenced by existing mappings; a mapping whose classifierID is
    missing points at a classifier deleted from the tenant. Requires 'cfc_read'.
    """
    if not _allowed("cfc"):
        return _deny("cfc")
    if not _budget("get_cfc_classifiers", _CAP_MODULE_TOOL):
        return "Budget for CFC classifier lookups is exhausted for this turn."
    try:
        seen = {}
        for d in connector.collection(Collections.CFC_SHARING).find(
                {}, {"_id": 0, "mappings": 1}).limit(_MAX_LIST_DOCS):
            for m in (d.get("mappings") or []):
                if isinstance(m, dict) and m.get("classifierName"):
                    seen[m.get("classifierID") or m.get("classifierName")] = {
                        "name": m.get("classifierName"), "id": m.get("classifierID"),
                        "trainingType": m.get("trainingType"),
                    }
        return _wrap("cfc_classifiers", {
            "inUse": list(seen.values()),
            "note": "Classifiers referenced by existing sharing mappings. The complete live list is "
                    "fetched from the Netskope tenant on the Sharing screen; a mapping missing a "
                    "classifierID points at a classifier deleted from the tenant.",
        })
    except Exception:
        logger.error("Error getting CFC classifiers.", details=traceback.format_exc())
        return "Error getting CFC classifiers. See CE logs for details."


# --- users / RBAC (admin only) ----------------------------------------
@tool
def list_users_and_scopes() -> str:
    """List users and their scopes (no passwords/tokens). Requires the 'admin' scope."""
    if "admin" not in _scopes():
        return "Access denied: listing users requires the 'admin' scope."
    if not _budget("list_users_and_scopes", _CAP_USERS):
        return "Budget for user reads is exhausted for this turn."
    try:
        raw = list(
            connector.collection(Collections.USERS)
            .find({}, {"_id": 0, "username": 1, "scopes": 1})
            .limit(_MAX_LIST_DOCS)
        )
        # PII masking: a raw username is often an email/identity, and the copilot's RBAC guidance
        # (least-privilege recipes, over-broad-scope callouts) reasons over SCOPES, not identities —
        # it never needs the real username. Replace it with a stable, per-turn pseudonymous handle
        # ("user-1", "user-2", …) so the model can still refer to a specific user ("user-3 has admin
        # but only needs cte_read") without the actual username/email ever reaching the LLM. The
        # ordering is the DB order (stable within a turn); the admin maps the handle back in the UI.
        rows = [
            {"user": f"user-{i}", "scopes": doc.get("scopes", [])}
            for i, doc in enumerate(raw, start=1)
        ]
        return _wrap("users_and_scopes", _capped(rows, _MAX_LIST_DOCS))
    except Exception:
        logger.error("Error listing users.", details=traceback.format_exc())
        return "Error listing users. See CE logs for details."


@tool
def get_security_scopes_reference() -> str:
    """Explain CE security scopes and least-privilege role recipes. Requires 'admin'."""
    if "admin" not in _scopes():
        return "Access denied: the scopes reference requires the 'admin' scope."
    return get_knowledge("rbac").get("content", "")


def build_config_tool_registry(scopes=None) -> tuple:
    """Select the scope-permitted Configuration Copilot tools (name -> tool) for a turn.

    The tools themselves are module-level singletons (defined above); this only chooses
    which the caller may use, by scope. Per-turn budget caps + web-usage live on the
    ContextVar set by ``copilot_turn_context`` — so call this INSIDE that context manager
    (``run_copilot_turn`` does). Every tool also re-checks scope internally (defense in depth).

    Args:
        scopes: iterable of the caller's security scopes (used for gating).

    Returns:
        (permitted, ctx) — ``permitted`` maps tool name -> singleton tool; ``ctx`` is the
        active ``CopilotTurnContext`` (budget counters), returned for post-turn reads.
    """
    scope_set = set(scopes or [])
    # Establish the per-turn context if the caller didn't already (via copilot_turn_context):
    # this is the per-turn entry point, called once by run_copilot_turn in the request task,
    # so the ContextVar it sets is visible to every tool for the rest of the turn and dies
    # with the task. If a context IS already active (tests, explicit scoping) we reuse it.
    ctx = _TURN.get()
    if ctx is None:
        ctx = CopilotTurnContext(scopes=frozenset(scope_set))
        _TURN.set(ctx)
    # Build the scope-permitted client tools (name -> tool). Each tool also
    # re-checks scope internally as defense-in-depth.
    has_cte = "cte_read" in scope_set
    has_cto = "cto_read" in scope_set
    has_cls = "cls_read" in scope_set
    has_cre = "cre_read" in scope_set
    has_edm = "edm_read" in scope_set
    has_cfc = "cfc_read" in scope_set
    # Any config module unlocks the shared config-tool set (each tool self-gates per module).
    has_module = has_cte or has_cto or has_cls or has_cre or has_edm or has_cfc
    has_settings = "settings_read" in scope_set
    has_ai = "ai_read" in scope_set

    # Free/local, always available — no scope gate, on ANY page:
    # - get_ce_knowledge: the curated Markdown pack (no DB, no network).
    # - get_deployment_details: env-only platform metadata, the same read /api/ce-details
    #   serves to every authenticated user (Security(..., scopes=[])). Not module data, so
    #   gating it behind a module scope would only make "how is my CE deployed?"
    #   unanswerable for a settings-only or ai_read-only caller.
    permitted = {
        get_ce_knowledge.name: get_ce_knowledge,
        get_deployment_details.name: get_deployment_details,
    }
    if has_module:
        for _tool in (
            list_configurations, get_configuration_details, list_available_plugins,
            get_plugin_capabilities, get_plugin_schema, get_plugin_walkthrough, get_business_rules,
            get_business_rule_format, validate_draft, get_plugin_run_status, get_dashboard_data,
            get_plugin_guide, get_plugin_prefilters,
        ):
            permitted[_tool.name] = _tool
        if has_cte:
            permitted[analyze_cte_config.name] = analyze_cte_config
        if has_cto:
            permitted[analyze_cto_config.name] = analyze_cto_config
        # Per-module inspection tools (the analogue of CTE sharing / CTO queues), scope-gated.
        if has_cls:
            permitted[get_cls_mappings.name] = get_cls_mappings
        if has_cre:
            permitted[get_cre_entities.name] = get_cre_entities
            permitted[get_cre_actions.name] = get_cre_actions
            permitted[get_unified_mappings.name] = get_unified_mappings
        if has_edm:
            permitted[get_edm_sharing.name] = get_edm_sharing
            permitted[get_edm_hash_status.name] = get_edm_hash_status
        if has_cfc:
            permitted[get_cfc_sharing.name] = get_cfc_sharing
            permitted[get_cfc_classifiers.name] = get_cfc_classifiers
    if has_module or has_settings:
        permitted[get_settings.name] = get_settings
        permitted[get_dashboard_data.name] = get_dashboard_data
    if has_settings:
        permitted[get_system_health.name] = get_system_health
        permitted[get_dashboard_data.name] = get_dashboard_data
        # provider (Netskope Tenant) plugin listing self-gates on settings_read — reachable with
        # settings_read even without a module scope. (dict-keyed, so re-adding is a no-op.)
        permitted[list_available_plugins.name] = list_available_plugins
    if has_ai:
        # ai_read reaches list_available_plugins('llm_provider') and get_settings('llm_provider'),
        # both of which self-gate on ai_read — so an ai_read-only caller (no module/settings scope)
        # can still list AI providers and read the active-provider status. Mirrors the internal gates.
        permitted[list_available_plugins.name] = list_available_plugins
        permitted[get_settings.name] = get_settings
    if "admin" in scope_set:
        permitted[list_users_and_scopes.name] = list_users_and_scopes
        permitted[get_security_scopes_reference.name] = get_security_scopes_reference

    return permitted, _current()


@tool
def get_ce_knowledge(area: Optional[str] = None, key: Optional[str] = None) -> str:
    """Return curated Cloud Exchange knowledge (free, local) for grounding answers.

    Use this for "why / what's a good value / what does this dashboard metric
    mean / how do I fix CE_xxxx / what does this scope grant". Free — does not
    count against the web_search budget. For full third-party plugin setup steps,
    point the user to the plugin guide on docs.netskope.com (found via web_search).

    Also carries curated notes for NEWER / PREVIEW CE features not yet on
    docs.netskope.com (the 'feature_*' areas: Unified Mapping, CRE Auto-Mapper,
    LLM Provider, Posture/Log Assessment, the Copilot itself, Secrets Manager,
    Health Check). For those, search docs.netskope.com FIRST and prefer/cite it if
    it covers the feature; use this pack as the fallback only when the docs don't.

    Args:
        area: a knowledge area, e.g. 'dashboard_system', a per-module config pack
            ('config_cte'/'config_cto'/'config_cls'/'config_cre'/'config_edm'/'config_cfc'),
            'plugin_tile', 'rbac'. Matched loosely
            (so 'cte', 'system', 'errors' also work). Omit to list available areas.
        key: optional section/heading or code within the area (e.g. 'CE_1075').
    """
    try:
        result = get_knowledge(area, key)
        if "content" in result:
            header = f"{result.get('area', area)}"
            if result.get("key"):
                header += f" / {result['key']}"
            content = result["content"]
            return header + ":\n" + content
        if "areas" in result:
            lines = [f"- {name}: {summary}" for name, summary in result["areas"].items()]
            return "Available knowledge areas:\n" + "\n".join(lines)
        if "error" in result:
            return f"{result['error']} Available areas: {', '.join(result.get('available_areas', list_areas()))}"
        return json.dumps(result, default=str)
    except Exception:
        logger.error("Error reading CE knowledge.", details=traceback.format_exc())
        return "Error reading CE knowledge. See CE logs for details."
