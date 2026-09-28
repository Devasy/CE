"""Analytics mappings."""

REPOSITORY_MAPPING = {
    "Default": "0",
    "Beta": "1",
    "Crest Hotfix 1": "2",
    "Custom Plugin": "3",
    "Crest Hotfix 2": "4",
    "Custom Repo": "e",
    "Unknown": "f",
}

OS_MAPPING = {
    "Ubuntu 18": "0",
    "Ubuntu 20": "1",
    "Ubuntu 22": "2",
    "Ubuntu 24": "3",
    "Ubuntu": "4",
    "RHEL 7": "5",
    "RHEL 8": "6",
    "RHEL 9": "7",
    "RHEL": "8",
    "CentOS 7": "9",
    "CentOS 8": "a",
    "CentOS": "b",
    "Unknown": "f",
}

MODULES_MAPPING_NUMBERS = {
    "CLS": 1,
    "CTO": 2,
    "CTE": 4,
    "CREV2": 8,
    "EDM": 16,
    "CFC": 32,
}

PLUGINS_STATE_MAPPING = {
    "PULL": 1,
    "SHARE": 2,
    "SYNC": 4,
    "UPDATE": 8,
}

PLUGIN_STATS = {False: "0", True: "1", None: "e"}

HOST_PLATFORM_MAPPING = {
    "vmware": "0",
    "aws": "1",
    "azure": "2",
    "gcp": "3",
    "microsoft": "4",  # Hyper-V
    "custom": "f",
}

# --------------------------------------------------------------------------------------
# AI Copilot analytics ("ai" analytics type).
#
# These are WIRE FORMATS: the encoded value is a position or a code, not a label. Adding
# an entry is a one-line append; RENUMBERING or REORDERING one silently reinterprets
# every report already collected under the old assignment.
# --------------------------------------------------------------------------------------

# P3. Values match the AIProvider enum; None = no active provider.
AI_PROVIDER_MAPPING = {"anthropic": "0", "custom": "1", None: "f"}

# P4, 2 chars: 00-fd for supported models, "fe" = unrecognised, "ff" = none set. Two
# chars rather than one because a single nibble exhausts at 14 slots and the model list
# turns over faster than anything else here. Mirrors SUPPORTED_MODELS in anthropic_llm.
AI_MODEL_MAPPING = {
    "claude-opus-4-8": "00",
    "claude-opus-5": "01",
    "claude-sonnet-5": "02",
}

# P5. Mirrors ModelSpec.effort_levels in the anthropic_llm plugin. "e" = other, "f" = unset.
AI_EFFORT_MAPPING = {"low": "0", "medium": "1", "high": "2"}

# Reliability buckets for x4/x5/x6. Resolution is by EXCLUSION: anything classified that
# is not api_token and not network falls into "code", so an LLMErrorType added later lands
# somewhere instead of vanishing and inflating the derived success count.
#
# "client_disconnected" (the user pressing Stop / closing the drawer) is bucketed as
# "network" so that every error type resolves somewhere and
# `success = turns - code - apiToken - network` stays exact. The trade-off: "network"
# mixes provider timeouts with user abandonment. The diagnose report separates them.
#
# "iteration_limit" sits in "code" AND has its own counter (L1/C1), so it is a subset.
AI_ERROR_BUCKETS = {
    "code": ["parse_error", "schema_compilation", "deprecated_model", "iteration_limit"],
    "api_token": ["rate_limit", "auth_error", "context_limit_exceeded"],
    "network": ["timeout", "server_error", "client_disconnected"],
}

# C8 histogram order. Mirrors DOWN_REASONS in the UI's CopilotFeedbackBar.jsx.
AI_DOWN_REASON_ORDER = [
    "Incorrect",
    "Incomplete",
    "Not relevant",
    "Wrong suggestion",
    "Other",
]

# N2/N3: nibble position = index in this list. APPEND-ONLY — a new rule kind goes on the
# END even though that breaks the alphabetical ordering, because position IS the wire
# format. Mirrors the kind= values in utils/attention_rules.py — 11 kinds, so N2 and N3
# are 11 nibbles each and segment N is 24 chars.
#
# "platform_stall" (the mass-staleness storm guard) held slot 5 and is GONE: the engine no
# longer collapses a platform-wide stall into one summary finding — every stale config now
# surfaces as its own "staleness" finding. Its slot was RECLAIMED rather than left as
# permanent dead space because the "ai" analytics type has not shipped (7.0.0 pre-GA), so
# no collected report carries the old positions. That window closes at GA: from then on
# a retired kind keeps its slot and only appends are allowed.
#
# Renumbering here is a WIRE-FORMAT change — the reference decoder
# (.claude/skills/ai-copilot/references/decode_ai_analytics.py) and the Netskope-facing
# mapping sheet carry the same order and must be updated in the same change.
AI_FINDING_KIND_ORDER = [
    "cert_expiry",
    "cfc_deleted_classifier",
    "edm_apply_stuck",
    "module_unconfigured",
    "no_business_rules",
    "plugin_error_logs",
    "queue_backpressure",
    "rule_unwired",
    "run_cadence_drift",
    "run_failure_streak",
    "staleness",
]

# N1 module bitmask. Same bit values as MODULES_MAPPING_NUMBERS, plus the "system"
# pseudo-module that platform-level rules (queue_backpressure, cert_expiry) report under.
# The module ids are config_tools._MODULE_COLLECTION's keys — lowercase, and note "cto",
# not the "itsm" settings alias.
AI_FINDING_MODULE_NUMBERS = {
    "cls": 1,
    "cto": 2,
    "cte": 4,
    "cre": 8,
    "edm": 16,
    "cfc": 32,
    "system": 64,
}
