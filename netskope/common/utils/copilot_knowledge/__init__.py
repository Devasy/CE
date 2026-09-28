"""Curated, local knowledge pack for the Configuration Copilot.

Version-controlled **Markdown** the ``get_ce_knowledge`` agent tool reads to
ground "why / what's a good value / what does this metric mean / how do I fix
CE_xxxx / what does this scope grant" answers — without spending the web-search
budget. Markdown (not JSON) because the content is mostly prose the model
consumes directly: it's human-authorable and reviewable, multi-line text
needs no escaping, and the model reads it natively. Deliberately small; grows
with feedback. Full third-party plugin-setup steps are NOT curated here — they
come from ``web_search`` of the plugin guide on docs.netskope.com, so we don't
maintain a parallel (and drifting) copy. NOTE these packs are the PREFERRED,
free, local grounding source AND the ONLY grounding on an air-gapped deployment
where docs.netskope.com is unreachable — they are not merely a "web is down"
fallback. So CE-platform concepts (config best-practices, settings, dashboard
metrics, error codes, RBAC) are curated here; vendor/plugin specifics are left
to web_search. The ``deployment`` area is the deployment OPTION MATRIX (what each
platform-provider / host-OS / SA-vs-HA / container-vs-VM value means); the LIVE values
for the running host come from the ``get_deployment_details`` tool, not from here.
Module SETTINGS guidance lives inside each ``config/<module>.md``
(its "Setting" section); the ``dashboard_<module>`` areas alias to the module
config pack so a dashboard question still resolves offline with the module's
flow context (the live numbers come from get_dashboard_data).

Each area is a ``.md`` file under this package directory. The loader reads them
relative to ``__file__`` (the files ship with the package, like plugin
manifests) and caches per file. Returns are plain dict so the tool wrapper can
serialise them for the model; ``content`` is the Markdown text.
"""

import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

_PACK_DIR = Path(__file__).parent

# Public area name -> Markdown file relative to this package. These names are
# what get_ce_knowledge(area=...) accepts; keep them human-guessable.
_AREAS: dict[str, str] = {
    "dashboard_system": "dashboards/system.md",
    "dashboard_cte": "dashboards/cte.md",
    "dashboard_cto": "dashboards/cto.md",
    "plugin_tile": "dashboards/plugin_tile.md",
    "config_cte": "config/cte.md",
    "config_cto": "config/cto.md",
    "config_cls": "config/cls.md",
    "config_cre": "config/cre.md",
    "config_edm": "config/edm.md",
    "config_cfc": "config/cfc.md",
    "config_system": "config/system.md",  # Settings > General (proxy, HA, secrets, logging)
    "rbac": "rbac.md",
    # The deployment OPTION MATRIX (what the possible platform/OS/type/flavour values mean).
    # The LIVE values for this host come from the get_deployment_details tool, not from here.
    "deployment": "deployment.md",
    # Preview/newer CE features not yet on docs.netskope.com. These packs are the FALLBACK the model
    # uses when a web_search of the public docs returns nothing for the feature (web still wins the
    # moment a public page ships — each pack's header says so). See the <grounding> block in
    # config_copilot.py: try docs.netskope.com FIRST, fall back to get_ce_knowledge for these.
    "feature_unified_mapping": "features/unified_mapping.md",
    "feature_cre_auto_mapper": "features/cre_auto_mapper.md",
    "feature_llm_provider": "features/llm_provider.md",
    "feature_posture_assessment": "features/posture_assessment.md",
    "feature_ai_copilot": "features/ai_copilot.md",
    "feature_secret_managers": "features/secret_managers.md",
    "feature_health_check": "features/health_check.md",
}


@lru_cache(maxsize=None)
def _load_file(rel: str) -> str:
    """Load and cache one knowledge Markdown file; '' if missing/unreadable."""
    path = _PACK_DIR / rel
    if not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def list_areas() -> list[str]:
    """All knowledge areas the pack can answer for."""
    return sorted(_AREAS)


def _summary(text: str) -> str:
    """First content paragraph of a doc (skips the leading H1), for the index."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            return stripped
    return ""


def _resolve_area(area: str) -> Optional[str]:
    """Map a user/model-supplied area to a known key (fuzzy, case-insensitive)."""
    if not area:
        return None
    norm = area.strip().lower().replace("-", "_").replace(" ", "_")
    if norm in _AREAS:
        return norm
    aliases = {
        "system": "dashboard_system",
        "system_health": "dashboard_system",
        "health": "dashboard_system",
        "queues": "dashboard_system",
        "cte": "config_cte",
        "threat_exchange": "config_cte",
        "cto": "config_cto",
        "itsm": "config_cto",
        "ticket_orchestrator": "config_cto",
        "cls": "config_cls",
        "log_shipper": "config_cls",
        "cre": "config_cre",
        "crev2": "config_cre",
        "risk_exchange": "config_cre",
        "edm": "config_edm",
        "exact_data_match": "config_edm",
        "cfc": "config_cfc",
        "custom_file_classification": "config_cfc",
        # Module dashboards have no dedicated pack (only system/cte/cto do); resolve a new-module
        # dashboard question to that module's config pack (flow + screen context) — the live
        # metrics come from get_dashboard_data, this supplies the "what it means" grounding.
        "dashboard_cls": "config_cls",
        "dashboard_cre": "config_cre",
        "dashboard_edm": "config_edm",
        "dashboard_cfc": "config_cfc",
        # Settings > General (config guidance) — distinct from "system"/"health" above, which
        # resolve to the system-status DASHBOARD. Proxy/HA/secrets/logging questions land here.
        "config_system": "config_system",
        "system_settings": "config_system",
        "general": "config_system",
        "general_settings": "config_system",
        "settings": "config_system",
        "proxy": "config_system",
        "scopes": "rbac",
        "users": "rbac",
        # The signed-in user's own Account dialog (change password / logout) is documented
        # in the rbac pack's "Account Settings" section — same identity/permissions topic.
        "account": "rbac",
        "account_settings": "rbac",
        "change_password": "rbac",
        "password": "rbac",
        "password_policy": "rbac",
        "logout": "rbac",
        "profile": "rbac",
        # Deployment option matrix. Colloquial names for the four axes and for the
        # concrete shapes users name them by ("the OVA", "are we on HA", "CE as VM").
        "deployment_options": "deployment",
        "deployment_type": "deployment",
        "deployment_types": "deployment",
        "platform_provider": "deployment",
        "host_os": "deployment",
        "flavour": "deployment",
        "flavor": "deployment",
        "topology": "deployment",
        "install": "deployment",
        "installation": "deployment",
        "ha": "deployment",
        "high_availability": "deployment",
        "standalone": "deployment",
        "sa": "deployment",
        "vm": "deployment",
        "ce_as_vm": "deployment",
        "appliance": "deployment",
        "ova": "deployment",
        "vhdx": "deployment",
        "ami": "deployment",
        "container": "deployment",
        "containers": "deployment",
        # Preview-feature packs — colloquial names users/model may ask by.
        "unified_mapping": "feature_unified_mapping",
        "unified_join": "feature_unified_mapping",
        "unified_join_builder": "feature_unified_mapping",
        "join_builder": "feature_unified_mapping",
        "unified_view": "feature_unified_mapping",
        "unified_schema": "feature_unified_mapping",
        "universal_schema": "feature_unified_mapping",
        "universal_schema_builder": "feature_unified_mapping",
        "usb": "feature_unified_mapping",
        "unified_mapping_rule": "feature_unified_mapping",
        "unified_mapping_rules": "feature_unified_mapping",
        "unified_business_rule": "feature_unified_mapping",
        "cre_auto_mapper": "feature_cre_auto_mapper",
        "auto_mapper": "feature_cre_auto_mapper",
        "automapper": "feature_cre_auto_mapper",
        "auto_map": "feature_cre_auto_mapper",
        "field_mapping": "feature_cre_auto_mapper",
        "llm_provider": "feature_llm_provider",
        "llm": "feature_llm_provider",
        "ai_provider": "feature_llm_provider",
        "provider": "feature_llm_provider",
        "posture": "feature_posture_assessment",
        "posture_assessment": "feature_posture_assessment",
        "log_analysis": "feature_posture_assessment",
        "analyze": "feature_posture_assessment",
        "analyse": "feature_posture_assessment",
        "assess": "feature_posture_assessment",
        "triage": "feature_posture_assessment",
        "ai_copilot": "feature_ai_copilot",
        "copilot": "feature_ai_copilot",
        "assistant": "feature_ai_copilot",
        "secret_managers": "feature_secret_managers",
        "secret_manager": "feature_secret_managers",
        "secrets_manager": "feature_secret_managers",
        "secrets": "feature_secret_managers",
        "vault": "feature_secret_managers",
        "hashicorp": "feature_secret_managers",
        "key_vault": "feature_secret_managers",
        "health_check": "feature_health_check",
        "healthcheck": "feature_health_check",
        "pre_upgrade": "feature_health_check",
        "pre_upgrade_check": "feature_health_check",
        "upgrade_check": "feature_health_check",
        "preflight": "feature_health_check",
    }
    if norm in aliases:
        return aliases[norm]
    for key in _AREAS:  # substring fallback: "cte dashboard" -> dashboard_cte
        if norm in key or key in norm:
            return key
    return None


def _extract_section(text: str, key: str) -> Optional[str]:
    """Return the Markdown section whose heading contains ``key`` (best-effort).

    Matches a heading line (``#``..``######``) that contains the key
    case-insensitively, and returns from that heading up to the next heading of
    the same or higher level. Returns None if no heading matches.
    """
    lines = text.splitlines()
    needle = key.strip().lower()
    start = None
    start_level = 0
    for i, line in enumerate(lines):
        m = re.match(r"^(#{1,6})\s+(.*)$", line)
        if m and needle in m.group(2).strip().lower():
            start = i
            start_level = len(m.group(1))
            break
    if start is None:
        return None
    end = len(lines)
    for j in range(start + 1, len(lines)):
        m = re.match(r"^(#{1,6})\s+", lines[j])
        if m and len(m.group(1)) <= start_level:
            end = j
            break
    return "\n".join(lines[start:end]).strip()


def get_knowledge(area: Optional[str] = None, key: Optional[str] = None) -> dict:
    """Return curated knowledge for an area (and optionally a section by heading).

    - area=None              -> an index: available areas + each doc's one-line summary.
    - area set, key=None     -> the whole area document (Markdown in ``content``).
    - area + key             -> just the matching section if found, else the whole doc.

    Always returns a dict (never raises) so the tool wrapper can serialise it.
    """
    if not area:
        return {
            "areas": {name: _summary(_load_file(rel)) for name, rel in _AREAS.items()},
            "usage": "Call get_ce_knowledge(area=<one of the keys>) for the full content.",
        }

    resolved = _resolve_area(area)
    if resolved is None:
        return {"error": f"Unknown area '{area}'.", "available_areas": list_areas()}

    text = _load_file(_AREAS[resolved])
    if not text:
        return {"area": resolved, "content": "", "note": "No content available for this area yet."}

    if key:
        section = _extract_section(text, key)
        if section:
            return {"area": resolved, "key": key, "content": section}
        return {"area": resolved, "key": key, "matched": False, "content": text}

    return {"area": resolved, "content": text}
