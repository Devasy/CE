"""Mongo input loaders for the attention rule engine (plan v5 §6).

Shared by the periodic ``attention_scan`` celery task AND the on-demand
``POST /copilot/attention/{id}/verify`` endpoint, so both evaluate the rules over the SAME
inputs. Keeping them identical is load-bearing: ``attention_rules.evaluate`` treats a missing
input family (``rules``/``error_logs``/``enabled_modules`` = None) as "skip these rules", and a
verify that skips a family would wrongly auto-resolve any still-true finding of that kind
(the finding lands in neither candidates nor... the caller's still-open check).

Lives outside ``celery/`` so the API worker can import it without pulling in the celery app.
"""

import traceback
from datetime import datetime, timedelta

from netskope.common.utils import Collections, DBConnector
from netskope.common.utils.logger import PrefixedLogger

connector = DBConnector()
logger = PrefixedLogger("[AI-COPILOT]")

# Only the fields the rule engine needs — keeps the scan cheap + never pulls secret params.
# ``plugin`` (the plugin id) is projected so load_configs can resolve the plugin manifest and
# precompute ``_runnable`` (see below) — never a secret param.
CONFIG_PROJECTION = {
    "_id": 0, "name": 1, "active": 1, "lastRunSuccess": 1, "lastRunAt": 1,
    "lockedAt": 1, "pollInterval": 1, "pollIntervalUnit": 1, "lastErrorCode": 1, "plugin": 1,
}

# plugin_error_logs looks at this much recent history each scan. Wider than the 5-min scan
# cadence on purpose: a code must recur across scans to stay open, and a burst that stops
# auto-resolves once it ages out of the window.
ERROR_LOG_WINDOW = timedelta(minutes=30)

# The settings keys the rule engine reads (should_pull/certExpiry) + the module toggles.
SETTINGS_PROJECTION = {"_id": 0, "should_pull": 1, "certExpiry": 1, "platforms": 1}

# Resolved findings linger a month (TTL on expireAt) so the UI can show "recently resolved".
# ONE definition, used by BOTH the periodic scan's auto-resolve update AND verify_finding's — so
# the retention window can't drift between the two paths that write it (they share this collection).
RESOLVED_TTL = timedelta(days=30)


def build_auto_resolve_update(now):
    """Build the `$set` payload that auto-resolves a finding (status + resolvedAt + expireAt TTL).

    Shared by the scan and the verify endpoint so both prune resolved findings on the SAME schedule.
    """
    return {"status": "auto_resolved", "resolvedAt": now, "expireAt": now + RESOLVED_TTL}


def llm_provider_configured() -> bool:
    """Return True if ANY LLM provider is configured (active OR disabled).

    EXISTENCE only — deliberately NOT ``active`` (a disabled-but-configured provider can be
    re-enabled without losing history), which is why this is distinct from the analyze route's
    active-provider resolve. ONE shared definition for the "should AI even bother" gate used by the
    attention feed/summary, the attention scan (write-side), and provider delete (last-provider
    teardown) — so the three can't drift (e.g. one silently adding an ``active`` clause). Lives HERE
    (not the langchain-heavy llm_invoke) so the celery scan never drags that import into its runtime.
    Cheap indexed ``find_one``; never raises/logs.
    """
    return connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find_one(
        {}, {"_id": 1}
    ) is not None


# module id -> its config collection is the CANONICAL config_tools._MODULE_COLLECTION (imported
# lazily in load_configs so this celery-side module stays light, and so the scan + the copilot's
# config-read tools can never read DIFFERENT collections for the same module). The module id is the
# copilot's own key; it does NOT always equal the collection prefix (cto -> itsm_*, cre -> crev2_*).
# CLS/CRE/EDM/CFC configs all write a bare-bool lastRunSuccess (one "pull" op) — see
# attention_rules._MODULE_OPS.


def load_configs() -> list:
    """All CE module config docs (CTE/CTO/CLS/CRE/EDM/CFC), each tagged with its module.

    Only the health fields the rule engine reads (never secret params). ~6 small collections,
    a few docs each, so this stays well under the <1s scan budget even at N=500.

    Each config is tagged with a precomputed ``_runnable`` bool — the PREREQUISITE for the
    run-based rules (staleness / cadence-drift / run-failure). A run-based alert only makes sense
    if there is actually something that RUNS: ``_runnable`` is True iff the config's plugin is
    INSTALLED (find_by_id resolves it) AND the plugin is pull-capable (manifest ``pull_supported``).
    A push-only destination (e.g. a syslog forwarder) never runs a scheduled pull, so "hasn't run
    in N min" / "cadence drifted" would be a false alarm — the engine skips run rules when
    ``_runnable`` is False (and resolves any stale ones). Resolving the manifest here keeps the pure
    rule engine Mongo/registry-free (mirrors the ``_sharingWired`` precompute in load_rules).
    ``active`` (module-enabled/config-enabled) is evaluated in the engine, not folded in here, so an
    inactive-but-runnable config still resolves correctly on re-activation.
    """
    from netskope.common.utils.plugin_helper import PluginHelper
    helper = PluginHelper()
    runnable_cache: dict = {}  # plugin id -> bool (resolve each installed plugin's manifest once)

    def _is_runnable(plugin_id) -> bool:
        if not plugin_id:
            return False
        if plugin_id in runnable_cache:
            return runnable_cache[plugin_id]
        result = False
        try:
            plugin_cls = helper.find_by_id(plugin_id)  # None => plugin not installed
            if plugin_cls is not None:
                meta = getattr(plugin_cls, "metadata", {}) or {}
                # Pull-capable plugins run on a schedule; a push-only plugin has no scheduled run.
                # Absent pull_supported (older/loosely-specced manifest) => assume runnable, so we
                # never SILENCE a real alert on missing metadata (fail toward alerting).
                result = meta.get("pull_supported", True) is not False
        except Exception:
            # Registry hiccup — don't let it silence alerts; treat as runnable (fail open).
            logger.debug("Plugin runability probe fell back to runnable.",
                         details=traceback.format_exc())
            result = True
        runnable_cache[plugin_id] = result
        return result

    # CLS ONLY: a CLS source's recurring pull is WIRING-DRIVEN — the tenant common.pull beat is
    # scheduled only while some business rule maps that source in siemMappings (verified against
    # plugin_provider_helper.get_all_configured_subtypes' br_query gate). Delete its Log Delivery
    # mapping and the schedule is legitimately torn down, so lastRunAt stops advancing BY DESIGN —
    # a staleness/cadence alert there is a false alarm. Precompute which CLS source configs are
    # still referenced by a siemMappings KEY (source side) so the engine can suppress run rules for
    # the unwired ones. Done here (not the pure engine) to keep attention_rules Mongo-free — same
    # pattern as _sharingWired in load_rules. Every OTHER module pulls on its own PeriodicTask
    # independent of wiring (CTE/CTO/CRE/EDM/CFC — verified), so this flag is CLS-specific.
    cls_pull_wired: set = set()
    for rule in connector.collection(Collections.CLS_BUSINESS_RULES).find(
            {}, {"_id": 0, "siemMappings": 1}):
        # siemMappings = {<sourceConfigName>: [<destConfigName>, ...]}; the KEY is the source that
        # gets scheduled. MATCH THE REAL SCHEDULER GATE EXACTLY: get_all_configured_subtypes
        # (plugin_provider_helper) keeps a CLS source's pull scheduled iff a rule has the KEY
        # `siemMappings.<name>` EXIST — `{"siemMappings.%s": {"$exists": true}}` — regardless of
        # whether its destination list is empty. So a source is pull-wired if it is a KEY at all
        # (do NOT additionally require a non-empty dest list — that was stricter than the scheduler
        # and would wrongly suppress a source that IS still scheduled with an empty dest list).
        for source in (rule.get("siemMappings") or {}):
            cls_pull_wired.add(source)

    # Canonical module->collection map (config_tools), imported lazily to keep this module light.
    from netskope.common.utils.config_tools import _MODULE_COLLECTION
    out = []
    for module, coll in _MODULE_COLLECTION.items():
        for doc in connector.collection(coll).find({}, CONFIG_PROJECTION):
            doc["module"] = module
            doc["_runnable"] = _is_runnable(doc.get("plugin"))
            # Only CLS gets a wiring-driven pull; for every other module _pullWired is True (their
            # pull runs regardless of downstream wiring, so run rules always apply).
            doc["_pullWired"] = (
                doc.get("name") in cls_pull_wired if module == "cls" else True
            )
            out.append(doc)
    return out


def load_rules() -> dict:
    """Load business rules per rule-module — only the wiring fields (name + routing targets).

    EDM is intentionally omitted: its ``edm_business_rules`` collection stores 1:1 sharing
    (the flow itself), not filter rules, so the no_business_rules / rule_unwired families do
    not apply to it. CFC wiring is CROSS-COLLECTION — a CFC rule is "wired" iff some
    ``cfc_sharing.mappings[]`` entry references it by name; that join is done HERE (the rule
    engine stays pure/Mongo-free) and surfaced as a synthetic ``_sharingWired`` bool per rule.
    """
    out = {
        # CTE carries TWO routing targets, not one: a Threat Indicators rule shares via
        # `sharedWith`, a CRE-entity rule via `creShare` — project both or every CRE-entity rule
        # looks unwired. `entity` names what the rule matches (so the finding title can say
        # "'Users' records" instead of "indicators"), and `disabledByCre` marks a rule the CRE
        # module's disable switch has LOCKED against edits. Same shape on the CRE side with
        # `disabledByCte`. See _WIRING / _CROSS_MODULE_LOCK in attention_rules.
        "cte": list(connector.collection(Collections.CTE_BUSINESS_RULES).find(
            {}, {"_id": 0, "name": 1, "sharedWith": 1, "creShare": 1, "entity": 1,
                 "disabledByCre": 1})),
        "cto": list(connector.collection(Collections.ITSM_BUSINESS_RULES).find(
            {}, {"_id": 0, "name": 1, "queues": 1})),
        "cls": list(connector.collection(Collections.CLS_BUSINESS_RULES).find(
            {}, {"_id": 0, "name": 1, "siemMappings": 1})),
        "cre": list(connector.collection(Collections.CREV2_BUSINESS_RULES).find(
            {}, {"_id": 0, "name": 1, "actions": 1, "disabledByCte": 1})),
    }
    # CFC: rule is wired iff referenced by a cfc_sharing mapping (the `mapped` flag the UI shows).
    cfc_rules = list(connector.collection(Collections.CFC_BUSINESS_RULES).find(
        {}, {"_id": 0, "name": 1}))
    wired_names = set()
    for sharing in connector.collection(Collections.CFC_SHARING).find(
            {}, {"_id": 0, "mappings.businessRule": 1}):
        for mapping in sharing.get("mappings") or []:
            if mapping.get("businessRule"):
                wired_names.add(mapping["businessRule"])
    for rule in cfc_rules:
        rule["_sharingWired"] = rule.get("name") in wired_names
    out["cfc"] = cfc_rules
    return out


def load_error_logs(now: datetime) -> list:
    """Load recent plugin-raised error/warning logs grouped by errorCode.

    The errorCode IS the plugins' structured error format; one sample message + resolution
    per code rides along for the finding evidence (the [Diagnose] degradation hint).
    """
    pipeline = [
        {"$match": {
            "type": {"$in": ["error", "warning"]},
            "errorCode": {"$nin": [None, ""]},
            "createdAt": {"$gte": now - ERROR_LOG_WINDOW},
        }},
        {"$group": {
            "_id": "$errorCode",
            "count": {"$sum": 1},
            "message": {"$last": "$message"},
            "resolution": {"$last": "$resolution"},
        }},
        {"$sort": {"count": -1}},
        {"$limit": 25},
    ]
    return [
        {"errorCode": r["_id"], "count": r["count"],
         "message": r.get("message"), "resolution": r.get("resolution")}
        for r in connector.collection(Collections.LOGS).aggregate(pipeline)
    ]


# All CE module ids the attention engine senses. Platform toggle keys match the module id for
# every module EXCEPT CTO (whose settings.platforms key is "itsm") — see settings.py, which
# seeds cls/cre/edm/cfc keys directly.
_ALL_MODULES = {"cte", "cto", "cls", "cre", "edm", "cfc"}


def load_module_signals() -> dict:
    """Per-module signals for the module-specific attention rules (Phase 3).

    Small collections, loaded every scan (well under the <1s budget). Each family the rule engine
    treats as "skip when absent", so passing this dict is safe even for deployments with no data.
    - ``edm_apply``: EDM in-flight tenant apply tracking (edm_hashes_status) — a doc that never
      clears is a STUCK apply (edm_apply_stuck).
    - ``cfc_sharing``: CFC sharing docs with their rule->classifier mappings — a mapping missing
      its classifierID (or carrying an errorState) points at a classifier deleted/failed on the
      tenant (cfc_deleted_classifier).
    """
    return {
        "edm_apply": list(connector.collection(Collections.EDM_HASHES_STATUS).find(
            {}, {"_id": 0, "fileSourceType": 1, "fileSourceID": 1, "message": 1, "createdAt": 1})),
        "cfc_sharing": list(connector.collection(Collections.CFC_SHARING).find(
            {}, {"_id": 0, "sourceConfiguration": 1, "destinationConfiguration": 1, "mappings": 1})),
    }


# settings.platforms stores the CTO toggle under its ITSM name; every other module key == module id.
_PLATFORM_ALIAS = {"itsm": "cto"}


def disabled_modules(settings: dict) -> set:
    """Modules explicitly toggled OFF under Settings → General (copilot module ids).

    Only an explicit ``false`` counts — an absent key means the module was never toggled (enabled).
    ONE definition of the platforms→disabled derivation (incl. the itsm→cto alias), shared by the
    attention scan (``enabled_modules``) and the copilot's journey guidance (``config_copilot.
    _disabled_modules``) so a module the scan senses can't disagree with one the copilot thinks is off.
    """
    platforms = (settings or {}).get("platforms") or {}
    return {_PLATFORM_ALIAS.get(k, k) for k, v in platforms.items() if v is False}


def enabled_modules(settings: dict) -> set:
    """Modules toggled ON under Settings → General (absent key == enabled by default)."""
    return _ALL_MODULES - disabled_modules(settings)
