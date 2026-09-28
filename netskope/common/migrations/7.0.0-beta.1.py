"""Migrations for 7.0.0-beta.1 release."""

import traceback

import os
import shutil
import subprocess
import sys
import zipfile

from pymongo import ASCENDING, UpdateMany

from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    SecretDict,
)
from netskope.integrations.cte.utils.entity import THREAT_INDICATORS_ENTITY

connector = DBConnector()
logger = Logger()
plugin_helper = PluginHelper()

LAST_UPDATED_BACKFILL_BATCH_SIZE = 5000

def _ensure_index(col, keys, **opts):
    """Create an index only if an index on the SAME key spec does not already exist.

    Why this instead of a bare ``create_index``: a migration can be re-invoked after a partial
    failure (a later step raised, the runner retries the whole script). A bare ``create_index`` is
    only safely idempotent when the re-run passes IDENTICAL options; if a prior attempt created the
    same key with different options / a different name, the retry raises IndexOptionsConflict /
    IndexKeySpecsConflict and aborts the migration AGAIN — unrecoverable without manual DB surgery.
    Checking existence first makes the retry a clean no-op. ``keys`` is a str (single ascending
    field) or a list of (field, direction) pairs, matching pymongo's ``create_index``.
    """
    key_spec = [(keys, 1)] if isinstance(keys, str) else list(keys)
    try:
        existing = col.index_information()  # {name: {"key": [(field, dir), ...], ...}}
    except Exception:
        existing = {}
    for info in existing.values():
        if list(info.get("key", [])) == key_spec:
            return  # an index on this exact key spec already exists — nothing to do
    col.create_index(keys, **opts)


# CLS / EDM / CFC only — the modules that use the affected bundled libraries.
_CONFIG_COLLECTIONS = (
    Collections.CLS_CONFIGURATIONS,
    Collections.EDM_CONFIGURATIONS,
    Collections.CFC_CONFIGURATIONS,
)

PLUGINS_TO_UPDATE = [
    "amazon_security_lake",
    "kafka_cls",
    "microsoft_sql_edm",
    "mysql_edm",
    "oracledb_edm",
    "smb_file_share_cfc",
    "smb_file_share_edm",
]


def _disable_plugin_configurations(plugin_id):
    """Disable active CLS, EDM, and CFC configurations for *plugin_id*.

    Uses a direct MongoDB ``update_many`` so configurations are disabled
    even when the plugin cannot be imported on Python 3.12 (e.g. custom
    uploads still shipping cpython-311 native libraries).
    """
    disabled = 0
    for collection in _CONFIG_COLLECTIONS:
        result = connector.collection(collection).update_many(
            {"plugin": plugin_id, "active": True},
            {"$set": {"active": False}},
        )
        disabled += result.modified_count
    if disabled:
        logger.info(f"Disabled {disabled} configuration(s) for {plugin_id}.")
    return disabled


def migrate_native_lib_plugins():
    """Refresh Python 3.12-affected plugins from the image zip and disable configs."""
    base_path = "/opt"
    zip_file_path = os.path.join(base_path, "default_plugins.zip")
    plugins_base_path = os.path.join(base_path, "netskope", "plugins")

    try:
        refreshed = []

        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_namelist = zip_ref.namelist()

            for plugin in PLUGINS_TO_UPDATE:
                prefix = f"netskope/repos/Default/{plugin}/"
                source_plugin_path = os.path.join(
                    base_path, "netskope", "repos", "Default", plugin
                )

                plugin_files = [f for f in zip_namelist if f.startswith(prefix)]

                # Use a flag instead of early 'continue' to ensure the disable logic still runs
                source_is_ready = False

                if plugin_files:
                    if os.path.isdir(source_plugin_path):
                        shutil.rmtree(source_plugin_path)

                    for file_path in plugin_files:
                        zip_ref.extract(file_path, base_path)

                    if os.path.isdir(source_plugin_path):
                        source_is_ready = True

                # Single unified loop for both copy and disable operations
                for item in os.listdir(plugins_base_path):
                    item_path = os.path.join(plugins_base_path, item)

                    if not os.path.isdir(item_path) or item in [
                        "custom_plugins",
                        "__pycache__",
                    ]:
                        continue

                    # 1. Attempt the copy ONLY if the zip extraction succeeded
                    if source_is_ready:
                        destination_path = os.path.join(item_path, plugin)
                        try:
                            if os.path.exists(destination_path):
                                shutil.rmtree(destination_path)
                            shutil.copytree(source_plugin_path, destination_path)
                            refreshed.append(f"netskope.plugins.{item}.{plugin}.main")
                        except Exception as e:
                            logger.error(
                                f"Failed to copy plugin {plugin} to {destination_path}: {e}"
                            )

                    # 2. Disable configurations (runs unconditionally)
                    plugin_id = f"netskope.plugins.{item}.{plugin}.main"
                    _disable_plugin_configurations(plugin_id)

        disable_custom_plugins_with_incompatible_libs()

        if refreshed:
            logger.info("Python 3.12 native library plugins refreshed successfully.")
        else:
            logger.info(
                "No Python 3.12 native library plugins were refreshed from the image zip."
            )
    except Exception as e:
        logger.error(
            "Error occurred while migrating Python 3.12 native library plugins.",
            details=traceback.format_exc(),
            error_code="CE_1071",
        )
        raise e


def disable_custom_plugins_with_incompatible_libs():
    """Disable configurations for custom uploads of PLUGINS_TO_UPDATE only."""
    custom_plugins_path = os.path.join("/opt", "netskope", "plugins", "custom_plugins")
    if not os.path.isdir(custom_plugins_path):
        return

    try:
        for plugin_name in os.listdir(custom_plugins_path):
            if plugin_name not in PLUGINS_TO_UPDATE:
                continue
            plugin_dir = os.path.join(custom_plugins_path, plugin_name)
            if not os.path.isdir(plugin_dir):
                continue

            plugin_id = f"netskope.plugins.custom_plugins.{plugin_name}.main"
            _disable_plugin_configurations(plugin_id)

        logger.info("Python 3.12 custom plugin configuration disable step completed.")
    except Exception:
        logger.error(
            "Error occurred while disabling custom plugin configurations "
            "for Python 3.12.",
            details=traceback.format_exc(),
            error_code="CE_1071",
        )
        raise


def migrate_ai_scopes():
    """Grant ai_read + ai_write to every user that has the 'admin' scope."""
    try:
        result = connector.collection(Collections.USERS).update_many(
            {"scopes": "admin"},
            {"$addToSet": {"scopes": {"$each": ["ai_read", "ai_write"]}}},
        )
        logger.info(
            f"AI scope migration: granted ai_read+ai_write to "
            f"{result.modified_count} admin user(s)."
        )
    except Exception:
        logger.error("Failed to migrate AI scopes.", details=traceback.format_exc())
        raise


def create_ai_usage_indexes():
    """Create indexes on the ai_usage_metrics collection for dashboard queries."""
    try:
        col = connector.collection(Collections.AI_USAGE_METRICS)
        _ensure_index(col, [("feature", 1), ("timestamp", -1)])
        _ensure_index(col, [("username", 1), ("timestamp", -1)])
        logger.info("AI usage metrics indexes created.")
    except Exception:
        logger.error(
            "Failed to create AI usage metrics indexes.",
            details=traceback.format_exc(),
        )
        raise


def create_llm_provider_indexes():
    """Create indexes for llm_provider_configurations.

    1. Unique index on 'name' — prevents duplicate-name races on concurrent
       create requests (the manual check is not atomic without this).
    2. Partial unique index on 'plugin' where active=True — enforces the
       "one active config per plugin" invariant at the DB level, closing the
       TOCTOU window between the read-check and the update_many in POST/PATCH.

    Idempotent + re-invoke-safe via ``_ensure_index`` (skips if the key spec already exists,
    so a retried migration can't hit an IndexOptionsConflict).
    """
    try:
        col = connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS)
        _ensure_index(col, "name", unique=True)
        _ensure_index(
            col,
            "plugin",
            unique=True,
            partialFilterExpression={"active": True},
            name="unique_active_plugin",
        )
        logger.info("LLM provider configurations indexes created.")
    except Exception:
        logger.error(
            "Failed to create LLM provider configurations indexes.",
            details=traceback.format_exc(),
        )
        raise


def ensure_threat_indicators_entity():
    """Ensure Threat Indicators entity exists with CTE Threat IOCs field parity."""
    from netskope.integrations.crev2.utils.threat_indicators_entity import (
        THREAT_INDICATORS_ENTITY_NAME,
        get_threat_indicators_entity_fields,
    )

    entity_name = THREAT_INDICATORS_ENTITY_NAME
    canonical_fields = get_threat_indicators_entity_fields()

    try:
        existing_entity = connector.collection(Collections.CREV2_ENTITIES).find_one(
            {"name": entity_name}
        )
        if not existing_entity:
            connector.collection(Collections.CREV2_ENTITIES).insert_one(
                {"name": entity_name, "fields": canonical_fields}
            )
            logger.info("Created Threat Indicators entity in CREV2_ENTITIES.")
            return

        connector.collection(Collections.CREV2_ENTITIES).update_one(
            {"name": entity_name},
            {"$set": {"fields": canonical_fields}},
        )
        logger.info(
            "Patched Threat Indicators entity fields to match CTE Threat IOCs schema."
        )
    except Exception:
        logger.error(
            "Error occurred while ensuring Threat Indicators CRE entity.",
            details=traceback.format_exc(),
        )
        raise


def migrate_cre_generate_alerts_setting():
    """Force cre.generateAlerts to True on upgrade.

    Before this release, the CRE backend never read cre.generateAlerts, so
    any deployment that had toggled it off did so with no actual effect.
    Now that the backend enforces it (evaluate_records suppresses all CRE
    alerts when it is False), force it back to True on upgrade so existing
    CRE alerting is not silently broken by a stale, previously-inert value.
    """
    try:
        result = connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"cre.generateAlerts": True}}
        )
        logger.info(
            f"CRE generateAlerts migration: reset to True on "
            f"{result.modified_count} settings document(s)."
        )
    except Exception:
        logger.error(
            "Failed to migrate cre.generateAlerts setting.",
            details=traceback.format_exc(),
        )
        raise


def migrate_cte_business_rule_entity():
    """Backfill ``entity`` and ``creShare`` on existing CTE business rules.

    Rules created before entity-aware business rules implicitly target Threat
    Indicators; set ``entity`` to "Threat Indicators" and initialise an empty
    ``creShare`` so the new model fields are always present.
    """
    try:
        result = connector.collection(Collections.CTE_BUSINESS_RULES).update_many(
            {"entity": {"$exists": False}},
            {"$set": {"entity": THREAT_INDICATORS_ENTITY}},
        )
        connector.collection(Collections.CTE_BUSINESS_RULES).update_many(
            {"creShare": {"$exists": False}},
            {"$set": {"creShare": {}}},
        )
        logger.info(
            f"CTE business rule entity migration: backfilled {result.modified_count} "
            f"rule(s) to entity '{THREAT_INDICATORS_ENTITY}'."
        )
    except Exception:
        logger.error(
            "Failed to backfill CTE business rule entity/creShare fields.",
            details=traceback.format_exc(),
        )
        raise


def backfill_indicator_last_updated():
    """Backfill the CE-stamped ``lastUpdated`` field on existing indicators.

    ``lastUpdated`` (CE storage time) is stamped on every insert/update by
    ``insert_or_update_indicator`` from this release on. IOCs pulled before
    the upgrade get their ``lastSeen`` as a starting value so manual-sync
    recency windows behave sensibly; scheduled unified mapping runs are safe
    either way (the first run has no checkpoint and shares everything once).

    Runs detached from the upgrade — see ``start_indicator_last_updated_backfill``.
    """
    collection = connector.collection(Collections.INDICATORS)
    updated = 0
    last_id = None
    while True:
        try:
            # Page forward on _id instead of re-querying from the start: the
            # filter has no index, so a restarting scan re-reads every
            # already-backfilled document and turns this quadratic. Advancing
            # the cursor also skips documents with no lastSeen to copy (the
            # pipeline $set leaves them untouched) instead of re-selecting them.
            query = {"lastUpdated": {"$exists": False}}
            if last_id is not None:
                query["_id"] = {"$gt": last_id}
            ids = [
                doc["_id"]
                for doc in collection.find(query, {"_id": 1})
                .sort("_id", ASCENDING)
                .limit(LAST_UPDATED_BACKFILL_BATCH_SIZE)
            ]
            if not ids:
                break
            last_id = ids[-1]
            updated += collection.update_many(
                {"_id": {"$in": ids}},
                [{"$set": {"lastUpdated": "$lastSeen"}}],
            ).modified_count
        except Exception:
            # Non-fatal: recency_match_stage falls back to lastSeen for any
            # document left without lastUpdated, so the upgrade can continue.
            logger.warn(
                f"Stopped backfilling lastUpdated after {updated} indicator "
                "document(s); the remaining ones fall back to lastSeen at query time.",
                details=traceback.format_exc(),
            )
            return
    logger.info(f"Backfilled lastUpdated on {updated} existing indicator document(s).")


def start_indicator_last_updated_backfill():
    """Run ``backfill_indicator_last_updated`` as a detached background process.

    Same pattern as the 4.0.0 indicator and 6.0.0 CTO collection migrations:
    re-invoke this script with a marker argument and do not wait for it. The
    sweep touches every pre-upgrade IOC — millions on large deployments — and
    the core container cannot start celery/gunicorn until migrate.py returns,
    so running it inline stalls the whole upgrade. Deferring it is safe because
    ``recency_match_stage`` falls back to lastSeen for any document the backfill
    has not reached yet.
    """
    try:
        script_path = os.path.join(os.path.dirname(__file__), "7.0.0-beta.1.py")
        subprocess.Popen(
            [sys.executable, script_path, "backfill_indicator_last_updated"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        logger.info(
            "Started the indicator lastUpdated backfill in the background."
        )
    except Exception:
        logger.warn(
            "Could not start the background indicator lastUpdated backfill; "
            "existing indicators fall back to lastSeen at query time.",
            details=traceback.format_exc(),
        )


def create_unified_mapping_rules_indexes():
    """Create the unique name index for the unified mapping business rules collection.

    Idempotent + re-invoke-safe via ``_ensure_index`` (skips if the key spec already exists,
    so a retried migration can't hit an IndexOptionsConflict).
    """
    try:
        _ensure_index(
            connector.collection(Collections.UNIFIED_MAPPING_RULES),
            [("name", ASCENDING)],
            name="name_1",
            unique=True,
            background=True,
        )
        logger.info("Created unique name index on unified_mapping_rules.")
    except Exception:
        logger.error(
            "Failed to create the unified_mapping_rules name index.",
            details=traceback.format_exc(),
        )
        raise



def create_unified_mapping_marker_indexes():
    """Create the indexes backing the CRE unified-mapping act-once marker ledger.

    The unique ``(rule, ruleKey, rowHash)`` index is load-bearing, not just an
    optimisation: it is what makes marker writes idempotent, so a retried task or
    two workers racing the same joined row can never perform the same CRE action
    twice. It also serves the per-batch "already acted on?" lookup.

    The ``(mapping, _id)`` index backs the pruning task's per-mapping sweeps,
    which page through a mapping's entries in ``_id`` order.

    Idempotent + re-invoke-safe via ``_ensure_index`` (skips if the key spec already exists,
    so a retried migration can't hit an IndexOptionsConflict).
    """
    try:
        col = connector.collection(Collections.UNIFIED_MAPPING_MARKERS)
        _ensure_index(
            col,
            [("rule", ASCENDING), ("ruleKey", ASCENDING), ("rowHash", ASCENDING)],
            name="rule_1_ruleKey_1_rowHash_1",
            unique=True,
            background=True,
        )
        _ensure_index(
            col,
            [("mapping", ASCENDING), ("_id", ASCENDING)],
            name="mapping_1__id_1",
            background=True,
        )
        logger.info("Created indexes on unified_mapping_markers.")
    except Exception:
        logger.error(
            "Failed to create the unified_mapping_markers indexes.",
            details=traceback.format_exc(),
        )
        raise


def register_unified_mapping_marker_prune_schedule():
    """Register the ``cre.um_prune_markers`` housekeeping schedule.

    A single low-frequency global schedule that sweeps the act-once marker
    ledger. The per-mapping share/action schedules need no registration here —
    they are created with the mapping itself (see ``create_unified_mapping`` in
    the unified_mapping router), and unified mapping ships for the first time in
    this release, so no mapping can exist before this migration runs.
    """
    try:
        schedules = connector.collection(Collections.SCHEDULES)
        schedules.update_one(
            {"task": "cre.um_prune_markers"},
            {
                "$set": {
                    "_cls": "PeriodicTask",
                    "name": "UNIFIED MAPPING INTERNAL MARKER PRUNE TASK",
                    "enabled": True,
                    "args": [],
                    "task": "cre.um_prune_markers",
                    "interval": {
                        "every": 1,
                        "period": "days",
                    },
                }
            },
            upsert=True,
        )
        logger.info("Registered the cre.um_prune_markers schedule.")
    except Exception:
        logger.error(
            "Failed to register the cre.um_prune_markers schedule.",
            details=traceback.format_exc(),
        )
        raise


def register_unmute_unified_mapping_schedule():
    """Register the periodic task that auto-unmutes due unified mapping business rules.

    Mirrors the existing per-module unmute schedules (e.g. "cte.unmute").
    """
    try:
        connector.collection(Collections.SCHEDULES).update_one(
            {"task": "common.unmute_unified_mapping"},
            {
                "$set": {
                    "_cls": "PeriodicTask",
                    "name": "UNIFIED MAPPING INTERNAL UNMUTE TASK",
                    "enabled": True,
                    "args": [],
                    "task": "common.unmute_unified_mapping",
                    "interval": {
                        "every": 5,
                        "period": "minutes",
                    },
                }
            },
            upsert=True,
        )
        logger.info("Registered the common.unmute_unified_mapping schedule.")
    except Exception:
        logger.error(
            "Failed to register the common.unmute_unified_mapping schedule.",
            details=traceback.format_exc(),
        )
        raise


def create_copilot_session_indexes():
    """Create indexes on copilot_sessions for owner-scoped listing and lookup.

    1. (username, updatedAt desc) — the session-list query (a user's sessions,
       newest first).
    2. unique sessionId — fast resume-by-id and a uniqueness guard.

    Idempotent + re-invoke-safe via ``_ensure_index`` (skips if the key spec already exists,
    so a retried migration can't hit an IndexOptionsConflict).
    """
    try:
        col = connector.collection(Collections.COPILOT_SESSIONS)
        _ensure_index(col, [("username", 1), ("updatedAt", -1)])
        _ensure_index(col, "sessionId", unique=True)
        logger.info("Copilot session indexes created.")
    except Exception:
        logger.error(
            "Failed to create copilot session indexes.",
            details=traceback.format_exc(),
        )
        raise


def create_copilot_turns_indexes():
    """Indexes for the SSE-recovery turn records (plan v5 §8).

    Only the (messageId, username) owner-scoped recovery lookup is created here. The TTL on
    ``createdAt`` is NO LONGER hardcoded — turn retention now rides the settings-driven
    ``aiDataCleanup`` window via ``reconcile_ai_ttls`` (called below), so an admin can tune it
    and the change reflects within a TTL sweep. Idempotent.
    """
    try:
        col = connector.collection(Collections.COPILOT_TURNS)
        _ensure_index(col, [("messageId", 1), ("username", 1)])
        logger.info("Copilot turn indexes created.")
    except Exception:
        logger.error("Failed to create copilot turn indexes.", details=traceback.format_exc())
        raise


def reconcile_ai_retention_ttls():
    """Create the settings-driven TTL indexes for the AI collections (plan v5 §retention).

    Sessions + turns age on ``aiDataCleanup``; usage metrics on ``aiStatsCleanup`` — replacing
    the old daily ``delete_logs`` AI pass (removed) with TTLs the settings save reconciles.
    Best-effort inside ``reconcile_ai_ttls``; safe to run on every migration (idempotent).
    """
    from netskope.common.utils.ai_retention import reconcile_ai_ttls
    reconcile_ai_ttls()


def create_copilot_findings_indexes():
    """Indexes for the proactive findings store (plan v5 §6).

    1. unique findingKey — one doc per condition (the scan upserts by it).
    2. (status, module, severity, lastSeenAt desc) — the feed/summary queries.
    3. TTL on expireAt — watching (7d) + resolved (30d) findings self-purge. Idempotent.
    """
    try:
        col = connector.collection(Collections.COPILOT_FINDINGS)
        _ensure_index(col, "findingKey", unique=True)
        _ensure_index(col, [("status", 1), ("module", 1), ("severity", 1), ("lastSeenAt", -1)])
        _ensure_index(col, "expireAt", expireAfterSeconds=0)
        logger.info("Copilot findings indexes created.")
    except Exception:
        logger.error("Failed to create copilot findings indexes.", details=traceback.format_exc())
        raise


def register_copilot_attention_scan():
    """Schedule the proactive attention scan, but ONLY when an LLM provider is configured.

    Every 5 minutes (plan v5 §6); idempotent upsert. A deployment with zero providers configured
    gets this schedule registered later instead, by its first successful ``POST /llm/providers``
    call (see ``llm_providers.py`` -> ``attention_scan.register_attention_scan_schedule``) — this
    avoids celery beat firing (and immediately early-returning from) a periodic task with nothing
    to scan on a fresh or AI-disabled deployment. Not importing ``attention_scan.py`` here on
    purpose (keeps this migration free of celery-task-module imports, matching its existing
    inline-Scheduler style).
    """
    try:
        if connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find_one(
            {}, {"_id": 1}
        ) is None:
            logger.info(
                "No LLM provider is configured yet; deferring the copilot attention scan "
                "schedule until one is added."
            )
            return

        from netskope.common.utils.scheduler import Scheduler
        from netskope.common.models.other import PollIntervalUnit

        Scheduler().upsert(
            name="INTERNAL COPILOT ATTENTION SCAN",
            task_name="common.attention_scan",
            poll_interval=5,
            poll_interval_unit=PollIntervalUnit.MINUTES,
            args=[],
            queue="cloudexchange_6",
        )
        logger.info("Copilot attention scan scheduled.")
    except Exception:
        logger.error("Failed to schedule copilot attention scan.", details=traceback.format_exc())
        raise


def clear_leaked_cte_pull_locks():
    """Clear the permanently-stuck ``lockedAt.pull`` on netskope CTE configurations.

    A ``netskope``-flagged CTE plugin's pull runs via common.pull /
    historical_alerts, which dispatch cte.execute_plugin WITHOUT the locking
    kwargs, so @track() never releases the lock — while execute_plugin's batch
    loop still stamps ``lockedAt.pull`` as a heartbeat. Those configurations
    also get no cte.execute_plugin schedule, so celery beat's stale-lock
    reclaim never touches the field either. The result: once such a plugin
    pulled a batch of IOCs, ``lockedAt.pull`` stayed set forever and the
    Plugins page reported "(Running)" for an idle (or disabled) plugin.
    end_life now releases the lock, but that only helps configurations that
    pull again — a disabled one never will. Clear the already-leaked values
    here so the upgrade fixes them deterministically.

    Scoped to netskope-flagged plugins ON PURPOSE. Those never hold a beat
    lock, so their pull lock is orphaned by definition. Every other CTE
    configuration's ``lockedAt.pull`` IS a live beat lock, and an HA
    deployment can be running this migration on one node while another node's
    worker holds it — clearing those could double-dispatch a pull.

    Cosmetic data only, and it runs ONCE per deployment (a system already past
    7.0.0-beta.1 never re-executes this script), so both halves of the error
    handling matter and are deliberate:

    * The PER-CONFIG try/except keeps one bad document — an unimportable
      plugin, a transient PyMongoError on its update — from skipping every
      remaining configuration in the batch. A disabled netskope config that
      gets skipped never pulls again, so it would stay stuck at "(Running)"
      forever with no second chance.
    * The OUTER try/except is what makes "can never abort an upgrade" true.
      migrate.py runs each version script as a subprocess and, on a non-zero
      exit, calls sys.exit(1) WITHOUT advancing databaseVersion — so an
      exception escaping here fails the whole upgrade and the core container
      does not come up. A cosmetic lock reset must never be able to do that.

    The find() is materialised for the same reason: a cursor that fails partway
    through iteration (getMore timeout, replica-set step-down) would otherwise
    abandon the rest of the batch. CTE configuration counts are small.

    Idempotent.
    """
    try:
        cleared = 0
        skipped = 0
        configs = list(
            connector.collection(Collections.CONFIGURATIONS).find(
                {"lockedAt.pull": {"$ne": None}}, {"name": 1, "plugin": 1}
            )
        )
        for config in configs:
            try:
                PluginClass = plugin_helper.find_by_id(config["plugin"])  # NOSONAR
                if PluginClass is None or not PluginClass.metadata.get(
                    "netskope", False
                ):
                    # Not resolvable, or a third-party plugin whose lock is a
                    # live beat lock — leave it to release_lock / beat reclaim.
                    skipped += 1
                    continue
                connector.collection(Collections.CONFIGURATIONS).update_one(
                    {"_id": config["_id"]}, {"$set": {"lockedAt.pull": None}}
                )
                cleared += 1
            except Exception:
                skipped += 1
                logger.error(
                    f"Could not clear the leaked pull lock on CTE configuration "
                    f"'{config.get('name')}'; it may keep showing '(Running)' "
                    "until its next pull cycle.",
                    details=traceback.format_exc(),
                )
        logger.info(
            f"Cleared a leaked pull lock on {cleared} of {len(configs)} locked "
            f"CTE configuration(s); left {skipped} untouched."
        )
    except Exception:
        logger.error(
            "Error occurred while clearing leaked CTE pull locks. Affected "
            "plugins may keep showing '(Running)' until their next pull cycle.",
            details=traceback.format_exc(),
        )


def _strip_action_fields(fields):
    """Strip plugin action field dicts to persisted display metadata.

    Keeps only key/label/show_in_action_config — never choices/type/default,
    which can be huge live-fetched lists.
    """
    return [
        {
            "key": field["key"],
            "label": field.get("label", field["key"]),
            "show_in_action_config": field.get("show_in_action_config", True),
        }
        for field in (fields or [])
        if isinstance(field, dict) and field.get("key")
    ]


def _get_action_field_snapshots(pairs):
    """Resolve fieldDetails snapshots for (configuration, action value) pairs.

    Instantiates each still-existing configuration's plugin once and calls
    ``get_action_params`` per action value. Pairs whose configuration or
    plugin no longer exists, or whose plugin call fails, are skipped —
    their labels are unrecoverable and the UI falls back to formatted
    parameter keys.

    Args:
        pairs (dict): (configuration name, action value) -> action label.

    Returns:
        dict: (configuration name, action value) -> stripped field list.
    """
    from netskope.integrations.crev2.models import Action

    snapshots = {}
    plugins = {}  # configuration name -> plugin instance | None
    for (config_name, value), label in pairs.items():
        if config_name not in plugins:
            plugins[config_name] = None
            config = connector.collection(
                Collections.CREV2_CONFIGURATIONS
            ).find_one({"name": config_name})
            if config is None:
                logger.info(
                    f"Skipping action field details backfill for configuration "
                    f"'{config_name}': the configuration no longer exists, so "
                    "its action parameter labels cannot be recovered."
                )
                continue
            PluginClass = plugin_helper.find_by_id(config["plugin"])  # NOSONAR
            if PluginClass is None:
                logger.info(
                    f"Skipping action field details backfill for configuration "
                    f"'{config_name}': plugin '{config['plugin']}' is not installed."
                )
                continue
            try:
                plugins[config_name] = PluginClass(
                    config["name"],
                    SecretDict(config.get("parameters") or {}),
                    config.get("storage") or {},
                    None,
                    logger,
                )
            except Exception:
                logger.warn(
                    f"Could not load plugin for configuration '{config_name}' "
                    "while backfilling action field details.",
                    details=traceback.format_exc(),
                )
                continue
        plugin = plugins[config_name]
        if plugin is None:
            continue
        try:
            fields = plugin.get_action_params(
                Action(label=label or "", value=value)
            )
            snapshots[(config_name, value)] = _strip_action_fields(fields)
        except Exception:
            logger.warn(
                f"Could not fetch action field details for action '{value}' of "
                f"configuration '{config_name}' during migration. Action log "
                "labels will fall back to formatted parameter keys.",
                details=traceback.format_exc(),
            )
    # Persist storage mutated by get_action_params (token caches etc.),
    # mirroring the POST /configurations/{name}/fields router.
    for config_name, plugin in plugins.items():
        if plugin is None or not plugin.storage:
            continue
        connector.collection(Collections.CREV2_CONFIGURATIONS).update_one(
            {"name": config_name},
            {"$set": {"storage": plugin.storage}},
        )
    return snapshots


def backfill_crev2_action_field_details():
    """Backfill ``Action.fieldDetails`` on CREv2 business rules and action logs.

    7.0.0 snapshots each business-rule action's parameter field labels
    (``fieldDetails``) at rule save and copies them onto action logs, so the
    Action Logs UI can label parameters without a live plugin call — and
    even after the configuration is deleted. This backfills the snapshot
    for pre-7.0.0 rules and logs wherever the referenced configuration and
    plugin still exist.

    Cosmetic data only: every failure is logged and skipped so this can
    never abort a CE upgrade. Safe to re-run — documents that already have
    fieldDetails are not matched again.
    """
    try:
        # In MongoDB, {"field": None} matches both missing and null.
        pairs = {}  # (configuration name, action value) -> action label
        for doc in connector.collection(Collections.CREV2_ACTION_LOGS).aggregate(
            [
                {"$match": {"action.fieldDetails": None}},
                {
                    "$group": {
                        "_id": {
                            "configuration": "$configuration",
                            "value": "$action.value",
                        },
                        "label": {"$first": "$action.label"},
                    }
                },
            ],
            allowDiskUse=True,
        ):
            config_name = doc["_id"].get("configuration")
            value = doc["_id"].get("value")
            if config_name and value:
                pairs[(config_name, value)] = doc.get("label") or ""

        rules = list(
            connector.collection(Collections.CREV2_BUSINESS_RULES).find(
                {"actions": {"$nin": [None, {}]}}
            )
        )
        for rule in rules:
            for config_name, actions in (rule.get("actions") or {}).items():
                for action in actions or []:
                    # [] is a valid snapshot (no-parameter action) — only
                    # null/missing needs backfilling.
                    if action.get("fieldDetails") is not None or not action.get(
                        "value"
                    ):
                        continue
                    pairs.setdefault(
                        (config_name, action["value"]), action.get("label") or ""
                    )

        if not pairs:
            logger.info(
                "No CREv2 business-rule actions or action logs require an "
                "action field details backfill."
            )
            return

        logger.info(
            f"Backfilling action field details for {len(pairs)} unique "
            "(configuration, action) pair(s) found across business rules "
            "and action logs."
        )
        snapshots = _get_action_field_snapshots(pairs)

        for rule in rules:
            changed = False
            for config_name, actions in (rule.get("actions") or {}).items():
                for action in actions or []:
                    # [] is a valid snapshot (no-parameter action) — only
                    # null/missing needs backfilling.
                    if action.get("fieldDetails") is not None or not action.get(
                        "value"
                    ):
                        continue
                    details = snapshots.get((config_name, action["value"]))
                    if details is not None:
                        action["fieldDetails"] = details
                        changed = True
            if changed:
                connector.collection(Collections.CREV2_BUSINESS_RULES).update_one(
                    {"_id": rule["_id"]},
                    {"$set": {"actions": rule["actions"]}},
                )

        backfilled_logs = 0
        for (config_name, value), details in snapshots.items():
            result = connector.collection(Collections.CREV2_ACTION_LOGS).update_many(
                {
                    "configuration": config_name,
                    "action.value": value,
                    "action.fieldDetails": None,
                },
                {"$set": {"action.fieldDetails": details}},
            )
            backfilled_logs += result.modified_count
        logger.info(
            f"Backfilled action field details for {len(snapshots)} "
            f"configuration action(s) across {backfilled_logs} action log(s)."
        )
    except Exception:
        logger.error(
            "Error occurred while backfilling CREv2 action field details. "
            "Affected action log labels will fall back to formatted "
            "parameter keys.",
            details=traceback.format_exc(),
        )


def repair_flattened_lock_fields():
    """Rebuild lock and run-state fields that were flattened to a scalar.

    A task queued before the per-op lock split carries a frozen
    ``lock_field: "lockedAt"``; running it overwrites the whole sub-document
    with a scalar, after which the configuration cannot be deleted, disabled or
    updated. Repair to unlocked -- never a timestamp, which would block the
    next run.

    NOTE: migrate.py exits early once databaseVersion == LATEST_VERSION, so a
    deployment that already advanced to 7.0.0-beta.1 during the beta will not
    re-run this script. Repair those manually:

        python 7.0.0-beta.1.py repair_flattened_lock_fields
    """
    repairs = {
        Collections.CONFIGURATIONS: {"pull": None, "share": None},
        Collections.ITSM_CONFIGURATIONS: {
            "pull": None,
            "sync": None,
            "update": None,
        },
    }
    schedule_lock_fields = [
        ("cte.execute_plugin", "lockedAt.pull"),
        ("cte.share_indicators", "lockedAt.share"),
        ("itsm.pull_data_items", "lockedAt.pull"),
        ("itsm.sync_states", "lockedAt.sync"),
        ("itsm.update_incidents", "lockedAt.update"),
    ]
    try:
        for collection, empty_sub_document in repairs.items():
            repaired = 0
            for field in ["lockedAt", "lastRunAt", "lastRunSuccess", "task"]:
                # "task" holds per-op status objects, not datetimes.
                value = {} if field == "task" else empty_sub_document
                result = connector.collection(collection).update_many(
                    # $type alone would also match an absent field, which is
                    # valid -- the model defaults cover it.
                    {
                        field: {"$exists": True},
                        "$nor": [{field: {"$type": "object"}}],
                    },
                    {"$set": {field: value}},
                )
                repaired += result.modified_count
            if repaired:
                logger.info(
                    f"Repaired {repaired} flattened lock/run-state field(s) in "
                    f"{collection}."
                )

        # A stale message could also have been re-scheduled with the flat field.
        # UpdateMany: schedules holds one document PER CONFIGURATION, so each
        # task name can have many rows carrying the stale flat lock field.
        connector.collection(Collections.SCHEDULES).bulk_write([
            UpdateMany(
                {"task": task, "kwargs.lock_field": "lockedAt"},
                {"$set": {"kwargs.lock_field": lock_field}},
            )
            for task, lock_field in schedule_lock_fields
        ])
    except Exception:
        logger.error(
            "Error occurred while repairing flattened lock fields. Affected "
            "configurations may remain unmanageable until repaired.",
            details=traceback.format_exc(),
        )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "backfill_indicator_last_updated":
        backfill_indicator_last_updated()
        sys.exit(0)

    if len(sys.argv) > 1 and sys.argv[1] == "repair_flattened_lock_fields":
        repair_flattened_lock_fields()
        sys.exit(0)

    migrate_native_lib_plugins()
    migrate_ai_scopes()
    create_ai_usage_indexes()
    create_llm_provider_indexes()
    ensure_threat_indicators_entity()
    migrate_cte_business_rule_entity()
    start_indicator_last_updated_backfill()
    create_unified_mapping_rules_indexes()
    create_unified_mapping_marker_indexes()
    register_unified_mapping_marker_prune_schedule()
    register_unmute_unified_mapping_schedule()
    create_copilot_session_indexes()
    create_copilot_turns_indexes()
    create_copilot_findings_indexes()
    reconcile_ai_retention_ttls()
    register_copilot_attention_scan()
    backfill_crev2_action_field_details()
    migrate_cre_generate_alerts_setting()
    repair_flattened_lock_fields()
    clear_leaked_cte_pull_locks()
