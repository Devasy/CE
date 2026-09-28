"""AI-data retention: settings-driven TTL indexes for the copilot collections.

Retention for the AI collections is expressed as MongoDB TTL indexes so a document
auto-expires ``expireAfterSeconds`` after its timestamp field — no periodic delete task
needed, and (unlike the old daily ``delete_logs`` pass) a settings change reflects within
one TTL-monitor sweep (~60s) once reconciled.

Two admin knobs drive it (both in days, on the global settings doc):
  * ``aiDataCleanup``  → CHAT data: ``copilot_sessions`` + ``copilot_turns`` (conversation
    content + transient SSE-recovery records; a privacy control).
  * ``aiStatsCleanup`` → STATISTICS: ``ai_usage_metrics`` (the analytics-dashboard telemetry;
    aggregate spend/usage, typically kept longer than raw transcripts).

``copilot_findings`` is intentionally NOT here — it carries a per-document ``expireAt`` TTL
(watching 7d / resolved 30d) set by the attention engine, a different lifecycle.

``reconcile_ai_ttls`` is called (a) at migration time to create the indexes, and (b) on a
settings save that changes either knob, so operator changes take effect promptly. It is
idempotent: it creates a missing TTL, ``collMod``s one whose duration drifted, and repairs a
legacy PLAIN (non-TTL) index left on the same field by dropping + recreating it as a TTL.
"""

import traceback
from typing import Optional

from pymongo.errors import OperationFailure

from netskope.common.utils.db_connector import Collections, DBConnector
from netskope.common.utils.logger import Logger

connector = DBConnector()
logger = Logger()

# Default retention (days) — mirror the Settings model defaults so a reconcile before the
# settings doc exists (or with a missing key) still applies a sane window.
_DEFAULT_AI_DATA_DAYS = 30
_DEFAULT_AI_STATS_DAYS = 365

_SECONDS_PER_DAY = 86400

# (collection, timestamp field the TTL ages by, settings key, default days). One TTL index
# per collection, on its own timestamp field.
_AI_TTL_SPECS = [
    (Collections.COPILOT_SESSIONS, "updatedAt", "aiDataCleanup", _DEFAULT_AI_DATA_DAYS),
    (Collections.COPILOT_TURNS, "createdAt", "aiDataCleanup", _DEFAULT_AI_DATA_DAYS),
    (Collections.AI_USAGE_METRICS, "timestamp", "aiStatsCleanup", _DEFAULT_AI_STATS_DAYS),
]


def _days_from_settings(settings_doc: Optional[dict], key: str, default_days: int) -> int:
    """Resolve a retention window (days) from the settings doc; fall back to the default.

    Guards a missing/None/non-positive value so a bad setting never yields a 0-second TTL
    (which Mongo treats as "expire immediately").
    """
    value = (settings_doc or {}).get(key)
    try:
        days = int(value)
    except (TypeError, ValueError):
        return default_days
    return days if days > 0 else default_days


def _ttl_index_on(col, field: str):
    """Return (index_name, index_info) for an existing index keyed solely on ``field``, or None."""
    try:
        info = col.index_information()
    except Exception:
        return None
    for name, spec in info.items():
        key = spec.get("key") or []
        if len(key) == 1 and key[0][0] == field:
            return name, spec
    return None


def _reconcile_one(db, col, field: str, expire_seconds: int) -> None:
    """Ensure ``col`` has a TTL index on ``field`` with exactly ``expire_seconds``.

    - No index on the field        → create the TTL.
    - TTL present, wrong duration  → collMod to the new duration.
    - TTL present, same duration   → no-op.
    - PLAIN (non-TTL) index present → drop + recreate as TTL (collMod can't add a TTL to an
      index that wasn't created with one).
    """
    existing = _ttl_index_on(col, field)
    if existing is None:
        col.create_index(field, expireAfterSeconds=expire_seconds)
        return
    name, spec = existing
    current = spec.get("expireAfterSeconds")
    if current is None:
        # Legacy plain index on the same field — replace it with a TTL variant.
        col.drop_index(name)
        col.create_index(field, expireAfterSeconds=expire_seconds)
        return
    if int(current) == int(expire_seconds):
        return
    # Duration drifted — collMod is the ONLY way to change a live TTL's expireAfterSeconds
    # (re-issuing create_index with a different duration is a conflict Mongo ignores).
    db.command({
        "collMod": col.name,
        "index": {"keyPattern": {field: 1}, "expireAfterSeconds": int(expire_seconds)},
    })


def reconcile_ai_ttls(settings_doc: Optional[dict] = None) -> None:
    """Create/adjust the AI-collection TTL indexes to match current retention settings.

    Idempotent and best-effort: a failure on one collection is logged and does not abort the
    others (or the caller — a settings save / migration must not fail on a TTL hiccup).
    ``settings_doc`` may be passed to avoid a re-read; otherwise the global settings doc is
    loaded here.
    """
    if settings_doc is None:
        try:
            settings_doc = connector.collection(Collections.SETTINGS).find_one({}) or {}
        except Exception:
            settings_doc = {}
    db = connector.database
    for collection, field, key, default_days in _AI_TTL_SPECS:
        try:
            days = _days_from_settings(settings_doc, key, default_days)
            _reconcile_one(db, connector.collection(collection), field, days * _SECONDS_PER_DAY)
        except OperationFailure:
            logger.warn(
                f"Could not reconcile the TTL for '{collection}'.",
                details=traceback.format_exc(),
            )
        except Exception:
            logger.warn(
                f"Unexpected error reconciling the TTL for '{collection}'.",
                details=traceback.format_exc(),
            )
