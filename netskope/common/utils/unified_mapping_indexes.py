"""Join-index lifecycle for unified mappings.

A unified mapping's joins are only fast if the ``$lookup`` fields are indexed, so
saving (and previewing) a mapping creates the indexes it needs, and deleting or
re-pointing a join reclaims the ones no longer referenced by any saved mapping.
That bookkeeping is self-contained and has no HTTP semantics beyond the errors it
raises, so it lives here rather than in the router
(``netskope/common/api/routers/unified_mapping.py``):

  * ``ensure_unique_name_index`` — the unique index backing mapping-name uniqueness.
  * ``ensure_join_indexes`` — create the ``um_join_*`` indexes a mapping's joins need.
  * ``drop_unused_join_indexes`` — reference-counted cleanup across saved mappings.

Every index this module creates is namespaced with ``JOIN_INDEX_PREFIX`` so cleanup
can never touch an index the feature did not create.
"""

import traceback
from typing import Optional

from fastapi import HTTPException

from . import Collections, DBConnector, PrefixedLogger
from ..models.unified_mapping import UnifiedMappingIn
from .unified_mapping_fields import cached_table_fields, effective_join_field

logger = PrefixedLogger("[Unified Mapping]")

db_connector = DBConnector()


# Indexes this feature creates are namespaced so they can be safely reference-counted
# and dropped without ever touching pre-existing indexes (e.g. indicators.value unique
# index from migration 4.1.0, or CRE entity unique compound indexes).
JOIN_INDEX_PREFIX = "um_join_"


def _index_name(field: str, case_insensitive: bool = False) -> str:
    """Return the namespaced index name for a join field.

    Case-insensitive indexes get a ``_ci`` suffix so they coexist with plain
    indexes on the same field without colliding.
    """
    suffix = "_ci" if case_insensitive else ""
    return f"{JOIN_INDEX_PREFIX}{field.replace('.', '_')}{suffix}"


def ensure_unique_name_index() -> None:
    """Best-effort creation of the unique index backing mapping-name uniqueness.

    ``assert_unique_mapping_name`` is a check-then-insert (TOCTOU); this index makes the
    database the final arbiter so two concurrent creates cannot both persist the
    same name. Idempotent — re-creating an existing index is a no-op. If the index
    cannot be created (e.g. pre-existing duplicate names), the check-based guard
    still applies, so this is logged but never fatal.
    """
    try:
        db_connector.collection(Collections.UNIFIED_MAPPING).create_index(
            "name", unique=True, name="um_unique_name", background=True
        )
    except Exception:
        logger.warn(
            "Could not ensure unique index on unified_mapping.name.",
            details=traceback.format_exc(),
        )


def _field_already_indexed(collection_name: str, field: str, case_insensitive: bool = False) -> bool:
    """Return True if a suitable index whose leading key is ``field`` already exists.

    For case-sensitive joins (``case_insensitive=False``): any plain index (no collation)
    with ``field`` as the leading key is sufficient — MongoDB's prefix rule means a compound
    index also works here.

    For case-insensitive joins (``case_insensitive=True``): only an index that was created
    with a case-insensitive collation (strength ≤ 2) is accepted.  A plain index cannot
    serve a collation-specified $lookup.

    Pre-existing plain indexes (e.g. ``indicators.value_1``) satisfy case-sensitive joins
    but NOT case-insensitive ones — a separate ``um_join_*_ci`` index is created for those.
    """
    try:
        index_info = db_connector.collection(collection_name).index_information()
    except Exception as exc:
        logger.error(
            f"Failed to read index information for collection '{collection_name}'.",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            500, f"Could not read index information for collection '{collection_name}'."
        ) from exc
    for spec in index_info.values():
        key = spec.get("key", [])
        if not key or key[0][0] != field:
            continue
        if not case_insensitive:
            # Plain (no collation) index is needed for a case-sensitive $lookup.
            if not spec.get("collation"):
                return True
        else:
            # A collation with strength 1 (case+accent insensitive) or 2 (case-insensitive,
            # accent-sensitive) satisfies a case-insensitive $lookup.
            collation = spec.get("collation", {})
            if collation and collation.get("strength", 3) <= 2:
                return True
    return False


def _join_index_keys(
    mapping: UnifiedMappingIn, fields_cache: Optional[dict[str, dict[str, dict]]] = None
) -> set[tuple[str, str, str, bool]]:
    """Return (collection, effective_field, raw_field, case_insensitive) tuples.

    Always indexes the right-side field (it is the $lookup target).  Indexes the
    left-side field only for the first join, where the left side is the real base
    collection — chained-join left sides are in-pipeline intermediates whose index
    would not help the lookup.

    The ``mapping.caseInsensitive`` flag determines which index variant (plain or
    ``_ci`` collation) is created and tracked for every join in the mapping.

    ``fields_cache`` — see ``effective_join_field`` in unified_mapping_fields;
    threaded through so a table's
    field metadata isn't re-fetched if this and other calls in the same request
    already looked it up.
    """
    case_insensitive = mapping.caseInsensitive
    keys: set[tuple[str, str, str, bool]] = set()
    for join in mapping.joins:
        keys.add((
            join.rightTable,
            effective_join_field(join.rightTable, join.condition.rightField, fields_cache),
            join.condition.rightField,
            case_insensitive,
        ))
        if join.leftTable == mapping.baseTable:
            keys.add((
                join.leftTable,
                effective_join_field(join.leftTable, join.condition.leftField, fields_cache),
                join.condition.leftField,
                case_insensitive,
            ))
    return keys


def ensure_join_indexes(
    mapping: UnifiedMappingIn,
    strict: bool = True,
    fields_cache: Optional[dict[str, dict[str, dict]]] = None,
) -> None:
    """Create namespaced indexes for a mapping's join fields.

    ``strict=True``  (save path): raises 500 on index-creation failure so the
    caller knows the mapping may not perform well and can surface the error.
    ``strict=False`` (preview path): logs a warning and continues so the preview
    is never blocked by an index failure — the query still works, just slower.

    Case-insensitive views (``mapping.caseInsensitive=True``) get a collation index
    (``um_join_*_ci``, locale "en", strength 2) because a plain index cannot
    serve a collation-specified $lookup.  Case-sensitive views get plain indexes.

    ``fields_cache`` — see ``cached_table_fields`` in unified_mapping_fields. Defaults
    to a cache local to this call, shared between _join_index_keys and this function's
    own known-fields check; callers can pass a request-wide dict to also share it with
    ``build_pipeline`` in unified_mapping_exec.
    """
    if fields_cache is None:
        fields_cache = {}
    for collection_name, field, raw_field, case_insensitive in _join_index_keys(mapping, fields_cache):
        known_fields = set(cached_table_fields(collection_name, fields_cache))
        if raw_field not in known_fields:
            raise HTTPException(
                400,
                f"Cannot create index: field '{raw_field}' is not present in collection '{collection_name}'.",
            )
        if _field_already_indexed(collection_name, field, case_insensitive=case_insensitive):
            logger.debug(
                f"Skipping index creation on '{collection_name}.{field}' "
                f"(case_insensitive={case_insensitive}): a suitable index already exists."
            )
            continue
        index_name = _index_name(field, case_insensitive)
        index_opts: dict = {"name": index_name, "background": True}
        if case_insensitive:
            # Collation strength 2: case-insensitive, accent-sensitive.
            # The $lookup aggregate must also specify this collation at execute time.
            index_opts["collation"] = {"locale": "en", "strength": 2}
        try:
            db_connector.collection(collection_name).create_index(field, **index_opts)
            logger.debug(
                f"Created index '{index_name}' on '{collection_name}.{field}'."
            )
        except Exception as exc:
            if strict:
                logger.error(
                    f"Failed to create index '{index_name}' on "
                    f"'{collection_name}.{field}'.",
                    details=traceback.format_exc(),
                )
                raise HTTPException(
                    500, f"Failed to create index on '{collection_name}.{field}'."
                ) from exc
            logger.warn(
                f"Could not create index '{index_name}' on '{collection_name}.{field}' "
                "(preview will run without it, query may be slow).",
                details=traceback.format_exc(),
            )


def _mapping_doc_join_keys(
    doc: dict, fields_cache: Optional[dict[str, dict[str, dict]]] = None
) -> set[tuple[str, str, bool]]:
    """Return (collection, field, case_insensitive) triples referenced by a stored mapping doc.

    ``fields_cache`` — see ``cached_table_fields`` in unified_mapping_fields; matters
    most to drop_unused_join_indexes,
    which calls this once per saved mapping and shares one cache across the whole scan.
    """
    base = doc.get("baseTable")
    case_insensitive = bool(doc.get("caseInsensitive", False))
    keys: set[tuple[str, str, bool]] = set()
    for join in doc.get("joins", []):
        left_table = join.get("leftTable") or base
        condition = join.get("condition", {})
        if condition:
            right_field = effective_join_field(join["rightTable"], condition["rightField"], fields_cache)
            keys.add((join["rightTable"], right_field, case_insensitive))
            if left_table == base:
                left_field = effective_join_field(left_table, condition["leftField"], fields_cache)
                keys.add((left_table, left_field, case_insensitive))
    return keys


def drop_unused_join_indexes(collections: set[str]) -> None:
    """Scan all ``um_join_*`` indexes on the given collections and drop any not referenced by a saved mapping.

    Scan-based rather than key-diff-based so that orphaned indexes from prior
    caseInsensitive changes or multi-mapping reference-counting edge cases are always
    cleaned up.  Only indexes whose name starts with ``JOIN_INDEX_PREFIX`` are ever
    touched — pre-existing indexes are safe regardless of their field coverage.

    ``collections`` is the union of collections referenced in both the old and new
    mapping so that orphans on either side of a join-field change are caught.

    Called AFTER the mapping has been persisted or deleted so that the DB already
    reflects the correct final state — no exclusion needed.
    """
    if not collections:
        return
    # Shared across every saved mapping in the scan below — multiple mappings
    # referencing the same CRE entity table only pay for that table's field
    # metadata once, not once per mapping.
    fields_cache: dict[str, dict[str, dict]] = {}
    still_used: set[tuple[str, str, bool]] = set()
    for other in db_connector.collection(Collections.UNIFIED_MAPPING).find(
        {}, {"baseTable": 1, "joins": 1, "caseInsensitive": 1}
    ):
        still_used |= _mapping_doc_join_keys(other, fields_cache)

    for collection_name in collections:
        try:
            existing = db_connector.collection(collection_name).index_information()
        except Exception:
            logger.warn(
                f"Could not read index information for '{collection_name}' during cleanup.",
                details=traceback.format_exc(),
            )
            continue
        for index_name, spec in existing.items():
            if not index_name.startswith(JOIN_INDEX_PREFIX):
                continue
            key = spec.get("key", [])
            if not key:
                continue
            field = key[0][0]
            case_insensitive = bool(spec.get("collation"))
            field_variants = {field}
            if field.endswith(".value"):
                field_variants.add(field[: -len(".value")])
            else:
                field_variants.add(f"{field}.value")
            if any(
                (collection_name, variant, case_insensitive) in still_used
                for variant in field_variants
            ):
                logger.debug(
                    f"Keeping join index '{index_name}' on '{collection_name}': "
                    "still used by another saved mapping."
                )
                continue
            try:
                db_connector.collection(collection_name).drop_index(index_name)
                logger.debug(
                    f"Dropped unused join index '{index_name}' on '{collection_name}'."
                )
            except Exception:
                # Non-fatal: a leftover um_join_ index only costs disk/write overhead,
                # it never affects correctness. Log so the leak is visible instead of silent.
                logger.warn(
                    f"Could not drop unused join index '{index_name}' on '{collection_name}'.",
                    details=traceback.format_exc(),
                )
