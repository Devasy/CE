"""Share indicators task."""

from __future__ import absolute_import, unicode_literals
import copy
import json
import math
import random
import os
import re
import string
import traceback
from typing import List, Dict, Optional
from datetime import datetime, timedelta

from pymongo import ASCENDING

from netskope.common.celery.main import APP
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.utils import (
    Logger,
    DBConnector,
    Collections,
    cto_alerts_enabled,
    integration,
    is_platform_enabled,
    log_cto_alerts_skipped,
    parse_dates,
    track,
    SecretDict,
    has_source_info_args
)
from netskope.common.models import SettingsDB
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.integrations.itsm.models import Alert
from netskope.integrations.itsm.tasks.pull_data_items import store_cte_alerts
from netskope.integrations.cte.plugin_base import PushResult, ValidationResult
from netskope.integrations.cte.models import (
    ConfigurationDB,
    IndicatorGenerator,
)
from netskope.integrations.cte.models.indicator import (
    Indicator,
    IndicatorDBWithSources,
    IndicatorSourceDB,
)
from netskope.integrations.cte.models.business_rule import Action
from netskope.integrations.cte.models.business_rule import BusinessRuleDB
from netskope.integrations.cte.models.business_rule import (
    NUMERIC_IOC_DEFAULTS,
    NUMERIC_IOC_RANGES,
)
from netskope.common.utils.unified_mapping_fields import (
    IOC_MULTI_VALUED_FIELDS,
)
from netskope.integrations.cte.utils import RETRACTION_IOC_BATCH_SIZE
from netskope.common.utils.unified_mapping_exec import (
    build_mapping_pipeline,
    exception_match_stage,
    mapping_unwinds_sources,
    matched_only_stage,
    recency_match_stage,
    rule_match_stage,
    unwrap_normalized_row_values,
)
from netskope.integrations.cte.utils.constants import (
    CTE_ALERT_DISPATCH_BATCH_SIZE,
    CTE_FAILED_IOC_QUERY_CHUNK,
    CTE_NO_ACTION_VALUE,
    CTE_UM_ROW_CHUNK_SIZE,
)
from netskope.integrations.cte.utils.entity import (
    THREAT_INDICATORS_ENTITY,
    build_cre_entity_match_query,
    cre_source_name,
    get_entity_collection,
    um_source_name,
    load_mongo_filters as _load_mongo_filters,
)
from netskope.integrations.cte.tasks.plugin_lifecycle_task import get_possible_destinations

connector = DBConnector()
logger = Logger()
helper = PluginHelper()

try:
    EXPIRY_DAYS = int(os.getenv("EXPIRY_DAYS", 90))
except (TypeError, ValueError):
    EXPIRY_DAYS = 90


def validate_result_and_update(
    shared_with: str,
    push_result: PushResult,
    filters: dict = None,
    source_config_name: str = None
) -> bool:
    """Validate push result and update all the indicators.

    Args:
        shared_with (str): Name of the configuration that is being shared with.
        push_result (PushResult): Result of the push method.
        total (int): total number of indicators
        total_inactive (int): total number of inactive indicators
        business_rule_qualified (int): number of indicators which qualifies the business rule.
        filters (dict): mongo filter dictionary
    """
    if not isinstance(push_result, PushResult):
        logger.error(
            f"Could not share indicators with configuration "
            f"'{shared_with}'. Invalid return type.",
            error_code="CTE_1006",
        )
        return False, False, [], []
    if push_result.success is True:
        # add shared_with config name in the sharedWith array for all
        # the indicators
        if not push_result.already_shared:
            if push_result.failed_iocs:
                connector.collection(Collections.INDICATORS).update_many(
                    {
                        "value": {"$in": push_result.failed_iocs},
                        "sources": {
                            "$elemMatch": {
                                "source": source_config_name,
                                "destinations": {
                                    "$elemMatch": {
                                        "name": shared_with,
                                        "status": "inprogress"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "$set": {
                            "sources.$[elem].destinations.$[dest].status": "failed"
                        }
                    },
                    array_filters=[
                        {"elem.source": source_config_name},
                        {"dest.name": shared_with}
                    ]
                )
                filters["$and"].append({"value": {"$nin": push_result.failed_iocs}})
            if push_result.skipped_iocs:
                connector.collection(Collections.INDICATORS).update_many(
                    {
                        "value": {"$in": push_result.skipped_iocs},
                        "sources": {
                            "$elemMatch": {
                                "source": source_config_name,
                                "destinations": {
                                    "$elemMatch": {
                                        "name": shared_with,
                                        "status": "inprogress"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "$set": {
                            "sources.$[elem].destinations.$[dest].status": "N/A"
                        }
                    },
                    array_filters=[
                        {"elem.source": source_config_name},
                        {"dest.name": shared_with}
                    ]
                )
                filters["$and"].append({"value": {"$nin": push_result.skipped_iocs}})
                logger.info(
                    f"Skipped {len(push_result.skipped_iocs)} indicator(s) "
                    f"during sharing from source configuration '{source_config_name}' "
                    f"to configuration '{shared_with}'. These indicators were discarded by the plugin."
                )
            connector.collection(Collections.INDICATORS).update_many(
                filters, {"$addToSet": {"sharedWith": shared_with}}
            )
            if source_config_name:
                # For indicators that have no destination entry yet, create one
                # with "inprogress" status so the deferred sweep can mark them
                # "shared" after all business rules have executed.
                no_dest_filters = {}
                if "$and" in filters:
                    no_dest_filters["$and"] = list(filters["$and"])
                if "$nor" in filters:
                    no_dest_filters["$nor"] = filters["$nor"]
                no_dest_filters.setdefault("$and", []).append(
                    {
                        "sources": {
                            "$elemMatch": {
                                "source": source_config_name,
                                "destinations": {
                                    "$not": {"$elemMatch": {"name": shared_with}}
                                },
                            }
                        }
                    }
                )
                connector.collection(Collections.INDICATORS).update_many(
                    no_dest_filters,
                    {
                        "$push": {
                            "sources.$[elem].destinations": {
                                "name": shared_with,
                                "status": "inprogress",
                            }
                        }
                    },
                    array_filters=[{"elem.source": source_config_name}],
                )
            logger.info(
                f"Completed Sharing of indicators from source "
                f"configuration '{source_config_name}' to configuration '{shared_with}'."
            )
            return (
                True,
                push_result.should_run_cleanup,
                push_result.failed_iocs if push_result.failed_iocs else [],
                push_result.skipped_iocs if push_result.skipped_iocs else [],
            )
    else:
        logger.error(
            f"Could not share indicators with configuration "
            f"'{shared_with}'. {re.sub(r'token=([0-9a-zA-Z]*)', 'token=********&', push_result.message)}",
            details=re.sub(
                r"token=([0-9a-zA-Z]*)", "token=********&", push_result.message
            ),
            error_code="CTE_1007",
        )
        return False, push_result.should_run_cleanup, [], []


def _update_storage(name: str, storage: dict):
    connector.collection(Collections.CONFIGURATIONS).update_one(
        {"name": name}, {"$set": {"storage": storage}}
    )


def _action_field(action_dict, key, default=None):
    """Read a field from an Action model or a plain dict."""
    if isinstance(action_dict, Action):
        return getattr(action_dict, key, default)
    return action_dict.get(key, default)


def _any_action_alerts(actions) -> bool:
    """Return True if any of the actions asks for a CTO alert."""
    return any(
        _action_field(action, "generateAlert", False) for action in (actions or [])
    )


def _build_indicator_alert(indicator, status: str, meta: dict) -> Alert:
    """Build a CTO alert for a shared indicator.

    ``rawData.plugin`` names the source configuration's plugin, so it is only
    present when the share had a source configuration. CRE-entity sharing reads
    the CRE entity collection directly and has no source plugin, so its callers
    omit ``source_plugin_name`` from ``meta`` and the key is left out of the
    alert entirely rather than carrying a placeholder. ``sourceConfiguration``
    still identifies the origin (``cre_<entity>_<rule>``).
    """
    raw_data = {}
    if "source_plugin_name" in meta:
        raw_data["plugin"] = meta["source_plugin_name"]
    raw_data.update(
        {
            "action": meta["action_label"],
            "status": status,
            "sourceConfiguration": meta["source_config_name"],
            "destinationConfiguration": meta["destination_config_name"],
            "businessRule": meta["rule_name"],
            "indicatorValue": indicator.value,
            "indicatorType": getattr(
                indicator.type, "value", str(indicator.type)
            ),
            "severity": getattr(
                indicator.severity, "value", str(indicator.severity)
            ),
            "reputation": indicator.reputation,
            "firstSeen": indicator.firstSeen,
            "lastSeen": indicator.lastSeen,
            "tags": ", ".join(indicator.tags or []),
        }
    )
    return Alert(
        id="".join(
            random.SystemRandom().choice(string.hexdigits) for _ in range(24)
        ),
        configuration="CTE",
        alertName=meta["rule_name"],
        alertType="CTE",
        app="CTE",
        appCategory="CTE",
        type="CTE",
        timestamp=datetime.now(),
        rawData=raw_data,
    )


def _generate_alerts_for_query(query: dict, status: str, meta: dict) -> int:
    """Generate one alert per indicator matching the query.

    Alerts are dispatched to CTO in batches to bound memory and task size.
    """
    total = 0
    batch = []
    cursor = connector.collection(Collections.INDICATORS).aggregate(
        [{"$match": query}],
        allowDiskUse=True,
    )
    # IndicatorGenerator merges the per-source severity/reputation/tags so
    # the alerts reflect what was actually shared.
    for indicator in IndicatorGenerator(cursor, meta["source_config_name"]).all():
        if indicator is None:
            continue
        batch.append(_build_indicator_alert(indicator, status, meta))
        if len(batch) >= CTE_ALERT_DISPATCH_BATCH_SIZE:
            execute_celery_task(
                store_cte_alerts.apply_async,
                "itsm.store_cte_alerts",
                args=[batch],
            )
            total += len(batch)
            batch = []
    if batch:
        execute_celery_task(
            store_cte_alerts.apply_async,
            "itsm.store_cte_alerts",
            args=[batch],
        )
        total += len(batch)
    return total


def _generate_alerts_for_action(
    alert_query: dict,
    success: bool,
    failed_iocs: List[str],
    meta: dict,
    skipped_iocs: List[str] = None,
):
    """Generate alerts for the indicators processed by a sharing action.

    Args:
        alert_query (dict): Snapshot of the action query taken before the
            push (the original is mutated by validate_result_and_update and
            failed indicators lose their "inprogress" status).
        success (bool): Whether the push succeeded.
        failed_iocs (List[str]): Values of the indicators the plugin failed
            to push.
        meta (dict): Alert context (rule/source/destination/plugin/action).
        skipped_iocs (List[str]): Indicators skipped by the plugin (e.g.
            already present at destination) — excluded from success alerts.
    """
    try:
        total = 0
        if success:
            failed_values = list(set(failed_iocs or []))
            skipped_values = list(set(skipped_iocs or []))
            success_query = copy.deepcopy(alert_query)
            if failed_values:
                success_query["$and"].append(
                    {"value": {"$nin": failed_values}}
                )
            if skipped_values:
                success_query["$and"].append(
                    {"value": {"$nin": skipped_values}}
                )
            total += _generate_alerts_for_query(success_query, "success", meta)
            # Failed indicators were already flipped out of "inprogress", so
            # they are matched by value instead of the snapshot query.
            for index in range(
                0, len(failed_values), CTE_FAILED_IOC_QUERY_CHUNK
            ):
                chunk = failed_values[index: index + CTE_FAILED_IOC_QUERY_CHUNK]
                failed_query = {
                    "$and": [
                        {"value": {"$in": chunk}},
                        {
                            "sources": {
                                "$elemMatch": {
                                    "source": meta["source_config_name"],
                                    "$or": [
                                        {"retracted": False},
                                        {"retracted": {"$exists": False}},
                                    ],
                                    # Scope to docs actually marked failed on this
                                    # destination so cross-destination value
                                    # collisions cannot generate spurious alerts.
                                    "destinations": {
                                        "$elemMatch": {
                                            "name": meta["destination_config_name"],
                                            "status": "failed",
                                        }
                                    },
                                }
                            }
                        },
                        {"active": True},
                    ]
                }
                total += _generate_alerts_for_query(failed_query, "failed", meta)
        else:
            # Total failure: no statuses were updated, the snapshot still
            # matches every indicator sent to the plugin.
            total += _generate_alerts_for_query(alert_query, "failed", meta)
        if total:
            logger.info(
                f"Generated {total} alert(s) on CTO for indicators shared from "
                f"'{meta['source_config_name']}' to "
                f"'{meta['destination_config_name']}' using the business rule "
                f"'{meta['rule_name']}'."
            )
    except Exception:
        logger.error(
            f"Error occurred while generating alerts for indicators shared "
            f"from '{meta['source_config_name']}' to "
            f"'{meta['destination_config_name']}' using the business rule "
            f"'{meta['rule_name']}'.",
            details=traceback.format_exc(),
            error_code="CTE_1030",
        )


def _generate_cre_entity_alerts(
    indicators, source_name: str, destination_config_name: str, status: str, meta: dict
):
    """Generate CTO alerts for the CRE-entity indicators of one sharing action.

    The CRE-entity share flow has no per-IOC push result (the whole batch is
    advanced to "shared" or "failed" together), so alerts are emitted after the
    status update and scoped by indicator value + synthetic CRE source +
    destination status. That keeps the alert status in step with what was
    actually recorded and cannot pick up indicators shared by another rule or to
    another destination.

    Args:
        indicators (list[Indicator]): The indicators that were pushed.
        source_name (str): Synthetic CRE source label (``cre_<entity>_<rule>``).
        destination_config_name (str): Destination configuration name.
        status (str): Destination status just written ("shared" or "failed").
        meta (dict): Alert context (rule/source/destination/entity/action).
    """
    try:
        values = list({indicator.value for indicator in indicators})
        if not values:
            return
        alert_status = "success" if status == "shared" else "failed"
        total = 0
        # Chunked like the TI failed-IOC alert query so a large share does not
        # build an unbounded ``$in`` list.
        for index in range(0, len(values), CTE_FAILED_IOC_QUERY_CHUNK):
            chunk = values[index: index + CTE_FAILED_IOC_QUERY_CHUNK]
            query = {
                "$and": [
                    {"value": {"$in": chunk}},
                    {
                        "sources": {
                            "$elemMatch": {
                                "source": source_name,
                                "destinations": {
                                    "$elemMatch": {
                                        "name": destination_config_name,
                                        "status": status,
                                    }
                                },
                            }
                        }
                    },
                ]
            }
            total += _generate_alerts_for_query(query, alert_status, meta)
        if total:
            logger.info(
                f"Generated {total} alert(s) on CTO for CRE entity records "
                f"shared from '{source_name}' to '{destination_config_name}' "
                f"using the business rule '{meta['rule_name']}'."
            )
    except Exception:
        logger.error(
            f"Error occurred while generating alerts for CRE entity records "
            f"shared from '{source_name}' to '{destination_config_name}' using "
            f"the business rule '{meta['rule_name']}'.",
            details=traceback.format_exc(),
            error_code="CTE_1030",
        )


def build_mongo_query(
    rule: BusinessRuleDB,
    source: Optional[str] = None,
    lastseen: Optional[datetime] = None,
) -> Dict:
    """Build a mongo query for the business rule.

    Args:
        rule (BusinessRuleDB): Business rule to build the query for.

    Returns:
        Dict: Mongo query.
    """
    query = {
        "$and": [
            json.loads(
                rule.filters.mongo,
                object_hook=lambda pair: parse_dates(pair),
            )
        ]
    }
    for mute in rule.exceptions:  # exclude iocs matching the mute rule
        if mute.filters:
            mute_query = json.loads(
                mute.filters.mongo, object_hook=lambda pair: parse_dates(pair)
            )
            query["$nor"] = query.get("$nor", []) + [mute_query]
        if mute.tags:
            query["$and"].append({"sources": {"$elemMatch": {"tags": {"$nin": mute.tags}}}})
    if source:
        query["$and"].append(
            {
                "sources": {
                    "$elemMatch": {
                        "source": source,
                        "$or": [
                            {"retracted": False},
                            {"retracted": {"$exists": False}},
                        ],
                    }
                }
            }
        )
    else:
        query["$and"].append(
            {
                "sources": {
                    "$elemMatch": {
                        "$or": [{"retracted": False}, {"retracted": {"$exists": False}}]
                    }
                }
            }
        )
    if lastseen:
        query["$and"].append(
            {"sources": {"$elemMatch": {"lastSeen": {"$gt": lastseen}}}}
        )
    return query


def end_life(name: str, success: bool) -> bool:
    """Update the lastRunSuccess and lastRunAt and exit.

    Args:
        name (str): Name of the configuration.
        success (bool): lastRunSuccess value to be updated.

    Returns:
        bool: Value of `success`.
    """
    connector.collection(Collections.CONFIGURATIONS).update_one(
        {"name": name},
        {
            "$set": {
                "lastRunAt.share": datetime.now(),
                "lastRunSuccess.share": success,
                # "lockedAt": None,
            }
        },
    )
    return success


# A CRE STRING field with normalization enabled is stored as
# ``{"value": <canonical>, "plugins": [{"config": ..., "value": <raw>}]}``
# instead of a plain string (crev2 fetch_records._store_records).
_NORMALIZED_FIELD_KEYS = {"value", "plugins"}


def _unwrap_normalized_value(value):
    """Canonical value of a normalized CRE field; anything else is returned as-is.

    Mapping specs name the field itself (``$email``) because that is what the
    UI's dropdowns emit, so a normalized field resolves to its wrapper object.
    Passing that on drops the record at ``Indicator(...)`` for every IOC field
    except ``value``, which would stringify the wrapper into a junk IOC and ship
    it. Same reason ``effective_join_field`` appends ``.value`` on the join side.

    Shape-matched rather than schema-driven so an already-saved mapping is fixed
    without a migration: the entity schema is not available here, and only a
    normalized field produces exactly these keys.
    """
    if (
        isinstance(value, dict)
        and "value" in value
        and not set(value) - _NORMALIZED_FIELD_KEYS
    ):
        return value.get("value")
    return value


def _resolve_record_field(record, path):
    """Resolve a (possibly dotted) field path on a CRE record.

    Mirrors CRE's generalized ``_dot_walk``: walks dotted paths and, when a
    segment is an array of objects, returns the first resolvable value. A
    normalized field's wrapper is reduced to its canonical value, so ``$email``
    and ``$email.value`` resolve alike.

    Args:
        record (dict): A CRE entity record document.
        path (str): Field name, optionally dotted (e.g. ``host.name``).

    Returns:
        Any: The resolved value, or ``None`` if the path does not resolve.
    """
    if not path:
        return None
    obj = record
    keys = path.split(".")
    for index, segment in enumerate(keys):
        if obj is None:
            return None
        if isinstance(obj, list):
            remainder = ".".join(keys[index:])
            for item in obj:
                if isinstance(item, dict):
                    resolved = _resolve_record_field(item, remainder)
                    if resolved is not None:
                        return resolved
            return None
        if isinstance(obj, dict):
            obj = obj.get(segment)
        else:
            return None
    return _unwrap_normalized_value(obj)


def _coerce_list_ioc_value(ioc_field, value):
    """Wrap a scalar into a one-element list for a list-valued IOC field.

    ``Indicator.tags`` is a ``List[str]``, so a scalar reaching it would fail
    validation and drop the WHOLE record as CTE_1101. Only
    ``IOC_MULTI_VALUED_FIELDS`` members are wrapped -- a list reaching a scalar
    IOC field must keep failing, since it would ship as "['a', 'b']".

    Args:
        ioc_field (str): IOC field the value is mapped to.
        value (Any): Value resolved from the mapping spec.

    Returns:
        Any: ``value``, wrapped in a list when the field needs one.
    """
    if ioc_field in IOC_MULTI_VALUED_FIELDS and not isinstance(value, list):
        return [value]
    return value


def _resolve_field_spec(record, spec):
    """Resolve a field mapping spec against a CRE record.

    Mirrors the CRE ``_map_params`` convention:
    - ``$<path>``    – resolve *path* from the record (possibly dotted).
    - ``fixed:<v>``  – the explicit static-literal marker emitted by the UI;
      the ``fixed:`` prefix is stripped and ``<v>`` is used verbatim.
    - bare value     – returned as-is (also treated as a static literal).

    Note: because ``fixed:`` is the static-literal marker, a literal value that
    itself begins with ``fixed:`` cannot be expressed without the prefix being
    stripped.
    """
    if spec in (None, ""):
        return None
    if isinstance(spec, str):
        if spec.startswith("$"):
            return _resolve_record_field(record, spec[1:])
        if spec.startswith("fixed:"):
            return spec[len("fixed:"):]
    return spec


def _coerce_numeric_ioc_value(ioc_field, value):
    """Substitute the field default when a numeric IOC value is out of range.

    Static mapping values are range-checked when the rule is saved
    (``_validate_static_value``), but a ``$``-reference is only resolved per
    record at share time and so reaches here unchecked. A record field holding
    e.g. 1000 days would otherwise produce an indicator far beyond the 365-day
    ceiling, and an out-of-range ``reputation`` would fail the ``Indicator``
    model and drop the whole record. Substituting the default keeps the
    indicator shareable and bounded.

    Non-numeric and non-finite values are returned unchanged so the
    ``Indicator`` model still coerces or rejects them per record, preserving
    the existing behaviour for e.g. a datetime carried over from an older
    config or an ``"inf"``/``"nan"`` string.

    Args:
        ioc_field (str): IOC field the value is mapped to.
        value (Any): Value resolved from the mapping spec.

    Returns:
        Any: ``value``, or the field's default when it is a finite number
            outside the field's inclusive ``NUMERIC_IOC_RANGES`` bound.
    """
    bounds = NUMERIC_IOC_RANGES.get(ioc_field)
    if bounds is None or isinstance(value, bool):
        return value
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    if not math.isfinite(number):
        return value
    minimum, maximum = bounds
    if minimum <= number <= maximum:
        return value
    default = NUMERIC_IOC_DEFAULTS[ioc_field]
    logger.info(
        f"The '{ioc_field}' value '{value}' resolved from a CRE record is "
        f"outside the allowed range {minimum}-{maximum}; using the default "
        f"'{default}' instead."
    )
    return default


def _resolve_expires_at(value):
    """Interpret an ``expiresAt`` mapping value as a number of days.

    The sharing UI stores ``expiresAt`` as a number ("expire after N days"),
    so a numeric value is converted to an absolute datetime relative to the
    current share run (each push gets a fresh expiry). A day count outside the
    allowed range is first replaced with the default by
    :func:`_coerce_numeric_ioc_value`, so an indicator shared from a CRE record
    always ages out within the 365-day ceiling. Non-numeric and non-finite
    values (e.g. a datetime carried over from an older config, or an
    ``"inf"``/``"nan"`` string) are returned unchanged for the ``Indicator``
    model to coerce or reject per record.
    """
    if isinstance(value, bool):
        return value
    value = _coerce_numeric_ioc_value("expiresAt", value)
    try:
        return datetime.now() + timedelta(days=float(value))
    except (TypeError, ValueError, OverflowError):
        return value


def build_indicators_from_records(records, field_mapping):
    """Build CTE indicators from CRE entity records using a field mapping.

    Only ``value`` and ``type`` are required; every other mapped IOC field is
    applied when it resolves, otherwise the ``Indicator`` model default is used.

    Numeric IOC fields (``expiresAt``, ``reputation``) are bounded by
    :func:`_coerce_numeric_ioc_value`: a value resolved from a record field is
    only known here, so one outside the field's range is replaced with the
    field's default rather than dropping the record.

    Each mapping spec follows the CRE ``$``-convention:
    - ``$<path>``    – resolved from the record (possibly dotted field path).
    - ``fixed:<v>``  – explicit static-literal marker (prefix stripped).
    - bare value     – used as a static literal (no record lookup).

    Args:
        records (Iterable[dict]): CRE entity record documents.
        field_mapping (dict): IOC field name -> field spec
            (``$<path>``, ``fixed:<value>``, or bare static value).

    Returns:
        list[Indicator]: Indicators ready to push. Records that do not yield a
            valid indicator are skipped.
    """
    value_spec = (field_mapping or {}).get("value")
    type_spec = (field_mapping or {}).get("type", "")
    indicators = []
    container_values = 0
    for record in records:
        value = _resolve_field_spec(record, value_spec)
        if value in (None, ""):
            continue
        if isinstance(value, (dict, list)):
            # ``str()`` below would ship the container's repr as the IOC value,
            # and Indicator has no validator on ``value`` to catch it. Counted
            # rather than logged per record: a bad mapping breaks every record,
            # so one line beats thousands. Empty is "no value", like None.
            if value:
                container_values += 1
            continue
        ioc_type = _resolve_field_spec(record, type_spec)
        ioc_kwargs = {"value": str(value), "type": ioc_type}
        for ioc_field, spec in (field_mapping or {}).items():
            if ioc_field in ("value", "type"):
                continue
            resolved = _resolve_field_spec(record, spec)
            if resolved is not None:
                if ioc_field == "expiresAt":
                    resolved = _resolve_expires_at(resolved)
                elif ioc_field in NUMERIC_IOC_DEFAULTS:
                    resolved = _coerce_numeric_ioc_value(ioc_field, resolved)
                ioc_kwargs[ioc_field] = _coerce_list_ioc_value(
                    ioc_field, resolved
                )
        try:
            indicators.append(Indicator(**ioc_kwargs))
        except Exception:
            logger.error(
                "Skipping a CRE record that did not map to a valid indicator.",
                error_code="CTE_1101",
                details=traceback.format_exc(),
            )
    if container_values:
        logger.error(
            f"Skipped {container_values} CRE record(s) whose 'value' mapping "
            "resolved to a list or an object rather than a single value; the "
            "rule's 'value' mapping references a multi-valued field.",
            error_code="CTE_1101",
        )
    return indicators


def _dedupe_indicators_by_value(indicators):
    """Collapse indicators that share a value, keeping the last one.

    ``value`` may map a non-unique field, so several records can resolve to one
    indicator value -- and sharing wants one IOC for them. Duplicates must not
    reach the store: a rule owns a single ``cre_<entity>_<rule>`` source entry
    per indicator, so each would overwrite the previous one's content and read
    as "changed" every cycle, inflating ``externalHits`` and re-pushing forever.

    Callers must pass indicators built from a deterministically ordered record
    set (see the ``sort`` in :func:`share_cre_entity_records`).

    Args:
        indicators (list[Indicator]): Indicators built from CRE records.

    Returns:
        list[Indicator]: One indicator per value, the last occurrence winning,
            in first-seen order.
    """
    deduped = {}
    for indicator in indicators:
        # Re-assigning an existing key keeps its original position, so the
        # result is first-seen order with last-wins content.
        deduped[indicator.value] = indicator
    return list(deduped.values())


# Content fields persisted on each per-source entry (IndicatorSourceDB) that a
# rule's fieldMapping can populate. Used to detect whether a shared CRE record's
# content actually changed since the last share.
_CRE_SOURCE_CONTENT_FIELDS = (
    "severity",
    "reputation",
    "comments",
    "tags",
    "extendedInformation",
)
# Mapped content fields that live only on the top-level indicator document (not
# on the source entry) but are still worth comparing for change detection.
# ``expiresAt`` is deliberately NOT compared: it is a rolling TTL recomputed as
# ``now + N days`` every cycle, so comparing it would always report a change and
# defeat the skip. It is still reconciled to the latest value at top level.
_CRE_TOPLEVEL_COMPARE_FIELDS = ("type", "active", "test", "safe")


def _cre_values_differ(new_value, old_value, field):
    """Return True if a mapped field's new value differs from the stored one."""
    if field == "tags":
        return set(new_value or []) != set(old_value or [])
    if isinstance(new_value, datetime):
        new_value = new_value.replace(tzinfo=None)
    if isinstance(old_value, datetime):
        old_value = old_value.replace(tzinfo=None)
    # SeverityType / IndicatorType are ``str`` enums; normalize to their value so
    # an enum member compares equal to the plain string stored in Mongo.
    new_value = getattr(new_value, "value", new_value)
    old_value = getattr(old_value, "value", old_value)
    return new_value != old_value


def _cre_content_changed(indicator, existing_doc, matched_source, field_mapping):
    """Whether the rule's mapped content differs from what was last shared.

    Compares every field named in ``field_mapping`` against where it is stored:
    the per-source entry for source-level fields, the top-level document for the
    rest. ``value`` (the dedup key) and ``expiresAt`` (a rolling TTL) are ignored.
    A field the source entry does not carry yet counts as changed.
    """
    for field in field_mapping:
        if field in ("value", "expiresAt"):
            continue
        if field in _CRE_SOURCE_CONTENT_FIELDS:
            new_value = getattr(indicator, field, None)
            old_value = (matched_source or {}).get(field)
            if _cre_values_differ(new_value, old_value, field):
                return True
        elif field in _CRE_TOPLEVEL_COMPARE_FIELDS:
            new_value = getattr(indicator, field, None)
            old_value = (existing_doc or {}).get(field)
            if _cre_values_differ(new_value, old_value, field):
                return True
    return False


def _cre_apply_source_content(source_entry, indicator, field_mapping):
    """Write the rule's mapped source-level content onto the source entry."""
    for field in field_mapping:
        if field in _CRE_SOURCE_CONTENT_FIELDS:
            value = getattr(indicator, field, None)
            source_entry[field] = list(value or []) if field == "tags" else value


def _cre_destination_present(source_entry, destination_config_name):
    """Whether the source entry already lists this destination."""
    return any(
        d.get("name") == destination_config_name
        for d in (source_entry.get("destinations") or [])
    )


def _cre_reconcile_top_level(existing, indicator, now):
    """Top-level dynamic fields, reconciled like CTE's ``_update_existing_indicator``.

    ``active`` is OR'd, ``expiresAt`` keeps the latest, ``lastSeen`` advances to
    ``now``. Top-level *content* (severity/reputation/comments/tags/...) is never
    rewritten after creation — it stays the creating source's snapshot, matching
    a CTE-pulled indicator.
    """
    existing_expires = existing.get("expiresAt")
    if (
        existing_expires is None
        or indicator.expiresAt is None
        or indicator.expiresAt.replace(tzinfo=None)
        > existing_expires.replace(tzinfo=None)
    ):
        expires_at = indicator.expiresAt
    else:
        expires_at = existing_expires
    return {
        "lastSeen": now,
        "active": bool(indicator.active) or bool(existing.get("active")),
        "expiresAt": expires_at,
        "test": indicator.test,
        "safe": indicator.safe,
    }


def persist_cre_entity_indicators(
    indicators,
    source_name,
    destination_config_name,
    status="inprogress",
    force=False,
    field_mapping=None,
    always_reshare=False,
):
    """Persist shared CRE-entity indicators into the CTE indicators store.

    Lets CRE records that were shared appear on the Threat IOCs page. Each
    indicator is attributed to the derived source ``source_name``
    (``cre_<entity>_<ruleName>``) and marked shared with the destination. This
    is the one place the share flow creates indicator documents; it does NOT
    run the pull/pending promotion and does NOT mutate the CRE records. Dedup
    is by indicator ``value`` (a colliding value merges into the existing
    document's ``cre_<entity>_<rule>`` source entry).

    Change-aware behavior (recurring share, ``force=False``, ``always_reshare=False``):
    a record whose mapped content is unchanged AND is already shared with this
    destination is skipped entirely — no push, no ``lastSeen`` bump, no
    ``externalHits`` increment. New values, new source entries, and changed
    content are persisted and returned for pushing. An unchanged record going
    to a *new* destination is pushed but does not increment hits.

    Manual sync (``force=True``): every windowed record is re-pushed and its
    ``lastSeen`` advanced, but ``externalHits`` is NOT incremented for unchanged
    content — only a genuine content change (or a new value/source) counts a hit.

    Always-reshare (``always_reshare=True``, unified mapping only — see
    ``share_unified_mapping_records``): mirrors real CTE plugin re-pulls
    (``insert_or_update_indicator``/``_update_existing_indicator``), which
    unconditionally reset every destination back to pending and count a hit on
    every pull, with no content comparison at all. Every recency-eligible row
    is treated as changed regardless of ``_cre_content_changed``'s result:
    content refreshes, ``lastSeen`` advances, a hit is counted, and an
    already-shared destination's status resets to ``status`` (not left as
    "shared") so it re-qualifies the same way a repeated CTE pull would. CRE
    entity sharing keeps the change-aware behavior above unchanged — it is not
    yet updated to match this contract.

    Top-level document *content* is never rewritten after creation (mirrors CTE);
    only ``lastSeen``/``active``/``expiresAt``/``externalHits`` and the CRE source
    entry stay dynamic.

    Args:
        indicators (list[Indicator]): Indicators built from CRE records.
        source_name (str): Derived source label, e.g. ``cre_Devices_myrule``.
        destination_config_name (str): Destination the indicators were shared to.
        status (str): Initial destination status: "inprogress" (before push) or
            "shared" (if persisting post-validation). Defaults to "inprogress".
        force (bool): Manual-sync mode — re-share every windowed record; no hit
            is counted for unchanged content.
        field_mapping (dict): Rule fieldMapping; drives content-change detection.
        always_reshare (bool): Treat every recency-eligible record as changed,
            matching real CTE pull semantics. Used by unified mapping sharing only.

    Returns:
        list[Indicator]: The subset that should be pushed (skipped records
            excluded).
    """
    now = datetime.now().replace(tzinfo=None)
    dest_entry = {"name": destination_config_name, "status": status}
    field_mapping = field_mapping or {}
    to_share = []

    def _build_source(indicator):
        """Construct a fresh CRE source entry for an indicator."""
        return IndicatorSourceDB(
            **{
                **indicator.model_dump(),
                "internalHits": 0,
                "externalHits": 1,
                "source": source_name,
                "destinations": [dest_entry],
                # This function is the only place a CRE- or unified-view-sourced
                # entry gets built — real plugin pulls never call it.
                "derived": True,
            }
        )

    # Prefetch all existing documents for these values in one query (dedup key
    # is ``value``) instead of a find_one per indicator. The map is kept in sync
    # with each write so repeated values within this batch still merge.
    values = [indicator.value for indicator in indicators]
    existing_by_value = {
        doc["value"]: doc
        for doc in connector.collection(Collections.INDICATORS).find(
            {"value": {"$in": values}}
        )
    } if values else {}
    for indicator in indicators:
        try:
            indicator.firstSeen = indicator.firstSeen or now
            indicator.lastSeen = now
            indicator.expiresAt = indicator.expiresAt or (now + timedelta(days=EXPIRY_DAYS))
            existing = existing_by_value.get(indicator.value)
            if existing is None:
                # Brand-new value: create the document from this creating source.
                indicator_model = IndicatorDBWithSources(
                    **{
                        **indicator.model_dump(),
                        "internalHits": 0,
                        "externalHits": 1,
                        "source": source_name,
                        "sharedWith": [destination_config_name],
                    },
                    sources=[_build_source(indicator)],
                )
                doc = indicator_model.model_dump()
                connector.collection(Collections.INDICATORS).update_one(
                    {"value": indicator.value},
                    {"$set": doc},
                    upsert=True,
                )
                existing_by_value[indicator.value] = doc
                to_share.append(indicator)
                continue

            sources = existing.get("sources", [])
            matched = next(
                (s for s in sources if s.get("source") == source_name), None
            )
            increment_hit = False
            if matched is None:
                # First contribution from this rule for an existing value.
                sources.append(_build_source(indicator).model_dump())
                increment_hit = True
            else:
                destination_present = _cre_destination_present(
                    matched, destination_config_name
                )
                content_changed = _cre_content_changed(
                    indicator, existing, matched, field_mapping
                )
                if always_reshare or content_changed:
                    # Genuine content change (any trigger, incl. manual sync) OR
                    # always_reshare (unified mapping: every touch counts, matching
                    # real CTE pull semantics regardless of _cre_content_changed):
                    # refresh source content, advance lastSeen, count a hit.
                    _cre_apply_source_content(matched, indicator, field_mapping)
                    matched["lastSeen"] = now
                    matched["externalHits"] = matched.get("externalHits", 0) + 1
                    increment_hit = True
                    if not destination_present:
                        matched.setdefault("destinations", []).append(dest_entry)
                    elif always_reshare:
                        # A repeated CTE pull always resets an existing
                        # destination back to pending, even with unchanged
                        # content — mirror that instead of leaving a stale
                        # "shared"/"failed" status untouched.
                        for d in matched.get("destinations", []):
                            if d.get("name") == destination_config_name:
                                d["status"] = status
                                break
                elif force or not destination_present:
                    # Manual sync re-share, or an unchanged record reaching a new
                    # destination: re-push and advance lastSeen, but do NOT count
                    # a hit — the content did not change.
                    matched["lastSeen"] = now
                    if not destination_present:
                        matched.setdefault("destinations", []).append(dest_entry)
                else:
                    # Unchanged content, already shared, not a manual sync: skip
                    # entirely — no re-push, no hit, no lastSeen refresh.
                    continue

            shared_with = existing.get("sharedWith", []) or []
            if destination_config_name not in shared_with:
                shared_with.append(destination_config_name)
            set_fields = {
                "sources": sources,
                "sharedWith": shared_with,
                **_cre_reconcile_top_level(existing, indicator, now),
            }
            update = {"$set": set_fields}
            if increment_hit:
                update["$inc"] = {"externalHits": 1}
            connector.collection(Collections.INDICATORS).update_one(
                {"value": indicator.value},
                update,
            )
            # Reflect the write so a repeated value later in this batch
            # continues to merge into the same document.
            existing["sources"] = sources
            existing["sharedWith"] = shared_with
            existing["active"] = set_fields["active"]
            existing["expiresAt"] = set_fields["expiresAt"]
            to_share.append(indicator)
        except Exception:
            logger.error(
                f"Could not persist shared CRE indicator with value "
                f"'{getattr(indicator, 'value', '')}' as a threat IOC.",
                error_code="CTE_1103",
                details=traceback.format_exc(),
            )
    return to_share


def update_cre_entity_indicator_status(indicators, source_name, destination_config_name, status):
    """Update destination status for CRE-entity indicators after push.

    Used to advance indicators from "inprogress" to "shared" (accepted by the
    plugin) or "failed" (push failure), without re-running the full persist
    logic. Indicators the plugin discarded get no status: they are removed by
    ``_cre_discard_indicators`` instead.

    Args:
        indicators (list[Indicator]): Indicators to update.
        source_name (str): Derived source label, e.g. ``cre_Devices_myrule``.
        destination_config_name (str): Destination name to update.
        status (str): New destination status ("shared" or "failed").
    """
    indicator_values = [ind.value for ind in indicators]
    if not indicator_values:
        return
    try:
        connector.collection(Collections.INDICATORS).update_many(
            {
                "value": {"$in": indicator_values},
                "sources": {
                    "$elemMatch": {
                        "source": source_name,
                        "destinations": {
                            "$elemMatch": {"name": destination_config_name}
                        },
                    }
                },
            },
            {
                "$set": {
                    "sources.$[elem].destinations.$[dest].status": status,
                    "sources.$[elem].lastSeen": datetime.now().replace(tzinfo=None),
                }
            },
            array_filters=[
                {"elem.source": source_name},
                {"dest.name": destination_config_name},
            ],
        )
    except Exception:
        logger.error(
            f"Could not update CRE indicators' status to '{status}' for "
            f"source '{source_name}' / destination '{destination_config_name}'.",
            error_code="CTE_1104",
            details=traceback.format_exc(),
        )


def _cre_unshare_destination(values, destination_config_name):
    """Drop a destination from ``sharedWith`` for indicators it never reached.

    ``persist_cre_entity_indicators`` adds the destination to ``sharedWith``
    before the push (the indicators are created there), so an indicator the
    plugin then failed on or discarded would keep claiming it was shared on the
    dashboard and on the Threat IOCs page. A purely CRE-sourced indicator is
    never retracted (the retraction sweep excludes ``cre_*`` sources), but a
    value that a plugin source also contributed shares one ``sharedWith`` array
    with it, so a stale entry there can offer that indicator for retraction to a
    destination this rule never reached.

    The pull is guarded: if any source entry on the document reports the
    destination as "shared" (another CRE rule or a Threat Indicators share of
    the same value) or as "inprogress", ``sharedWith`` is left alone.
    "inprogress" counts because the Threat Indicators flow adds ``sharedWith``
    up front and only settles the status to "shared" in its deferred sweep at
    the end of ``share_iocs``; whoever owns that in-flight share settles the
    value, and erring toward keeping ``sharedWith`` over-reports rather than
    silently dropping an indicator out of retraction.

    Callers must pass only values that no action of this rule delivered: every
    action of a rule writes the same ``destinations`` entry (it is keyed by
    destination name alone), so one action's skip can overwrite an earlier
    action's "shared" status and defeat the guard above.

    Args:
        values (Iterable[str]): Indicator values that did not reach the
            destination (failed or skipped by every action).
        destination_config_name (str): Destination to remove.
    """
    values = list(values)
    if not values:
        return
    try:
        for index in range(0, len(values), CTE_FAILED_IOC_QUERY_CHUNK):
            chunk = values[index: index + CTE_FAILED_IOC_QUERY_CHUNK]
            connector.collection(Collections.INDICATORS).update_many(
                {
                    "value": {"$in": chunk},
                    "sharedWith": destination_config_name,
                    "sources": {
                        "$not": {
                            "$elemMatch": {
                                "destinations": {
                                    "$elemMatch": {
                                        "name": destination_config_name,
                                        "status": {
                                            "$in": ["shared", "inprogress"]
                                        },
                                    }
                                }
                            }
                        }
                    },
                },
                {"$pull": {"sharedWith": destination_config_name}},
            )
    except Exception:
        logger.error(
            f"Could not remove destination '{destination_config_name}' from "
            f"'sharedWith' for CRE indicators that were not shared with it.",
            error_code="CTE_1107",
            details=traceback.format_exc(),
        )


def _cre_discard_indicators(values, source_name, destination_config_name):
    """Undo the pre-push persistence for indicators the plugin discarded.

    CRE-entity indicators exist only because the share flow created them
    (``persist_cre_entity_indicators`` writes them before the push so they show
    as "inprogress" on the Threat IOCs page). When the destination plugin
    discards one — unsupported type, invalid value, destination profile limits
    — there is no share to represent, so the record is removed rather than kept
    in a "discarded" state that the Threat Indicators flow has no equivalent of
    (there a skipped IOC still exists because a source plugin pulled it).

    Removal is layered so a value another source also contributed survives:
    the destination entry goes first, then this rule's source entry once it
    targets no destination, then the document once it has no sources left.
    Each layer re-evaluates the current document, so a concurrent share of the
    same rule to another destination (destinations run as separate tasks) stops
    the removal as soon as its entry is written. The one uncovered interleaving
    is that share writing between this delete and its own non-upserting update
    of a pre-existing document, which would drop the indicator it just shared.

    Callers must exclude values any action of this rule delivered or failed on:
    a delivered indicator is real, and a failed one stays visible as "failed",
    mirroring the Threat Indicators flow.

    Args:
        values (Iterable[str]): Indicator values every action discarded.
        source_name (str): Synthetic source label, e.g. ``cre_Devices_myrule``.
        destination_config_name (str): Destination that discarded them.
    """
    values = list(values)
    if not values:
        return
    try:
        indicators = connector.collection(Collections.INDICATORS)
        for index in range(0, len(values), CTE_FAILED_IOC_QUERY_CHUNK):
            chunk = values[index: index + CTE_FAILED_IOC_QUERY_CHUNK]
            indicators.update_many(
                {"value": {"$in": chunk}, "sources.source": source_name},
                {
                    "$pull": {
                        "sources.$[elem].destinations": {
                            "name": destination_config_name
                        }
                    }
                },
                array_filters=[{"elem.source": source_name}],
            )
            # The source entry only ever exists to carry destinations, so an
            # empty list means this rule no longer contributes to the value.
            indicators.update_many(
                {"value": {"$in": chunk}},
                {
                    "$pull": {
                        "sources": {
                            "source": source_name,
                            "destinations": {"$size": 0},
                        }
                    }
                },
            )
            # Scoped to the values just processed: a document left without any
            # source existed only for this share. Same orphan-cleanup shape the
            # configuration-delete flow uses (cte/routers/configurations.py).
            indicators.delete_many(
                {
                    "value": {"$in": chunk},
                    "sources": {"$exists": True, "$eq": []},
                }
            )
    except Exception:
        logger.error(
            f"Could not remove {len(values)} CRE indicator(s) discarded by "
            f"destination '{destination_config_name}' for source "
            f"'{source_name}'; they remain on the Threat IOCs page as "
            f"'inprogress' for this destination.",
            error_code="CTE_1105",
            details=traceback.format_exc(),
        )


def _split_cre_push_outcome(to_share, result):
    """Split the pushed CRE indicators by what the plugin actually accepted.

    A successful ``PushResult`` does not mean every indicator reached the
    destination: plugins report the ones they dropped (unsupported type,
    invalid value, destination profile limits) through ``skipped_iocs``, and
    the ones they attempted but could not push through ``failed_iocs``. Without
    this split the whole batch would be reported and recorded as shared even
    when the plugin discarded all of it. Mirrors the Threat Indicators flow
    (``validate_result_and_update``).

    Args:
        to_share (list[Indicator]): Indicators handed to ``plugin.push``.
        result: Value returned by ``plugin.push`` — a ``PushResult``, or any
            other value for plugins that do not return one (treated as
            "everything was accepted").

    Returns:
        tuple[list, list, list]: ``(shared, failed, skipped)`` indicators.
    """
    if not isinstance(result, PushResult):
        return list(to_share), [], []
    failed_values = set(result.failed_iocs or [])
    skipped_values = set(result.skipped_iocs or [])
    if not failed_values and not skipped_values:
        return list(to_share), [], []
    shared, failed, skipped = [], [], []
    for indicator in to_share:
        if indicator.value in failed_values:
            failed.append(indicator)
        elif indicator.value in skipped_values:
            skipped.append(indicator)
        else:
            shared.append(indicator)
    return shared, failed, skipped


def share_cre_entity_records(
    destination_config_name, plugin, rule, actions, lastseen=None,
    alerts_enabled=True,
):
    """Push CRE entity records matched by a rule to a CTE destination.

    Qualification is a Mongo ``$match`` (the single qualification source) on the
    rule's CRE entity collection. Indicators are built once from the rule-level
    ``fieldMapping`` (identical for every action) and persisted with status
    "inprogress" before push, then advanced to "shared" (success) or "failed"
    (failure) per action based on the push result. CRE record documents are
    left unmutated.

    Args:
        destination_config_name (str): CTE destination configuration name.
        plugin: Instantiated destination plugin.
        rule (BusinessRuleDB): The CRE-entity business rule being shared.
        actions (list[Action]): ``creShare`` actions for this destination.
        lastseen (Optional[int]): Days window for a manual sync; ``None`` = all.
        alerts_enabled (bool): Whether CTE -> CTO alerts may be generated
            (``settings.cte.generateAlerts`` on and the CTO module enabled).
            An action's own ``generateAlert`` only emits alerts while this is
            on. Resolved by the caller so it is read once per share.

    Returns:
        bool: ``True`` if any Netskope-style action cleanup should run.
    """
    # Built by the same helper the Test endpoint uses, so a manual/scheduled
    # share always matches what Test predicted.
    match_query = build_cre_entity_match_query(rule, lastseen)
    collection = connector.collection(get_entity_collection(rule.entity))
    is_run_action_cleanup = False
    source_name = cre_source_name(rule.entity, rule.name)
    # Materialized once (a cursor is consumed on first iteration, leaving later
    # actions empty) and ordered so the collapse below picks a stable winner.
    records = list(collection.find(match_query).sort("_id", ASCENDING))
    # The mapping lives on the rule, so the built indicators are identical for
    # every action: build and persist once, then push per action.
    field_mapping = rule.fieldMapping or {}
    indicators = build_indicators_from_records(records, field_mapping)
    if not indicators:
        logger.info(
            f"No CRE records from entity '{rule.entity}' matched rule "
            f"'{rule.name}' for destination '{destination_config_name}'."
        )
        return is_run_action_cleanup
    collapsed = _dedupe_indicators_by_value(indicators)
    if len(collapsed) != len(indicators):
        # Logged rather than silent: a share that emits fewer IOCs than it
        # matched records otherwise reads as data loss.
        logger.info(
            f"{len(indicators)} indicator(s) built from entity '{rule.entity}' "
            f"records for rule '{rule.name}' collapsed into {len(collapsed)} "
            "by value; the rule's 'value' mapping is not unique per record."
        )
    indicators = collapsed
    # Persist indicators with "inprogress" status before push so they are visible
    # on the Threat IOCs page during the share. Persisting also decides which
    # indicators are new/changed and returns just that subset to push: on the
    # recurring share (``lastseen`` is None), records whose mapped content is
    # unchanged and already shared with this destination are skipped (no push,
    # no hit). A manual sync (``lastseen`` set) forces a re-share of every
    # windowed record; hits still count only genuine content changes.
    force = lastseen is not None
    to_share = persist_cre_entity_indicators(
        indicators,
        source_name,
        destination_config_name,
        status="inprogress",
        force=force,
        field_mapping=field_mapping,
    )
    if not to_share:
        logger.info(
            f"No new or changed CRE records from entity '{rule.entity}' for "
            f"rule '{rule.name}' to share with '{destination_config_name}'."
        )
        return is_run_action_cleanup
    # Every action of this rule writes the same destination entry, so the
    # outcome is only final once all of them have run: collect the per-action
    # verdicts and settle the records after the loop.
    delivered_values = set()
    failed_values = set()
    discarded_values = set()
    for action in actions:
        action_dict_for_push = (
            action.model_dump() if isinstance(action, Action) else action
        )
        action_value = _action_field(action, "value")
        is_no_action = action_value == CTE_NO_ACTION_VALUE
        generate_alert = (
            bool(_action_field(action, "generateAlert", False)) and alerts_enabled
        )
        alert_meta = {
            "rule_name": rule.name,
            # CRE-entity sharing has no source configuration, so there is no
            # source plugin to name: ``source_plugin_name`` is deliberately
            # absent and _build_indicator_alert omits ``rawData.plugin``
            # entirely. The synthetic CRE source label (what the Threat IOCs
            # page shows for these indicators) identifies the origin instead.
            "source_config_name": source_name,
            "destination_config_name": destination_config_name,
            "action_label": _action_field(action, "label", action_value),
        }
        if is_no_action:
            # Core-level pseudo-action: nothing is pushed to the destination
            # plugin; the qualified indicators only drive alert generation. The
            # destination status is still finalized so the Threat IOCs page and
            # the rule's sharing view agree.
            alert_note = (
                "alerts will be generated"
                if generate_alert
                else "no alerts will be generated"
            )
            logger.info(
                f"{len(to_share)} indicator(s) built from entity '{rule.entity}' "
                f"records matched the 'No Action' target for rule '{rule.name}'; "
                f"nothing is pushed to '{destination_config_name}' and "
                f"{alert_note}."
            )
            update_cre_entity_indicator_status(
                to_share, source_name, destination_config_name, "shared"
            )
            delivered_values.update(indicator.value for indicator in to_share)
            if generate_alert:
                _generate_cre_entity_alerts(
                    to_share,
                    source_name,
                    destination_config_name,
                    "shared",
                    alert_meta,
                )
            continue
        try:
            share_source_info = has_source_info_args(
                plugin, "push", ["source", "business_rule", "plugin_name"]
            )
            result = (
                plugin.push(to_share, action_dict_for_push, None, rule.name, None)
                if share_source_info
                else plugin.push(to_share, action_dict_for_push)
            )
            if isinstance(result, PushResult) and not result.success:
                logger.error(
                    f"Failed to share CRE records from entity '{rule.entity}' "
                    f"for rule '{rule.name}' to '{destination_config_name}'. "
                    f"{result.message}",
                    error_code="CTE_1102",
                )
                # Update indicators to "failed" status.
                update_cre_entity_indicator_status(
                    to_share, source_name, destination_config_name, "failed"
                )
                if generate_alert:
                    _generate_cre_entity_alerts(
                        to_share,
                        source_name,
                        destination_config_name,
                        "failed",
                        alert_meta,
                    )
                failed_values.update(
                    indicator.value for indicator in to_share
                )
            else:
                # A successful push can still have dropped indicators: report
                # and record only what the plugin actually accepted.
                shared_iocs, failed_iocs, skipped_iocs = _split_cre_push_outcome(
                    to_share, result
                )
                if skipped_iocs:
                    logger.info(
                        f"Skipped {len(skipped_iocs)} indicator(s) built from "
                        f"entity '{rule.entity}' records while sharing rule "
                        f"'{rule.name}' to '{destination_config_name}'. These "
                        f"indicators were discarded by the plugin."
                    )
                if failed_iocs:
                    logger.error(
                        f"Could not share {len(failed_iocs)} indicator(s) built "
                        f"from entity '{rule.entity}' records for rule "
                        f"'{rule.name}' to '{destination_config_name}'.",
                        error_code="CTE_1102",
                    )
                if shared_iocs:
                    logger.info(
                        f"Shared {len(shared_iocs)} indicators built from entity "
                        f"'{rule.entity}' records for rule '{rule.name}' to "
                        f"'{destination_config_name}'."
                    )
                else:
                    logger.info(
                        f"No indicators built from entity '{rule.entity}' "
                        f"records for rule '{rule.name}' were shared to "
                        f"'{destination_config_name}': the plugin discarded "
                        f"{len(skipped_iocs)} and failed on "
                        f"{len(failed_iocs)} of the {len(to_share)} "
                        f"indicator(s) sent."
                    )
                # Record the real per-indicator outcome: "shared" for the
                # accepted ones and "failed" for the ones the plugin could not
                # push. Discarded ones get no status of their own — they are
                # removed once every action has run, so a CRE source entry
                # never carries a "discarded" state.
                if shared_iocs:
                    update_cre_entity_indicator_status(
                        shared_iocs, source_name, destination_config_name, "shared"
                    )
                if failed_iocs:
                    update_cre_entity_indicator_status(
                        failed_iocs, source_name, destination_config_name, "failed"
                    )
                if generate_alert:
                    # Skipped indicators get no alert, mirroring the Threat
                    # Indicators flow (_generate_alerts_for_action).
                    if shared_iocs:
                        _generate_cre_entity_alerts(
                            shared_iocs,
                            source_name,
                            destination_config_name,
                            "shared",
                            alert_meta,
                        )
                    if failed_iocs:
                        _generate_cre_entity_alerts(
                            failed_iocs,
                            source_name,
                            destination_config_name,
                            "failed",
                            alert_meta,
                        )
                delivered_values.update(
                    indicator.value for indicator in shared_iocs
                )
                failed_values.update(
                    indicator.value for indicator in failed_iocs
                )
                discarded_values.update(
                    indicator.value for indicator in skipped_iocs
                )
                # Honour the plugin's own request as well as the Netskope
                # metadata flag: the Threat Indicators flow acts on
                # ``should_run_cleanup`` (validate_result_and_update), so a
                # non-Netskope plugin asking for cleanup was being ignored here.
                if plugin.metadata.get("netskope", False) or (
                    isinstance(result, PushResult) and result.should_run_cleanup
                ):
                    is_run_action_cleanup = True
            _update_storage(destination_config_name, plugin.storage)
        except Exception:
            logger.error(
                f"Error sharing CRE records for rule '{rule.name}' to "
                f"'{destination_config_name}'.",
                error_code="CTE_1102",
                details=traceback.format_exc(),
            )
            # Update indicators to "failed" status on exception.
            update_cre_entity_indicator_status(
                to_share, source_name, destination_config_name, "failed"
            )
            if generate_alert:
                _generate_cre_entity_alerts(
                    to_share,
                    source_name,
                    destination_config_name,
                    "failed",
                    alert_meta,
                )
            failed_values.update(indicator.value for indicator in to_share)
    # Nothing an action delivered is touched. What every action discarded is
    # removed outright — a CRE indicator only exists to represent a share, so a
    # discarded one represents nothing (a value that also failed on some action
    # is kept as "failed" instead, mirroring the Threat Indicators flow). What
    # is left over stops counting as shared with the destination.
    _cre_discard_indicators(
        discarded_values - delivered_values - failed_values,
        source_name,
        destination_config_name,
    )
    _cre_unshare_destination(
        (failed_values | discarded_values) - delivered_values,
        destination_config_name,
    )
    return is_run_action_cleanup


def share_cre_entity_rules(
    destination_config_name, plugin, rule_provided=None, lastseen=None
):
    """Share all CRE-entity rules whose ``creShare`` targets this destination.

    Args:
        destination_config_name (str): CTE destination configuration name.
        plugin: Instantiated destination plugin.
        rule_provided (Optional[str]): Limit to a single rule (manual sync).
        lastseen (Optional[int]): Days window for a manual sync; ``None`` = all.

    Returns:
        bool: ``True`` if any Netskope-style action cleanup should run.
    """
    base_query = {
        "entity": {"$ne": THREAT_INDICATORS_ENTITY},
        f"creShare.{destination_config_name}": {"$exists": True},
    }
    if rule_provided:
        base_query["name"] = rule_provided
    # CRE-entity sharing reads live CRE entity records, so it can't run while the
    # CRE module is disabled. Disabling a module is an intentional admin action,
    # not a failure, so record this at debug level instead of error, and only when
    # this destination actually has CRE-entity sharing configured.
    if not is_platform_enabled("cre"):
        if connector.collection(Collections.CTE_BUSINESS_RULES).find_one(base_query):
            logger.debug(
                f"Skipped sharing CRE-entity business rules to configuration "
                f"'{destination_config_name}' because the CRE module is disabled. "
                f"Enable the CRE module to resume sharing these rules.",
            )
        return False
    query = {**base_query, "muted": False}
    is_run_action_cleanup = False
    # Global CTE -> CTO alert kill-switch, read once per share instead of per
    # rule/action (same setting the Threat Indicators flow consults).
    settings = SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
    module_alerts_on = bool(settings.cte and settings.cte.generateAlerts)
    cto_on = cto_alerts_enabled(settings)
    alerts_enabled = module_alerts_on and cto_on
    # Reported once per run, and only for a rule that actually asked for alerts,
    # so a disabled CTO module does not log on every share cycle.
    cto_skip_pending = module_alerts_on and not cto_on
    for rule_doc in connector.collection(Collections.CTE_BUSINESS_RULES).find(query):
        rule = BusinessRuleDB(**rule_doc)
        actions = (rule.creShare or {}).get(destination_config_name, [])
        if not actions:
            continue
        if cto_skip_pending and _any_action_alerts(actions):
            log_cto_alerts_skipped(
                f"CRE entity records shared to '{destination_config_name}'"
            )
            cto_skip_pending = False
        should_run_cleanup = share_cre_entity_records(
            destination_config_name,
            plugin,
            rule,
            actions,
            lastseen,
            alerts_enabled=alerts_enabled,
        )
        is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
    return is_run_action_cleanup


def build_indicators_from_unified_mapping_rows(rows, field_mapping, base_table):
    """Build CTE indicators from unified mapping rows using a field mapping.

    Field-mapping specs reference the flattened unified keys
    (``$<table.field>``). After the join, base-table fields live at the top
    level of each row, so the base-table prefix is stripped from those specs;
    joined-table specs are left as-is — ``_resolve_record_field`` dot-walks the
    nested sub-documents. Everything else (``fixed:``/bare-literal handling,
    value/type requirements) delegates to ``build_indicators_from_records``.

    Args:
        rows (list[dict]): Materialized unified mapping aggregation documents.
        field_mapping (dict): IOC field name -> field spec.
        base_table (str): The mapping's base collection name.

    Returns:
        list[Indicator]: Indicators ready to push.
    """
    base_prefix = f"${base_table}."
    translated_mapping = {}
    for ioc_field, spec in (field_mapping or {}).items():
        if isinstance(spec, str) and spec.startswith(base_prefix):
            translated_mapping[ioc_field] = f"${spec[len(base_prefix):]}"
        else:
            translated_mapping[ioc_field] = spec
    return build_indicators_from_records(rows, translated_mapping)


def share_unified_mapping_records(
    destination_config_name,
    plugin,
    rule_doc,
    actions,
    mapping_doc,
    lastseen=None,
    advance_checkpoint=True,
    alerts_enabled=True,
):
    """Push unified mapping rows matched by a rule to a CTE destination.

    Qualification is a Mongo aggregation (the single qualification source): the
    mapping's stored join pipeline + the rule's filter + mute exceptions + recency
    (``lastUpdated`` vs. the per-destination ``lastPerformed`` checkpoint, or a
    manual ``lastseen`` days window) + the matched-only stage — sharing always
    enforces MATCHED_ONLY regardless of the mapping's stored ``matchMode``.
    Indicators are persisted with status "inprogress" before push, then
    advanced to "shared"/"failed" based on the push result.

    Args:
        destination_config_name (str): CTE destination configuration name.
        plugin: Instantiated destination plugin.
        rule_doc (dict): The unified mapping business rule document.
        actions (list[dict]): ``cteShare`` actions for this destination.
        mapping_doc (dict): The stored unified mapping document.
        lastseen (Optional[int]): Days window for a manual sync; ``None`` =
            checkpoint-based recency.
        advance_checkpoint (bool): Advance ``lastPerformed`` on success.
            Manual syncs with an explicit window do not move it.
        alerts_enabled (bool): Whether CTE -> CTO alerts may be generated
            (global kill-switch on and the CTO module enabled).

    Returns:
        tuple[bool, bool]: ``(run_action_cleanup, success)`` — whether any
            Netskope-style action cleanup should run, and whether every
            push for this rule/destination succeeded.
    """
    mapping_name = mapping_doc["name"]
    base_table = mapping_doc["baseTable"]
    rule_name = rule_doc["name"]
    sources_expanded = mapping_unwinds_sources(mapping_doc)
    pipeline = (
        (build_mapping_pipeline(mapping_doc) or [])
        + rule_match_stage(rule_doc, base_table, sources_expanded)
        + exception_match_stage(rule_doc, base_table, sources_expanded)
    )
    if lastseen:
        pipeline += recency_match_stage(
            mapping_doc, datetime.now() - timedelta(days=lastseen)
        )
    else:
        checkpoint = (rule_doc.get("lastPerformed") or {}).get(
            destination_config_name
        )
        # First run (no checkpoint): skip the recency stage — share everything once.
        if checkpoint:
            pipeline += recency_match_stage(mapping_doc, checkpoint)
    pipeline += matched_only_stage(mapping_doc)
    # The checkpoint advances to the cycle start (not post-push time) so rows
    # updated mid-push are not skipped next cycle.
    cycle_start = datetime.now()
    agg_opts = {"allowDiskUse": True}
    if mapping_doc.get("caseInsensitive"):
        agg_opts["collation"] = {"locale": "en", "strength": 2}
    source_name = um_source_name(mapping_name)
    is_run_action_cleanup = False
    all_actions_success = True
    # Rule-level mapping: every action/destination on this rule shares the
    # same joined rows, so the built indicators are identical for every
    # action — build and persist once, then push per action (mirrors
    # share_cre_entity_records).
    field_mapping = rule_doc.get("fieldMapping") or {}
    indicators = []
    try:
        # Rows are converted a chunk at a time so peak memory tracks the chunk
        # and the built indicators, not the whole joined row set — a join can
        # return far more rows than any single entity collection holds.
        chunk = []
        for row in connector.collection(base_table).aggregate(pipeline, **agg_opts):
            chunk.append(row)
            if len(chunk) >= CTE_UM_ROW_CHUNK_SIZE:
                indicators.extend(
                    build_indicators_from_unified_mapping_rows(
                        unwrap_normalized_row_values(chunk, mapping_doc),
                        field_mapping,
                        base_table,
                    )
                )
                chunk = []
        if chunk:
            indicators.extend(
                build_indicators_from_unified_mapping_rows(
                    unwrap_normalized_row_values(chunk, mapping_doc),
                    field_mapping,
                    base_table,
                )
            )
    except Exception:
        logger.error(
            f"Error executing unified mapping '{mapping_name}' pipeline for rule "
            f"'{rule_name}'.",
            error_code="CTE_1109",
            details=traceback.format_exc(),
        )
        return False, False
    if not indicators:
        logger.info(
            f"No unified mapping rows from mapping '{mapping_name}' matched rule "
            f"'{rule_name}' for destination '{destination_config_name}'."
        )
        return is_run_action_cleanup, all_actions_success
    # Persist indicators with "inprogress" status before push so they are
    # visible on the Threat IOCs page during the share. always_reshare=True
    # matches real CTE plugin-pull semantics: any row that's recency-eligible
    # (some constituent table's lastUpdated moved past the checkpoint) is
    # treated as touched and re-shared, the same way a plugin re-pulling an
    # identical IOC always resets it to pending rather than skipping it for
    # having unchanged content.
    to_share = persist_cre_entity_indicators(
        indicators,
        source_name,
        destination_config_name,
        status="inprogress",
        force=lastseen is not None,
        field_mapping=field_mapping,
        always_reshare=True,
    )
    if not to_share:
        logger.info(
            f"No new or changed unified mapping rows from mapping '{mapping_name}' for "
            f"rule '{rule_name}' to share with '{destination_config_name}'."
        )
        return is_run_action_cleanup, all_actions_success
    # Every action of this rule writes the same destination entry, so the
    # outcome is only final once all of them have run: collect the per-action
    # verdicts and settle the records after the loop.
    delivered_values = set()
    failed_values = set()
    discarded_values = set()
    for action in actions:
        action_value = _action_field(action, "value")
        generate_alert = (
            bool(_action_field(action, "generateAlert", False)) and alerts_enabled
        )
        alert_meta = {
            "rule_name": rule_name,
            # No source configuration: the synthetic unified mapping source
            # label identifies the origin instead (as CRE-entity sharing does).
            "source_config_name": source_name,
            "destination_config_name": destination_config_name,
            "action_label": _action_field(action, "label", action_value),
        }
        if action_value == CTE_NO_ACTION_VALUE:
            # Core-level pseudo-action: nothing is pushed to the destination
            # plugin; the qualified indicators only drive alert generation.
            alert_note = (
                "alerts will be generated"
                if generate_alert
                else "no alerts will be generated"
            )
            logger.info(
                f"{len(to_share)} indicator(s) built from unified mapping "
                f"'{mapping_name}' matched the 'No Action' target for rule "
                f"'{rule_name}'; nothing is pushed to '{destination_config_name}' "
                f"and {alert_note}."
            )
            update_cre_entity_indicator_status(
                to_share, source_name, destination_config_name, "shared"
            )
            delivered_values.update(indicator.value for indicator in to_share)
            if generate_alert:
                _generate_cre_entity_alerts(
                    to_share,
                    source_name,
                    destination_config_name,
                    "shared",
                    alert_meta,
                )
            continue
        try:
            share_source_info = has_source_info_args(
                plugin, "push", ["source", "business_rule", "plugin_name"]
            )
            result = (
                plugin.push(to_share, action, None, rule_name, None)
                if share_source_info
                else plugin.push(to_share, action)
            )
            if isinstance(result, PushResult) and not result.success:
                logger.error(
                    f"Failed to share unified mapping rows from mapping '{mapping_name}' "
                    f"for rule '{rule_name}' to '{destination_config_name}'. "
                    f"{result.message}",
                    error_code="CTE_1102",
                )
                update_cre_entity_indicator_status(
                    to_share, source_name, destination_config_name, "failed"
                )
                if generate_alert:
                    _generate_cre_entity_alerts(
                        to_share,
                        source_name,
                        destination_config_name,
                        "failed",
                        alert_meta,
                    )
                failed_values.update(indicator.value for indicator in to_share)
                all_actions_success = False
            else:
                # A successful push can still have dropped indicators: report
                # and record only what the plugin actually accepted.
                shared_iocs, failed_iocs, skipped_iocs = _split_cre_push_outcome(
                    to_share, result
                )
                if skipped_iocs:
                    logger.info(
                        f"Skipped {len(skipped_iocs)} indicator(s) built from "
                        f"unified mapping '{mapping_name}' while sharing rule "
                        f"'{rule_name}' to '{destination_config_name}'. These "
                        f"indicators were discarded by the plugin."
                    )
                if failed_iocs:
                    logger.error(
                        f"Could not share {len(failed_iocs)} indicator(s) built "
                        f"from unified mapping '{mapping_name}' for rule "
                        f"'{rule_name}' to '{destination_config_name}'.",
                        error_code="CTE_1102",
                    )
                logger.info(
                    f"Shared {len(shared_iocs)} indicators built from unified "
                    f"mapping '{mapping_name}' rows for rule '{rule_name}' to "
                    f"'{destination_config_name}'."
                )
                if shared_iocs:
                    update_cre_entity_indicator_status(
                        shared_iocs, source_name, destination_config_name, "shared"
                    )
                if failed_iocs:
                    update_cre_entity_indicator_status(
                        failed_iocs, source_name, destination_config_name, "failed"
                    )
                if generate_alert:
                    # Skipped indicators get no alert, mirroring the Threat
                    # Indicators flow (_generate_alerts_for_action).
                    if shared_iocs:
                        _generate_cre_entity_alerts(
                            shared_iocs,
                            source_name,
                            destination_config_name,
                            "shared",
                            alert_meta,
                        )
                    if failed_iocs:
                        _generate_cre_entity_alerts(
                            failed_iocs,
                            source_name,
                            destination_config_name,
                            "failed",
                            alert_meta,
                        )
                delivered_values.update(
                    indicator.value for indicator in shared_iocs
                )
                failed_values.update(indicator.value for indicator in failed_iocs)
                discarded_values.update(
                    indicator.value for indicator in skipped_iocs
                )
                if plugin.metadata.get("netskope", False) or (
                    isinstance(result, PushResult) and result.should_run_cleanup
                ):
                    is_run_action_cleanup = True
            _update_storage(destination_config_name, plugin.storage)
        except Exception:
            logger.error(
                f"Error sharing unified mapping rows for rule '{rule_name}' to "
                f"'{destination_config_name}'.",
                error_code="CTE_1102",
                details=traceback.format_exc(),
            )
            update_cre_entity_indicator_status(
                to_share, source_name, destination_config_name, "failed"
            )
            if generate_alert:
                _generate_cre_entity_alerts(
                    to_share,
                    source_name,
                    destination_config_name,
                    "failed",
                    alert_meta,
                )
            failed_values.update(indicator.value for indicator in to_share)
            all_actions_success = False
    # Nothing an action delivered is touched; what every action discarded is
    # removed outright, since a unified mapping indicator only exists to
    # represent a share (a value that also failed is kept as "failed").
    _cre_discard_indicators(
        discarded_values - delivered_values - failed_values,
        source_name,
        destination_config_name,
    )
    _cre_unshare_destination(
        (failed_values | discarded_values) - delivered_values,
        destination_config_name,
    )
    if advance_checkpoint and all_actions_success:
        # A whole-push failure leaves the checkpoint so the same window
        # retries next cycle; per-value failed_iocs are not retried by
        # checkpoint (matches creShare behavior).
        connector.collection(Collections.UNIFIED_MAPPING_RULES).update_one(
            {"name": rule_name},
            {"$set": {f"lastPerformed.{destination_config_name}": cycle_start}},
        )
    return is_run_action_cleanup, all_actions_success


def share_unified_mapping_rules(
    destination_config_name, plugin, rule_provided=None, lastseen=None
):
    """Share all unified mapping rules whose ``cteShare`` targets this destination.

    Used by the manual-sync drain in ``share_indicators``. A rule whose mapping no
    longer exists is logged and skipped (no HTTPException in tasks).

    Args:
        destination_config_name (str): CTE destination configuration name.
        plugin: Instantiated destination plugin.
        rule_provided (Optional[str]): Limit to a single rule (manual sync).
        lastseen (Optional[int]): Days window for a manual sync; ``None`` = all.

    Returns:
        tuple[bool, Optional[bool]]: ``(run_action_cleanup, success)`` —
            whether any Netskope-style action cleanup should run, and whether
            every rule shared without failures. ``success`` is ``None`` when
            the CRE module is disabled: nothing ran, so the caller should
            skip ``end_life`` rather than record a vacuous success — same as
            the CRE-entity manual-sync path never calls ``end_life`` on its
            own module-disabled skip (see ``share_cre_entity_rules`` above).
    """
    query = {
        "muted": False,
        f"cteShare.{destination_config_name}": {"$exists": True},
    }
    if rule_provided:
        query["name"] = rule_provided
    settings_doc = connector.collection(Collections.SETTINGS).find_one({}) or {}
    if not settings_doc.get("platforms", {}).get("cre", False):
        if connector.collection(Collections.UNIFIED_MAPPING_RULES).find_one(query):
            logger.debug(
                f"Skipped sharing for unified mapping rules to configuration "
                f"'{destination_config_name}' because the CRE module, whose "
                "entities are used in unified mapping, is disabled. Enable "
                "the CRE module to resume sharing these rules."
            )
        return False, None
    settings = SettingsDB(**settings_doc)
    is_run_action_cleanup = False
    all_rules_success = True
    module_alerts_on = bool(settings.cte and settings.cte.generateAlerts)
    cto_on = cto_alerts_enabled(settings)
    alerts_enabled = module_alerts_on and cto_on
    cto_skip_pending = module_alerts_on and not cto_on
    for rule_doc in connector.collection(Collections.UNIFIED_MAPPING_RULES).find(query):
        actions = (rule_doc.get("cteShare") or {}).get(destination_config_name, [])
        if not actions:
            continue
        if cto_skip_pending and _any_action_alerts(actions):
            log_cto_alerts_skipped(
                f"unified mapping rows shared to '{destination_config_name}'"
            )
            cto_skip_pending = False
        mapping_doc = connector.collection(Collections.UNIFIED_MAPPING).find_one(
            {"name": rule_doc["view"]}
        )
        if mapping_doc is None:
            logger.error(
                f"Skipping unified mapping rule '{rule_doc['name']}': unified "
                f"mapping '{rule_doc['view']}' no longer exists.",
                error_code="CTE_1108",
            )
            continue
        should_run_cleanup, rule_success = share_unified_mapping_records(
            destination_config_name,
            plugin,
            rule_doc,
            actions,
            mapping_doc,
            lastseen=lastseen,
            advance_checkpoint=lastseen is None,
            alerts_enabled=alerts_enabled,
        )
        is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
        all_rules_success = all_rules_success and rule_success
    return is_run_action_cleanup, all_rules_success


@APP.task(name="cte.um_share_indicators", acks_late=False)
@integration("cte")
@track()
def um_share_indicators(
    view: str,
    rule: Optional[str] = None,
    lastseen: Optional[int] = None,
):
    """Share unified mapping rows for every rule referencing a mapping.

    Triggered by the per-mapping ``UM Share cte.<mapping>`` schedule created with the
    unified mapping (interval = the mapping's ``syncInterval``). For each unmuted
    rule referencing the mapping, shares the qualifying joined rows to every
    destination in the rule's ``cteShare``. It does NOT pull indicators, does
    not touch the TI/CRE share flows, and does not perform a rule's
    ``creActions`` — those run under the mapping's own
    ``cre.um_evaluate_records`` schedule so either module can be disabled
    independently.

    Args:
        view (str): Unified mapping name (also the beat-lock key).
        rule (Optional[str]): Limit to a single rule.
        lastseen (Optional[int]): Days window override; ``None`` = checkpoint.
    """
    mapping_doc = connector.collection(Collections.UNIFIED_MAPPING).find_one(
        {"name": view}
    )
    if mapping_doc is None:
        logger.error(
            f"Could not run unified mapping sharing; unified mapping '{view}' "
            "does not exist.",
            error_code="CTE_1108",
        )
        return
    success = True
    skip_bookkeeping = False
    try:
        settings_doc = connector.collection(Collections.SETTINGS).find_one({}) or {}
        if not settings_doc.get("platforms", {}).get("cre", False):
            logger.debug(
                f"Skipped unified mapping sharing for mapping '{view}' because "
                "the CRE module, whose entities are used in unified mapping, "
                "is disabled. Enable the CRE module to resume sharing."
            )
            skip_bookkeeping = True
            return
        settings = SettingsDB(**settings_doc)
        rule_query = {"view": view, "muted": False}
        if rule:
            rule_query["name"] = rule
        module_alerts_on = bool(settings.cte and settings.cte.generateAlerts)
        cto_on = cto_alerts_enabled(settings)
        alerts_enabled = module_alerts_on and cto_on
        cto_skip_pending = module_alerts_on and not cto_on
        for rule_doc in connector.collection(Collections.UNIFIED_MAPPING_RULES).find(
            rule_query
        ):
            cte_share = rule_doc.get("cteShare") or {}
            if not cte_share:
                # A rule may legitimately be CRE-action-only; its actions run
                # under the mapping's separate cre.um_evaluate_records schedule.
                logger.debug(
                    f"Skipping unified mapping rule '{rule_doc['name']}': no CTE "
                    "sharing configured."
                )
                continue
            for destination_config_name, actions in cte_share.items():
                # Update lockedAt field so that task will not exceed max lock wait time.
                connector.collection(Collections.UNIFIED_MAPPING).update_one(
                    {"name": view},
                    {"$set": {"lockedAt.share": datetime.now()}},
                )
                if not actions:
                    continue
                if cto_skip_pending and _any_action_alerts(actions):
                    log_cto_alerts_skipped(f"unified mapping '{view}'")
                    cto_skip_pending = False
                configuration_dict = connector.collection(
                    Collections.CONFIGURATIONS
                ).find_one({"name": destination_config_name})
                if configuration_dict is None:
                    logger.error(
                        f"Could not share unified mapping records with configuration "
                        f"'{destination_config_name}'; it does not exist.",
                        error_code="CTE_1008",
                    )
                    continue
                configuration = ConfigurationDB(**configuration_dict)
                if not configuration.active:
                    logger.debug(
                        f"Configuration '{destination_config_name}' is disabled; "
                        "unified mapping sharing skipped."
                    )
                    continue
                Plugin = helper.find_by_id(configuration.plugin)  # NOSONAR S117
                if Plugin is None:
                    logger.error(
                        f"Could not share unified mapping records with configuration "
                        f"'{destination_config_name}'; plugin with "
                        f"id='{configuration.plugin}' does not exist.",
                        error_code="CTE_1009",
                    )
                    continue
                plugin = Plugin(
                    configuration.name,
                    SecretDict(configuration.parameters),
                    configuration.storage,
                    configuration.checkpoint,
                    logger,
                    ssl_validation=configuration.sslValidation,
                )
                should_run_cleanup, dest_success = share_unified_mapping_records(
                    destination_config_name,
                    plugin,
                    rule_doc,
                    actions,
                    mapping_doc,
                    lastseen=lastseen,
                    alerts_enabled=alerts_enabled,
                )
                # A pipeline or push failure must surface on the mapping's
                # lastRunSuccess, not just in the logs.
                success = success and dest_success
                end_life(destination_config_name, dest_success)
                if should_run_cleanup:
                    plugin.run_action_cleanup()
                    _update_storage(configuration.name, plugin.storage)
        logger.info(
            f"Completed unified mapping sharing for the mapping '{view}'."
        )
    except Exception:
        success = False
        logger.error(
            f"Error occurred while sharing unified mapping records for mapping "
            f"'{view}'.",
            details=traceback.format_exc(),
            error_code="CTE_1109",
        )
    finally:
        # Analog of end_life, but targeting the mapping document. The beat lock
        # (lockedAt.share) is released by the @track decorator. Skipped for a
        # clean CRE-disabled skip (see skip_bookkeeping above); still runs for
        # every genuine error, including one raised while reading settings.
        if not skip_bookkeeping:
            connector.collection(Collections.UNIFIED_MAPPING).update_one(
                {"name": view},
                {"$set": {"lastRunAt": datetime.now(), "lastRunSuccess": success}},
            )


@APP.task(name="cte.share_indicators")
@integration("cte")
@track()
def share_indicators(
    source_config_name: Optional[str] = None,
    destination_config_name: Optional[str] = None,
    rule: Optional[str] = None,
    action: Optional[Dict] = {},
    lastseen: Optional[int] = None,
    indicators: Optional[List] = None,
    share_new_indicators: Optional[bool] = False,
):
    """Evaluate business rules and push iocs.

    Args:
        rule (Optional[str], optional): Specific business rule to evaluate.
        All rules if None. Defaults to None.
        source_config (Optional[str], optional): Name of the source configuration.
        Defaults to None.
        destination_config_name (Optional[str], optional): Name of the destination configuration.
        Defaults to None.
        action (Optional[Dict], optional): Name of a specific action to
        perform. Defaults to None.
        lastseen (Optional[datetime], optional): Evaluate only on the
        indicators appears lastseen after. Defaults to None.
    """
    try:
        if indicators:
            possible_destinations = get_possible_destinations(source_config_name)
            connector.collection(Collections.INDICATORS).update_many(
                {"value": {"$in": indicators}},
                {
                    "$set": {
                        "sources.$[elem].destinations": possible_destinations
                    }
                },
                array_filters=[
                    {"elem.source": source_config_name},
                ]
            )
            logger.info(
                f"{len(indicators)} indicators from the source configuration named '{source_config_name}' "
                "will be shared in the next sharing cycle with possible destinations."
            )
            return True
        destination_config = connector.collection(Collections.CONFIGURATIONS).find_one(
            {"name": destination_config_name}
        )
        is_run_action_cleanup = False
        # CRE-entity and unified mapping manual syncs are handled separately
        # (after the plugin is built) since they push records, not indicators.
        cre_manual_syncs = []
        um_manual_syncs = []
        if destination_config and share_new_indicators:
            # Run pending historical tasks
            destination_config_model = ConfigurationDB(**destination_config)
            # Resolve the entity of every queued rule in a single query rather
            # than one find_one per manual-sync entry.
            rule_names = [
                ms.rule for ms in destination_config_model.manualSync
                if ms.ruleType != "unified_mapping"
            ]
            synced_rules_by_name = {
                doc["name"]: doc
                for doc in connector.collection(
                    Collections.CTE_BUSINESS_RULES
                ).find({"name": {"$in": rule_names}})
            } if rule_names else {}
            for manual_sync_config in destination_config_model.manualSync:
                # Unified mapping entries resolve against UNIFIED_MAPPING_RULES, not
                # CTE business rules — route them before the CTE rule lookup.
                if manual_sync_config.ruleType == "unified_mapping":
                    um_manual_syncs.append(manual_sync_config)
                    continue
                synced_rule = synced_rules_by_name.get(manual_sync_config.rule)
                if synced_rule is None:
                    # Rule was deleted after the sync was queued; drop the stale
                    # entry instead of falling through to share_iocs(source=None),
                    # which would fan out to every source configuration.
                    logger.info(
                        f"Skipping queued manual sync for rule "
                        f"'{manual_sync_config.rule}' on destination "
                        f"'{destination_config_name}': rule no longer exists."
                    )
                    continue
                if (
                    synced_rule.get("entity", THREAT_INDICATORS_ENTITY)
                    != THREAT_INDICATORS_ENTITY
                ):
                    cre_manual_syncs.append(manual_sync_config)
                    continue
                should_run_cleanup = share_iocs(
                    source_config_name=manual_sync_config.source,
                    destination_config_name=destination_config_name,
                    rule_provided=manual_sync_config.rule,
                    action=manual_sync_config.action,
                    lastseen=manual_sync_config.lastseen
                )
                connector.collection(Collections.CONFIGURATIONS).update_one(
                    {"name": destination_config_name},
                    {
                        "$set": {
                            "lockedAt.share": datetime.now(),
                        }
                    }
                )
                is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
            connector.collection(Collections.CONFIGURATIONS).update_one(
                {"name": destination_config_name}, {"$set": {"manualSync": []}}
            )
        # Run task either for maintenance window or queued manual sync task or any indicator insert/update through APIs.
        should_run_cleanup = share_iocs(
            source_config_name, destination_config_name, rule, action, lastseen, indicators, share_new_indicators
        )
        is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
        # create plugin class
        configuration = ConfigurationDB(**destination_config)
        Plugin = helper.find_by_id(configuration.plugin)  # NOSONAR S117
        if Plugin is None:
            logger.error(
                f"Could not share indicators with configuration "
                f"'{configuration.name}'; plugin with "
                f"id='{configuration.plugin}' does not exist.",
                error_code="CTE_1009",
            )
        plugin = Plugin(
            configuration.name,
            SecretDict(configuration.parameters),
            configuration.storage,
            configuration.checkpoint,
            logger,
            ssl_validation=configuration.sslValidation,
        )
        # CRE-entity sharing: push CRE records (qualified by the rule, mapped to
        # IOCs) to this destination. Separate from the indicator status machine.
        if Plugin is not None:
            if share_new_indicators:
                should_run_cleanup = share_cre_entity_rules(
                    destination_config_name, plugin
                )
                is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
            for manual_sync_config in cre_manual_syncs:
                should_run_cleanup = share_cre_entity_rules(
                    destination_config_name,
                    plugin,
                    rule_provided=manual_sync_config.rule,
                    lastseen=manual_sync_config.lastseen,
                )
                is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
            for manual_sync_config in um_manual_syncs:
                should_run_cleanup, um_success = share_unified_mapping_rules(
                    destination_config_name,
                    plugin,
                    rule_provided=manual_sync_config.rule,
                    lastseen=manual_sync_config.lastseen,
                )
                # um_success is None when the CRE module is disabled — nothing
                # ran, so don't record a vacuous success via end_life.
                if um_success is not None:
                    end_life(destination_config_name, um_success)
                is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
        logger.info(
            f"Completed Sharing of indicators for the configuration '{destination_config_name}'."
        )
        settings = SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
        if share_new_indicators and settings.cte and settings.cte.iocRetraction:
            if not plugin.metadata.get("delete_supported", False):
                logger.info(
                    f"Destination configuration with name '{configuration.name}' "
                    f"doesn't support deletion of retracted indicators."
                )
            else:
                should_run_cleanup = cte_retract_indicators(
                    destination_config_name
                )
                is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
        if is_run_action_cleanup:
            plugin.run_action_cleanup()
            _update_storage(configuration.name, plugin.storage)

    except Exception:
        logger.error(
            f"Error occurred while sharing indicators to configuration "
            f"'{destination_config_name}'.",
            details=traceback.format_exc(),
            error_code="CTE_1012",
        )


def share_iocs(
    source_config_name: Optional[str] = None,
    destination_config_name: Optional[str] = None,
    rule_provided: Optional[str] = None,
    action: Optional[Dict] = {},
    lastseen: Optional[int] = None,
    indicators: Optional[List] = None,
    share_new_indicators: Optional[bool] = False,
    is_run_action_cleanup: Optional[bool] = False,
    is_retraction_call: Optional[bool] = False
):
    """Evaluate business rules and push iocs.

    Args:
        rule (Optional[str], optional): Specific business rule to evaluate.
        All rules if None. Defaults to None.
        source_config (Optional[str], optional): Name of the source configuration.
        Defaults to None.
        destination_config_name (Optional[str], optional): Name of the destination configuration.
        Defaults to None.
        action (Optional[Dict], optional): Name of a specific action to
        perform. Defaults to None.
        lastseen (Optional[datetime], optional): Evaluate only on the
        indicators appears lastseen after. Defaults to None.
    """
    source_configs_list = (
        [source_config_name]
        if source_config_name
        else list(
            connector.collection(Collections.CONFIGURATIONS).distinct(
                "name",
                {"name": {"$nin": [destination_config_name]}}
            )
        )
    )
    destination_dict = {}
    actions = []

    # Global kill-switch for CTE -> CTO alert generation. Status bookkeeping
    # (including the No Action "shared"/sharedWith finalization) is unaffected;
    # only alert emission is suppressed when the toggle is off.
    settings = SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
    module_alerts_on = bool(settings.cte and settings.cte.generateAlerts)
    cto_on = cto_alerts_enabled(settings)
    alerts_enabled = module_alerts_on and cto_on
    cto_skip_pending = module_alerts_on and not cto_on

    for source_config_name in source_configs_list:
        failed_iocs_by_destination = {}
        evaluated_destinations = set()
        # Update LockedAt field so that task will not exceed max lock wait time.
        connector.collection(Collections.CONFIGURATIONS).update_one(
            {"name": destination_config_name},
            {
                "$set": {
                    "lockedAt.share": datetime.now(),
                }
            }
        )
        # Only update status when maintenance sharing running.
        pending_count = 0
        if share_new_indicators:
            pending_count = connector.collection(Collections.INDICATORS).count_documents(
                {
                    "sources": {
                        "$elemMatch": {
                            "source": source_config_name,
                            "destinations": {
                                "$elemMatch": {
                                    "name": destination_config_name,
                                    "status": "pending"
                                }
                            }
                        }
                    }
                }
            )
            if not pending_count:
                logger.info(
                    f"No indicators to share from source configuration '{source_config_name}' "
                    f"to destination configuration '{destination_config_name}'."
                )

        for rule in connector.collection(Collections.CTE_BUSINESS_RULES).find(
            {"name": rule_provided} if rule_provided else {"muted": False}
        ):
            rule = BusinessRuleDB(**rule)
            sharedWith = rule.sharedWith
            # if destination plugin is configured then only we will share the indicators.
            if not (sharedWith and sharedWith.get(source_config_name, {})):
                continue
            if source_config_name not in sharedWith.keys():
                # Skip if source configuration not found in business rule
                continue
            if destination_config_name is None:
                destination_dict = sharedWith.get(source_config_name, {})
            else:
                source_dict = sharedWith.get(source_config_name, {})
                destination_dict[destination_config_name] = source_dict.get(
                    destination_config_name, []
                )
            if (
                share_new_indicators
                and destination_config_name in destination_dict.keys()
                and not destination_dict[destination_config_name]
            ):
                continue
            if share_new_indicators and not pending_count:
                end_life(destination_config_name, True)
                continue
            logger.debug(
                f"Sharing indicators to all the configured destinations for the '{source_config_name}' configuration, "
                f"using the business rule '{rule.name}'."
            )
            query = {"$and": []}
            query_inactive = {"$and": []}
            for mute in rule.exceptions:  # exclude iocs matching the mute rule
                if mute.filters:
                    mute_query = json.loads(
                        mute.filters.mongo,
                        object_hook=lambda pair: parse_dates(pair),
                    )
                    query["$nor"] = query.get("$nor", []) + [mute_query]
                if mute.tags:
                    query['$and'].append({"sources": {"$elemMatch": {"tags": {"$nin": mute.tags}}}})
            for config, actions_list in destination_dict.items():
                evaluated_destinations.add(config)
                # TODO for actions
                configuration_dict = connector.collection(
                    Collections.CONFIGURATIONS
                ).find_one({"name": config})
                if configuration_dict is None:
                    # configuration does not exist anymore
                    logger.error(
                        f"Could not share indicators with configuration "
                        f"'{config}'; it does not exist.",
                        error_code="CTE_1008",
                    )
                    continue
                configuration = ConfigurationDB(**configuration_dict)
                if not configuration.active:  # If plugin is disabled
                    logger.debug(f"Configuration '{config}' is disabled; sharing skipped.")
                    continue
                Plugin = helper.find_by_id(configuration.plugin)  # NOSONAR S117
                if Plugin is None:
                    logger.error(
                        f"Could not share indicators with configuration "
                        f"'{config}'; plugin with "
                        f"id='{configuration.plugin}' does not exist.",
                        error_code="CTE_1009",
                    )
                    continue
                generate_alert = False
                alert_query = None
                alert_meta = None
                try:
                    logger.info(
                        f"Indicator sharing has been initiated from '{source_config_name}' to '{config}' "
                        f"using the business rule '{rule.name}'."
                    )
                    plugin = Plugin(
                        configuration.name,
                        SecretDict(configuration.parameters),
                        configuration.storage,
                        configuration.checkpoint,
                        logger,
                        ssl_validation=configuration.sslValidation,
                    )

                    # Build base query conditions (mute rules and business rule filters)
                    base_query_conditions = [
                        # add if any previous query
                        *query["$and"],
                        # apply the usual filters
                        _load_mongo_filters(rule.filters.mongo),
                    ]

                    # Add lastseen filter to base conditions if provided
                    if lastseen:
                        base_query_conditions.append(
                            {
                                "sources": {
                                    "$elemMatch": {
                                        "lastSeen": {
                                            "$gt": datetime.now()
                                            - timedelta(days=lastseen)
                                        }
                                    }
                                }
                            }
                        )

                    # Build query for inactive count (doesn't need patch_supported)
                    query_inactive["$and"] = [
                        *base_query_conditions,
                        # only from the configuration that is configured to share with us
                        {
                            "sources": {
                                "$elemMatch": {
                                    "source": source_config_name
                                }
                            }
                        },
                        # only inactive
                        {"active": False},
                    ]

                    # Build preliminary query to check if any documents exist (without patch_supported)
                    preliminary_query = {"$and": [
                        *base_query_conditions,
                        {
                            "sources": {
                                "$elemMatch": {
                                    "source": source_config_name,
                                    "$or": [{"retracted": False}, {"retracted": {"$exists": False}}],
                                }
                            }
                        },
                        {"active": True},
                    ]}

                    total_inactive = list(
                        connector.collection(Collections.INDICATORS).aggregate(
                            [
                                {"$match": query_inactive},
                                {"$group": {"_id": None, "count": {"$sum": 1}}},
                            ],
                            allowDiskUse=True,
                        )
                    )

                    # Get preliminary count to decide if we should enter action loop
                    count_documents = connector.collection(Collections.INDICATORS).count_documents(
                        preliminary_query
                    )
                    action_run_success = True
                    failed_iocs = []
                    if count_documents > 0 or is_retraction_call:
                        if not action:
                            actions = actions_list.copy()
                        else:
                            actions.append(action)
                        for action_dict in actions:
                            # Get action value - works for both Action object and dict
                            if isinstance(action_dict, Action):
                                action_value = action_dict.value
                            else:
                                action_value = action_dict.get('value')
                            is_no_action = action_value == CTE_NO_ACTION_VALUE
                            # Alerts are not regenerated for retraction-triggered
                            # re-shares; the indicators were already alerted when
                            # they were first shared. The global generateAlerts
                            # toggle suppresses emission entirely when off.
                            wants_alert = bool(
                                _action_field(action_dict, "generateAlert", False)
                            ) and not is_retraction_call
                            generate_alert = wants_alert and alerts_enabled
                            if cto_skip_pending and wants_alert:
                                log_cto_alerts_skipped(
                                    f"indicators shared to '{config}'"
                                )
                                cto_skip_pending = False
                            alert_query = None
                            alert_meta = None
                            # Get action-level patch_supported from plugin's get_actions()
                            action_patch_supported = plugin.metadata.get("patch_supported", False)
                            if is_no_action:
                                # Forced so only newly qualified ("inprogress")
                                # indicators are processed each maintenance
                                # cycle instead of the full matching set.
                                action_patch_supported = True
                            elif hasattr(plugin, 'get_actions'):
                                for plugin_action in plugin.get_actions():
                                    if plugin_action.value == action_value:
                                        # Use action's patch_supported if set, otherwise fall back to plugin metadata
                                        action_patch_supported = (
                                            plugin_action.patch_supported
                                            if plugin_action.patch_supported is not None
                                            else plugin.metadata.get('patch_supported', False)
                                        )
                                        break

                            # Build action-specific query once based on action's patch_supported
                            action_query = {"$and": [
                                *base_query_conditions,
                                # only from the configuration that is configured to share with us
                                (
                                    {
                                        "sources": {
                                            "$elemMatch": {
                                                "source": source_config_name,
                                                "$or": [{"retracted": False}, {"retracted": {"$exists": False}}],
                                                "destinations": {
                                                    "$elemMatch": {
                                                        "name": destination_config_name,
                                                        "status": "inprogress"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if share_new_indicators and action_patch_supported
                                    else {
                                        "sources": {
                                            "$elemMatch": {
                                                "source": source_config_name,
                                                "$or": [{"retracted": False}, {"retracted": {"$exists": False}}],
                                            }
                                        }
                                    }
                                ),
                                {"active": True},
                            ]}

                            if generate_alert or is_no_action:
                                # Alerts are scoped to indicators newly promoted
                                # to "inprogress" this cycle, independent of the
                                # action's patch support. This prevents non-patch
                                # actions (whose action_query matches the entire
                                # qualifying set) from re-alerting already-shared
                                # indicators every maintenance cycle. The query is
                                # evaluated against live DB state after the push,
                                # where successful indicators are still
                                # "inprogress" (failed -> "failed", skipped ->
                                # "N/A"), so it captures exactly what was shared.
                                alert_query = {"$and": [
                                    *base_query_conditions,
                                    {
                                        "sources": {
                                            "$elemMatch": {
                                                "source": source_config_name,
                                                "$or": [
                                                    {"retracted": False},
                                                    {"retracted": {"$exists": False}},
                                                ],
                                                "destinations": {
                                                    "$elemMatch": {
                                                        "name": destination_config_name,
                                                        "status": "inprogress",
                                                    }
                                                },
                                            }
                                        }
                                    },
                                    {"active": True},
                                ]}

                            if share_new_indicators:
                                promote_query = {"$and": [
                                    *base_query_conditions,
                                    {
                                        "sources": {
                                            "$elemMatch": {
                                                "source": source_config_name,
                                                "$or": [{"retracted": False}, {"retracted": {"$exists": False}}],
                                                "destinations": {
                                                    "$elemMatch": {
                                                        "name": destination_config_name,
                                                        "status": "pending"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    {"active": True},
                                ]}
                                connector.collection(Collections.INDICATORS).update_many(
                                    promote_query,
                                    {
                                        "$set": {
                                            "sources.$[elem].destinations.$[dest].status": "inprogress"
                                        }
                                    },
                                    array_filters=[
                                        {"elem.source": source_config_name},
                                        {"dest.name": destination_config_name, "dest.status": "pending"}
                                    ]
                                )
                            else:
                                # Non-maintenance mode: promote existing destination entries
                                # matching the current rule to "inprogress" so the deferred
                                # sweep can mark them "shared" after all rules complete.
                                connector.collection(
                                    Collections.INDICATORS
                                ).update_many(
                                    {
                                        "$and": [
                                            *base_query_conditions,
                                            {
                                                "sources": {
                                                    "$elemMatch": {
                                                        "source": source_config_name,
                                                        "$or": [
                                                            {"retracted": False},
                                                            {
                                                                "retracted": {
                                                                    "$exists": False
                                                                }
                                                            },
                                                        ],
                                                        "destinations.name": destination_config_name,
                                                    }
                                                }
                                            },
                                            {"active": True},
                                        ]
                                    },
                                    {
                                        "$set": {
                                            "sources.$[elem].destinations.$[dest].status": "inprogress"
                                        }
                                    },
                                    array_filters=[
                                        {"elem.source": source_config_name},
                                        {"dest.name": destination_config_name},
                                    ],
                                )

                            # Use same action_query for both cursor and count
                            cursor = connector.collection(Collections.INDICATORS).aggregate(
                                [
                                    {"$match": action_query},
                                    {"$sort": {"sources.lastSeen": -1}},
                                ],
                                allowDiskUse=True,
                            )
                            length_ = connector.collection(Collections.INDICATORS).count_documents(
                                action_query
                            )

                            inactive_count = total_inactive[0].get("count", 0) if len(total_inactive) > 0 else 0
                            share_source_info = has_source_info_args(
                                plugin,
                                "push",
                                ["source", "business_rule", "plugin_name"]
                            )
                            plugin_name = None
                            if share_source_info or generate_alert:
                                source_config = connector.collection(Collections.CONFIGURATIONS).find_one(
                                    {"name": source_config_name}
                                )
                                PluginClass = helper.find_by_id(source_config.get("plugin"))
                                plugin_name = PluginClass.metadata.get("name", "")
                            if generate_alert or is_no_action:
                                alert_meta = {
                                    "rule_name": rule.name,
                                    "source_config_name": source_config_name,
                                    "destination_config_name": config,
                                    "source_plugin_name": plugin_name,
                                    "action_label": _action_field(
                                        action_dict, "label", action_value
                                    ),
                                }
                            if is_no_action:
                                # Core-level No Action target: nothing is
                                # pushed to the destination plugin; the
                                # qualified indicators are only used to
                                # generate alerts on CTO. The destination
                                # statuses are finalized by the existing
                                # bookkeeping ("inprogress" -> "shared").
                                alert_note = (
                                    "alerts will be generated"
                                    if generate_alert
                                    else "no alerts will be generated"
                                )
                                logger.info(
                                    f"{length_} qualified indicator(s) matched the "
                                    f"'No Action' target based on the rule name "
                                    f"'{rule.name}'; nothing is pushed to "
                                    f"'{config}' and {alert_note}."
                                )
                                # No push, so the back-fill validate_result_and_update
                                # normally does runs here: an indicator predating this
                                # destination has no entry for alert_query to match.
                                if (
                                    length_
                                    and destination_config_name
                                    and not share_new_indicators
                                ):
                                    connector.collection(
                                        Collections.INDICATORS
                                    ).update_many(
                                        {"$and": [
                                            *base_query_conditions,
                                            {
                                                "sources": {
                                                    "$elemMatch": {
                                                        "source": source_config_name,
                                                        "$or": [
                                                            {"retracted": False},
                                                            {
                                                                "retracted": {
                                                                    "$exists": False
                                                                }
                                                            },
                                                        ],
                                                        "destinations": {
                                                            "$not": {
                                                                "$elemMatch": {
                                                                    "name": destination_config_name
                                                                }
                                                            }
                                                        },
                                                    }
                                                }
                                            },
                                            {"active": True},
                                        ]},
                                        {
                                            "$push": {
                                                "sources.$[elem].destinations": {
                                                    "name": destination_config_name,
                                                    "status": "inprogress",
                                                }
                                            }
                                        },
                                        array_filters=[
                                            {"elem.source": source_config_name}
                                        ],
                                    )
                                if generate_alert and length_:
                                    _generate_alerts_for_action(
                                        alert_query, True, [], alert_meta
                                    )
                                # The destination status is finalized to "shared"
                                # by the post-loop sweep; record it in the
                                # top-level sharedWith too so both views agree and
                                # retraction promotes/finalizes the No Action
                                # retractionDestinations correctly (no plugin push
                                # happens, mirroring validate_result_and_update).
                                if length_:
                                    connector.collection(
                                        Collections.INDICATORS
                                    ).update_many(
                                        alert_query,
                                        {"$addToSet": {"sharedWith": config}},
                                    )
                                generate_alert = False
                                continue
                            logger.info(f"Sending total of {length_} qualified indicators based on the rule name"
                                        f" '{rule.name}' for sharing. {inactive_count} indicators are inactive.")

                            # Convert Action to dict only when passing to plugin.push()
                            if isinstance(action_dict, Action):
                                action_dict_for_push = action_dict.model_dump()
                            else:
                                action_dict_for_push = action_dict
                            (
                                validate_result,
                                should_run_action_cleanup,
                                action_failed_iocs,
                                action_skipped_iocs,
                            ) = validate_result_and_update(
                                configuration.name,
                                plugin.push(
                                    IndicatorGenerator(cursor, source_config_name).all(),
                                    action_dict_for_push,
                                    source_config_name,
                                    rule.name,
                                    plugin_name
                                ) if share_source_info else plugin.push(
                                    IndicatorGenerator(cursor, source_config_name).all(),
                                    action_dict_for_push
                                ),
                                filters=action_query,
                                source_config_name=source_config_name
                            )
                            failed_iocs.extend(action_failed_iocs)
                            #  Retrun True when IoCs are added to URLlist in Netskope CTE Plugin
                            if (
                                plugin.metadata.get("netskope", False)
                                and validate_result
                                and should_run_action_cleanup
                            ):
                                is_run_action_cleanup = True
                            # All action should performed successfully.
                            action_run_success = action_run_success and validate_result
                            # Update the storage; failure must not abort the status sweep.
                            try:
                                _update_storage(configuration.name, plugin.storage)
                            except Exception:
                                logger.warn(
                                    f"Failed to update storage for configuration "
                                    f"'{configuration.name}'.",
                                    details=traceback.format_exc(),
                                )
                            # Generate alerts independently of the storage update:
                            # _generate_alerts_for_action never raises, and clearing
                            # generate_alert here prevents the outer except from
                            # re-emitting these (already-handled) indicators as failed.
                            if generate_alert:
                                _generate_alerts_for_action(
                                    alert_query,
                                    validate_result,
                                    action_failed_iocs,
                                    alert_meta,
                                    skipped_iocs=action_skipped_iocs,
                                )
                                generate_alert = False
                        # mark all indicator as shared.
                        if destination_config_name and action_run_success:
                            if destination_config_name not in failed_iocs_by_destination:
                                failed_iocs_by_destination[destination_config_name] = failed_iocs
                            else:
                                failed_iocs_by_destination[destination_config_name].extend(failed_iocs)
                        elif share_new_indicators and not action_run_success:
                            # Push failed in maintenance mode — revert any promoted indicators
                            # from "inprogress" back to "pending" so the next maintenance
                            # cycle can retry them (prevents permanent "inprogress" stuck state).
                            connector.collection(Collections.INDICATORS).update_many(
                                {
                                    "$and": [
                                        *base_query_conditions,
                                        {
                                            "sources": {
                                                "$elemMatch": {
                                                    "source": source_config_name,
                                                    "destinations": {
                                                        "$elemMatch": {
                                                            "name": destination_config_name,
                                                            "status": "inprogress"
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {"active": True},
                                    ]
                                },
                                {
                                    "$set": {
                                        "sources.$[elem].destinations.$[dest].status": "pending"
                                    }
                                },
                                array_filters=[
                                    {"elem.source": source_config_name},
                                    {"dest.name": destination_config_name, "dest.status": "inprogress"}
                                ]
                            )
                    else:
                        logger.info(
                            f"No indicators from source {source_config_name} to share "
                            f"on destination {configuration.name}.",
                        )
                    end_life(destination_config_name, action_run_success)
                except NotImplementedError:
                    logger.error(
                        f"Could not share indicators with configuration "
                        f"'{configuration.name}'. Push method not implemented.",
                        details=traceback.format_exc(),
                        error_code="CTE_1010",
                    )
                    if generate_alert and alert_query:
                        # Best effort: the push crashed, alert the indicators
                        # it was given as failed.
                        _generate_alerts_for_action(
                            alert_query, False, [], alert_meta
                        )
                except Exception:
                    logger.error(
                        f"Error occurred while sharing indicators with configuration "
                        f"'{configuration.name}'.",
                        details=traceback.format_exc(),
                        error_code="CTE_1011",
                    )
                    if generate_alert and alert_query:
                        # Best effort: the push crashed, alert the indicators
                        # it was given as failed.
                        _generate_alerts_for_action(
                            alert_query, False, [], alert_meta
                        )
        # Sweep remaining "pending" entries to "N/A" for all evaluated destinations.
        # Only in maintenance mode: non-maintenance mode promotes all pending→inprogress
        # before pushing, so no pending entries remain for evaluated destinations after the push.
        if share_new_indicators:
            for dest_name in evaluated_destinations:
                connector.collection(Collections.INDICATORS).update_many(
                    {
                        "sources": {
                            "$elemMatch": {
                                "source": source_config_name,
                                "destinations": {
                                    "$elemMatch": {
                                        "name": dest_name,
                                        "status": "pending"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "$set": {
                            "sources.$[elem].destinations.$[dest].status": "N/A"
                        }
                    },
                    array_filters=[
                        {"elem.source": source_config_name},
                        {"dest.name": dest_name}
                    ]
                )

        for destination_name, list_of_failed_iocs in failed_iocs_by_destination.items():
            connector.collection(Collections.INDICATORS).update_many(
                {
                    "value": {"$nin": list(set(list_of_failed_iocs))},
                    "sources": {
                        "$elemMatch": {
                            "source": source_config_name,
                            "destinations": {
                                "$elemMatch": {
                                    "name": destination_name,
                                    "status": "inprogress"
                                }
                            }
                        }
                    }
                },
                {
                    "$set": {
                        "sources.$[elem].destinations.$[dest].status": "shared"
                    }
                },
                array_filters=[
                    {"elem.source": source_config_name},
                    {"dest.name": destination_name}
                ]
            )
    return is_run_action_cleanup


def get_all_actions_from_rule(
    destination_config_name: str = ""
):
    """Get all action configured for any destination plugin.

    Args:
        destination_config_name (str, optional): _description_. Defaults to "".

    Returns:
        _type_: _description_
    """
    actions = {}
    rules = connector.collection(Collections.CTE_BUSINESS_RULES).find(
        {"muted": False}
    )
    for rule in rules:
        rule = BusinessRuleDB(**rule)
        sharedWith = rule.sharedWith
        for source, destinations in sharedWith.items():
            if destination_config_name in destinations.keys():
                if source in actions:
                    actions[source].extend(destinations.get(destination_config_name, []))
                else:
                    actions[source] = destinations.get(destination_config_name, [])
    return actions


def cte_retract_indicators(
    destination_config_name: str = None,
):
    """Retract indicators for destination configuration.

    Args:
        destination_config_name (str): Name of the destination configuration.
    """
    try:
        configuration_dict = connector.collection(
            Collections.CONFIGURATIONS
        ).find_one({"name": destination_config_name})
        if configuration_dict is None:
            # configuration does not exist anymore
            logger.info(
                f"Could not share indicators with configuration "
                f"'{destination_config_name}'; it does not exist."
            )
            return
        configuration = ConfigurationDB(**configuration_dict)
        if not configuration.active:  # If plugin is disabled
            logger.debug(f"Configuration '{destination_config_name}' is disabled; IoC Retraction skipped.")
            return
        actions = get_all_actions_from_rule(configuration.name)
        if not actions:
            logger.info(
                f"Destination configuration with name '{configuration.name}' isn't added in sharing configurations. "
                f"Skipping IoC(s) Retraction."
            )
            return
        # CRE-sourced and unified-view-sourced indicators are not retractable;
        # exclude them from the retraction sweep via the flag
        # persist_cre_entity_indicators sets on every source entry it builds
        # (the only place either kind of derived entry is created).
        ioc_update_result = connector.collection(Collections.INDICATORS).update_many(
            {
                "sources": {
                    "$elemMatch": {
                        "retracted": True,
                        "derived": {"$ne": True},
                        "retractionDestinations": {
                            "$elemMatch": {
                                "name": configuration.name,
                                "status": "pending"
                            }
                        }
                    }
                }
            },
            {
                "$set": {
                    "sources.$[elm].retractionDestinations.$[dest].status": "inprogress"
                }
            },
            array_filters=[
                {"elm.retracted": True, "elm.derived": {"$ne": True}},
                {"dest.name": configuration.name}
            ]
        )
        if not ioc_update_result.modified_count:
            logger.info(
                f"No indicators to be retracted from destination configuration '{configuration.name}'."
            )
            return
        Plugin = helper.find_by_id(configuration.plugin)  # NOSONAR S117
        if Plugin is None:
            logger.error(
                f"Could not retract indicators from configuration "
                f"'{configuration.name}'; plugin with "
                f"id='{configuration.plugin}' does not exist.",
                error_code="CTE_1009",
            )
            return
        plugin = Plugin(
            configuration.name,
            SecretDict(configuration.parameters),
            configuration.storage,
            configuration.checkpoint,
            logger,
            ssl_validation=configuration.sslValidation,
        )

        # Get batch size from plugin if any else default
        retraction_batch = RETRACTION_IOC_BATCH_SIZE
        try:
            retraction_batch = plugin.retraction_batch
        except AttributeError:
            pass
        disabled_retraction = False
        is_run_action_cleanup = False
        failed_source_config_list = []
        for source_config_name, action_config_list in actions.items():
            # Process each action with its own patch_supported setting
            for action_dict in action_config_list:
                # Get action value - works for both Action object and dict
                if isinstance(action_dict, Action):
                    action_value = action_dict.value
                else:
                    action_value = action_dict.get('value')

                if action_value == CTE_NO_ACTION_VALUE:
                    # Nothing was pushed for the core-level No Action target,
                    # so there is nothing to retract; plugins must never
                    # receive this action. Its retractionDestinations are
                    # finalized by the status update below.
                    continue

                # Get action-level patch_supported from plugin's get_actions()
                action_patch_supported = plugin.metadata.get("patch_supported", False)
                if hasattr(plugin, 'get_actions'):
                    for plugin_action in plugin.get_actions():
                        if plugin_action.value == action_value:
                            # Use action's patch_supported if set, else plugin metadata
                            action_patch_supported = (
                                plugin_action.patch_supported
                                if plugin_action.patch_supported is not None
                                else plugin.metadata.get('patch_supported', False)
                            )
                            break

                # For plugins/actions which don't support patch (need full re-share)
                if not action_patch_supported:
                    should_run_cleanup = share_iocs(
                        destination_config_name=configuration.name,
                        is_retraction_call=True
                    )
                    is_run_action_cleanup = is_run_action_cleanup or should_run_cleanup
                    continue

                # For actions that support patch - retract specific indicators
                query = {}
                # Get all indicators which are retracted from source and shared with destination.
                query["$and"] = [
                    {
                        "sources": {
                            "$elemMatch": {
                                "source": source_config_name,
                                "retracted": True,
                                "retractionDestinations.name": configuration.name,
                                "retractionDestinations.status": "inprogress"
                            }
                        },
                        "sharedWith": {"$in": [configuration.name]}
                    }
                ]
                pipeline = [
                    {
                        "$facet": {
                            "filteredResult": [
                                {
                                    "$match": query
                                },
                                {
                                    "$group": {
                                        "_id": None,
                                        "filteredCount": {"$sum": 1},
                                    }
                                },
                            ],
                        }
                    }
                ]
                result = list(
                    connector.collection(Collections.INDICATORS).aggregate(
                        pipeline, allowDiskUse=True
                    )
                )
                count_matrix = result[0]
                count_documents = (
                    result[0]["filteredResult"][0]["filteredCount"]
                    if count_matrix["filteredResult"]
                    else 0
                )
                if count_documents > 0:
                    # get all indicators which needs to be retracted.
                    cursor = connector.collection(Collections.INDICATORS).aggregate(
                        [
                            {"$match": query},
                            {"$sort": {"sources.lastSeen": -1}},
                        ],
                        allowDiskUse=True,
                    )
                    batch_retraction_results = plugin.retract_indicators(
                        IndicatorGenerator(
                            cursor, source_config_name
                        ).all(batch_size=retraction_batch),
                        [action_dict]
                    )
                    success = True
                    for batch_result in batch_retraction_results:
                        if not isinstance(batch_result, ValidationResult):
                            logger.error(
                                f"Could not Retract indicators in batch for "
                                f"configuration '{configuration.name}'. "
                                "Invalid return type.",
                                error_code="CTE_1006",
                            )
                            success = False
                            break
                        if not batch_result.success:
                            success = False
                            if batch_result.disabled:
                                disabled_retraction = True
                                break
                            logger.error(
                                f"Could not retract indicators for configuration "
                                f"'{configuration.name}'. "
                                f"{re.sub(r'token=([0-9a-zA-Z]*)', 'token=********&', batch_result.message)}",
                                details=re.sub(
                                    r"token=([0-9a-zA-Z]*)",
                                    "token=********&",
                                    batch_result.message
                                ),
                                error_code="CTE_1007",
                            )
                            break
                    # Retraction failed.
                    if not success:
                        failed_source_config_list.append(source_config_name)
                        continue
                    logger.info(
                        f"Completed retraction of {count_documents} indicators "
                        f"for destination configuration '{configuration.name}', "
                        f"which are retracted from configuration "
                        f"'{source_config_name}'."
                    )
                else:
                    logger.info(
                        f"No retracted indicators found for source "
                        f"configuration '{source_config_name}'."
                    )
        # A plugin reporting `disabled` gates retraction in its own
        # configuration parameters, so the gate covers every source and every
        # promoted indicator for this destination: nothing was retracted, and
        # nothing failed either. Finalize all promoted entries as "N/A" and
        # skip the failed/retracted bookkeeping, which would otherwise consume
        # the same "inprogress" entries first and mark them "failed" (the
        # plugin reports `disabled` alongside `success=False`).
        if disabled_retraction:
            connector.collection(Collections.INDICATORS).update_many(
                {
                    "sources": {
                        "$elemMatch": {
                            "retracted": True,
                            "retractionDestinations": {
                                "$elemMatch": {
                                    "name": configuration.name,
                                    "status": "inprogress"
                                }
                            }
                        }
                    }
                },
                {
                    "$set": {
                        "sources.$[elm].retractionDestinations.$[dest].status": "N/A"
                    }
                },
                array_filters=[
                    {"elm.retracted": True},
                    {"dest.name": configuration.name}
                ]
            )
            logger.info(
                f"IoC(s) Retraction is disabled for destination configuration '{configuration.name}'. "
                "Added N/A as a retraction result."
            )
        else:
            source_list = list(set(actions.keys()))
            for src_name in source_list:
                connector.collection(Collections.INDICATORS).update_many(
                    {
                        "sources": {
                            "$elemMatch": {
                                "retracted": True,
                                "source": src_name,
                                "retractionDestinations": {
                                    "$elemMatch": {
                                        "name": configuration.name,
                                        "status": "inprogress"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "$set": {
                            "sources.$[elm].retractionDestinations.$[dest].status": (
                                "failed"
                                if src_name in failed_source_config_list
                                else "retracted"
                            )
                        }
                    },
                    array_filters=[
                        {"elm.source": src_name},
                        {"dest.name": configuration.name}
                    ]
                )
        # Persist any storage mutations made by the plugin during retraction
        # (e.g. by get_actions()/retract_indicators()) back to the DB. Done once
        # per plugin instance, after all retraction work for this configuration.
        _update_storage(configuration.name, plugin.storage)
    except Exception:
        logger.error(
            f"Error occurred while retracting indicators for configuration "
            f"'{destination_config_name}'.",
            details=traceback.format_exc(),
            error_code="CTE_1011",
        )
    # Their will not be any action cleanup since netskope is not there as destination.
    return False
