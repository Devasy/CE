"""Unmute unified mapping business rule task."""
from __future__ import absolute_import, unicode_literals
from datetime import datetime

from netskope.common.celery.main import APP
from netskope.common.utils import (
    DBConnector,
    Collections,
    PrefixedLogger,
    track,
)


connector = DBConnector()
logger = PrefixedLogger("[Unified Mapping]")


@APP.task(name="common.unmute_unified_mapping")
@track()
def unmute_unified_mapping_rule():
    """Unmute all the due unified mapping business rules.

    Triggered by the "UNIFIED MAPPING INTERNAL UNMUTE TASK" schedule every five
    minutes. It only clears expired mutes - it never shares indicators or
    performs CRE actions; those run under the mapping's own
    ``cte.um_share_indicators`` and ``cre.um_evaluate_records`` schedules, which
    are module-gated in their own right.

    Deliberately named ``common.*`` rather than ``cte.*``: a unified mapping rule
    may carry only ``creActions``, and the beat scheduler derives the module gate
    from the task-name prefix (``is_enabled`` in ``celery/scheduler.py``). Under a
    ``cte.`` name a CRE-action-only rule would stay muted forever on a tenant
    with the CTE module turned off.
    """
    current_time = datetime.now()
    update_result = connector.collection(
        Collections.UNIFIED_MAPPING_RULES
    ).update_many(
        {"unmuteAt": {"$ne": None, "$lte": current_time}, "muted": True},
        {"$set": {"muted": False, "unmuteAt": None}},
    )
    if update_result.modified_count > 0:
        logger.debug(
            f"Unmuted {update_result.modified_count} unified mapping "
            "business rule(s)."
        )
    return update_result.modified_count
