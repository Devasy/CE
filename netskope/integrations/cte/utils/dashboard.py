"""Dashboard helpers for the CTE module.

CRE-sourced indicators live in the CTE ``indicators`` collection under a synthetic
source label ``cre_<entity>_<ruleName>``. That label is deliberately never parsed
back into its parts (rule names may contain ``_`` and ``/``); build it with
``cre_source_name`` and match it by prefix. See
``netskope.integrations.cte.utils.entity`` for the label convention.

Unified mapping sharing does the same with ``[Unified Mapping] <mappingName>``.
Both labels are *derived* — no CTE plugin configuration exists under either — so
the "Pulled IoCs" widget excludes both (``derived_source_regex``) and each gets
its own dashboard tab instead.
"""

import re
from typing import Dict

from netskope.integrations.cte.utils.entity import (
    CRE_SOURCE_PREFIX,
    UM_SOURCE_PREFIX,
)

# Destination statuses a CRE-entity indicator's ``sources[].destinations[]`` entry
# can carry, used to seed the status counts so every real status is always keyed.
#
# ``pending`` is excluded: the CRE-entity share flow writes ``inprogress`` up front
# (``persist_cre_entity_indicators``) then advances to ``shared``/``failed``
# (``update_cre_entity_indicator_status``), and every backend path that writes
# ``pending`` is keyed to a plugin source configuration, so a ``cre_*`` source
# entry never reaches it. ``N/A`` is only ever used for ``retractionDestinations``,
# and CRE-sourced indicators are not retractable.
#
# The sharing endpoint still accumulates any status it actually finds, so an
# unexpected value would be reported rather than dropped.
DESTINATION_STATUSES = ("inprogress", "shared", "failed")


def derived_source_regex() -> Dict:
    """Mongo regex matching any derived (non-plugin-pulled) source label.

    Both CRE-entity sharing and unified mapping sharing attribute the indicators
    they build to a synthetic label that is not a CTE plugin configuration, so
    neither belongs on the "Pulled IoCs" widget — it would render them as
    phantom plugins.

    Deliberately the only exclusion helper: a per-flow one (``^cre_`` alone) is
    what let unified mapping sources leak onto that widget, so there is no
    single-prefix variant to reach for by mistake.

    Returns:
        Dict: A single anchored alternation over both prefixes, so one ``$not``
            excludes both. ``re.escape`` matters — ``UM_SOURCE_PREFIX`` contains
            ``[`` and ``]``, which unescaped read as a character class and match
            nothing.
    """
    prefixes = "|".join(
        re.escape(prefix) for prefix in (CRE_SOURCE_PREFIX, UM_SOURCE_PREFIX)
    )
    return {"$regex": f"^({prefixes})"}
