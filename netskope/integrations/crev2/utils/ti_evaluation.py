"""Queue CRE business-rule evaluation for Threat Indicator records."""

from typing import Iterable, Union

from bson import ObjectId
from netskope.common.utils import Collections, DBConnector

from netskope.common.celery.scheduler import execute_celery_task
from netskope.integrations.crev2.tasks.evaluate_records import evaluate_records

from . import THREAT_INDICATORS_ENTITY

connector = DBConnector()
RECORD_BATCH_SIZE = 1000


def has_active_ti_business_rules() -> bool:
    """Return True if any non-muted Threat Indicators rule has actions."""
    return (
        connector.collection(Collections.CREV2_BUSINESS_RULES).find_one(
            {
                "entity": THREAT_INDICATORS_ENTITY,
                "actions": {"$ne": {}},
                "muted": False,
            },
            {"_id": True},
        )
        is not None
    )


def queue_threat_indicator_evaluation(
    indicator_ids: Iterable[Union[ObjectId, str]],
) -> None:
    """Queue cre.evaluate_records for ingested/updated indicator documents."""
    ids = list(indicator_ids)
    if not ids or not has_active_ti_business_rules():
        return

    for offset in range(0, len(ids), RECORD_BATCH_SIZE):
        batch = ids[offset: offset + RECORD_BATCH_SIZE]
        execute_celery_task(
            evaluate_records.apply_async,
            "cre.evaluate_records",
            args=[THREAT_INDICATORS_ENTITY, batch],
        )
