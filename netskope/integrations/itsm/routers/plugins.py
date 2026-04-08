"""Plugin related endpoints."""

from fastapi import APIRouter, Security

from netskope.common.models import User
from netskope.common.api.routers.auth import get_current_user
from netskope.common.utils import PluginHelper, DBConnector, Collections

router = APIRouter()
connector = DBConnector()

helper = PluginHelper()


@router.get("/dashboard", tags=["ITSM Dashboard"])
async def get_dashboard(
    user: User = Security(get_current_user, scopes=["cto_read"]),
):
    """Get dashboard data."""
    dedupe_count = list(
        connector.collection(Collections.ITSM_TASKS).aggregate(
            [{"$group": {"_id": None, "total": {"$sum": "$dedupeCount"}}}]
        )
    )
    if dedupe_count:
        dedupe_count = dedupe_count.pop().get("total")
    else:
        dedupe_count = 0
    by_status = list(
        connector.collection(Collections.ITSM_TASKS).aggregate(
            [
                {
                    "$match": {
                        "status": {
                            "$nin": [None, ""]
                        }
                    }
                },
                {"$group": {"_id": "$status", "count": {"$sum": 1}}}
            ]
        )
    )
    return {"dedupeCount": dedupe_count, "groupByStatus": by_status}
