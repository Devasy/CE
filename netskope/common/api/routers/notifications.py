"""Provides indicator related endpoints."""

from typing import List, Optional
from fastapi import APIRouter, Security, HTTPException, Path
from bson.objectid import ObjectId
from pymongo import DESCENDING

from ...utils import DBConnector, Collections, Logger
from .auth import get_current_user
from ...models import User, Notification, NotificationType
from netskope.common.utils.const import MAX_NOTIFICATIONS

router = APIRouter()
db_connector = DBConnector()
logger = Logger()


@router.get(
    "/notifications/",
    response_model=List[Notification],
    tags=["Notifications"],
    description="Read notifications.",
)
async def read_notifications(
    include_all: bool = False,
    user: User = Security(get_current_user, scopes=["settings_read"]),
):
    """Read all the un-acknowledged notifications.

    Returns:
        List[Notification]: List of all the notifications.
    """
    query = {}
    if not include_all:
        query["acknowledged"] = False
        query["type"] = {
            "$nin": [
                NotificationType.BANNER_INFO,
                NotificationType.BANNER_WARNING,
                NotificationType.BANNER_ERROR,
            ]
        }
    back_pressure_notifications_list = []
    info = list(
        db_connector.collection(Collections.NOTIFICATIONS).find(
            {
                "$and": [
                    {"acknowledged": False},
                    {"type": NotificationType.BANNER_INFO},
                ]
            }
        )
    )
    warning = list(
        db_connector.collection(Collections.NOTIFICATIONS).find(
            {
                "$and": [
                    {"acknowledged": False},
                    {"type": NotificationType.BANNER_WARNING},
                ]
            }
        )
    )
    banner_error = list(
        db_connector.collection(Collections.NOTIFICATIONS).find(
            {
                "$and": [
                    {"acknowledged": False},
                    {"type": NotificationType.BANNER_ERROR},
                ]
            }
        )
    )
    if info:
        back_pressure_notifications_list = back_pressure_notifications_list + info
    if warning:
        back_pressure_notifications_list = back_pressure_notifications_list + warning
    if banner_error:
        back_pressure_notifications_list = (
            back_pressure_notifications_list + banner_error
        )
    notifications_dict = list(
        db_connector.collection(Collections.NOTIFICATIONS)
        .find(query)
        .sort("createdAt", DESCENDING)
        .limit(MAX_NOTIFICATIONS)
    )
    if notifications_dict:
        back_pressure_notifications_list = (
            back_pressure_notifications_list + notifications_dict
        )
    notifications = []

    for notification in back_pressure_notifications_list:
        notifications.append(
            Notification(
                id=str(notification["_id"]),
                acknowledged=notification["acknowledged"],
                message=notification["message"],
                createdAt=notification["createdAt"],
                type=notification["type"],
                is_promotion=notification.get("is_promotion", False),
            )
        )
    return notifications


@router.patch(
    "/notifications/clear",
    tags=["Notifications"],
    description="Clear all notifications.",
)
async def clear_notifications(
    user: User = Security(get_current_user, scopes=["settings_write"]),
):
    """Clear all notifications."""
    notifications_dict = list(
        db_connector.collection(Collections.NOTIFICATIONS)
        .find(
            {
                "acknowledged": False,
                "$or": [
                    {
                        "type": {
                            "$nin": [
                                NotificationType.BANNER_INFO,
                                NotificationType.BANNER_WARNING,
                                NotificationType.BANNER_ERROR,
                            ]
                        }
                    },
                    {"is_promotion": True},
                ],
            }
        )
        .sort("createdAt", DESCENDING)
        .limit(MAX_NOTIFICATIONS)
    )
    for notification_dict in notifications_dict:
        db_connector.collection(Collections.NOTIFICATIONS).update_one(
            {"_id": notification_dict["_id"]}, {"$set": {"acknowledged": True}}
        )
    logger.info("User removed all notification and promotion banners.")
    return {}


@router.patch(
    "/notifications/clear/{notification_id}",
    tags=["Notifications"],
    description="Clear particular notifications.",
)
async def clear_notification(
    notification_id: Optional[str] = Path(...),
    user: User = Security(get_current_user, scopes=["settings_write"]),
):
    """Clear particular notifications."""
    update_result = db_connector.collection(Collections.NOTIFICATIONS).update_many(
        {"_id": ObjectId(notification_id)}, {"$set": {"acknowledged": True}}
    )
    if update_result.modified_count == 0:
        raise HTTPException(
            404, "Notification already acknowledged or it does not exist."
        )
    logger.info(
        message=f"User removed notification or promotion banner with id='{notification_id}'",
        details=db_connector.collection(
            Collections.NOTIFICATIONS
        ).find_one({"_id": ObjectId(notification_id)}).get("message")
    )
    return {}
