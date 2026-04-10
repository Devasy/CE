"""Contains notification related classes."""

from datetime import datetime
from pymongo.errors import DuplicateKeyError

from .singleton import Singleton
from .db_connector import DBConnector
from ..models.other import Notification, NotificationType
from ..utils import Collections


class Notifier(metaclass=Singleton):
    """Class used to log messages."""

    def __init__(self):
        """Initialize a new logger."""
        self._connector = DBConnector()

    def info(self, message: str):
        """Add an information notification.

        Args:
            message (str): Message to be notified.
        """
        notification = Notification(
            message=message,
            type=NotificationType.INFO,
            createdAt=datetime.now(),
        )
        notification = notification.model_dump()
        notification.pop("_id", None)  # remove the ID
        self._connector.collection(Collections.NOTIFICATIONS).insert_one(notification)

    def warn(self, message: str):
        """Log a warning message.

        Args:
            message (str): Message to be notified.
        """
        notification = Notification(
            message=message,
            type=NotificationType.WARNING,
            createdAt=datetime.now(),
        )
        notification = notification.model_dump()
        notification.pop("_id", None)  # remove the ID
        self._connector.collection(Collections.NOTIFICATIONS).insert_one(notification)

    def error(self, message: str):
        """Log an error message.

        Args:
            message (str): Message to be notified.
        """
        notification = Notification(
            message=message,
            type=NotificationType.ERROR,
            createdAt=datetime.now(),
        )
        notification = notification.model_dump()
        notification.pop("_id", None)  # remove the ID
        self._connector.collection(Collections.NOTIFICATIONS).insert_one(notification)

    def banner_info(self, id: str, message: str):
        """Add an banner information notification.

        Args:
            message (str): Message to be notified.
        """
        notification = Notification(
            id=id,
            message=message,
            type=NotificationType.BANNER_INFO,
            createdAt=datetime.now(),
        )
        notification = notification.model_dump()
        notification.pop("_id", None)  # remove the ID
        try:
            self._connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": id}, {"$set": notification}, upsert=True
            )
        except DuplicateKeyError:
            pass

    def banner_error(self, id: str, message: str):
        """Log an error message.

        Args:
            message (str): Message to be notified.
        """
        notification = Notification(
            id=id,
            message=message,
            type=NotificationType.BANNER_ERROR,
            createdAt=datetime.now(),
        )
        notification = notification.model_dump()
        notification.pop("_id", None)  # remove the ID
        try:
            self._connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": id}, {"$set": notification}, upsert=True
            )
        except DuplicateKeyError:
            pass

    def banner_warning(self, id: str, message: str):
        """Log an banner_warning message.

        Args:
            message (str): Message to be notified.
        """
        notification = Notification(
            id=id,
            message=message,
            type=NotificationType.BANNER_WARNING,
            createdAt=datetime.now(),
        )
        notification = notification.model_dump()
        notification.pop("_id", None)  # remove the ID
        try:
            self._connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": id}, {"$set": notification}, upsert=True
            )
        except DuplicateKeyError:
            pass

    def get_banner_details(self, id: str):
        """Get all the banner details of the given banner ID.

        Args:
            id (str): ID of the banner
        """
        banner_details = self._connector.collection(Collections.NOTIFICATIONS).find_one(
            {"id": id}
        )

        return banner_details

    def update_banner_acknowledged(self, id: str, acknowledged: bool):
        """Update banner acknowledged field of the given banner ID.

        Args:
            id (str): ID of the banner
            acknowledged (bool): Whether banner is acknowledged or not
        """
        self._connector.collection(Collections.NOTIFICATIONS).update_one(
            {"id": id},
            {
                "$set": {
                    "acknowledged": acknowledged,
                },
            },
            upsert=True,
        )
