"""Contains notification related classes."""

import re
from datetime import datetime
from pymongo.errors import DuplicateKeyError

from .singleton import Singleton
from .db_connector import DBConnector
from .logger import Logger
from ..models.other import Notification, NotificationType
from ..utils import Collections

logger = Logger()

CONFIG_BANNER_ID_PREFIX = "BANNER_ERROR_9999_"


def config_banner_suffix(configuration_name: str) -> str:
    """Build the trailing segment that a per-configuration banner id must end with.

    CONTRACT for anyone raising a banner about a single configuration: the banner id
    MUST be ``<your prefix>_`` + this suffix, and the raiser SHOULD also pass
    ``configuration=<name>`` to the ``banner_*`` methods.

    That is how core traces a banner back to its configuration and acknowledges it
    when the configuration is removed (see
    :meth:`Notifier.acknowledge_config_banners`). A banner that follows neither rule
    is unreachable: it stays on screen forever with nothing left to dismiss it, which
    is the defect this contract exists to prevent.

    Nothing in core can validate the id you choose, so the id and the ``configuration``
    argument are the whole defence. Prefer passing ``configuration`` -- the suffix match
    is a lossy fallback for plugins that predate that argument, since the
    space-to-underscore, upper-case transform is not invertible (``"web tx"`` and
    ``"web_tx"`` collapse to one id, and a config named ``Tx`` matches a banner
    belonging to ``Web Tx``).

    Args:
        configuration_name (str): Name of the configuration.

    Returns:
        str: Suffix the banner id must end with.
    """
    return configuration_name.replace(" ", "_").upper()


def config_banner_id(configuration_name: str) -> str:
    """Build the default id of a banner scoped to a single configuration.

    Keep this the single definition of the derivation so core and the plugins cannot
    drift apart. A plugin needing more than one banner per configuration uses its own
    prefix with the same suffix -- see :func:`config_banner_suffix` for the contract.

    Args:
        configuration_name (str): Name of the configuration.

    Returns:
        str: Id of the configuration scoped banner.
    """
    return f"{CONFIG_BANNER_ID_PREFIX}{config_banner_suffix(configuration_name)}"


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

    def banner_info(
        self, id: str, message: str, is_promotion: bool = False, configuration: str = None
    ):
        """Add an banner information notification.

        Args:
            id (str): Stable id used as the upsert key.
            message (str): Message to be notified.
            is_promotion (bool): When True the banner is dismissed through the standard server-side
                ``acknowledged`` path — the UI only server-clears promotion banners on close
                (``clear/{_id}``), so the dismissal is GLOBAL and persists across sessions. Default
                False keeps the plain per-browser-session (sessionStorage) dismissal.
            configuration (str): Name of the configuration this banner is about. Pass it whenever
                the banner is scoped to one configuration, so it can be acknowledged when that
                configuration is removed. See :func:`config_banner_suffix` for the contract.
        """
        notification = Notification(
            id=id,
            message=message,
            type=NotificationType.BANNER_INFO,
            createdAt=datetime.now(),
            is_promotion=is_promotion,
            configuration=configuration,
        )
        notification = notification.model_dump()
        notification.pop("_id", None)  # remove the ID
        try:
            self._connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": id}, {"$set": notification}, upsert=True
            )
        except DuplicateKeyError:
            pass

    def banner_error(self, id: str, message: str, configuration: str = None):
        """Log an error message.

        Args:
            id (str): Stable id used as the upsert key.
            message (str): Message to be notified.
            configuration (str): Name of the configuration this banner is about. Pass it whenever
                the banner is scoped to one configuration, so it can be acknowledged when that
                configuration is removed. See :func:`config_banner_suffix` for the contract.
        """
        notification = Notification(
            id=id,
            message=message,
            type=NotificationType.BANNER_ERROR,
            createdAt=datetime.now(),
            configuration=configuration,
        )
        notification = notification.model_dump()
        notification.pop("_id", None)  # remove the ID
        try:
            self._connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": id}, {"$set": notification}, upsert=True
            )
        except DuplicateKeyError:
            pass

    def banner_warning(self, id: str, message: str, configuration: str = None):
        """Log an banner_warning message.

        Args:
            id (str): Stable id used as the upsert key.
            message (str): Message to be notified.
            configuration (str): Name of the configuration this banner is about. Pass it whenever
                the banner is scoped to one configuration, so it can be acknowledged when that
                configuration is removed. See :func:`config_banner_suffix` for the contract.
        """
        notification = Notification(
            id=id,
            message=message,
            type=NotificationType.BANNER_WARNING,
            createdAt=datetime.now(),
            configuration=configuration,
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

    def acknowledge_config_banners(self, configuration_name: str) -> int:
        """Acknowledge every unacknowledged banner scoped to the given configuration.

        A plugin normally acknowledges its own banners from ``cleanup()``, but that
        only runs while the plugin class is still resolvable. When a plugin is
        removed from disk before its configurations are deleted, ``cleanup()`` is
        skipped and its banners are left visible forever with nothing able to dismiss
        them. This is the core side backstop for that case: it needs no plugin class.

        A banner is matched either by its ``configuration`` field (exact, for raisers
        that pass it) or by its id ending in the configuration suffix (lossy fallback
        for raisers that predate the field). See :func:`config_banner_suffix`.

        Args:
            configuration_name (str): Name of the configuration being removed.

        Returns:
            int: Number of banners acknowledged.
        """
        suffix = config_banner_suffix(configuration_name)
        query = {
            "acknowledged": False,
            # The name is escaped because configuration names are user supplied and
            # may hold regex metacharacters.
            "$or": [
                {"configuration": configuration_name},
                {"id": {"$regex": f"_{re.escape(suffix)}$"}},
            ],
        }
        collection = self._connector.collection(Collections.NOTIFICATIONS)
        # Shortlist first so the ids can be logged. Support needs to see WHICH banners
        # were cleared, and just as importantly which were not: a banner that follows
        # neither half of the contract is invisible here, and the empty shortlist is
        # the only signal that it was missed.
        shortlist = list(collection.find(query, {"id": 1, "configuration": 1}))
        logger.info(
            f"Shortlisted {len(shortlist)} banner(s) to acknowledge for the removed "
            f"configuration '{configuration_name}' "
        )
        if not shortlist:
            return 0
        # update_many on the shortlisted ids, never update_one with upsert: this must
        # not be able to insert a notification carrying neither message, type nor
        # createdAt, which the notifications endpoint reads unguarded.
        result = collection.update_many(
            {"_id": {"$in": [banner["_id"] for banner in shortlist]}},
            {"$set": {"acknowledged": True}},
        )
        logger.info(
            f"Acknowledged {result.modified_count} banner(s) left behind by the "
            f"removed configuration '{configuration_name}'."
        )
        return result.modified_count
