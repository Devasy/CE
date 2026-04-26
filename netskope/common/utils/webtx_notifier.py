"""WebTx error banner operations."""
import traceback

from netskope.common.utils import Logger
from netskope.common.utils import DBConnector, Collections
from netskope.common.utils.notifier import Notifier
from netskope.common.utils import const

logger = Logger()
connector = DBConnector()
notifier = Notifier()


def add_or_acknowledge_webtx_disabled_banner():
    """Add or acknowledge webtx banner."""
    try:
        webtx_configurations = list(connector.collection(Collections.CLS_CONFIGURATIONS).find(
            {
                "plugin": "netskope.plugins.Default.netskope_webtx.main",
                "active": False,
                "$or": [
                    {"tenant": {"$exists": False}},
                    {"tenant": None}
                ]
            },
            {"name": 1, "_id": 0}
        ))
        names = [name["name"] for name in webtx_configurations]
        if names:
            message = const.WEB_TX_ERROR_BANNER_MESSAGE.format(", ".join(names))
            notifier.banner_error(const.WEB_TX_ERROR_BANNER_ID, message)
            logger.info(message)
            return message
        else:
            notifier.update_banner_acknowledged(const.WEB_TX_ERROR_BANNER_ID, True)
    except Exception:
        logger.warn("Error occurred while adding or acknowledging webtx banner.", details=traceback.format_exc())

    return None
