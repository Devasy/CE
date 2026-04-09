"""Web transaction initializer."""

from pymongo.errors import DuplicateKeyError
from netskope.common.utils import (
    DBConnector,
    Collections,
    Logger,
    PluginHelper,
    Scheduler,
)
from netskope.common.models import SettingsDB

connector = DBConnector()
logger = Logger()
helper = PluginHelper()
scheduler = Scheduler()


if __name__ == "__main__":

    settings = connector.collection(Collections.SETTINGS).find_one({})
    if settings is None:
        exit(1)
    settings = SettingsDB(**settings)
    if not settings.platforms.get("cls"):
        logger.debug(
            "CLS is disabled. Not initializing web transaction process."
        )
        exit(0)
    for configuration in connector.collection(
        Collections.CLS_CONFIGURATIONS
    ).find({}):
        if "netskope_webtx.main" in configuration.get("plugin", ""):
            try:
                rule = connector.collection(Collections.CLS_BUSINESS_RULES).find_one(
                    {f"siemMappings.{configuration.get('name')}": {"$exists": True}},
                    {"_id": 0}
                )
                if not (rule and configuration.get('tenant')):
                    continue
                scheduler.schedule(
                    name=f"tenant.{configuration.get('tenant')}.{configuration.get('name')}.webtx",
                    task_name="common.pull",
                    poll_interval=30,
                    poll_interval_unit="seconds",
                    args=["webtx", configuration.get("tenant")],
                    kwargs={"configuration_name": configuration.get("name")},
                )
            except DuplicateKeyError:
                pass
