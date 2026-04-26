"""Back pressure and Resource limit."""
import os
import math
import time
import traceback

from datetime import datetime
from netskope.common.utils.disk_free_alarm import get_available_disk_space

from netskope.common.utils import (Collections, DBConnector, Logger, Notifier,
                                   PluginHelper)
from netskope.common.utils.rabbitmq_helper import make_rabbitmq_api_call
from netskope.common import RABBITMQ_QUORUM_QUEUE_NAME
from threading import Event
from .const import LOWER_THRESHOLD, UPPER_THRESHOLD

connector = DBConnector()
notifier = Notifier()
logger = Logger()
helper = PluginHelper()
STOP_PULLING = False


def threaded_back_pressure():  # TODO: Need to remove this method
    """Threaded method for back pressure."""
    global STOP_PULLING
    while True:
        time.sleep(300)
        check_back_pressure_flag = back_pressure_mechanism()
        if not check_back_pressure_flag:
            STOP_PULLING = True
            return
        else:
            STOP_PULLING = False


def should_stop_pulling(should_exit: Event) -> None:
    """Return true if back pressure occurred."""
    global STOP_PULLING
    while not should_exit.is_set():
        time.sleep(300)
        STOP_PULLING = not back_pressure_mechanism()


def back_pressure_mechanism(force_check=False):
    """Back pressure mechanism and Resource limit implementation."""
    try:
        tenant_count = connector.collection(
            Collections.NETSKOPE_TENANTS
        ).count_documents({})
        if tenant_count != 0 or force_check:
            back_pressure = back_pressure_for_disk_space()
            return back_pressure
    except Exception:
        logger.warn(
            "Not able to check the resources or physical disk space.",
            error_code="CE_1051", details=traceback.format_exc(),
        )
    return True


def get_current_queue_depth():
    """Check rabbitmq queue depth."""
    message_bytes_persistent = 0
    for i in [3, 6, 9]:
        response = make_rabbitmq_api_call(f"/api/queues/%2f/{RABBITMQ_QUORUM_QUEUE_NAME.format(i)}")
        message_bytes_persistent += response["message_bytes_persistent"]
    return message_bytes_persistent


def _convert_size(size_bytes):
    if size_bytes == 0:
        return "0 Bytes"
    size_name = ("Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB")
    unit = int(math.floor(math.log(size_bytes, 1024)))
    bytes_per_unit = math.pow(1024, unit)
    size = round(size_bytes / bytes_per_unit, 2)
    return f"{size} {size_name[unit]}"


def back_pressure_for_disk_space():
    """Check for available physical disk space."""
    if os.environ.get("RABBITMQ_AVAILABLE_STORAGE"):
        persistent_bytes = get_current_queue_depth()
        logger.debug(f"Current queue size is {_convert_size(persistent_bytes)}.")
        available_space = 100 - int(persistent_bytes * 100 / int(os.environ["RABBITMQ_AVAILABLE_STORAGE"]))
        if available_space < 0:
            available_space = 0
    else:
        available_space = get_available_disk_space()
        logger.debug(f"Current available disk space is {available_space}%.")

    should_pull = True
    sizing_recommendation = " Refer to the Netskope Cloud Exchange " \
        "[sizing recommendations](https://docs.netskope.com/en/cloud-exchange-system-requirements.html#UUID-92edc283-a3a0-2a63-1312-513929a52ed0_N1666326801974)."  # NOQA

    log_warn_message = (
        "You're running out of disk space on your host. "
        f"The available disk space is {available_space}%. "
        "The new data pull from Netskope will be paused soon. Please "
        "free up your disk space or provide additional disk space to prevent the pulling from being stopped."
    )
    log_blocker_message = (
        "You're running out of disk space on your host. "
        f"The available disk space ({available_space}%) is critically low. "
        "The new data pull from Netskope has been paused. "
        "You will have to free up the disk space or provision additional disk space to make the "
        f"available disk space more than {LOWER_THRESHOLD}% of the total disk space to resume the pulling."
    )
    warning_message = log_warn_message + sizing_recommendation
    blocker_message = log_blocker_message + sizing_recommendation
    settings = connector.collection(Collections.SETTINGS).find_one({})

    if "should_pull" in settings:
        should_pull = settings.get("should_pull")
    else:  # else add it to database.
        connector.collection(Collections.SETTINGS).update_one(
            {},
            {"$set": {"should_pull": True}},
        )
    # If storage is in between (10-25] %.
    if LOWER_THRESHOLD >= available_space > UPPER_THRESHOLD and should_pull:
        query = connector.collection(Collections.NOTIFICATIONS).find_one(
            {"id": "BANNER_WARN_1000"}
        )
        if query:
            # Warning Message
            warn_update_result = connector.collection(
                Collections.NOTIFICATIONS
            ).update_one(
                {"id": "BANNER_WARN_1000"},
                {
                    "$set": {
                        "message": warning_message,
                        "acknowledged": False,
                    },
                },
            )
            if warn_update_result.modified_count != 0:
                logger.warn(log_warn_message, error_code="CE_1116")
            connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": "BANNER_WARN_1000"},
                {
                    "$set": {
                        "message": warning_message,
                        "createdAt": datetime.now(),
                    },
                },
            )
        else:
            notifier.banner_warning("BANNER_WARN_1000", warning_message)
            logger.warn(log_warn_message, error_code="CE_1116")
        # if error message is there then removed it.
        connector.collection(
            Collections.NOTIFICATIONS
        ).update_one(
            {"id": "BANNER_ERROR_1001"},
            {
                "$set": {
                    "message": blocker_message,
                    "acknowledged": True,
                },
            },
        )
        return True
    # If storage is in between (10-25] %. and should_pull is false
    elif LOWER_THRESHOLD >= available_space > UPPER_THRESHOLD and not should_pull:
        query = connector.collection(Collections.NOTIFICATIONS).find_one(
            {"id": "BANNER_ERROR_1001"}
        )
        if query:
            err_update_result = connector.collection(
                Collections.NOTIFICATIONS
            ).update_one(
                {"id": "BANNER_ERROR_1001"},
                {
                    "$set": {
                        "message": blocker_message,
                        "acknowledged": False,
                    },
                },
            )
            if err_update_result.modified_count != 0:
                logger.error(log_blocker_message, error_code="CE_1103")
            connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": "BANNER_ERROR_1001"},
                {
                    "$set": {
                        "message": blocker_message,
                        "createdAt": datetime.now(),
                    },
                },
            )
        else:
            notifier.banner_error("BANNER_ERROR_1001", blocker_message)
            logger.error(log_blocker_message, error_code="CE_1103")
        connector.collection(
            Collections.NOTIFICATIONS
        ).update_one(
            {"id": "BANNER_WARN_1000"},
            {
                "$set": {
                    "message": warning_message,
                    "acknowledged": True,
                },
            },
        )
        return False

    # if storage is greater than 25% and should_pull is false then
    # we will start pulling again.
    elif available_space > LOWER_THRESHOLD and not should_pull:
        # update should_pull to True.
        connector.collection(Collections.SETTINGS).update_one(
            {},
            {
                "$set": {
                    "should_pull": True,
                },
            },
        )
        connector.collection(
            Collections.NOTIFICATIONS
        ).update_one(
            {"id": "BANNER_WARN_1000"},
            {
                "$set": {
                    "message": warning_message,
                    "acknowledged": True,
                },
            },
        )
        connector.collection(
            Collections.NOTIFICATIONS
        ).update_one(
            {"id": "BANNER_ERROR_1001"},
            {
                "$set": {
                    "message": blocker_message,
                    "acknowledged": True,
                },
            },
        )
        return True

    # if storage is less than 10% and should_pull is set to False.
    elif available_space <= UPPER_THRESHOLD or not should_pull:
        query = connector.collection(Collections.NOTIFICATIONS).find_one(
            {"id": "BANNER_ERROR_1001"}
        )
        if query:
            err_update_result = connector.collection(
                Collections.NOTIFICATIONS
            ).update_one(
                {"id": "BANNER_ERROR_1001"},
                {
                    "$set": {
                        "message": blocker_message,
                        "acknowledged": False,
                    },
                },
            )
            if err_update_result.modified_count != 0:
                logger.error(log_blocker_message, error_code="CE_1050")
            connector.collection(Collections.NOTIFICATIONS).update_one(
                {"id": "BANNER_ERROR_1001"},
                {
                    "$set": {
                        "message": blocker_message,
                        "createdAt": datetime.now(),
                    },
                },
            )
        else:
            notifier.banner_error("BANNER_ERROR_1001", blocker_message)
            logger.error(log_blocker_message, error_code="CE_1050")
        connector.collection(
            Collections.NOTIFICATIONS
        ).update_one(
            {"id": "BANNER_WARN_1000"},
            {
                "$set": {
                    "message": warning_message,
                    "acknowledged": True,
                },
            },
        )
        # update the should_pull to False.
        connector.collection(Collections.SETTINGS).update_one(
            {},
            {
                "$set": {
                    "should_pull": False,
                },
            },
        )
        return False

    # if storage is greater than 25% and should_pull is True.
    connector.collection(Collections.NOTIFICATIONS).update_one(
        {"id": "BANNER_WARN_1000"},
        {
            "$set": {
                "message": warning_message,
                "acknowledged": True,
            },
        },
    )
    connector.collection(Collections.NOTIFICATIONS).update_one(
        {"id": "BANNER_ERROR_1001"},
        {
            "$set": {
                "message": blocker_message,
                "acknowledged": True,
            },
        },
    )
    return True
