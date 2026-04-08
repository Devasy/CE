"""Constants."""

ALERTS_EVENT_UNIQUE_CRITERIA = {
    "alert": {},
    "event": {
        "incident": ["rawData_object_id"]
    },
}
MAX_BODY_SIZE = 10 * 1024 * 1024  # 10 MB

MAX_BATCH_SIZE = 10000
MAX_WAIT_ON_LOCK_IN_MINUTES = 60
