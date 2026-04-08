"""Commonly used validators in EDM module."""

from pydantic import FieldValidationInfo

from netskope.common.models import PollIntervalUnit


def validate_poll_interval(cls, value: int, info: FieldValidationInfo):
    """Determine if the provided configuration has valid pollInterval set."""
    if value is None:
        return None
    multiplier = {
        PollIntervalUnit.SECONDS: 1,
        PollIntervalUnit.MINUTES: 60,
        PollIntervalUnit.HOURS: 60 * 60,
        PollIntervalUnit.DAYS: 60 * 60 * 24,
    }
    if "pollIntervalUnit" not in info.data:
        raise ValueError("Invalid pollIntervalUnit provided.")
    interval_in_seconds = value * multiplier[info.data["pollIntervalUnit"]]
    # checks interval in seconds if interval is in range of 12 hours to 1 year
    if not (60 * 60 * 12) <= interval_in_seconds <= (60 * 60 * 24 * 365):
        raise ValueError(
            "Poll interval must be between 12 hours and 1 year."
        )
    return value
