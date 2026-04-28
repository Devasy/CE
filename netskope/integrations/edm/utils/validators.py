"""Commonly used validators in EDM module."""

import re

from pydantic import FieldValidationInfo

from netskope.common.models import PollIntervalUnit

MAX_FILENAME_LEN = 240
VALID_FILENAME_PATTERN = re.compile(r'^[a-zA-Z0-9_]+$')


def validate_edm_filename(filename: str) -> tuple:
    """Validate EDM filename for hash generation compatibility.

    Args:
        filename: The filename to validate.

    Returns:
        tuple: (is_valid, error_message)
    """
    if not filename:
        return False, "Filename cannot be empty."

    # Get base name without extension
    base_name = filename.rsplit('.', 1)[0] if '.' in filename else filename

    if len(filename) > MAX_FILENAME_LEN:
        return False, f"Filename too long. Maximum {MAX_FILENAME_LEN} characters allowed."

    if not VALID_FILENAME_PATTERN.match(base_name):
        return False, "Filename must contain only letters (a-z, A-Z), numbers (0-9), and underscores (_)."

    return True, ""


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
    # checks interval in seconds if interval is in range of 1 hour to 1 year
    if not (60 * 60 * 1) <= interval_in_seconds <= (60 * 60 * 24 * 365):
        raise ValueError(
            "Poll interval must be between 1 hour and 1 year."
        )
    return value
