"""Migrations for 5.1.0 release."""

import traceback

from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
)

connector = DBConnector()
logger = Logger()


def remove_old_user_scopes():
    """Remove old user scopes."""
    try:
        connector.collection(Collections.USERS).update_many(
            {},
            {
                "$pullAll": {
                    "scopes": ["ure_read", "are_read", "are_write", "ure_write"]
                }
            }
        )
    except Exception as error:
        logger.error(
            f"Failed to remove old user scopes, {error}",
            error_code="CE_1052",
            details=traceback.format_exc()
        )
        raise error


if __name__ == "__main__":
    remove_old_user_scopes()
