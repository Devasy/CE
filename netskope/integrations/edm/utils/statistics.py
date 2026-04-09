"""Helper methods for updating EDM module statistics details."""

from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger
)

db_connector = DBConnector()
logger = Logger()


def increment_count(field_name, increment_value=1) -> None:
    """
    Increment the count of a field in the EDM statistics.

    Args:
        field_name (str): The name of the field to increment.
        increment_value (int, optional): The value to increment the field by. Defaults to 1.
    """
    # Update the count of the field in the document
    db_connector.collection(Collections.EDM_STATISTICS).find_one_and_update(
        {},
        {'$inc': {field_name: increment_value}},
        upsert=True
    )
