"""Get Installation Id."""
from netskope.common.utils.db_connector import DBConnector, Collections

connector = DBConnector()


def get_installation_id():
    """Get installation Id."""
    settings = connector.collection(Collections.SETTINGS).find_one({})
    return settings.get("uid", None)
