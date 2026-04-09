"""Update container health check time in node_health collection."""
from datetime import datetime
import traceback
from netskope.common.utils import DBConnector, Logger, Collections

logger = Logger()


def update_container_health(container_id):
    """
    Update the container health.

    :param container_id:
    :return:
    """
    try:
        if container_id is None:
            logger.error("The container ID is a mandatory field and must not be left blank.")
            return
        connector = DBConnector()
        connector.collection(Collections.NODE_HEALTH).update_one(
            {"worker_id": container_id},
            {"$set": {"worker_id": container_id, "check_time": datetime.now()}},
            upsert=True
        )
        logger.debug(f"Successfully updated health check for container with id {container_id}.")
    except Exception as e:
        logger.error(
            f"Error occurred while updating health check for container {container_id}, error: {e}",
            details=traceback.format_exc(),
            error_code="CE_1027",
        )
