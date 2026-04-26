"""Celery status decorator."""
from bson.objectid import ObjectId
from netskope.common.models.other import StatusType
from . import DBConnector, Collections


def status():
    """Task status decorator."""

    def decorator(func):
        def wrapper(*args, **argv):
            connector = DBConnector()
            query = {"_id": ObjectId(argv["uid"])}
            argv.pop("uid")
            newvalues = {"$set": {"status": StatusType.INPROGRESS}}
            connector.collection(Collections.TASK_STATUS).update_one(
                query, newvalues
            )
            try:
                response = func(*args, **argv)
            except Exception:
                newvalues = {"$set": {"status": StatusType.ERROR}}
                connector.collection(Collections.TASK_STATUS).update_one(
                    query, newvalues
                )
            else:
                newvalues = {"$set": {"status": StatusType.COMPLETED}}
                connector.collection(Collections.TASK_STATUS).update_one(
                    query, newvalues
                )
                return response

        return wrapper

    return decorator
