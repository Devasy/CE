"""Data Batch Manager."""

from datetime import datetime, UTC

from .db_connector import DBConnector, Collections
from ..models.data_batch import BatchDataType, CLSSIEMStatusType, CLSSIEMCountType


class DataBatchManager:
    """Data Batch Manager."""

    def __init__(self):
        """Create Data Batch Manager."""
        self.connector = DBConnector()
        self.collection = Collections.DATA_BATCHES

    def create(
        self,
        data_type: BatchDataType,
        sub_type,
        count: int,
        data_source: str,
        data_source_type: str,
    ):
        """Create data batch."""
        result = self.connector.collection(self.collection).insert_one(
            {
                "type": (
                    data_type.value
                    if isinstance(data_type, BatchDataType)
                    else data_type
                ),
                "sub_type": sub_type,
                "count": count,
                "data_source": data_source,
                "data_source_type": data_source_type,
                "createdAt": datetime.now(UTC),
                "cls": [],
            }
        )
        return self.connector.collection(self.collection).find_one(
            {"_id": result.inserted_id}
        )

    def add_cls_siem(
        self, id: str, source: str, destination: str, status: CLSSIEMStatusType
    ):
        """Add SIEM to data batch."""
        self.connector.collection(self.collection).update_one(
            {"_id": id},
            {
                "$push": {
                    "cls": {
                        "source": source,
                        "destination": destination,
                        "status": status.value,
                        "updated_at": datetime.now(UTC),
                        "count": {
                            "filtered": 0,
                            "transformed": 0,
                            "ingested": 0,
                        },
                    }
                }
            },
        )

    def update_cls_siem(
        self,
        id: str,
        source: str,
        destination: str,
        status: CLSSIEMStatusType = None,
        count_type: CLSSIEMCountType = None,
        count: int = None,
        size: int = None,
        error_on: CLSSIEMStatusType = None,
    ):
        """Update SIEM to data batch."""
        update_dict = {
            "cls.$.updated_at": datetime.now(UTC)
        }
        if status:
            update_dict["cls.$.status"] = status.value
        if error_on:
            update_dict["cls.$.error_on"] = error_on.value
        if count_type:
            update_dict[f"cls.$.count.{count_type.value}"] = count
            if size:
                update_dict[f"cls.$.size.{count_type.value}"] = size
        self.connector.collection(self.collection).update_one(
            {
                "_id": id,
                "cls": {"$elemMatch": {
                    "source": source,
                    "destination": destination
                }}
            },
            {
                "$set": update_dict,
            },
        )
        return self.connector.collection(self.collection).find_one({"_id": id})

    def get(self, id: str):
        """Get data batch."""
        return self.connector.collection(self.collection).find_one({"_id": id})

    def get_all(self):
        """Get all data batches."""
        return self.connector.collection(self.collection).find({})

    def get_by_filters(self, filters: dict):
        """Get data batches by filters."""
        return self.connector.collection(self.collection).find(filters)

    def aggregate(self, pipelines: list):
        """Aggregate data batches."""
        return self.connector.collection(self.collection).aggregate(pipelines)

    def delete_by_id(self, id):
        """Delete data batch by id."""
        return self.connector.collection(self.collection).delete_one({"_id": id})

    def delete_by_filters(self, filters: dict):
        """Delete data batch by filters."""
        return self.connector.collection(self.collection).delete_many(filters)

    def delete_cls_siem(self, id: str, source: str, destination: str):
        """Delete SIEM from data batch."""
        self.connector.collection(self.collection).update_one(
            {"_id": id},
            {"$pull": {"cls": {"source": source, "destination": destination}}},
        )
        return self.connector.collection(self.collection).find_one({"_id": id})

    def delete_all(self):
        """Delete all data batches."""
        return self.connector.collection(self.collection).delete_many({})
