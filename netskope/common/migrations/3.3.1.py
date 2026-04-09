"""Migrations for 3.3.1 release."""

from netskope.common.utils import DBConnector, Collections


if __name__ == "__main__":
    connector = DBConnector()
    connector.collection(Collections.CLS_BUSINESS_RULES).update_one(
        {"name": "All"}, {"$set": {"isDefault": True}}
    )
