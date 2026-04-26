# flake8: noqa E501 line too long
"""Migrations for 3.1.5 release."""

from netskope.common.utils import DBConnector, Collections

if __name__ == "__main__":
    connector = DBConnector()
    connector.collection(Collections.CLS_MAPPING_FILES).update_one(
        {"name": "ArcSight Default Mappings"}, {"$set": {"isDefault": True}}
    )
