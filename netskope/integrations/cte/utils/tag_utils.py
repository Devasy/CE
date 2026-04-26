"""Tagging related functionality for plugins."""
from netskope.common.utils.db_connector import DBConnector, Collections
from netskope.common.utils.logger import Logger
from ..models.tags import TagIn, TagDelete
from netskope.common.utils import Singleton


class TagUtils(metaclass=Singleton):
    """Provides tagging related utility methods."""

    source = None

    def __init__(self):
        """Initialize."""
        self.logger = Logger()
        self.connector = DBConnector()
        self._query = None

    def on_indicators(self, query: dict):
        """Set query for the indicators."""
        self._query = query
        return self

    def add(self, tag: str) -> bool:
        """Add tags to the indicators."""
        if self._query is None:
            raise ValueError("Query must be set before modifying indicators")
        if TagUtils.source is None:
            update_result = self.connector.collection(
                Collections.INDICATORS
            ).update_many(self._query, {"$addToSet": {"sources.$[].tags": tag}})
            self.logger.debug(
                f"Added {tag} tag to {update_result.matched_count} indicators."
            )
            self._query = None
        else:
            update_result = self.connector.collection(
                Collections.INDICATORS
            ).update_many(
                self._query,
                {"$addToSet": {"sources.$[i].tags": tag}},
                array_filters=[{"i.source": TagUtils.source}],
            )
            self.logger.debug(
                f"Added {tag} tag to {update_result.matched_count} indicators."
            )
            self._query = None

    def remove(self, tag: str) -> bool:
        """Remove tags from the indicators."""
        if self._query is None:
            raise ValueError("Query must be set before modifying indicators")
        if TagUtils.source is None:
            update_result = self.connector.collection(
                Collections.INDICATORS
            ).update_many(self._query, {"$pull": {"sources.$[].tags": tag}})
            if update_result.modified_count:
                self.logger.debug(
                    f"Removed {tag} from {update_result.modified_count} indicators."
                )
            self._query = None
        else:
            update_result = self.connector.collection(
                Collections.INDICATORS
            ).update_many(
                self._query,
                {"$pull": {"sources.$[i].tags": tag}},
                array_filters=[{"i.source": TagUtils.source}],
            )
            if update_result.modified_count:
                self.logger.debug(
                    f"Removed {tag} from {update_result.modified_count} indicators."
                )
            self._query = None

    def create_tag(self, tag: TagIn):
        """Create a new tag."""
        self.connector.collection(Collections.TAGS).insert_one(tag.model_dump())

    def remove_tag(self, tag: TagDelete):
        """Remove an existing tag."""
        self.connector.collection(Collections.INDICATORS).update_many(
            {"sources": {"$elemMatch": {"tags": {"$in": [tag.name]}}}},
            {"$pull": {"sources.$[].tags": tag.name}},
        )
        self.connector.collection(Collections.TAGS).delete_one({"name": tag.name})

    def exists(self, name: str) -> bool:
        """Check if a tag with given name exists or not."""
        tag = self.connector.collection(Collections.TAGS).find_one({"name": name})
        return tag is not None
