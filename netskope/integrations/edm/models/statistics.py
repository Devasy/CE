"""EDM module statistics related models."""
from enum import Enum
from pydantic import BaseModel, Field


class EDMStatistics(str, Enum):
    """Enumeration for EDM module statistics attributes."""

    SENT_HASHES_COUNT = "sentHashes"
    RECEIVED_HASHES_COUNT = "receivedHashes"


class EDMStatisticsDB(BaseModel):
    """Model for storing EDM statistics in a database."""

    sentHashes: int = Field(0, description="Number of EDM hashes sent to another CE/Tenant.")
    receivedHashes: int = Field(0, description="Number of EDM hashes received from another CE.")


class EDMActions(str, Enum):
    """Enumeration for EDM module actions."""

    SHARE_EDM_HASHES = "share_edm_hash"
