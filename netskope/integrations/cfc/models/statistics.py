"""EDM module statistics related models."""
from enum import Enum
from pydantic import BaseModel, Field


class CFCStatistics(str, Enum):
    """Enumeration for EDM module statistics attributes."""

    SENT_IMAGES_COUNT = "sentImages"


class CFCStatisticsDB(BaseModel):
    """Model for storing EDM statistics in a database."""

    sentImages: int = Field(0, description="Number of images sent to the Tenant.")
