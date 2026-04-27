"""Provides Image Metadata related models."""
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

from .classifier import TrainingType
from .task_status import CFCTaskType


class DestinationMetadata(BaseModel):
    """Sharings where business rule is used in."""

    destinationPlugin: str = Field(..., description="Destination configuration where files are shared")
    classifierName: str = Field(..., description="Name of the classifier associated with the destination configuration")
    classifierID: str = Field(..., description="ID of the classifier associated with the destination configuration")
    trainingType: TrainingType = Field(..., description="Type of training performed")
    lastShared: Optional[datetime] = Field(..., description="Last time image was shared with configuration")
    status: str = Field(..., description="Sharing status of the image for the current destination configuration.")


class ImageMetadataDB(BaseModel):
    """Image metadata database model."""

    sourcePlugin: str = Field(..., description="Name of source plugin that images are fetched from")
    sourcePluginID: str = Field(..., description="Database id of the Source Plugin")
    outdated: bool = Field(False, description="Indicated if an image data entry is outdated or not")
    file: str = Field(..., description="Name of file")
    path: str = Field(..., description="Remote file path from source")
    extension: str = Field(..., description="Extension of file")

    @field_validator("extension")
    @classmethod
    def _validate_extension_to_upper_case(cls, value: str):
        return value.upper()

    sourceType: CFCTaskType = Field(..., description="Type of the source")
    fileSize: int = Field(..., description="Size of file")
    sharedWith: Optional[List[DestinationMetadata]] = Field(None, description=("List of sharing mappings "
                                                                               "where business rules are applied"))
    lastFetched: datetime = Field(..., description="Last time image was fetched from source")
    dirUuid: str = Field(..., description="UUID to identify and store files")


class ImageMetadataOut(BaseModel):
    """Image metadata out model."""

    count: int = Field(..., description="Count of records found from query result")
    data: List[ImageMetadataDB] = Field(..., description="List of records found from query result")
