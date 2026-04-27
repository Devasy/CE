"""Provides manual upload related models."""

import os
from datetime import datetime, UTC
from typing import List, Optional
import traceback
import re

from pydantic import (
    BaseModel,
    Field,
    FieldValidationInfo,
    StringConstraints,
    field_validator,
)
from typing_extensions import Annotated

from netskope.common.utils import Collections, DBConnector, Logger, PluginHelper
from netskope.common.models import TenantDB
from netskope.integrations.cfc.utils import IMAGE_EXTENSION_SUPPORTED

from .configurations import ConfigurationDB
from .task_status import StatusType
from .sharing import ClassifierType, TrainingType, ErrorState
from ..utils import NetskopeClientCFC

connector = DBConnector()
helper = PluginHelper()
logger = Logger()


class FileUploadMetadataIn(BaseModel):
    """File upload metadata schema."""

    name: str = Field(..., description="File name")
    configurationName: str = Field(..., description="Configuration name")
    size: str = Field(..., description="File size")
    extension: str = Field(..., description="File extension")
    lastModified: datetime = Field(..., description="File last modified datetime")
    path: str = Field(..., description="File path")


class FileStatus(BaseModel):
    """File status schema."""

    file_name: str = Field(..., description="File name")
    status: str = Field(..., description="File upload status")
    updatedAt: datetime = Field(None, description="Last status update time")


class ManualUploadConfigurationIn(BaseModel):
    """Manual upload configuration in schema."""

    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
    ] = Field(
        ...,
        description="Name of the configuration.",
    )

    @field_validator("name")
    @classmethod
    def _validate_unique_name(cls, value: str):
        """Validate that the configuration name is unique."""
        if not re.match(r"^[a-zA-Z0-9 ]+$", value):
            raise ValueError(
                "Configuration name can only contain alpha-numeric characters and spaces."
            )
        if (
            connector.collection(
                Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
            ).find_one({"name": value})
            is not None
        ):
            raise ValueError(f"Configuration with name '{value}' already exists.")
        return value

    destinationConfiguration: str = Field(
        ..., description="Name of destination plugin configuration."
    )

    @field_validator("destinationConfiguration")
    @classmethod
    def _validate_destination_configuration(
        cls, v, info: FieldValidationInfo, **kwargs
    ):
        """Validate that the destination configuration exists."""
        dest_config = connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
            {"name": v}
        )
        if not dest_config:
            raise ValueError(f"CFC configuration '{v}' does not exist.")

        dest_config = ConfigurationDB(**dest_config)
        metadata = helper.find_by_id(dest_config.plugin).metadata
        if not metadata.get("push_supported"):
            raise ValueError(
                f"Data push is not supported for configuration '{metadata.get('push_supported')}'."
            )
        return v

    classifierName: Optional[str] = Field(None, description="Classifier Name")
    classifierType: ClassifierType = Field(
        ClassifierType.CUSTOM, description="Type of the classifier"
    )
    classifierID: str = Field(..., description="Classifier ID")

    @field_validator("classifierID")
    @classmethod
    def _validate_classifier_id(cls, value: str, info: FieldValidationInfo):
        """Validate the classifier id."""
        info.data["classifierName"] = f"Class Name ({value})"
        destinationConfiguration = info.data["destinationConfiguration"]
        destination_configuration = connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
            {"name": destinationConfiguration}
        )
        tenant = destination_configuration.get("tenant")

        if tenant is None:
            raise ValueError("Destination configuration does not have tenant specified.")

        tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
            {"name": tenant}
        )
        tenant = TenantDB(**tenant)
        netskope_client = NetskopeClientCFC(
            api_token_v2=tenant.parameters.get("v2token"),
            tenant_base_url=tenant.parameters.get("tenantName"),
        )

        try:
            if info.data["classifierType"] == ClassifierType.CUSTOM:
                classifier = netskope_client.classifier_by_id(class_id=value)
                if not classifier:
                    raise ValueError(
                        f"Classifier: '{info.data['classifierName']}' does not exists or deleted from the tenant."
                    )
                info.data["classifierName"] = classifier["name"]
            # elif info.data["classifierType"] == ClassifierType.PREDEFINED:
            #     if info.data["trainingType"] == TrainingType.POSITIVE:
            #         raise ValueError("Predefined Classifiers can be trained as Negative only.")
            #     overlay_classifier, predefined_classifier = netskope_client.find_overlay_classifier(
            #         value
            #     )
            #     info.data["classifierName"] = predefined_classifier["label"]
            return value
        except ValueError as error:
            raise error from error
        except Exception:
            logger.error(
                "Error occurred while validating the classifiers.",
                details=traceback.format_exc(),
                error_code="CFC_1034"
            )
            raise ValueError("Failed to validate the classifiers.")

    trainingType: TrainingType = Field(
        TrainingType.POSITIVE, description="Indicates training type"
    )

    files: List[str] = Field(..., description="List of files to be uploaded")

    @field_validator("files")
    @classmethod
    def _validate_files(cls, v, info: FieldValidationInfo, **kwargs):
        """Validate file extensions."""
        for image in v:
            if os.path.splitext(image)[1] not in IMAGE_EXTENSION_SUPPORTED:
                raise ValueError(f"{image} is not supported")
        return v

    createdAt: Optional[datetime] = Field(None, validate_default=True)
    updatedAt: Optional[datetime] = Field(None, validate_default=True)

    @field_validator("createdAt", "updatedAt")
    @classmethod
    def _datetime_now_validator(cls, value):
        return datetime.now(UTC)

    status: StatusType = Field(StatusType.SCHEDULED)


class ManualUploadConfigurationDB(BaseModel):
    """Manual Upload configuration database model."""

    name: str = Field(...)
    files: List[FileStatus] = Field(
        ..., description="List of uploaded files with status"
    )
    destinationConfiguration: str = Field(None, description="Destination Configuration")
    classifierName: str = Field(None, description="Classifier Name")
    classifierType: ClassifierType = Field(
        ClassifierType.CUSTOM, description="Type of the classifier"
    )
    classifierID: str = Field(..., description="Classifier ID")
    trainingType: TrainingType = Field(
        TrainingType.POSITIVE, description="Indicates training type"
    )
    status: StatusType = Field(StatusType.PENDING, description="Indicates manual upload status for Imagess")
    errorState: ErrorState = Field(...)
    createdAt: Optional[datetime] = Field(None, validate_default=True)
    updatedAt: Optional[datetime] = Field(None, validate_default=True)
    lastUploadTime: Optional[datetime] = Field(None, validate_default=True)


class ManualUploadConfigurationOut(BaseModel):
    """Manual upload configuration out."""

    name: str = Field(...)
    destinationConfiguration: str = Field(None, description="Destination Configuration")
    classifierName: str = Field(None, description="Classifier Name")
    classifierType: ClassifierType = Field(
        ClassifierType.CUSTOM, description="Type of the classifier"
    )
    classifierID: str = Field(..., description="Classifier ID")
    trainingType: TrainingType = Field(
        TrainingType.POSITIVE, description="Indicates training type"
    )
    files: List[FileStatus] = Field(
        ..., description="List of uploaded files with status"
    )


class ManualUploadConfigurationUpdateIn(BaseModel):
    """Manual upload configuration update."""

    name: str = Field(...)
    status: StatusType = Field(StatusType.SCHEDULED)
