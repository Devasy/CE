"""Sharing related models for CFC."""
import traceback

from datetime import datetime, UTC
from typing import List, Optional, Union

from pydantic import BaseModel, Field, FieldValidationInfo, field_validator

from netskope.common.models import TenantDB

from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    SecretDict
)

from netskope.integrations.cfc.utils import NetskopeClientCFC

from .configurations import ConfigurationDB
from .business_rule import Action
from .task_status import StatusType
from .classifier import ClassifierType, TrainingType

connector = DBConnector()
helper = PluginHelper()
logger = Logger()


@classmethod
def validate_sharing(
    cls, value: str, info: FieldValidationInfo
):
    """Validate sharing configuration."""
    sourceConfiguration = value
    destinationConfiguration = info.data["destinationConfiguration"]

    if sourceConfiguration == destinationConfiguration:
        raise ValueError("Destination configuration can not be same as source configuration.")

    src_config = connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
        {"name": sourceConfiguration}
    )
    if not src_config:
        raise ValueError(f"CFC configuration {sourceConfiguration} does not exist.")
    src_config = ConfigurationDB(**src_config)
    PluginClass = helper.find_by_id(src_config.plugin)
    if not PluginClass:
        raise ValueError(f"Plugin with id='{src_config.plugin}' does not exist.")
    metadata = PluginClass.metadata
    if not metadata.get("pull_supported"):
        raise ValueError(f"Data pull is not supported for configuration '{sourceConfiguration}'.")

    dest_config = connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
        {"name": destinationConfiguration}
    )
    if not dest_config:
        raise ValueError(f"CFC configuration '{destinationConfiguration}' does not exist.")
    dest_config = ConfigurationDB(**dest_config)
    PluginClass = helper.find_by_id(dest_config.plugin)
    if not PluginClass:
        raise ValueError(f"Plugin with id='{dest_config.plugin}' does not exist.")
    metadata = PluginClass.metadata
    if not metadata.get("push_supported"):
        raise ValueError(
            f"Data push is not supported for configuration '{destinationConfiguration}'."
        )
    return value


@classmethod
def validate_actions(
    cls, values: list, info: FieldValidationInfo,
):
    """Validate sharing configuration actions."""
    destinationConfiguration = info.data["destinationConfiguration"]
    dest_config = connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
        {"name": destinationConfiguration}
    )
    if not dest_config:
        raise ValueError(f"CFC configuration '{destinationConfiguration}' does not exist.")
    dest_config = ConfigurationDB(**dest_config)
    actions = values
    PluginClass = helper.find_by_id(dest_config.plugin)
    plugin = PluginClass(
        dest_config.name,
        SecretDict(dest_config.parameters),
        dest_config.storage,
        dest_config.checkpoint,
        logger,
    )
    if not actions:
        raise ValueError("At least one action is must")
    for action in actions:
        result = plugin.validate_action(action)
        connector.collection(Collections.CFC_CONFIGURATIONS).update_one(
            {"name": destinationConfiguration},
            {"$set": {"storage": dest_config.storage}}
        )
        if not result.success:
            raise ValueError(result.message)
    return values


@classmethod
def validate_duplicate_mappings(
    cls, value: list
):
    """Validate duplicate business rule classifier mapping."""
    mappings = set()
    for mapping in value:
        mapping_tuple = tuple((mapping.businessRule, mapping.classifierID, mapping.trainingType))
        if mapping_tuple in mappings:
            raise ValueError(
                f"Mapping with Business Rule: '{mapping.businessRule}' and Classifier: '{mapping.classifierName}' "
                f"with training type '{mapping.trainingType.value}' is already exists."
            )
        mappings.add(mapping_tuple)
    return value


class ErrorState(BaseModel):
    """Sharing Error state model."""

    error: bool = Field(
        False,
        description="Indicates there an error"
    )
    errorMessage: str = Field(
        "",
        description="Error message to show"
    )


class Mapping(BaseModel):
    """Business Rule and classifier mapping model."""

    businessRule: str = Field(
        ...,
        description="Name of the business rule."
    )

    @field_validator("businessRule")
    @classmethod
    def _validate_business_rule(cls, value: str):
        """Validate that the business rule exists."""
        if (
            connector.collection(Collections.CFC_BUSINESS_RULES).find_one({"name": value})
            is None
        ):
            raise ValueError("No business rule with this name exists.")
        return value

    classifierName: Optional[str] = Field(
        None,
        description="Classifier Name"
    )
    classifierType: ClassifierType = Field(
        ClassifierType.CUSTOM,
        description="Type of the classifier"
    )
    classifierID: Union[str, None] = Field(
        None,
        description="Classifier ID"
    )
    # preDefinedClassifierID: Optional[str] = Field(
    #     None,
    #     description="Classifier ID received as a id for a predefined classifier."
    # )
    trainingType: TrainingType = Field(
        TrainingType.POSITIVE,
        description="Indicates training type"
    )
    errorState: Optional[ErrorState] = Field(
        {},
        description="Indicates error in mapping."
    )


@classmethod
def validate_classifiers(cls, values: List[Mapping], info: FieldValidationInfo):
    """Validate classifier in each mapping."""
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
        mappings = values.copy()
        for mapping in mappings:
            if mapping.classifierType == ClassifierType.CUSTOM:
                classifier = netskope_client.classifier_by_id(class_id=mapping.classifierID)
                if not classifier:
                    raise ValueError(
                        f"Classifier: '{mapping.classifierName}' does not exists or deleted from the tenant."
                    )
                mapping.classifierName = classifier["name"]
            # elif mapping.classifierType == ClassifierType.PREDEFINED:
            #     if mapping.trainingType == TrainingType.POSITIVE:
            #         raise ValueError("Predefined Classifiers can be trained as Negative only.")
            #     overlay_classifier, predefined_classifier = netskope_client.get_or_create_overlay_classifier(
            #         mapping.classifierID
            #     )
            #     mapping.preDefinedClassifierID = overlay_classifier["id"]
            #     mapping.classifierName = predefined_classifier["label"]
        return mappings
    except ValueError as error:
        raise error from error
    except Exception:
        logger.error(
            "Error occurred while validating the classifiers.",
            details=traceback.format_exc(),
            error_code="CFC_1032"
        )
        raise ValueError("Failed to validate the classifiers.")


class SharingIn(BaseModel):
    """Incoming sharing model for create."""

    destinationConfiguration: str = Field(
        ...,
        description="Name of destination plugin."
    )
    actions: List[Action] = Field(
        ...,
        description="List of Action."
    )
    sourceConfiguration: str = Field(
        ...,
        description="Name of source plugin."
    )

    _validate_sharing = field_validator("sourceConfiguration")(validate_sharing)
    _validate_actions = field_validator("actions")(validate_actions)

    mappings: List[Mapping] = Field(
        ...,
        description="List of Business Rule - Classifier mapping."
    )

    _validate_mapping = field_validator("mappings")(validate_duplicate_mappings)
    _validate_classifiers = field_validator("mappings")(validate_classifiers)

    status: StatusType = Field(
        StatusType.SCHEDULED,
        description="Indicates status of sharing."
    )
    createdAt: Optional[datetime] = Field(
        default=None,
        validate_default=True,
        description="Indicates creation time of sharing configuration."
    )
    updatedAt: Optional[datetime] = Field(
        default=None,
        validate_default=True,
        description="Indicates update time of sharing configuration."
    )

    @field_validator("createdAt", "updatedAt")
    @classmethod
    def _datetime_now_validator(cls, v):
        return datetime.now(UTC)

    sharedAt: Optional[datetime] = Field(
        None,
        description="Indicates last shared time of sharing configuration."
    )


class SharingUpdate(BaseModel):
    """Incoming sharing model for update."""

    destinationConfiguration: str = Field(
        ...,
        description="Name of destination plugin."
    )
    actions: Union[List[Action], None] = Field(
        None,
        description="List of Action."
    )
    sourceConfiguration: str = Field(
        ...,
        description="Name of source plugin."
    )
    _validate_actions = field_validator("actions")(validate_actions)

    mappings: Union[List[Mapping], None] = Field(
        None,
        description="List of Business Rule - Classifier mapping."
    )

    _validate_mapping = field_validator("mappings")(validate_duplicate_mappings)
    _validate_classifiers = field_validator("mappings")(validate_classifiers)


class SharingDelete(BaseModel):
    """Sharing configuration delete model."""

    sourceConfiguration: str = Field(
        ...,
        description="Name of source plugin."
    )
    destinationConfiguration: str = Field(
        ...,
        description="Name of destination plugin."
    )

    @field_validator("destinationConfiguration")
    @classmethod
    def _validate_exists(cls, v: str, info: FieldValidationInfo):
        """Validate that the sharing exists."""
        sourceConfiguration = info.data["sourceConfiguration"]
        destinationConfiguration = v
        if (
            connector.collection(Collections.CFC_SHARING).find_one(
                {
                    "sourceConfiguration": sourceConfiguration,
                    "destinationConfiguration": destinationConfiguration
                }
            )
            is None
        ):
            raise ValueError(
                f"Sharing does not exist for Source: '{sourceConfiguration}' and "
                f"Destination: '{destinationConfiguration}'."
            )
        return v


class SharingOut(BaseModel):
    """The outgoing sharing model."""

    sourceConfiguration: str = Field(
        ...,
        description="Name of source plugin."
    )
    destinationConfiguration: str = Field(
        ...,
        description="Name of destination plugin."
    )
    actions: List[Action] = Field(
        [],
        description="List of Action."
    )
    mappings: List[Mapping] = Field(
        [],
        description="Lis of Business Rule - Classifier mapping."
    )
    status: StatusType = Field(
        StatusType.SCHEDULED,
        description="Indicates status of sharing."
    )
    errorState: ErrorState = Field(
        ...,
        description="Indicates error in sharing."
    )
    createdAt: datetime
    createdBy: Optional[str] = Field(
        None,
        description="User who created sharing."
    )
    updatedAt: Optional[datetime]
    updatedBy: Optional[str] = Field(
        None,
        description="User who updated sharing."
    )
    sharedAt: Optional[datetime] = Field(
        None,
        description="Indicates last shared time of sharing configuration."
    )


class SharingDB(BaseModel):
    """Database sharing model."""

    sourceConfiguration: str = Field(...)
    destinationConfiguration: str = Field(...)
    actions: List[Action] = Field([])
    mappings: List[Mapping] = Field([])
    status: StatusType = Field(StatusType.SCHEDULED)
    errorState: ErrorState = Field(...)
    createdAt: datetime
    createdBy: Optional[str] = Field(None)
    updatedAt: datetime
    updatedBy: Optional[str] = Field(None)
    sharedAt: Optional[datetime] = Field(None)


class Classifier(BaseModel):
    """Classifier model."""

    name: str
    type: ClassifierType
    id: str
