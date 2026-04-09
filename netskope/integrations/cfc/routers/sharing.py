"""Sharing related endpoints for CFC."""
import traceback
from datetime import datetime, UTC
from requests.exceptions import HTTPError
from typing import List

from fastapi import APIRouter, HTTPException, Query, Security

from netskope.common.api.routers.auth import get_current_user
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import User, TenantDB
from netskope.common.utils import Collections, DBConnector, Logger
from netskope.integrations.cfc.models import (
    SharingDB,
    SharingIn,
    SharingOut,
    SharingUpdate,
    SharingDelete,
    StatusType,
    ClassifierType,
    Classifier
)
from netskope.integrations.cfc.tasks.plugin_lifecycle_task import execute_plugin
from netskope.integrations.cfc.utils import NetskopeClientCFC
from netskope.integrations import trim_space_parameters_fields
from pymongo import ReturnDocument

router = APIRouter()
logger = Logger()
connector = DBConnector()


@router.get(
    "/classifiers",
    tags=["CFC Sharing"],
    description="Get list of all the classifiers for provided destination configuration.",
)
async def get_classifiers(
    destionationConfiguration: str,
    _: User = Security(get_current_user, scopes=["cfc_read"])
):
    """Get list of all the classifiers for provided destination configuration.

    Args:
        destionation_configuration (str): Destination configuration.
        _ (User, optional): Current user.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        Dict[str, List[Classifier]]: List of custom and predefined classifiers.
    """
    configuration = connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
        {"name": destionationConfiguration}
    )
    if configuration is None:
        raise HTTPException(400, "Destination configuration does not exist.")

    tenant = configuration.get("tenant")

    if tenant is None:
        raise HTTPException(400, "Destination configuration does not have tenant specified.")

    tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
        {"name": tenant}
    )
    tenant = TenantDB(**tenant)

    netskope_client = NetskopeClientCFC(
        api_token_v2=tenant.parameters.get("v2token"),
        tenant_base_url=tenant.parameters.get("tenantName"),
    )

    classifiers = {
        ClassifierType.CUSTOM: list(),
        # ClassifierType.PREDEFINED: list()
    }

    try:
        custom_classifiers = netskope_client.all_custom_classifiers(
            offset=0, limit=0, customOnly=True
        )["customClassifiers"]

        for classifier in custom_classifiers:
            if classifier["type"] == "image" and classifier["status"] == "ready":
                classifiers[ClassifierType.CUSTOM].append(Classifier(
                    name=classifier["name"],
                    type=ClassifierType.CUSTOM,
                    id=classifier["id"]
                ))

        # predefined_classifiers = netskope_client.all_predefined_classifiers()["predefinedClassifiers"]["classifiers"]

        # for classifier in predefined_classifiers:
        #     if classifier["model_type"] == "image":
        #         classifiers[ClassifierType.PREDEFINED].append(Classifier(
        #             name=classifier["label"],
        #             type=ClassifierType.PREDEFINED,
        #             id=classifier["id"]
        #         ))

        return classifiers
    except HTTPError as error:
        logger.error(
            "Error occurred while fetching classifiers.",
            details=traceback.format_exc(),
            error_code="CFC_1018",
        )
        raise HTTPException(
            400, "Connection error occurred with a tenant while fetching classifiers."
        ) from error
    except Exception as error:
        logger.error(
            "Error occurred while fetching classifiers.",
            details=traceback.format_exc(),
            error_code="CFC_1044",
        )
        raise HTTPException(
            500, "Error occurred while fetching classifiers."
        ) from error


@router.get(
    "/sharings",
    tags=["CFC Sharing"],
    description="Get list of all the sharing configurations."
)
async def get_sharings(
    _: User = Security(get_current_user, scopes=["cfc_read"])
) -> List[SharingOut]:
    """Get list of configured sharing.

    Returns:
        List[SharingOut]: List configured sharing.
    """
    return connector.collection(Collections.CFC_SHARING).find(
        {}
    ).sort("createdAt", -1)


@router.post(
    "/sharing",
    tags=["CFC Sharing"],
    description="Create new sharing configuration for CFC.",
)
async def create_sharing(
    sharing_conf: SharingIn,
    user: User = Security(get_current_user, scopes=["cfc_write"])
) -> SharingOut:
    """Create new sharing for CFC.

    Args:
        sharing_conf (SharingIn): Sharing to be created.
        user (User) = Authenticated User.


    Raises:
        HTTPException: In case of validation failures.

    Returns:
        SharingOut: Newly created sharing configuration.
    """
    try:
        sharing_db_dict = connector.collection(
            Collections.CFC_SHARING
        ).find_one(
            {
                "sourceConfiguration": sharing_conf.sourceConfiguration,
                "destinationConfiguration": sharing_conf.destinationConfiguration
            }
        )
        if sharing_db_dict:
            raise HTTPException(
                400,
                f"Sharing configuration with source as '{sharing_conf.sourceConfiguration}' and "
                f"destination as '{sharing_conf.destinationConfiguration}' is already configured."
            )
        new_sharing = SharingDB(
            **(sharing_conf.model_dump()),
            errorState={},
            createdBy=user.username,
            updatedBy=user.username,
        )
        connector.collection(Collections.CFC_SHARING).insert_one(
            new_sharing.model_dump()
        )
        logger.debug(
            f"CFC sharing configuration successfully created for Source: '{sharing_conf.sourceConfiguration}' "
            f"and Destination: '{sharing_conf.destinationConfiguration}'."
        )
    except HTTPException as error:
        raise HTTPException(
            error.status_code,
            error.detail
        )
    except Exception as error:
        logger.error(
            "Error occurred while creating a new sharing.",
            details=traceback.format_exc(),
            error_code="CFC_1017",
        )
        raise HTTPException(
            500, "Error occurred while creating a new sharing."
        ) from error
    return new_sharing.model_dump()


@router.patch(
    "/sharing",
    tags=["CFC Sharing"],
    description="Update sharing configuration for CFC.",
)
async def update_sharing(
    sharing_conf: SharingUpdate,
    user: User = Security(get_current_user, scopes=["cfc_write"])
) -> SharingOut:
    """Update sharing for CFC.

    Args:
        sharing_conf (SharingUpdate): Updated sharing configuration.
        user (User) = Authenticated User.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        SharingOut: Updated sharing configuration.
    """
    try:
        # to trim extra spaces for parameters fields.
        trim_space_parameters_fields(sharing_conf)
        update_payload = sharing_conf.model_dump(exclude_none=True)
        existing_sharing_dict = connector.collection(
            Collections.CFC_SHARING
        ).find_one(
            {
                "sourceConfiguration": sharing_conf.sourceConfiguration,
                "destinationConfiguration": sharing_conf.destinationConfiguration,
            }
        )
        if not existing_sharing_dict:
            raise HTTPException(
                400,
                f"Sharing does not exist for Source: '{sharing_conf.sourceConfiguration}' and "
                f"Destination: '{sharing_conf.destinationConfiguration}'."
            )

        update_payload["updatedBy"] = user.username
        update_payload["updatedAt"] = datetime.now(UTC)
        updated_sharing_from_db = connector.collection(
            Collections.CFC_SHARING
        ).find_one_and_update(
            {"_id": existing_sharing_dict["_id"]},
            {"$set": update_payload},
            return_document=ReturnDocument.AFTER,
        )
        if not updated_sharing_from_db:
            raise HTTPException(500, "Error occurred while updating the sharing configuration.")
        updated_sharing = SharingDB(**updated_sharing_from_db)
        logger.debug(
            f"CFC sharing configuration successfully updated for Source: '{updated_sharing.sourceConfiguration}' "
            f"and Destination: '{updated_sharing.destinationConfiguration}'."
        )
    except HTTPException as error:
        raise HTTPException(
            error.status_code,
            error.detail
        )
    except Exception as error:
        logger.error(
            "Error occurred while updating the sharing configuration.",
            details=traceback.format_exc(),
            error_code="CFC_1045",
        )
        raise HTTPException(
            500, "Error occurred while updating the sharing configuration."
        ) from error
    return updated_sharing.model_dump()


@router.delete(
    "/sharing",
    tags=["CFC Sharing"],
    description="Delete sharing configuration for CFC.",
)
async def delete_sharing(
    sharing: SharingDelete,
    _: User = Security(get_current_user, scopes=["cfc_write"])
):
    """Delete a sharing for CFC.

    Args:
        sharing (SharingDelete): Sharing to be deleted.

    Returns:
        dict: Deletion success.
    """
    connector.collection(Collections.CFC_SHARING).delete_one(
        {
            "sourceConfiguration": sharing.sourceConfiguration,
            "destinationConfiguration": sharing.destinationConfiguration
        }
    )
    logger.debug(
        f"CFC sharing configuration successfully deleted for Source: '{sharing.sourceConfiguration}' "
        f"and Destination: '{sharing.destinationConfiguration}'."
    )
    return {"success": True}


@router.post(
    "/sharing/sync",
    tags=["CFC Sharing"],
    description="Sync sharing for CFC.",
)
async def sync_sharing(
    sourceConfiguration: str = Query(...),
    destinationConfiguration: str = Query(...),
    _: User = Security(get_current_user, scopes=["cfc_write"])
):
    """Sync sharing.

    Args:
        sourceConfiguration (str, optional): Source of sharing.
        destinationConfiguration (str, optional): Destination of sharing.

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        dict: Sync success.
    """
    sharing_dict = connector.collection(Collections.CFC_SHARING).find_one(
        {
            "sourceConfiguration": sourceConfiguration,
            "destinationConfiguration": destinationConfiguration
        }
    )
    if sharing_dict is None:
        raise HTTPException(400, "Requested CFC sharing configuration does not exist.")
    sharing_dict = SharingDB(**sharing_dict)
    applicable_sharings = connector.collection(Collections.CFC_SHARING).find(
        {
            "sourceConfiguration": sourceConfiguration
        }
    )
    sharing_current_statuses = [SharingDB(**sharing).status for sharing in applicable_sharings]
    if set([StatusType.GENERATING_HASH, StatusType.UPLOADING_HASH]).intersection(set(sharing_current_statuses)):
        raise HTTPException(
            400,
            f"Sharing process for source configuration '{sourceConfiguration}'"
            f" is already in progress."
            " Please try the sync operation after some time."
        )
    logger.debug(
        f"Sync with CFC sharing for Source: '{sourceConfiguration}' "
        f" is triggered."
    )

    execute_celery_task(
        execute_plugin.apply_async,
        "cfc.execute_plugin",
        with_locks=True,
        args=[sourceConfiguration],
    )
    return {"success": True}
