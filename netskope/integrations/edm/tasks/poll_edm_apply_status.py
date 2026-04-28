"""EDM Hash Status Polling Tasks."""

import traceback
from datetime import datetime, UTC
from bson.objectid import ObjectId

from netskope.common.celery.main import APP
from netskope.common.utils import (
    Collections,
    DBConnector,
    Logger,
    integration,
    track,
    resolve_secret,
)
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.integrations.edm.models import (EDMTaskType, EDMHashesStatus, StatusType)
from netskope.integrations.edm.utils.edm.edm_uploader.edm_api_upload import StagingManager


connector = DBConnector()
logger = Logger()
helper = PluginHelper()


def sent_hashes_for_polling(
    fileSourceType: EDMTaskType,
    fileSourceID: str,
    tenant: str,
    file_id: str,
    upload_id: str,
):
    """
    Store EDM hashes for the polling task so polling task will pick hashes to check the apply status.

    **Note:** Call this method if and only  if hashes are uploaded and api call to apply hashes is successful.
    """
    db_dict = EDMHashesStatus(
        fileSourceType=fileSourceType,
        fileSourceID=fileSourceID,
        fileUploadedAtTenant=tenant,
        file_id=file_id,
        upload_id=upload_id,
        createdAt=datetime.now(UTC),
        updatedAt=datetime.now(UTC),
    )
    connector.collection(Collections.EDM_HASHES_STATUS).insert_one(
        db_dict.model_dump()
    )


def _change_the_file_source_status(
    fileSourceID: str,
    fileSourceType: EDMTaskType,
    status: StatusType,
):
    if fileSourceType == EDMTaskType.MANUAL:
        connector.collection(Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS).update_one(
            {"_id": ObjectId(fileSourceID)},
            {"$set": {"status": status}}
        )
    elif fileSourceType == EDMTaskType.PLUGIN:
        connector.collection(Collections.EDM_BUSINESS_RULES).update_one(
            {"_id": ObjectId(fileSourceID)},
            {"$set": {"status": status}}
        )
    else:
        return False
    return True


def _get_status_for_source(apply_status: str):
    if (
        apply_status == "pending" or
        apply_status == "inprogress"
    ):
        return StatusType.APPLY_IN_PROGRESS
    elif apply_status == "completed":
        return StatusType.COMPLETED
    elif apply_status == "error":
        return StatusType.FAILED


@APP.task(name="edm.poll_edm_hash_upload_status", acks_late=True)
@integration("edm")
@track()
def poll_edm_hash_upload_status():
    """EDM Hash upload status polling task."""
    hashes_dict = connector.collection(Collections.EDM_HASHES_STATUS).find({})
    provider_helper = PluginProviderHelper()
    for hash in hashes_dict:
        try:
            hash_db = EDMHashesStatus(
                **hash
            )
            _change_the_file_source_status(
                hash_db.fileSourceID,
                hash_db.fileSourceType,
                StatusType.CHECKING_APPLY_STATUS,
            )
            try:
                tenant = provider_helper.get_tenant_details(hash_db.fileUploadedAtTenant)
            except Exception:
                logger.error(
                    "Error occured while checking the status of the EDM hashes uploaded on"
                    f" {hash_db.fileUploadedAtTenant}. Tenant not found. "
                )
                continue
            staging_manager = StagingManager()
            staging_manager.set_server(
                tenant["parameters"]["tenantName"]
                .strip()
                .strip("/")
                .removeprefix("https://")
            )
            staging_manager.set_auth_token(
                resolve_secret(tenant["parameters"]["v2token"])
            )
            staging_manager.load_client()
            result, message, response = staging_manager.status(hash_db.file_id)
            if not result:
                logger.error(
                    "Error occurred while checking for the status of the uploaded EDM Hashes "
                    f"for file id {hash_db.file_id}.",
                    error_code="EDM_1039",
                    details=(
                        f"Tenant: '{hash_db.fileUploadedAtTenant}'\n"
                        f"Error: {message}"
                    )
                )
                continue
            apply_status = StatusType.COMPLETED
            if response:
                apply_status = response.get("apply_status", "pending")
                message = response["msg"]
                apply_status = _get_status_for_source(apply_status)
            _change_the_file_source_status(
                hash_db.fileSourceID,
                hash_db.fileSourceType,
                apply_status,
            )
            if apply_status in (StatusType.COMPLETED, StatusType.FAILED):
                connector.collection(Collections.EDM_HASHES_STATUS).delete_one(
                    {"_id": hash["_id"]}
                )
                if response:
                    del_result, del_message, _ = staging_manager.delete(hash_db.file_id)
                    if not del_result:
                        logger.error(
                            "Error occurred while cleaning the uploaded EDM Hash staging file "
                            f"for file id {hash_db.file_id}.",
                            error_code="EDM_1041",
                            details=(
                                f"Tenant: '{hash_db.fileUploadedAtTenant}'"
                                f"\nError: {del_message}"
                            )
                        )
                if apply_status == StatusType.FAILED:
                    logger.error(
                        "Error occurred while applying the uploaded EDM Hashes "
                        f"for file id {hash_db.file_id}",
                        error_code="EDM_1042",
                        details=(
                            f"Tenant: '{hash_db.fileUploadedAtTenant}'"
                            f"\nError: {message}"
                        )
                    )
        except Exception:
            logger.error(
                message=f"Error occurred while checking for the status of the uploaded EDM Hashes "
                f" on '{hash.get('fileUploadedAtTenant')}' tenant.",
                error_code="EDM_1040",
                details=traceback.format_exc()
            )
            continue
