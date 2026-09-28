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


# A record in one of these has already been claimed by the poller, so the
# transient "checking" marker must not drag it backwards every poll cycle.
POLLING_IN_PROGRESS_STATUSES = (
    StatusType.CHECKING_APPLY_STATUS,
    StatusType.APPLY_IN_PROGRESS,
)


def _change_the_file_source_status(
    fileSourceID: str,
    fileSourceType: EDMTaskType,
    status: StatusType,
    skip_if_status_in: tuple = (),
):
    """Set the status of the polled source and stamp updatedAt on real transitions.

    The status is only written when it actually differs from the stored one, so
    `updatedAt` reflects the last genuine status change and does not advance on
    every poll cycle. Mirrors the updatedAt handling in `utils/task_listing.py`.

    Args:
        fileSourceID (str): _id of the business rule or manual upload configuration.
        fileSourceType (EDMTaskType): whether the source is a plugin or a manual upload.
        status (StatusType): status to be set.
        skip_if_status_in (tuple, optional): leave the record untouched when its current
            status is one of these. Used to keep the transient CHECKING_APPLY_STATUS
            marker from moving a record that is already being polled.
    """
    if fileSourceType == EDMTaskType.MANUAL:
        collection = Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS
    elif fileSourceType == EDMTaskType.PLUGIN:
        collection = Collections.EDM_BUSINESS_RULES
    else:
        return False
    connector.collection(collection).update_one(
        {
            "_id": ObjectId(fileSourceID),
            "status": {"$nin": [status, *skip_if_status_in]},
        },
        {"$set": {"status": status, "updatedAt": datetime.now(UTC)}}
    )
    return True


def _get_status_for_source(apply_status: str, file_id: str = ""):
    """Map the tenant's apply status onto an EDM StatusType.

    Never returns None. `apply_status` comes straight off the tenant response,
    so an unknown value (a status added by a newer tenant release, or an
    explicit null, which `.get`'s default does not cover) must not reach the
    caller's `$set`: writing None would blank a non-Optional `status` field, so
    every later `GET /task_status/edm` would fail to build `EDMTask` and 500 the
    whole task list, and the record would never reach a terminal state so its
    EDM_HASHES_STATUS row would be polled forever.

    Args:
        apply_status (str): apply status as reported by the tenant.
        file_id (str, optional): staging file id, for log context.

    Returns:
        StatusType: mapped status; APPLY_IN_PROGRESS for anything unrecognised,
            which keeps the record polling until the tenant reports a terminal
            status.
    """
    if (
        apply_status == "pending" or
        apply_status == "inprogress"
    ):
        return StatusType.APPLY_IN_PROGRESS
    elif apply_status == "completed":
        return StatusType.COMPLETED
    elif apply_status == "error":
        return StatusType.FAILED
    logger.error(
        "Unexpected apply status reported for the uploaded EDM Hashes "
        f"for file id {file_id}. Treating it as apply in progress.",
        error_code="EDM_1047",
        details=(
            f"Received apply status: '{apply_status}'. Expected one of "
            "'pending', 'inprogress', 'completed' or 'error'."
        ),
    )
    return StatusType.APPLY_IN_PROGRESS


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
                skip_if_status_in=POLLING_IN_PROGRESS_STATUSES,
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
                apply_status = _get_status_for_source(
                    apply_status, hash_db.file_id
                )
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
