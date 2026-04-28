"""Provides utility methods for csv upload."""

import re
from datetime import datetime, UTC

from netskope.integrations.cfc.models.image_metadata import ImageMetadataDB
from netskope.common.utils import Collections, DBConnector
from .netskope_client import NetskopeClientCFC
from .constants import (
    REGEX_FOR_ZIP_FILE_PATH_FROM_RESPONSE,
    COMPRESSED_EXTENSION_SUPPORTED_FOR_REGEX,
)


class CFCFileUtils:
    """Manual CSV Upload Class."""

    connector = DBConnector()

    @staticmethod
    def add_image_data(
        connector: DBConnector,
        file_name: str,
        sourceType: str,
        source: str,
        source_id: str,
        size: int,
        last_modified: datetime,
        extension: str,
        path: str,
        sharedWith: list,
    ):
        """Add image data."""
        image_data = ImageMetadataDB(
            file=file_name,
            sourcePlugin=source,
            sourcePluginID=source_id,
            sourceType=sourceType,
            fileSize=size,
            last_modified=last_modified,
            lastFetched=datetime.now(UTC),
            dirUuid="1",
            extension=extension,
            path=path,
            sharedWith=sharedWith,
        )
        connector.collection(Collections.CFC_IMAGES_METADATA).update_one(
            {"file": file_name, "sourcePlugin": source, "path": path},
            {"$set": image_data.model_dump()},
            upsert=True,
        )

    @staticmethod
    def update_file_status_in_configuration(
        configuration_name: str, file_name: str, status: str
    ):
        """
        Update file status in the manual upload configuration.

        Args:
            configuration_name (str): The name of the configuration.
            file_name (str): The name of the file.
            status (str): The new status of the file.
        """
        CFCFileUtils.connector.collection(
            Collections.CFC_MANUAL_UPLOAD_CONFIGURATIONS
        ).update_one(
            {"name": configuration_name, "files.file_name": file_name},
            {
                "$set": {
                    "files.$.status": status,
                    "files.$.updatedAt": datetime.now(UTC),
                }
            },
        )

    @staticmethod
    def update_file_status(
        configuration_name,
        file_name,
        id,
        destination_plugin_name: str,
        classifier_id: str,
        classifier_name: str,
        training_type: str = "positive",
        status: str = "uploading_hash",
        update_last_shared: bool = False,
    ):
        """
        Update file status in both image metadata and configuration.

        Args:
            configuration_name (str): Name of the configuration.
            file_name (str): Name of the file.
            destination_plugin_name (str): Name of the destination plugin.
            classifier_id (str): ID of the classifier.
            classifier_name (str): Name of the classifier.
            training_type (str): Type of the training either positive or negative.
            status (str): Status of the file.
            update_last_shared (bool): Whether to update last shared date. Defaults to False.
        """
        CFCFileUtils.upsert_image_destination(
            id,
            destination_plugin_name,
            classifier_id,
            classifier_name,
            training_type,
            status,
            update_last_shared,
        )
        CFCFileUtils.update_file_status_in_configuration(
            configuration_name, file_name, status
        )

    @staticmethod
    def update_files_status(
        configuration_name,
        files,
        destination_plugin_name: str,
        classifier_id: str,
        classifier_name: str,
        training_type: str = "positive",
        status: str = "uploading_hash",
        update_last_shared: bool = False,
    ):
        """
        Update file status in both image metadata and configuration.

        Args:
            configuration_name (str): Name of the configuration.
            files (list): List of files to update.
            destination_plugin_name (str): Name of the destination plugin.
            classifier_id (str): ID of the classifier.
            classifier_name (str): Name of the classifier.
            training_type (str): Type of the training. Defaults to "positive".
            status (str): Status of the file. Defaults to "uploading_hash".
            update_last_shared (bool): Whether to update last shared date. Defaults to False.
        """
        for file in files:
            CFCFileUtils.upsert_image_destination(
                id=file["_id"],
                destination_plugin_name=destination_plugin_name,
                classifier_id=classifier_id,
                classifier_name=classifier_name,
                training_type=training_type,
                status=status,
                update_last_shared=update_last_shared,
            )
            CFCFileUtils.update_file_status_in_configuration(
                configuration_name,
                file["path"] if file["path"] else file["file"],
                status,
            )

    @staticmethod
    def upsert_image_destination(
        id: str,
        destination_plugin_name: str,
        classifier_id: str,
        classifier_name: str,
        training_type: str,
        status: str,
        update_last_shared: bool = True,
    ):
        """Update or insert details about destination in image data.

        Args:
            id (str): ID of the image.
            destination_plugin_name (str): Name of the destination plugin.
            classifier_id (str): ID of the classifier.
            classifier_name (str): Name of the classifier.
            training_type (str): Type of the training either positive or negative.
            status (str): Status of the file.
            update_last_shared (bool): Whether to update last shared date. Defaults to True.
        """
        if not CFCFileUtils.connector.collection(
            Collections.CFC_IMAGES_METADATA
        ).find_one({"_id": id}):
            raise ValueError(
                "No image metadata found to add or update its destinations."
            )
        result = CFCFileUtils.connector.collection(
            Collections.CFC_IMAGES_METADATA
        ).update_one(
            {
                "_id": id,
                "sharedWith": {
                    "$elemMatch": {
                        "destinationPlugin": destination_plugin_name,
                        "classifierID": classifier_id,
                        "trainingType": training_type,
                    }
                },
            },
            {
                "$set": {
                    **(
                        {"sharedWith.$.lastShared": datetime.now(UTC)}
                        if update_last_shared
                        else {}
                    ),
                    "sharedWith.$.status": status,
                    "sharedWith.$.classifierName": classifier_name,
                }
            },
        )

        if result.modified_count < 1:
            CFCFileUtils.connector.collection(
                Collections.CFC_IMAGES_METADATA
            ).update_one(
                {"_id": id},
                {
                    "$push": {
                        "sharedWith": {
                            "destinationPlugin": destination_plugin_name,
                            "classifierID": classifier_id,
                            "classifierName": classifier_name,
                            "trainingType": training_type,
                            "status": status,
                            "lastShared": (
                                datetime.now(UTC) if update_last_shared else None
                            ),
                        }
                    }
                },
            )

    @staticmethod
    def calculate_status_from_files(files: list) -> str:
        """Calculate overall status from individual file statuses.

        Args:
            files (list): List of file objects with 'status' field.

        Returns:
            str: Calculated status - 'success', 'partial_success', or 'failed'.
        """
        from netskope.integrations.cfc.models import StatusType

        if not files:
            return StatusType.FAILED.value

        statuses = [f.get("status") for f in files]
        success_statuses = {StatusType.SUCCESS.value, StatusType.COMPLETED.value}

        if all(status_value in success_statuses for status_value in statuses):
            return StatusType.SUCCESS.value
        elif any(status_value in success_statuses for status_value in statuses):
            return StatusType.PARTIAL_SUCCESS.value
        else:
            return StatusType.FAILED.value

    @staticmethod
    def get_sharing_status_for_destination(source_config_id: str, destination_name: str) -> str:
        """Determine the overall sharing status for a specific destination plugin."""
        from netskope.integrations.cfc.models import StatusType

        # Check if any file has at least one non-success entry for this destination.
        has_failed = (
            CFCFileUtils.connector.collection(
                Collections.CFC_IMAGES_METADATA
            ).find_one(
                {
                    "sourcePluginID": source_config_id,
                    "sourceType": "plugin",
                    "sharedWith": {
                        "$elemMatch": {
                            "destinationPlugin": destination_name,
                            "status": StatusType.FAILED.value,
                        }
                    }
                }
            )
            is not None
        )

        # Check if any file has at least one success entry for this destination.
        has_success = (
            CFCFileUtils.connector.collection(
                Collections.CFC_IMAGES_METADATA
            ).find_one(
                {
                    "sourcePluginID": source_config_id,
                    "sourceType": "plugin",
                    "sharedWith": {
                        "$elemMatch": {
                            "destinationPlugin": destination_name,
                            "status": {"$in": [StatusType.SUCCESS.value, StatusType.COMPLETED.value]},
                        }
                    }
                }
            )
            is not None
        )

        if has_success and not has_failed:
            return StatusType.SUCCESS.value
        elif has_success and has_failed:
            return StatusType.PARTIAL_SUCCESS.value
        else:
            return StatusType.FAILED.value

    @staticmethod
    def categorize_files(files, invalid_files, new_file_path_mappings):
        """Categorize files into valid and invalid based on validation results.

        Args:
            files (list): List of file metadata objects
            invalid_files (list): List of invalid files with status information
            new_file_path_mappings (dict): Mapping of filenames to file objects

        Returns:
            tuple: (valid_file_metadata, invalid_file_metadata, invalid_same_files, invalid_files_stats)
        """
        COMPILED_REGEX_FOR_ZIP_FILE_PATH_FROM_RESPONSE = re.compile(
            REGEX_FOR_ZIP_FILE_PATH_FROM_RESPONSE.format(
                "|".join(COMPRESSED_EXTENSION_SUPPORTED_FOR_REGEX)
            )
        )
        invalid_file_metadata = []
        valid_file_metadata = {file["_id"]: file for file in files}
        invalid_same_files = []
        invalid_files_stats = {
            NetskopeClientCFC.MAX_STATUS: 0,
            NetskopeClientCFC.SAME_HASH_STATUS: 0,
            NetskopeClientCFC.INVALID_FILE_STATUS: 0,
        }
        any_sub_file_available = False
        if invalid_files is None:
            valid_file_metadata = {}
            invalid_file_metadata = files
        else:
            for invalid_file in invalid_files:
                file_obj = new_file_path_mappings.get(invalid_file["filename"])
                if not file_obj:
                    matched = COMPILED_REGEX_FOR_ZIP_FILE_PATH_FROM_RESPONSE.match(
                        invalid_file["filename"]
                    )
                    if not matched:
                        continue
                    file_obj = new_file_path_mappings.get(matched.group(1))
                    if not file_obj:
                        continue
                    if matched.group(3):
                        any_sub_file_available = True
                if file_obj["_id"] not in valid_file_metadata:
                    continue
                valid_file_metadata.pop(file_obj["_id"])
                if invalid_file["status"] == NetskopeClientCFC.SAME_HASH_STATUS:
                    invalid_same_files.append(file_obj)
                else:
                    invalid_file_metadata.append(file_obj)
                if invalid_file["status"] == NetskopeClientCFC.MAX_STATUS:
                    invalid_files_stats[NetskopeClientCFC.MAX_STATUS] += 1
                elif invalid_file["status"] == NetskopeClientCFC.SAME_HASH_STATUS:
                    invalid_files_stats[NetskopeClientCFC.SAME_HASH_STATUS] += 1
                elif invalid_file["status"] == NetskopeClientCFC.INVALID_FILE_STATUS:
                    invalid_files_stats[NetskopeClientCFC.INVALID_FILE_STATUS] += 1
        return (
            valid_file_metadata,
            invalid_file_metadata,
            invalid_same_files,
            invalid_files_stats,
            any_sub_file_available,
        )
