"""Task to age out expired EDM Hashes/Raw files."""

from __future__ import absolute_import, unicode_literals

import os
import shutil
import traceback
from datetime import datetime, UTC

from netskope.common.celery.main import APP
from netskope.common.models import SettingsDB
from netskope.common.utils import (Collections, DBConnector, Logger,
                                   integration, track)
from netskope.integrations.edm.utils import FILE_PATH, MANUAL_UPLOAD_PATH
db_connector = DBConnector()
logger = Logger()


def _delete_aged_files_from_directory(folder_path: str, settings: SettingsDB) -> int:
    """Delete aged files from given folder path.

    Args:
        folder_path (str): Folder path
        settings (SettingsDB): settings from database

    Returns:
        int: Number of files/folders deleted
    """
    # Check if the configuration directory exists
    deleted_files_count = 0
    if (os.path.exists(folder_path)
            and os.path.isdir(folder_path)):

        files = os.listdir(folder_path)

        # Iterate through files in the configuration directory
        for file in files:
            try:
                file_path = os.path.join(folder_path, file)
                last_modified_timestamp = os.path.getmtime(file_path)
                last_modified_time = datetime.fromtimestamp(last_modified_timestamp, tz=UTC)
                time_difference_in_days = (
                    datetime.now(UTC)
                    - last_modified_time
                ).days

                # Check if the file exceeds the cleanup threshold
                if time_difference_in_days > settings.edm.edmFilesCleanup:
                    deleted_files_count = deleted_files_count + 1
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    else:
                        shutil.rmtree(file_path)
            except Exception as error:
                logger.error(
                    f"Error: '{error}' occurred while cleaning up files.",
                    details=traceback.format_exc()
                )
                continue
    return deleted_files_count


@APP.task(name="edm.age_edm_hashes")
@integration("edm")
@track()
def age_edm_hashes():
    """Age out expired EDM Hashes/Raw files collected for EDM from the data directory."""
    try:
        settings = SettingsDB(
            **db_connector.collection(Collections.SETTINGS).find_one({})
        )
        deleted_files_count = 0

        # Retrieve all EDM configurations
        configurations = db_connector.collection(Collections.EDM_CONFIGURATIONS).find()
        configuration_names = []

        for configuration in configurations:
            configuration_name = configuration["name"]
            configuration_names.append(configuration_name)
            configuration_directory_path = os.path.join(FILE_PATH, configuration_name)
            try:
                new_deleted_files_count = _delete_aged_files_from_directory(
                    configuration_directory_path,
                    settings
                )
                deleted_files_count += new_deleted_files_count
            except Exception as error:
                logger.error(
                    f"Error: '{error}' occurred while cleaning up files for configuration {configuration_name}.",
                    details=traceback.format_exc()
                )
                continue
        # Check folders without any configuration
        if os.path.exists(FILE_PATH) and os.path.isdir(FILE_PATH):
            for folder in os.listdir(FILE_PATH):
                folder_path = os.path.join(FILE_PATH, folder)
                if os.path.isdir(folder_path) and folder not in configuration_names:
                    try:
                        shutil.rmtree(folder_path)
                    except Exception as error:
                        logger.error(
                            f"Error: '{error}' occurred while cleaning up files for folder {folder}.",
                            details=traceback.format_exc(),
                        )
                        continue
        # Retrieve all EDM Manual Upload CSV Configurations
        configurations = db_connector.collection(
            Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS
        ).find()
        configuration_names = []
        for configuration in configurations:
            configuration_name = configuration["name"]
            configuration_names.append(configuration_name)
            configuration_directory_path = os.path.join(
                MANUAL_UPLOAD_PATH, configuration_name
            )
            try:
                new_deleted_files_count = _delete_aged_files_from_directory(
                    configuration_directory_path, settings
                )
                deleted_files_count += new_deleted_files_count
            except Exception as error:
                logger.error(
                    f"Error: '{error}' occurred while cleaning up files for configuration {configuration_name}.",
                    details=traceback.format_exc(),
                )
                continue
        # Check folders without any configuration
        if os.path.exists(MANUAL_UPLOAD_PATH) and os.path.isdir(MANUAL_UPLOAD_PATH):
            for folder in os.listdir(MANUAL_UPLOAD_PATH):
                folder_path = os.path.join(MANUAL_UPLOAD_PATH, folder)
                if os.path.isdir(folder_path) and folder not in configuration_names:
                    try:
                        shutil.rmtree(folder_path)
                    except Exception as error:
                        logger.error(
                            f"Error: '{error}' occurred while cleaning up files for folder {folder}.",
                            details=traceback.format_exc(),
                        )
                        continue
        if deleted_files_count:
            logger.info((f"Deleted {deleted_files_count} files/directories "
                         "as part of the autmatic cleanup."))
    except Exception as error:
        logger.error(
            f"Error: '{error}' occurred while cleaning up EDM hashes.",
            details=traceback.format_exc()
        )
