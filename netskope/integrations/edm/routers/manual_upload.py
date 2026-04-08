"""Provides CSV upload related endpoints."""
import csv
import os
import shutil
import traceback
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, File, HTTPException, Security, UploadFile

from netskope.common.api.routers.auth import get_current_user
from netskope.common.celery.scheduler import execute_celery_task
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger
from netskope.integrations.edm.models import (
    ManualUploadConfigurationDB,
    ManualUploadConfigurationIn,
    ManualUploadSanitizationConfigurationIn,
    ManualUploadSanitizationConfigurationOut,
    StatusType
)
from netskope.integrations.edm.tasks.manual_upload_task import (
    execute_manual_upload_task,
)
from netskope.integrations.edm.utils import MANUAL_UPLOAD_PATH, ManualUploadManager, MANUAL_UPLOAD_PREFIX

from .configurations import _clean_sample_files

router = APIRouter(prefix="/manual_upload")
logger = Logger()
db_connector = DBConnector()


def _get_columns_name(csv_path):
    """Get column names from provided csv file.

    Args:
        csv_path (str): path of csv

    Returns:
        list: list of columns
    """
    with open(csv_path, "r", newline="") as file:
        # Create a CSV reader
        csv_reader = csv.reader(file)
        first_row = next(csv_reader, None)
        if first_row:
            return first_row
        else:
            return None


def _create_sample_csv_files(csv_file_path, num_rows=20):
    """
    Create a new CSV file from an existing CSV file with a specified number of rows.

    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to the output CSV file to be created.
        num_rows (int): Number of rows to copy from the input file to the output file.
    """
    try:
        input_file = csv_file_path

        csv_file_name = os.path.basename(csv_file_path)
        output_file = f"{os.path.dirname(csv_file_path)}/sample_{os.path.splitext(csv_file_name)[0]}.csv"
        with open(input_file, "r", newline="") as in_csvfile, open(
            output_file, "w", newline=""
        ) as out_csvfile:
            reader = csv.reader(in_csvfile)
            writer = csv.writer(out_csvfile)

            # Write the header row (optional, remove if not needed)
            header = next(reader)
            writer.writerow(header)

            # Write the specified number of rows
            for _ in range(num_rows):
                try:
                    row = next(reader)
                    writer.writerow(row)
                except StopIteration:
                    break
        return output_file
    except Exception as error:
        logger.error(
            message="Error occurred while creating a sample csv file from upload csv file for sanitization.",
            details=traceback.format_exc(),
        )
        raise error


def validate_csv_file_records(csv_file_path: str, record_count: int = 0) -> dict:
    """Validate the content of a CSV file.

    Args:
        csv_file_path (str): The file path to the CSV file to be validated.
        record_count (int, optional): The maximum number of records to validate. If set
            to a positive value, only the first 'record_count' records will be validated.
            If set to 0 (default), all records in the file will be validated.

    Returns:
        dict: A dictionary containing validation boolean and message in case of fail.
    """
    row_count = 0
    header = None

    try:
        with open(csv_file_path, "r", encoding="UTF-8") as csv_file_object:
            # Create a CSV reader object to iterate through the CSV file.
            csv_reader = csv.reader(csv_file_object)

            for row in csv_reader:
                row_count += 1

                # Check if 'record_count' is specified and if the row count exceeds the limit.
                if record_count and row_count > record_count + 1:
                    break

                # Handle the first row as the header row.
                if row_count == 1:
                    header = row
                    if any(cell.strip() == "" for cell in header):
                        return {
                            "validate": False,
                            "message": "Column name in provided file should not have an empty value.",
                        }
                    if len(header) > 25:
                        return {
                            "validate": False,
                            "message": "Maximum of 25 columns are allowed.",
                        }

                # Check if the current row has the same number of columns as the header row.
                if len(row) != len(header):
                    return {
                        "validate": False,
                        "message": f"Row '{row_count}' does not contain the correct number of columns.",
                    }

            # Check if at least 1 record is present in the CSV file.
            if row_count < 2:
                return {
                    "validate": False,
                    "message": "At least 1 record must be present in the CSV file in addition to header row.",
                }
        return {"validate": True}
    except Exception as error:
        logger.error(
            message=f"{MANUAL_UPLOAD_PREFIX} CSV File - {str(error)}",
            details=traceback.format_exc(),
        )
        raise error


def _validate_file_name_destination(configuration: ManualUploadConfigurationDB):
    """Validate file name and destination pair.

    Args:
        configuration (ManualUploadConfigurationDB): manual configuration to validate
    Tuple(bool, str, ManualUploadConfigurationDB): _description_
    """
    new_destination = list(configuration.sharedWith.keys())[0]
    csv_config = db_connector.collection(
        Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS
    ).find({"fileName": configuration.fileName})
    for config in csv_config:
        config_db = ManualUploadConfigurationDB(**config)
        destination = list(config_db.sharedWith.keys())[0]
        if new_destination == destination:
            if (
                config_db.status not in [StatusType.COMPLETED, StatusType.FAILED, StatusType.SCHEDULED]
            ):
                return False, (
                        "EDM manual hashing flow is already inprogress for same "
                        + "file name and destination pair. Please try after sometime."
                    ), None
            else:
                return True, "Overriding existing file name destination pair.", config_db
    return True, "Create new entry.", None


def _generate_manual_config_name():
    """Generate unique manual configuration name as uuid."""
    name = None
    while (
        db_connector.collection(Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS).find_one(
            {"name": name}
        )
        or name is None
    ):
        name = str(uuid4())
    return name


@router.post("/upload", tags=["EDM Manual Upload"], description="Manual Upload")
async def store_manual_upload(
    file: Annotated[UploadFile, File()],
    _: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Store uploaded CSV.

    Args:
        file (UploadFile): zip file containing EDM Hash

    Raises:
        HTTPException: exception indication unusual and invalid request
    """
    try:
        # validating file
        if file.content_type != "text/csv":
            raise HTTPException(
                400,
                "Invalid file type supported. Only CSV file type is supported.",
            )
        name = _generate_manual_config_name()
        file_name = file.filename
        destination_path = f"{MANUAL_UPLOAD_PATH}/{name}"

        manual_upload_object = ManualUploadManager(name=name, file_name=file_name, logger=logger)

        manual_upload_object.create_directory(destination_path)

        csv_path = f"{destination_path}/{file_name}"

        if file:
            with open(csv_path, "wb") as file_obj:
                shutil.copyfileobj(file.file, file_obj)

        result = validate_csv_file_records(csv_path)
        if not result.get("validate", False):
            if os.path.isfile(csv_path):
                shutil.rmtree(os.path.dirname(csv_path))
            raise HTTPException(
                status_code=400,
                detail=f"{result.get('message','')} Upload a valid csv.",
            )

        columns = _get_columns_name(csv_path)

        if columns:
            response = {"status": True, "message": "", "data": {"columns": columns, "name": name}}
        else:
            raise HTTPException(
                status_code=400,
                detail="Provided csv file does not have records. Upload a valid csv.",
            )

        return response
    except HTTPException as error:
        logger.debug(
            str(error)
        )
        raise error
    except Exception as error:
        logger.error(
            "Error occurred While storing uploaded csv.",
            details=traceback.format_exc(),
            error_code="EDM_1021",
        )
        raise HTTPException(
            500, "Error occurred While storing uploaded csv."
        ) from error


@router.post(
    "/sanitize",
    response_model=ManualUploadSanitizationConfigurationOut,
    tags=["EDM Manual Upload"],
    description="Manual Upload sanitize",
)
async def sanitized_uploaded_file(
    configuration: ManualUploadSanitizationConfigurationIn,
    _: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Create Sample size csv and sanitize sample file.

    Args:
        configuration (EDMSanitizationConfigurationIn): configuration provided in payload
        _ (User, optional): Login user. Defaults to Security(get_current_user, scopes=["edm_write"]).

    Raises:
        HTTPException: exception indication unusual and invalid request
    """
    try:
        csv_name = configuration.fileName
        csv_file_path = (
            f"{MANUAL_UPLOAD_PATH}/{configuration.name}/{csv_name}"
        )

        sample_csv_file = _create_sample_csv_files(csv_file_path)

        manual_upload_object = ManualUploadManager(
            name=configuration.name, file_name=csv_name,
            logger=logger, configuration=configuration.parameters
        )

        manual_upload_object.csv_sanitize(sample_csv_file, sample_data=True)

        return {
            "name": configuration.name,
            "fileName": configuration.fileName,
            "sanitizationStatus": True,
            "message": "Sanitization Done Successfully",
        }

    except Exception as error:
        logger.error(
            f"Error occurred while Sanitizing Data for {configuration.fileName} ({configuration.name})",
            details=traceback.format_exc(),
            error_code="EDM_1022",
        )
        raise HTTPException(500, "Error occurred while Sanitizing Data.") from error


@router.post(
    "/configuration",
    tags=["EDM Manual Upload"],
    description="Create Manual Upload configuration",
)
async def create_manual_upload_configuration(
    configuration: ManualUploadConfigurationIn,
    _: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Create configuration for manual csv upload.

    Args:
        configuration (CSVUploadConfigurationIn): configuration in payload
        _ (User, optional): User. Defaults to Security(get_current_user, scopes=["edm_write"]).

    Raises:
        HTTPException: exception indication unusual and invalid request
    """
    config_db = None
    try:
        config_db = ManualUploadConfigurationDB(**(configuration.model_dump()))
        result, message, existing_config = _validate_file_name_destination(
            configuration=config_db
        )
        if not result:
            raise HTTPException(
                400,
                detail=message
            )
        db_connector.collection(
            Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS
        ).update_one(
            {"name": config_db.name},
            {"$set": config_db.model_dump()},
            upsert=True,
        )
        _clean_sample_files(
            configuration_name=config_db.name, source_type="manual_upload"
        )

        kwargs = {
            "configuration_name": configuration.name,
            "shared_with": configuration.sharedWith,
            "file_name": configuration.fileName
        }
        # Here starting share data task
        execute_celery_task(
            execute_manual_upload_task.apply_async,
            "edm.execute_manual_upload_task",
            args=[],
            kwargs=kwargs,
        )

        _clean_sample_files(
            configuration_name=configuration.name, source_type="manual_upload"
        )

        return {"success": True}
    except HTTPException as error:
        logger.debug(
            str(error)
        )
        raise error
    except Exception as error:
        logger.error(
            "Error occurred while scheduling the uploaded csv configuration.",
            details=traceback.format_exc(),
            error_code="EDM_1023",
        )
        raise HTTPException(
            500, "Error occurred while scheduling the uploaded csv configuration."
        ) from error
