"""Provides CSV upload related endpoints."""
import csv
import os
import shutil
import traceback
from typing import Annotated
from uuid import uuid4
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Security, UploadFile, Query

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
from netskope.integrations.edm.utils.validators import validate_edm_filename

from .configurations import _clean_sample_files

router = APIRouter(prefix="/manual_upload")
logger = Logger()
db_connector = DBConnector()


def _get_columns_name(csv_path, delimiter=","):
    """Get column names from provided csv file.

    Args:
        csv_path (str): path of csv

    Returns:
        list: list of columns
    """
    with open(csv_path, "r", newline="") as file:
        # Create a CSV reader
        csv_reader = csv.reader(file, delimiter=delimiter)
        first_row = next(csv_reader, None)
        if first_row:
            return first_row
        else:
            return None


def _create_sample_csv_files(csv_file_path, delimiter=",", num_rows=20, remove_quotes=False):
    """Create a new CSV file from an existing CSV file with a specified number of rows.

    Opens files in binary mode (rb/wb) so the output is never re-quoted or modified
    by a csv.writer — mirroring EdmDataSanitizer.py's parseCSV approach.

    Args:
        csv_file_path (str): Path to the input CSV file.
        delimiter (str): CSV delimiter character.
        num_rows (int): Number of data rows to copy (excluding header).
        remove_quotes (bool): When True uses QUOTE_ALL reader mode, else QUOTE_NONE.
    """
    try:
        encoding = "utf-8"
        csv_file_name = Path(csv_file_path).name
        output_file = f"{Path(csv_file_path).parent}/sample_{csv_file_name}"
        quoting_mode = csv.QUOTE_ALL if remove_quotes else csv.QUOTE_NONE

        # Local generator: feeds decoded text lines to csv.reader while keeping
        # the original raw bytes available for lossless write-back (mirrors
        # EdmDataSanitizer.py's char_encoder + CURLINE pattern).
        cur_line_box = [None]  # mutable container so the loop below can read it

        def _char_encoder(binary_file):
            while True:
                line = binary_file.readline()
                if not line:
                    return
                cur_line_box[0] = line  # raw bytes snapshot of the current line
                yield line.decode(encoding)

        with open(csv_file_path, "rb") as in_csvfile, open(output_file, "wb") as out_csvfile:

            # Header: read raw bytes once, write them back untouched.
            hdr_raw = in_csvfile.readline()
            out_csvfile.write(hdr_raw)

            # Data rows: csv.reader parses fields; rows are written by rejoining
            # fields with the delimiter — no csv.writer, so quoting is never added.
            reader = csv.reader(_char_encoder(in_csvfile), delimiter=delimiter, quoting=quoting_mode)

            for i, row in enumerate(reader):
                if i >= num_rows:
                    break
                out_csvfile.write(delimiter.join(row).encode(encoding))
                out_csvfile.write(b"\n")

        return output_file
    except Exception as error:
        logger.error(
            message="Error occurred while creating a sample csv file from upload csv file for sanitization.",
            details=traceback.format_exc(),
        )
        raise error


def validate_csv_file_records(csv_file_path: str, delimiter=",", record_count: int = 0) -> dict:
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
            csv_reader = csv.reader(csv_file_object, delimiter=delimiter)

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
                    "message": "At least 1 record must be present in the file in addition to header row.",
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


def _generate_manual_config_name(delimiter: str = ","):
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
    delimiter: Annotated[str, Query()] = ",",
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
        if file.content_type not in ["text/csv", "text/plain"]:
            raise HTTPException(
                400,
                "Invalid file type supported. Only CSV and text file type is supported.",
            )
        # Validation on file extension
        file_path = Path(file.filename)
        if file_path.suffix.lower().replace(".", "") not in ["csv", "txt"]:
            raise HTTPException(
                400,
                "Invalid file type supported. Only CSV and text file type is supported.",
            )
        # Validate filename characters for EDM compatibility
        is_valid, error_msg = validate_edm_filename(file.filename)
        if not is_valid:
            raise HTTPException(400, error_msg)
        if len(delimiter) != 1:
            raise HTTPException(
                status_code=400,
                detail="Delimiter should be a single character.",
            )
        name = _generate_manual_config_name(delimiter)
        file_name = file.filename
        destination_path = f"{MANUAL_UPLOAD_PATH}/{name}"

        manual_upload_object = ManualUploadManager(
            name=name,
            file_name=file_name,
            logger=logger,
            configuration={"delimiter": delimiter},
        )

        manual_upload_object.create_directory(destination_path)

        csv_path = f"{destination_path}/{file_name}"

        if file:
            with open(csv_path, "wb") as file_obj:
                shutil.copyfileobj(file.file, file_obj)

        result = validate_csv_file_records(csv_path, delimiter=delimiter)
        if not result.get("validate", False):
            if os.path.isfile(csv_path):
                shutil.rmtree(os.path.dirname(csv_path))
            raise HTTPException(
                status_code=400,
                detail=f"{result.get('message','')} Upload a valid csv.",
            )

        columns = _get_columns_name(csv_path, delimiter=delimiter)

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


@router.get(
    "/columns",
    tags=["EDM Manual Upload"],
    description="Get updated columns for an already uploaded CSV using a delimiter",
)
async def get_uploaded_columns(
    name: Annotated[str, Query(...)],
    fileName: Annotated[str, Query(...)],
    delimiter: Annotated[str, Query()] = ",",
    _: User = Security(get_current_user, scopes=["edm_write"]),
):
    """Return columns detected from a previously uploaded CSV using the provided delimiter.

    Blocks when the configuration is in-progress.
    """
    try:
        # If configuration exists and is in-progress, block the request
        config = db_connector.collection(Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS).find_one({"name": name})
        if config:
            status = config.get("status")
            # Allow only when status is one of COMPLETED, FAILED, or SCHEDULED
            if status not in [StatusType.COMPLETED, StatusType.FAILED, StatusType.SCHEDULED]:
                raise HTTPException(
                    400,
                    detail="Operation not allowed while configuration is in-progress. Please try again later.",
                )

        csv_file_path = f"{MANUAL_UPLOAD_PATH}/{name}/{fileName}"
        if not os.path.isfile(csv_file_path):
            raise HTTPException(404, detail="Uploaded CSV not found.")

        # Validate the file content with the new delimiter
        if len(delimiter) != 1:
            raise HTTPException(
                status_code=400,
                detail="Delimiter should be a single character.",
            )
        result = validate_csv_file_records(csv_file_path, delimiter=delimiter)
        if not result.get("validate", False):
            raise HTTPException(
                status_code=400,
                detail=f"{result.get('message','')} Upload a valid csv.",
            )

        columns = _get_columns_name(csv_file_path, delimiter=delimiter)
        if not columns:
            raise HTTPException(
                status_code=400,
                detail="Provided csv file does not have records. Upload a valid csv.",
            )
        return {"status": True, "message": "", "data": {"columns": columns, "name": name}}
    except HTTPException as error:
        logger.debug(str(error))
        raise error
    except Exception as error:
        logger.error(
            "Error occurred while fetching columns for uploaded csv.",
            details=traceback.format_exc(),
            error_code="EDM_1046",
        )
        raise HTTPException(500, "Error occurred while fetching columns for uploaded csv.") from error


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

        sample_csv_file = _create_sample_csv_files(
            csv_file_path,
            delimiter=configuration.parameters.get("delimiter"),
            remove_quotes=configuration.parameters.get("remove_quotes", False),
        )

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
