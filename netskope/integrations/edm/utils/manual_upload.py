"""Provides utility methods for csv upload."""

import os
import shutil
import traceback
from copy import deepcopy

from netskope.integrations.edm.utils import CONFIG_TEMPLATE
from netskope.integrations.edm.utils.edm.hash_generator.edm_hash_generator import (
    generate_edm_hash,
)
from netskope.integrations.edm.utils.exceptions import (
    CustomException as CSVUploadException,
)

from .constants import (
    EDM_HASH_CONFIG,
    MANUAL_UPLOAD_PATH,
    MANUAL_UPLOAD_PREFIX
)
from .sanitization import run_sanitizer


class ManualUploadManager:
    """Manual CSV Upload Class."""

    def __init__(
        self, logger, name, file_name, configuration=None, storage=None
    ) -> None:
        """Init method."""
        self.name = name
        self.configuration = configuration or {}
        self.storage = storage or {}
        self.file_name = file_name
        self.file_path = f"{MANUAL_UPLOAD_PATH}/{self.name}/{self.file_name}"
        self.logger = logger

    @staticmethod
    def strip_args(data):
        """Strip arguments from left and right directions.

        Args:
            data (dict): Dict object having all the
            configuration parameters.
        """
        keys = data.keys()
        for key in keys:
            if isinstance(data[key], str):
                data[key] = data[key].strip()

    def create_directory(self, dir_path):
        """Create a directory at the specified path, including all necessary parent directories.

        Args:
            dir_path (string): The path of the directory to be created.

        Raises:
            OSError: If there's an issue creating the directory.

        """
        try:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
        except Exception as error:
            self.logger.error(
                message=(
                    f"{MANUAL_UPLOAD_PREFIX} Hash generation - "
                    "Error occurred while creating nested directories to store sanitized data."
                ),
                details=traceback.format_exc(),
            )
            raise error

    def get_field_indices(self, sanity_inputs):
        """Get the indices of fields based on normalization and sensitivity.

        Args:
            sanity_inputs (dict): Sanitization inputs with field info.

        Returns:
            string: Three strings:
                 - case sensitive indices
                 - case insensitive indices
                 - number indices
        """
        try:
            string_case_sensitive_indices = []
            string_case_insensitive_indices = []
            number_indices = []
            string_indices = []

            for index, field_info in enumerate(
                sanity_inputs.get("sanitization_input", {})
            ):
                if field_info.get("normalization", "") == "number":
                    number_indices.append(index)
                elif field_info.get("normalization", "") == "string":
                    string_indices.append(index)

                if field_info.get("caseSensitivity", "") == "sensitive":
                    string_case_sensitive_indices.append(index)
                elif field_info.get("caseSensitivity", "") == "insensitive":
                    string_case_insensitive_indices.append(index)

            string_cs = ",".join(map(str, string_case_sensitive_indices))
            string_cins = ",".join(map(str, string_case_insensitive_indices))
            num_norm = ",".join(map(str, number_indices))
            str_norm = ",".join(map(str, string_indices))

            return string_cs, string_cins, num_norm, str_norm
        except Exception:
            self.logger.error(
                message=f"{MANUAL_UPLOAD_PREFIX} Hash generation - Error occurred while getting "
                "indices from sanity input fields based on normalization and sensitivity.",
                details=traceback.format_exc(),
            )

    def remove_files(self, temp_edm_hash_dir_path, input_file_dir, output_path):
        """Remove csv files and temp EDM hashes after EDM hash generation.

        Args:
            temp_edm_hash_dir_path (str): Temporary EDM Hash Path
            input_file_dir (str): Input CSV File Path
            output_path (str): Path where all CSV files are located

        Raises:
            error: If there's an issue removing files.
        """
        try:
            if os.path.exists(temp_edm_hash_dir_path):
                shutil.rmtree(temp_edm_hash_dir_path)
            if os.path.exists(input_file_dir):
                shutil.rmtree(input_file_dir)
            if os.path.exists(output_path) and os.path.isdir(output_path):
                for file in os.listdir(output_path):
                    file_path = os.path.join(output_path, file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
        except Exception as error:
            self.logger.error(
                message=f"{MANUAL_UPLOAD_PREFIX} Hash generation - Error occurred while removing"
                + "csv files.",
                details=traceback.format_exc(),
            )
            raise error

    def csv_sanitize(self, csv_path, sample_data=False):
        """Sanitizes csv data and store good and bad files using run_sanitizer.

        Args:
            configuration (dict): configuration provided by user.
            csv_path (str): path for csv file

        Raises:
            CSVUploadException : Custom error class
        """
        try:
            self.logger.info(
                message=f"{MANUAL_UPLOAD_PREFIX} Hash generation - Executing sanitize method for {self.file_name}'."
            )
            fields = self.configuration.get("sanity_inputs", {}).get(
                "sanitization_input", {}
            )
            exclude_stopwords = self.configuration.get("sanity_inputs", {}).get(
                "exclude_stopwords", False
            )
            for field in fields:  # strips spaces from front and end for all values.
                self.strip_args(field)

            # Construct edm_data_config based on fields configuration
            edm_data_config = deepcopy(CONFIG_TEMPLATE)
            edm_data_config.update(
                {
                    "names": [
                        field.get("field", "")
                        for field in fields
                        if field.get("nameColumn", False)
                    ]
                }
            )

            if "stopwords" in edm_data_config and not exclude_stopwords:
                del edm_data_config["stopwords"]

            if not os.path.isfile(csv_path):
                self.logger.error(
                    message=f"{MANUAL_UPLOAD_PREFIX} Hash generation - Error occurred while"
                    " performing EDM sanitization CSV file does not exist.",
                )
                raise CSVUploadException(
                    message="Error occurred while performing sanitization on uploaded csv."
                )

            output_path = os.path.dirname(csv_path)
            csv_file_name = os.path.basename(csv_path)
            file_path = f"{output_path}/{os.path.splitext(csv_file_name)[0]}"

            # Remove existing sanitized and non-sanitized files if they exist
            for file_extension in ["good", "bad"]:
                existing_file = f"{file_path}.{file_extension}"
                if os.path.isfile(existing_file):
                    os.remove(existing_file)

            self.create_directory(dir_path=os.path.dirname(output_path))
            run_sanitizer(csv_path, file_path, edm_data_config)
            if not sample_data:
                if os.path.exists(csv_path):
                    os.remove(csv_path)
                if os.path.exists(f"{file_path}.bad"):
                    os.remove(f"{file_path}.bad")
            self.logger.info(
                message=f"{MANUAL_UPLOAD_PREFIX} Hash generation - Executed sanitize method"
                " successfully for {self.name}."
            )

        except Exception as error:
            self.logger.error(
                message=f"{MANUAL_UPLOAD_PREFIX} Error occurred while performing sanitization of uploaded csv.",
                details=traceback.format_exc(),
            )
            raise CSVUploadException(
                value=error,
                message="Error occurred while performing sanitization of uploaded csv.",
            ) from error

    def generate_csv_edm_hash(self):
        """Generate EDM Hashes from sanitized data.

        Raises:
            CSVUploadException: If an error occurs while
            generating EDM hashes.
        """
        try:
            self.logger.info(
                message=f"{MANUAL_UPLOAD_PREFIX} Hash generation - Generating EDM Hash "
                + f"for configuration {self.file_name} ({self.name})."
            )

            output_path = os.path.dirname(self.file_path)
            good_csv_path = f"{output_path}/{os.path.splitext(self.file_name)[0]}.good"
            input_file_dir = f"{output_path}/input"
            self.create_directory(dir_path=input_file_dir)
            input_csv_file = f"{input_file_dir}/{self.file_name}"
            shutil.move(good_csv_path, input_csv_file)

            if not os.path.isfile(input_csv_file):
                self.logger.error(
                    message=f"{MANUAL_UPLOAD_PREFIX} Hash generation - Error occurred while generating "
                    "EDM Hash. '.good' file does not exist for "
                    + f"configuration '{self.file_name} ({self.name})'.",
                )
                raise CSVUploadException(
                    message="Error occurred while generating "
                    "EDM Hash of Manual CSV Hash generation."
                )

            temp_edm_hash_dir_path = f"{output_path}/temp_edm_hashes"
            self.create_directory(dir_path=temp_edm_hash_dir_path)
            output_dir_path = temp_edm_hash_dir_path

            sanity_inputs = self.configuration.get("sanity_inputs", {})
            dict_cs, dict_cins, norm_num, norm_str = self.get_field_indices(
                sanity_inputs
            )

            edm_hash_config = deepcopy(EDM_HASH_CONFIG)
            edm_hash_config.update(
                {
                    "dict_cs": dict_cs,
                    "dict_cins": dict_cins,
                    "norm_num": norm_num,
                    "input_csv": input_csv_file,
                    "output_dir": output_dir_path,
                    "norm_str": norm_str,
                    "skip_hash": False,
                }
            )

            status, metadata_file = generate_edm_hash(
                conf_name=self.name, edm_conf=edm_hash_config
            )

            if os.path.exists(input_file_dir):
                shutil.rmtree(input_file_dir)
            if status is True:
                edm_hash_dir_path = f"{output_path}/edm_hashes"
                if os.path.exists(edm_hash_dir_path):
                    shutil.rmtree(edm_hash_dir_path)
                self.create_directory(dir_path=edm_hash_dir_path)
                temp_metadata_file = f"{output_path}/{metadata_file}"
                if os.path.exists(temp_metadata_file):
                    shutil.move(temp_metadata_file, f"{edm_hash_dir_path}/")
                self.storage["edm_hash_folder"] = edm_hash_dir_path
                metadata_file = metadata_file.replace(".tgz", ".json")
                temp_metadata_file = f"{temp_edm_hash_dir_path}/{metadata_file}"
                edm_hash_cfg = f"{edm_hash_dir_path}/{metadata_file}"
                if os.path.exists(temp_metadata_file):
                    shutil.move(temp_metadata_file, f"{edm_hash_dir_path}/")
                if os.path.exists(edm_hash_cfg):
                    self.storage["edm_hashes_cfg"] = edm_hash_cfg
                self.remove_files(temp_edm_hash_dir_path, input_file_dir, output_path)

                self.logger.info(
                    message=f"{MANUAL_UPLOAD_PREFIX} Hash generation - EDM Hash generated successfully "
                    + f"for configuration '{self.file_name} ({self.name})'."
                )
            else:
                raise CSVUploadException(
                    message="Manual CSV Hash generation - Error occurred while generating EDM Hash "
                    + f"for configuration '{self.file_name} ({self.name})'."
                )
        except Exception as error:
            if self.storage.get("csv_path"):
                output_path = os.path.dirname(self.storage["csv_path"])
                input_file_dir = f"{output_path}/input"
                temp_edm_hash_dir_path = f"{output_path}/temp_edm_hashes"
                self.remove_files(
                    temp_edm_hash_dir_path=temp_edm_hash_dir_path,
                    input_file_dir=input_file_dir,
                    output_path=output_path
                )
            self.logger.error(
                message=f"{MANUAL_UPLOAD_PREFIX} Hash generation - Error occurred while generating "
                f"EDM Hash for uploaded csv {self.file_name} ({self.name}).",
                details=traceback.format_exc(),
            )
            raise CSVUploadException(
                value=error,
                message=f"Error occurred while generating EDM Hash of Uploaded csv {self.name}.",
            ) from error
