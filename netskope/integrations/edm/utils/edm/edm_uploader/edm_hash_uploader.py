"""EDM Hash uploader file."""
import os
import json
import traceback
from .edm_api_upload import StagingManager

from .lib.edm_hash_upload_helper import EDMUploadHelper
from .lib.exceptions import (
    PddCfgException,
)


class EDMHashUploader(EDMUploadHelper):
    """Supports three modes of operations.

    a) csv_hash_upload - generates hashes from csv file
                         and uploads it.
    b) csv_hash_gen - just generates hashes from csv file
                      and leaves it in the hash output
                      directory.
    c) hash_upload - uploads the pre-generated hash files
    """

    num_wkrs = 1

    def __init__(self, cfg: dict, status_file: str):
        """Class Constructor."""
        self.filename = cfg.get("filename")
        self.port = cfg.get("port")
        self.token = cfg.get("token")
        self.hostname = cfg.get("hostname")
        self.server_name = cfg.get("servername")
        self.source_name = cfg.get("source_name")
        self.dest_name = cfg.get("dest_name")

        self.hash_upload_cfg = cfg.get("hash_upload_cfg")
        self.hash_dir = cfg.get("hash_dir")
        self.hash_out_dir = cfg.get("hash_out_dir")
        self.hash_out_file = cfg.get("hash_out_file")
        self.work_dir = cfg.get("work_dir")

        self.cfg = cfg
        self.pddgen = None
        self.hash_col_data = None
        self.mode = cfg.get("mode")
        self.file_id = None
        self.gen_id = None
        self.total_size = 0
        self.no_cols = 0
        self.col_names = []
        self.col_data = {}
        self.dict_cols = []
        self.upload_done = False
        super().__init__(status_file=status_file)

    def _check_hash_upload_cfg(self):
        """Check and update the configuration for hash upload mode."""
        if not self.hash_dir:
            raise PddCfgException("Directory containing hashes not passed")

        if not os.path.isdir(self.hash_dir):
            raise PddCfgException("Directory %s not present" % (self.hash_dir))

        if not self.hash_upload_cfg:
            raise PddCfgException("Hash upload configuration not present")

        if not os.path.isfile(self.hash_upload_cfg):
            raise PddCfgException(
                "Hash upload cfg %s not present" % (self.hash_upload_cfg)
            )

        with open(self.hash_upload_cfg, "r") as fp:
            hash_upload_cfg = json.load(fp)

        self.gen_id = hash_upload_cfg.get("generation_id")
        if not self.gen_id:
            raise PddCfgException("Generation ID missing in metadata")

        self.filename = hash_upload_cfg.get("filename")
        if not self.filename:
            raise PddCfgException("Filename missing in metadata")

        self.col_names = hash_upload_cfg.get("column_names")
        if not self.col_names:
            raise PddCfgException("Column names missing in metadata")

        self.hash_col_data = hash_upload_cfg.get("column_data")
        if not self.hash_col_data:
            raise PddCfgException("Column data missing in metadata")

        # for col_data in self.hash_col_data:
        #     col_file = os.path.join(self.hash_dir, col_data)
        #     if not os.path.isfile(col_file):
        #         raise PddCfgException("Hashed file %s not present" % (col_file))

        dict_hash_cols = hash_upload_cfg.get("dict_data")
        if dict_hash_cols:
            self.dict_cols = dict_hash_cols

    def _check_cfg(self):
        """Make sure that all the required configuration parameters \
        are available. If not raise DlpPddException with appropriate \
        exception."""
        if not self.mode:
            raise PddCfgException("Configuration mode not specified")

        if (
            self.mode != "hash_upload"
        ):
            raise PddCfgException("Invalid mode %s specified" % (self.mode))

        if not self.work_dir:
            return PddCfgException("Working directory not specified")

        if self.mode == "hash_upload":
            self._check_hash_upload_cfg()
            return

    def hash_upload_start(self):
        """Upload the already generated hash file along with the metdata \
        information."""
        # self.logger.info('hash_upload_start entered')
        staging_manager = StagingManager()
        staging_manager.set_server(self.server_name)
        staging_manager.set_port(self.port)
        staging_manager.set_auth_token(self.token)
        staging_manager.set_keep_staging_file(False)
        staging_manager.load_client()
        file_name = self.hash_upload_cfg.replace(".json", ".tgz")
        success, msg, context = staging_manager.upload(file_name)
        if success:
            self.logger.info(
                f"Successfully uploaded the hashes from {file_name} to "
                f"Netskope cloud for Source: {self.source_name} and "
                f"Destination: {self.dest_name}"
            )
        else:
            self.logger.info(
                f"Failed to upload the hashes from {file_name} to "
                f"Netskope cloud for Source: {self.source_name} and "
                f"Destination: {self.dest_name}"
            )
        return success, msg, context

    def start(self):
        """Verify the configuration, start generating hashes from input \
        csv file, upload it and cleanup the residue files."""
        try:
            self._check_cfg()
            if self.mode == "hash_upload":
                return self.hash_upload_start()
        except PddCfgException as err:
            self.logger.error(
                message=f"Invalid configuration for Source: {self.source_name}"
                        f"Destination: {self.dest_name}",
                error_code="EDM_1031",
                details=traceback.format_exc(),
            )
            raise err

    @staticmethod
    def execute(config: dict, status_file: str):
        """Start uploading EDM Hash.

        Args:
            config (dict): EDM upload configurations
            status_file (str): EDM upload status log file path
        """
        try:
            pdd = None
            pdd = EDMHashUploader(cfg=config, status_file=status_file)
            return pdd.start()
        except Exception as err:
            raise err
