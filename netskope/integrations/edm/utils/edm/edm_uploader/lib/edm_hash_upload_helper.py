"""EDM Hash upload helper file."""
import hashlib
import os
from netskope.common.utils import Logger


class EDMUploadHelper(object):
    """EDM Upload Helper class."""

    # upload status notify variables
    STATUS_MODULO = 1000
    # Make sure the MAX_FILE_SIZE is atleast 256 bytes because in hash_upload
    # case we append the metadata in the later stage.
    MAX_FILE_SIZE = 128 * 1024 * 1024
    # edk file paths
    EDK_LIC_PATH = os.path.abspath(
        ".netskope/integrations/edm/utils/edm/hash_generator/edk/licensekey.dat"
    )
    EDK_TOOL_PATH = os.path.abspath(
        ".netskope/integrations/edm/utils/edm/hash_generator/edk/edktool.exe"
    )
    # hash file prefix
    OUT_FILE_PREFIX = "pdd_data_"
    # pdd metadata version. Version 1 doesn't support pbkdf.
    PDD_METADATA_VERSION = 2
    # override default connection timeout
    CONNECT_TIMEOUT = 180.0
    READ_TIMEOUT = 180.0

    def __init__(self, status_file: str) -> None:
        """Class Constructor."""
        self.logger = Logger()
        self.STATUS_FILE = status_file

    @staticmethod
    def get_md5sum(filename, metadata=None):
        """Compute the MD5 of the metadata string and the file."""
        md5 = hashlib.md5()
        if metadata:
            md5.update(bytes(metadata, "utf-8"))
        with open(filename, "rb") as fp:
            for data in iter(lambda: fp.read(8192), b""):
                md5.update(data)
        return md5.hexdigest()

    @staticmethod
    def file_copychunk(src_file, dst_fp, loc, size):
        """Copy data of size starting from loc of the source file to the \
        destination file descriptor."""
        chunk_size = 8192
        if size < chunk_size:
            chunk_size = size

        with open(src_file, "rb") as src_fp:
            src_fp.seek(loc)
            while size:
                data = src_fp.read(chunk_size)
                dst_fp.write(data)
                size -= chunk_size
                if size < chunk_size:
                    chunk_size = size
