"""Constants module."""

COMPRESSED_FILE_TYPES_SUPPORTED = ["application/zip", "application/x-zip-compressed"]
COMPRESSED_EXTENSION_SUPPORTED = [".zip", ".tgz"]
COMPRESSED_EXTENSION_SUPPORTED_FOR_REGEX = ["zip", "tgz"]
FILE_PATH = "/etc/files-data/cfc"
FILE_TYPES_SUPPORTED = [
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/svg+xml",
    "application/zip",
    "application/x-zip-compressed",
]

IMAGE_EXTENSION_SUPPORTED = [
    ".bmp",
    ".dib",
    ".jpeg",
    ".jpg",
    ".jpe",
    ".jp2",
    ".png",
    ".webp",
    ".avif",
    ".pbm",
    ".pgm",
    ".ppm",
    ".pxm",
    ".pnm",
    ".pfm",
    ".sr",
    ".ras",
    ".tiff",
    ".tif",
    ".exr",
    ".hdr",
    ".pic",
    ".zip",
    ".tgz",
]
MANUAL_UPLOAD_PATH = "/etc/files-data/_manual_uploads/cfc"
MAX_PENDING_STATUS_TIME = 120
MANUAL_UPLOAD_TASK_DELAY_TIME = 60
MANUAL_UPLOAD_PREFIX = "Manual Upload CFC -"
REGEX_FOR_ZIP_FILE_PATH_FROM_RESPONSE = r"^(\/etc\/files-data\/.*\.({})):(.*)$"
