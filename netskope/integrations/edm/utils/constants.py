"""Constants module."""

FILE_PATH = "/etc/files-data/edm"
EDM_STOPWORD = "/opt/netskope/integrations/edm/utils/stopwords"

CONFIG_TEMPLATE = {
    "delimiter": ",",
    "encoding": "utf-8",
    "has-column-header": True,
    "columns": [],
    "stopwords": EDM_STOPWORD,
    "names": [],
    "normalize-ids": [],
    "primary-secondary-ids": [],
}

EDM_HASH_CONFIG = {
    "parse_column_names": True,
    "skip_hash": False,
    "edk_lic_dir": "/opt/netskope/integrations/edm/utils/edm/" + "hash_generator/edk",
    "edk_tool_dir": "/opt/netskope/integrations/edm/utils/edm/" + "hash_generator/edk",
    "input_encoding": "utf-8",
    "mode": "hash",
}

MANUAL_UPLOAD_PATH = "/etc/files-data/_manual_uploads/edm"
MANUAL_UPLOAD_PREFIX = "Manual Upload EDM -"
UPLOAD_PATH = "/etc/files-data/_uploads/edm"
