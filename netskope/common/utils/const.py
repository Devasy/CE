"""Constants."""

# utils constants
API_MAX_LIMIT = 100
DEFAULT_TIMEOUT = 1800
FORBIDDEN_ERROR_BANNER_ID = "BANNER_ERROR_1003"
LOWER_THRESHOLD = 35  # Threshold value for available disk space
MONGODB_RABBITMQ_CERT_LOCATION = "/opt/certs/mongodb_rabbitmq_certs/tls_cert.crt"
MONGODB_RABBITMQ_CERT_BANNER_ID = "BANNER_ERROR_2001"
MODULES_MAP = {
    "cls": "CLS",
    "cto": "CTO",
    "itsm": "CTO",
    "cte": "CTE",
    "cre": "CRE",
    "edm": "EDM",
    "cfc": "CFC",
    "tenant": "TENANT",
    "provider": "TENANT",
    "": None
}
MAX_AUTO_RECONNECT_ATTEMPTS = 3
MAX_RETRY_COUNT = 3
SOCKET_DEFAULT_TIMEOUT = 300

# --- HTTP response decompression guards -------------------------------------
# Compensating controls for urllib3 1.26.x, which applies no bound on
# decompression (GHSA-gm62-xv2j-4w53, GHSA-2xpw-w6gg-jr37, GHSA-38jv-5279-wg99).
# urllib3 stays pinned at 1.26.19 while the cloudtrail plugin ships botocore
# 1.29.61; these limits bound the blast radius until that is re-vendored.
#
# MAX_RESPONSE_BYTES is sized from the largest data limit CE already enforces
# (EDM upload cap, 128 MiB) with 4x headroom. Real feed responses are paginated
# via API_MAX_LIMIT, so legitimate traffic stays far below this.
MAX_RESPONSE_BYTES = 512 * 1024 * 1024
# Observed gzip ratios on JSON/CSV feeds are ~5-20:1. Decompression bombs run
# 1000:1 and higher, so 200:1 separates them with a wide margin.
MAX_DECOMPRESS_RATIO = 200
# Ratio is only enforced once a response exceeds this, so small responses with a
# tiny Content-Length cannot trip it.
DECOMPRESS_RATIO_FLOOR_BYTES = 1 * 1024 * 1024
# Legitimate servers send at most one Content-Encoding (rarely two).
MAX_CONTENT_ENCODINGS = 2
# requests defaults to 30; nothing in CE legitimately needs that many hops.
MAX_REDIRECTS = 5

# --- Plugin archive (upload) guards ----------------------------------------
# Sized from real artifacts: the largest distributable plugin zip is ~108 KiB,
# and the largest plugin tree uncompressed is ~142 MiB (vendored pyarrow).
MAX_PLUGIN_ARCHIVE_BYTES = 128 * 1024 * 1024
MAX_PLUGIN_UNCOMPRESSED_BYTES = 512 * 1024 * 1024
MAX_PLUGIN_ARCHIVE_RATIO = 100
PLUGIN_UPLOAD_CHUNK_BYTES = 1024 * 1024
UNAUTHORIZED_BANNER_ID = "BANNER_ERROR_0999"
UI_CERT_LOCATION = "/opt/certs/cte_cert.crt"
UI_CERT_BANNER_ID = "BANNER_ERROR_2002"
UPPER_THRESHOLD = 20
WEB_TX_ERROR_BANNER_ID = "BANNER_ERROR_1004"
WEB_TX_ERROR_BANNER_MESSAGE = (
    "Following WebTx plugins have been disabled : {}. " +
    "Please configure Netskope tenant with required permissions " +
    "and reconfigure plugins with tenant and enable them manually."
)

# common api constants
ACCESS_TOKEN_EXPIRE_MINUTES = 120
DB_LOOKUP_INTERVAL = 120
MAX_LOG_COUNT = 10000
MAX_NOTIFICATIONS = 100
MAX_STATUS_COUNT = 10000

# common celery constants
ALERT_EVENT_SOFT_TIME_LIMIT = 5400
MAX_ANALYTICS_LENGTH = 255
SOFT_TIME_LIMIT = 1800
TASK_TIME_LIMIT = SOFT_TIME_LIMIT + 300
WEBTX_SOFT_TIME_LIMIT = 1800
