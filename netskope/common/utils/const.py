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
