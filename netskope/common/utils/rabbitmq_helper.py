"""Make an REST API call to the RabbitMQ server."""
import os
import requests
import ssl
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from urllib3.util.ssl_match_hostname import CertificateError

from requests.packages.urllib3.util.retry import Retry
import traceback
from urllib.parse import urlparse, unquote_plus

from netskope.common.utils import Logger
from netskope.common.utils.handle_exception import (
    handle_exception,
    handle_status_code,
)

logger = Logger()
g_orig_match_hostname = None


class IgnoreHostnameAdapter(HTTPAdapter):
    """Ignore hostname adapter."""

    def init_poolmanager(self, *args, **kwargs):
        """Initialize custom pool manager to bypass hostname verification."""
        global g_orig_match_hostname
        # Create context that keeps cert validation but skips hostname check
        ctx = create_urllib3_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED

        # Monkey-patch urllib3's hostname verification
        import urllib3.connection
        orig_match_hostname = urllib3.connection.match_hostname

        def patched_match_hostname(cert, hostname):
            try:
                return orig_match_hostname(cert, hostname)
            except CertificateError:
                return True  # Always accept hostname mismatch

        urllib3.connection.match_hostname = patched_match_hostname
        g_orig_match_hostname = orig_match_hostname
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


def _make_rabbitmq_api_call_helper(rabbitmq_url, endpoint):
    parsed_url = urlparse(rabbitmq_url)
    url = f"https://{parsed_url.hostname}:15671/{endpoint.lstrip('/')}"
    proxies = {"http": None, "https": None}

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.1)
    session.mount("https://", IgnoreHostnameAdapter(max_retries=retries))
    try:
        success, response = handle_exception(
            session.get,
            custom_message="Error occurred while connecting to rabbitmq server.",
            error_code="CE_1123",
            log_level="debug",
            url=url,
            auth=(parsed_url.username, unquote_plus(parsed_url.password)),
            proxies=proxies,
            timeout=30,
        )
    finally:
        import urllib3.connection
        urllib3.connection.match_hostname = g_orig_match_hostname

    if not success:
        raise response

    response = handle_status_code(
        response,
        custom_message="Error occurred while connecting to rabbitmq server.",
        error_code="CE_1124",
        log_level="debug",
        log=True,
    )
    return response


def make_rabbitmq_api_call(endpoint):
    """Make call to rabbitmq server."""
    rabbitmq_connection_string = os.environ["RABBITMQ_CONNECTION_STRING"]
    url_list = rabbitmq_connection_string.split(";")

    for rabbitmq_url in url_list:
        try:
            return _make_rabbitmq_api_call_helper(rabbitmq_url, endpoint)
        except requests.exceptions.ConnectionError:
            logger.debug("Failed to connect to RabbitMQ server.", details=traceback.format_exc())
    logger.error("Error occurred while connecting to rabbitmq server.",
                 error_code="CE_1130", details="".join(traceback.format_stack()))
    raise Exception("Error occurred while connecting to rabbitmq server.")
