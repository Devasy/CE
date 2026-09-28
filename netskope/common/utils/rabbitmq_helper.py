"""Make an REST API call to the RabbitMQ server."""
import functools
import os
import requests
import ssl
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

from requests.packages.urllib3.util.retry import Retry
import traceback
from urllib.parse import urlparse, unquote_plus

from netskope.common.utils import Logger
from netskope.common.utils.handle_exception import (
    handle_exception,
    handle_status_code,
)
from netskope.common.utils.requests_retry_mount import _BodyGuardMixin

logger = Logger()


class IgnoreHostnameAdapter(_BodyGuardMixin, HTTPAdapter):
    """Adapter that validates the certificate chain but skips hostname verification."""

    def init_poolmanager(self, *args, **kwargs):
        """Initialize a pool manager that keeps cert validation but bypasses the hostname check."""
        # Keep certificate-chain validation, but disable the SSL-layer hostname check.
        ctx = create_urllib3_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
        kwargs["ssl_context"] = ctx
        # Tell urllib3 not to match the peer certificate against the hostname.
        # This is the supported alternative to monkey-patching match_hostname,
        # which previously caused unbounded recursion across repeated calls.
        kwargs["assert_hostname"] = False
        return super().init_poolmanager(*args, **kwargs)


def _make_rabbitmq_api_call_helper(rabbitmq_url, endpoint, method="GET"):
    parsed_url = urlparse(rabbitmq_url)
    url = f"https://{parsed_url.hostname}:15671/{endpoint.lstrip('/')}"
    proxies = {"http": None, "https": None}

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.1)
    session.mount("https://", IgnoreHostnameAdapter(max_retries=retries))
    # handle_exception's own first parameter is also named `method` (the
    # requests callable to invoke) — passing the HTTP verb as method=method
    # collides with it (TypeError: multiple values for argument 'method').
    # Bind the verb into the callable itself so handle_exception only ever
    # sees its own `method` parameter.
    request_call = functools.partial(session.request, method)
    success, response = handle_exception(
        request_call,
        custom_message="Error occurred while connecting to rabbitmq server.",
        error_code="CE_1123",
        log_level="debug",
        url=url,
        auth=(parsed_url.username, unquote_plus(parsed_url.password)),
        proxies=proxies,
        timeout=30,
    )

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


def make_rabbitmq_api_call(endpoint, method="GET"):
    """Make call to rabbitmq server."""
    rabbitmq_connection_string = os.environ["RABBITMQ_CONNECTION_STRING"]
    url_list = rabbitmq_connection_string.split(";")

    for rabbitmq_url in url_list:
        try:
            return _make_rabbitmq_api_call_helper(rabbitmq_url, endpoint, method=method)
        except requests.exceptions.ConnectionError:
            logger.debug("Failed to connect to RabbitMQ server.", details=traceback.format_exc())
    logger.error("Error occurred while connecting to rabbitmq server.",
                 error_code="CE_1130", details="".join(traceback.format_stack()))
    raise Exception("Error occurred while connecting to rabbitmq server.")


# The one management-API query for CE's own queues (name filter `cloudexchange_[369]`). SINGLE
# source of truth for the endpoint + the queue-name regex so callers (the System dashboard, the
# copilot attention scan) don't each hand-roll the URL and drift. Callers pass the columns THEY
# need and map the returned `items` themselves.
def get_ce_queue_stats(columns):
    """Return the RabbitMQ `items` list for CE's queues, requesting `columns`.

    ``columns`` is a list of management-API column names (e.g. ["messages_ready",
    "messages_unacknowledged", "name"]). Returns the raw items list (``[]`` if none); raises on a
    RabbitMQ-management failure (callers decide how to degrade). ``name`` is always included so a
    caller can identify each queue.
    """
    cols = ",".join(dict.fromkeys(list(columns or []) + ["name"]))  # dedupe, ensure name
    result = make_rabbitmq_api_call(
        f"/api/queues/%2F/?page=1&page_size=500&name=cloudexchange_%5B369%5D&"
        f"columns={cols}&use_regex=true"
    )
    return (result or {}).get("items", [])
