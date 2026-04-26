"""Override session init."""
import os
import subprocess
from requests.adapters import HTTPAdapter
from requests.utils import default_headers
from requests.hooks import default_hooks
from requests.models import DEFAULT_REDIRECT_LIMIT
from requests.cookies import cookiejar_from_dict
from collections import OrderedDict
from .const import (
    MAX_RETRY_COUNT,
    DEFAULT_TIMEOUT,
)

try:
    TIME_OUT = int(os.environ.get("POPEN_TIMEOUT", DEFAULT_TIMEOUT))
    if TIME_OUT < 1:
        TIME_OUT = DEFAULT_TIMEOUT
except Exception:
    pass


class MaxRetryExceededException(Exception):
    """Max Retry Exceeded Exception."""

    pass


def override_session_init(self):
    """Overide the init of session to change adapters."""
    class TimeoutHTTPAdapter(HTTPAdapter):
        """Timeout Http Adapter for plugins calls.

        Args:
            HTTPAdapter
        """

        def send(self, request, **kwargs):
            """Send method override timeout."""
            if kwargs.get("timeout", None) is None:
                DEFAULT_REQUESTS_TIMEOUT = 300
                try:
                    timeout = int(os.environ.get(
                            "REQUESTS_TIMEOUT", DEFAULT_REQUESTS_TIMEOUT
                        ))
                    if timeout < 1:
                        timeout = DEFAULT_REQUESTS_TIMEOUT
                    kwargs["timeout"] = timeout
                except Exception:
                    kwargs["timeout"] = DEFAULT_REQUESTS_TIMEOUT
            return super().send(request, **kwargs)

    self.headers = default_headers()

    self.auth = None

    self.proxies = {}

    self.hooks = default_hooks()

    self.params = {}

    self.stream = False

    self.verify = True

    self.cert = None

    self.max_redirects = DEFAULT_REDIRECT_LIMIT

    self.trust_env = True

    self.cookies = cookiejar_from_dict({})

    self.adapters = OrderedDict()

    # created adapter with Retry mechanism
    adapter = TimeoutHTTPAdapter(max_retries=3)
    # mounted adapter into session class
    self.mount('https://', adapter)
    self.mount('http://', adapter)


def popen_retry_mount(process, is_wait: bool):
    """Retry mechanism in wait and communicates calls.

    Args:
        process (subprocess): Popen object.
        is_wait (bool): use wait method if is_wait is True else use communicate.
    Raises:
        MaxRetryExceededException: Exception when all retires completed

    Returns:
        tuple : output and error if any
    """
    retry_count = 0
    while retry_count < MAX_RETRY_COUNT:
        try:
            if is_wait:
                process.wait(timeout=TIME_OUT)
                return
            else:
                return process.communicate(timeout=TIME_OUT)
        except subprocess.TimeoutExpired:
            continue
        finally:
            retry_count += 1
    raise MaxRetryExceededException
