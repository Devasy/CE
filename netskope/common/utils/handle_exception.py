"""Handle Exception."""
import re
import requests
from netskope.common.utils import Logger, Notifier
import traceback

from netskope.common.utils.exceptions import IncompleteTransactionError, ForbiddenError

logger = Logger()
notifier = Notifier()


def call_log_func(
    log_func,
    error_code,
    custom_message,
    plugin,
    traceback_msg,
    exception_msg,
):
    """Add log using log_func."""
    traceback_details = re.sub(
        r"token=([0-9a-zA-Z]*)", "token=********&", traceback_msg
    )
    log_func(
        f"{plugin}: {(custom_message + ', ') if custom_message is not None else ''}{exception_msg}",
        details=traceback_details,
        error_code=error_code,
    ) if plugin else log_func(
        f"{(custom_message + ', ') if custom_message is not None else ''}{exception_msg}",
        details=traceback_details,
        error_code=error_code,
    )


def handle_exception(
    method: classmethod,
    error_code: str = None,
    custom_message: str = None,
    plugin: str = None,
    log_level: str = "error",
    *args,
    **kwargs,
):
    """Handle all the requests exceptions.

    Args:
        method (classmethod): requests method to call
        error_code (str, optional): Error code. Defaults to None.
        custom_message (str, optional): Custom message to write. Defaults to None.
        plugin (str, optional): Plugin name. Defaults to None.
        log_level (str, optional): Log level. Defaults to "error".

    """
    log_func = getattr(logger, log_level)
    try:
        response = method(*args, **kwargs)
    except requests.exceptions.ProxyError as err:
        call_log_func(
            log_func,
            error_code,
            custom_message,
            plugin,
            traceback.format_exc(),
            "Invalid proxy configuration.",
        )
        return False, err
    except requests.exceptions.SSLError as err:
        call_log_func(
            log_func,
            error_code,
            custom_message,
            plugin,
            traceback.format_exc(),
            "SSL error.",
        )
        return False, err
    except requests.exceptions.ConnectionError as err:
        call_log_func(
            log_func,
            error_code,
            custom_message,
            plugin,
            traceback.format_exc(),
            "Connection Error.",
        )
        return False, err
    except requests.exceptions.RequestException as err:
        call_log_func(
            log_func,
            error_code,
            custom_message,
            plugin,
            traceback.format_exc(),
            "Request exception occurred.",
        )
        return False, err
    except Exception as err:
        call_log_func(
            log_func,
            error_code,
            custom_message,
            plugin,
            traceback.format_exc(),
            "Exception occurred.",
        )
        return False, err
    return True, response


def handle_status_code(
    response,
    error_code: str = None,
    custom_message: str = None,
    plugin: str = None,
    log_level: str = "error",
    notify: bool = True,
    handle_forbidden: bool = False,
    parse_response: bool = True,
    log: bool = False,
):
    """Handle status code of response.

    Args:
        response (response): response of API call
        error_code (str, optional): error code. Defaults to None.
        custom_message (str, optional): custom message to write. Defaults to None.
        plugin (str, optional): plugin name. Defaults to None.
        log_level (str, optional): Log level. Defaults to "error".
        notify (bool, optional): notify an error. Defaults to True.
        handle_forbidden (bool, optional): Handle forbidden error. Defaults to False.
        parse_response (bool, optional): Parse response as JSON. Defaults to True.
    """
    log_func = getattr(logger, log_level)

    if response.status_code == 200 or response.status_code == 201:
        try:
            if (
                parse_response
                and "application/json"
                in response.headers.get("Content-Type", "").lower()
            ):
                return response.json()
            else:
                return response.content
        except Exception:
            error = (
                f"{(custom_message + ',') if custom_message is not None else ''} "
                f"Exception occurred while parsing JSON response."
            )
            if plugin:
                error = f"{plugin}: {error}"
            if log:
                log_func(error, details=traceback.format_exc(), error_code=error_code)
            raise IncompleteTransactionError(
                f"{error} Response: {response.text}"
            ) from None
    elif response.status_code == 401:
        if plugin:
            if notify:
                notifier.error(
                    f"{plugin}: {(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code 401, Authentication Error."
                )
            if log:
                log_func(
                    f"{plugin}: {(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code 401, Authentication Error.",
                    details=response.text,
                    error_code=error_code,
                )
        else:
            if notify:
                notifier.error(
                    f"{(custom_message + ', ') if custom_message is not None else ''}"
                    f"Received exit code 401, Authentication Error."
                )
            if log:
                log_func(
                    f"{(custom_message + ', ') if custom_message is not None else ''}"
                    f"Received exit code 401, Authentication Error.",
                    details=response.text,
                    error_code=error_code,
                )
    elif response.status_code == 403:
        if plugin:
            if notify:
                notifier.error(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"{plugin}: Received exit code 403, Forbidden Error."
                )
            if log:
                log_func(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"{plugin}: Received exit code 403, Forbidden Error.",
                    details=response.text,
                    error_code=error_code,
                )
        else:
            if notify:
                notifier.error(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code 403, Forbidden Error."
                )
            if log:
                log_func(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code 403, Forbidden Error.",
                    details=response.text,
                    error_code=error_code,
                )
        if handle_forbidden:
            raise ForbiddenError(
                f"{(custom_message + ',') if custom_message is not None else ''} "
                f"Received exit code 403, Forbidden Error."
            )
    elif response.status_code == 429:
        if plugin:
            if notify:
                notifier.error(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"{plugin}: Received exit code 429, Too many requests."
                    f" For URL: {response.url}"
                )
            if log:
                log_func(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"{plugin}: Received exit code 429, Too many requests."
                    f" For URL: {response.url}.",
                    details=f"For URL: {response.url}\n{response.text}",
                    error_code=error_code,
                )
        else:
            if notify:
                notifier.error(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code 429, Too many requests."
                    f" For URL: {response.url}"
                )
            if log:
                log_func(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code 429, Too many requests."
                    f" For URL: {response.url}.",
                    details=f"For URL: {response.url}\n{response.text}",
                    error_code=error_code,
                )
    elif response.status_code == 409:
        if plugin:
            if notify:
                notifier.error(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"{plugin}: Received exit code 409, Concurrency found while calling the API."
                )
            if log:
                log_func(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"{plugin}: Received exit code 409, Concurrency found while calling the API.",
                    details=response.text,
                    error_code=error_code,
                )
        else:
            if notify:
                notifier.error(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code 409, Concurrency found while calling the API."
                )
            if log:
                log_func(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code 409, Concurrency found while calling the API.",
                    details=response.text,
                    error_code=error_code,
                )
    elif response.status_code >= 400 and response.status_code < 500:
        if log:
            log_func(
                f"{plugin}: {(custom_message + ',') if custom_message is not None else ''} "
                f"Received exit code {response.status_code}, HTTP client Error.",
                details=response.text,
                error_code=error_code,
            ) if plugin else log_func(
                f"{(custom_message + ',') if custom_message is not None else ''} "
                f"Received exit code {response.status_code}, HTTP client Error.",
                details=response.text,
                error_code=error_code,
            )
    elif response.status_code >= 500 and response.status_code < 600:
        if plugin:
            if notify:
                notifier.error(
                    f"{plugin}: {(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code {response.status_code}, HTTP server Error."
                )
            if log:
                log_func(
                    f"{plugin}: {(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code {response.status_code}, HTTP server Error.",
                    details=response.text,
                    error_code=error_code,
                )
        else:
            if notify:
                notifier.error(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code {response.status_code}, HTTP server Error."
                )
            if log:
                log_func(
                    f"{(custom_message + ',') if custom_message is not None else ''} "
                    f"Received exit code {response.status_code}, HTTP server Error.",
                    details=response.text,
                    error_code=error_code,
                )
    response.raise_for_status()
