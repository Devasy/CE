"""Decorator utils."""
import time
import traceback
from pymongo.errors import AutoReconnect, ServerSelectionTimeoutError
from .const import MAX_AUTO_RECONNECT_ATTEMPTS


def retry(times, exceptions, sleep=0, log_level="error", exponential_sleep=0, task_name=None):
    """Retry Decorator on specified exception."""
    def decorator(func):
        """Decorator function for retry on specific exceptions."""  # noqa
        def function(*args, **kwargs):
            """Returns wrapper function for retry on specific exceptions."""  # noqa
            attempt = 0
            exp_sleep = sleep
            from netskope.common.utils import Logger
            logger = Logger()
            while attempt <= times:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt >= times:
                        logger.error(f"Max retry exceeded while executing '{func.__name__}'. {e}.",
                                     error_code="CE_1119",
                                     details=traceback.format_exc())
                        raise e
                    log_func = getattr(logger, log_level)
                    if task_name:
                        log_func(f"{e}, Publishing the task '{task_name}' to RabbitMQ. "
                                 f"Retrying (attempt {attempt + 1}) in {exp_sleep}s")
                    else:
                        log_func(f"{e}, Task: '{func.__name__}', attempt {attempt + 1} of {times}")
                    attempt += 1
                    time.sleep(exp_sleep)
                    exp_sleep += exponential_sleep
        return function
    return decorator


def graceful_auto_reconnect():
    """Gracefully handle a auto reconnect error."""
    def decorator(mongo_func):
        def function(*args, **kwargs):
            attempt = 0
            while attempt <= MAX_AUTO_RECONNECT_ATTEMPTS:
                try:
                    return mongo_func(*args, **kwargs)
                except (AutoReconnect, ServerSelectionTimeoutError) as ex:
                    if attempt >= MAX_AUTO_RECONNECT_ATTEMPTS:
                        print(f"Max retry exceeded while executing '{mongo_func.__name__}'",)
                        raise ex
                    attempt += 1
                    wait_t = 1 * pow(2, attempt)  # exponential back off
                    print(f"Attempt {attempt} of {MAX_AUTO_RECONNECT_ATTEMPTS} for mongo reconnect.")
                    time.sleep(wait_t)
        return function
    return decorator
