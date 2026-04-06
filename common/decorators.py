import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def rate_limit(calls_per_second):
    """Decorator to limit function calls to a specified rate.
    
    Args:
        calls_per_second: Maximum number of calls allowed per second
    """
    min_interval = 1.0 / calls_per_second

    def decorator(func):
        last_time_called = 0.0

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal last_time_called
            elapsed = time.perf_counter() - last_time_called
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            ret = func(*args, **kwargs)
            last_time_called = time.perf_counter()
            return ret

        return wrapper

    return decorator


def retry(exceptions, tries=3, delay=1):
    """Decorator to retry a function on specific exceptions.
    
    Args:
        exceptions: Exception or tuple of exceptions to catch
        tries: Number of attempts to make (default: 3)
        delay: Delay in seconds between retries (default: 1)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _tries = tries
            while _tries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    logger.info(f"Retrying... {_tries - 1} tries left")
                    time.sleep(delay)
                    _tries -= 1
            return func(*args, **kwargs)  # Last attempt

        return wrapper

    return decorator
