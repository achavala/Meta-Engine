"""
Retry utilities for API calls with exponential backoff.
"""

import logging
import time
from functools import wraps
from typing import Callable, Tuple, Type

logger = logging.getLogger("RetryUtils")


def retry_api_call(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """
    Decorator for retrying API calls with exponential backoff.

    Usage:
        @retry_api_call(max_retries=3)
        def fetch_data(url):
            return requests.get(url, timeout=10)
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(
                            "Retry %d/%d for %s: %s (waiting %.1fs)",
                            attempt + 1, max_retries, func.__name__, e, delay
                        )
                        time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


def safe_json_load(path, default=None):
    """Load JSON file with error handling for corrupted files."""
    import json
    from pathlib import Path

    p = Path(path)
    if not p.exists():
        return default

    try:
        with open(p) as f:
            data = json.load(f)
        return data if data is not None else default
    except json.JSONDecodeError:
        logger.warning("Corrupted JSON: %s — using default", path)
        return default
    except IOError as e:
        logger.warning("Cannot read %s: %s", path, e)
        return default
