"""Retry helpers for remote APIs that rate-limit with HTTP 429 (#167)."""

import os
import time

from utils.logging_config import Logger


def env_int(name, default):
    """Read an int env var, falling back to default when unset or invalid."""
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def env_float(name, default):
    """Read a float env var, falling back to default when unset or invalid."""
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _http_status(exc):
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None)


def is_rate_limited(exc):
    """True if exc is an HTTP 429 Too Many Requests."""
    if _http_status(exc) == 429:
        return True
    text = str(exc).lower()
    return "429" in text and "too many requests" in text


def _retry_after_seconds(exc):
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None) or {}
    try:
        value = headers.get("Retry-After")
    except AttributeError:
        return None
    try:
        return max(0.0, float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def retry_on_429(
    fn,
    what,
    attempts=6,
    base_delay=30.0,
    max_delay=300.0,
    sleep=None,
):
    """Call fn(), retrying with exponential backoff while it raises HTTP 429.

    Waits Retry-After when the response sends it, else base_delay doubling
    per attempt; either way capped at max_delay. Other errors are raised on
    the first try. After the last attempt the original 429 error is raised.
    """
    sleep = time.sleep if sleep is None else sleep
    attempts = max(1, attempts)
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:  # pylint: disable=broad-except
            if not is_rate_limited(e) or attempt == attempts:
                raise
            delay = _retry_after_seconds(e)
            if delay is None:
                delay = base_delay * (2 ** (attempt - 1))
            delay = min(delay, max_delay)
            Logger.warning(
                f"Rate limit (429) on {what}, attempt {attempt}/{attempts}; "
                f"retrying in {delay:.0f}s"
            )
            sleep(delay)
    return None  # unreachable
