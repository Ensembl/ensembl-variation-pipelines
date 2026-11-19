"""Bounded retries for read-only REST requests (compatible with Python 3.7)."""

import json
import math
import socket
import time
import warnings
from email.utils import parsedate_to_datetime
from http.client import IncompleteRead
from urllib import request
from urllib.error import HTTPError, URLError

RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
MAX_RETRY_DELAY = 300


def retry_after_seconds(headers):
    value = headers.get("Retry-After") if headers else None
    if value is None:
        return 0
    try:
        delay = float(value)
    except ValueError:
        try:
            delay = parsedate_to_datetime(value).timestamp() - time.time()
        except (TypeError, ValueError, OverflowError):
            return 0
    return max(0, delay) if math.isfinite(delay) else 0


def load_json_with_retry(url, warn=warnings.warn):
    """Retry transient failures; propagate permanent or exhausted errors.

    Honour Retry-After up to five minutes. A longer requested wait fails instead
    of retrying earlier than the service permits. Invalid JSON is not retried.
    """
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with request.urlopen(url, timeout=30) as response:
                payload = response.read()
        except (
            URLError,
            TimeoutError,
            socket.timeout,
            ConnectionError,
            IncompleteRead,
        ) as error:
            server_delay = 0
            if isinstance(error, HTTPError):
                server_delay = retry_after_seconds(error.headers)
                if error.fp is not None:
                    error.close()
                if error.code not in RETRYABLE_HTTP_CODES:
                    raise
            if attempt == MAX_ATTEMPTS or server_delay > MAX_RETRY_DELAY:
                warn(
                    f"REST request failed permanently after {attempt} attempt(s): "
                    f"{error}; Retry-After={server_delay:g}s; url={url}"
                )
                raise
            delay = max(5 * 2 ** (attempt - 1), server_delay)
            warn(
                f"REST request failed (attempt {attempt}/{MAX_ATTEMPTS}): {error}; "
                f"retrying in {delay:g}s; url={url}"
            )
            time.sleep(delay)
        else:
            return json.loads(payload)
