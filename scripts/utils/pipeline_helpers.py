"""
Legacy pipeline helper utilities recovered after refactor.
These functions handle caching, rate limiting, and safe JSON/file operations.
"""

import json
import time
from pathlib import Path
import requests
from functools import wraps

# --- File + JSON helpers ------------------------------------------------------

def save_json(data, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)

# --- Request + Caching --------------------------------------------------------

def cached_request(
    url: str,
    cache_path: Path,
    *,
    params: dict = None,
    headers: dict = None,
    rate_limiter=None,
    force_refresh: bool = False,
    timeout: int = 15
):
    """
    Perform a GET request with optional caching, query parameters, and rate limiting.
    """
    cache_path = Path(cache_path)
    if cache_path.exists() and not force_refresh:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)

    if rate_limiter:
        rate_limiter.wait()

    # perform the request with optional params/headers
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()

    data = resp.json()
    save_json(data, cache_path)
    return data


# --- Rate Limiter -------------------------------------------------------------

class RateLimiter:
    """Simple rate limiter: ensures max N calls per second."""
    def __init__(self, rate_per_sec=3):
        self.rate_per_sec = rate_per_sec
        self.interval = 1.0 / rate_per_sec
        self.last_time = 0

    def wait(self):
        now = time.time()
        delta = now - self.last_time
        if delta < 1.0 / self.rate_per_sec:
            time.sleep((1.0 / self.rate_per_sec) - delta)
        self.last_time = time.time()

    def limit(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self.wait()
            return func(*args, **kwargs)
        return wrapper

    def wait_for_slot(self):
        now = time.time()
        elapsed = now - self.last_time
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self.last_time = time.time()
