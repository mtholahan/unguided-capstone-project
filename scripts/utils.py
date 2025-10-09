"""
utils.py — Core utilities for the Discogs→TMDB pipeline
--------------------------------------------------------
Provides:
  • Title normalization & fuzzy matching helpers
  • Progress bar wrapper
  • Safe request + retry/backoff + caching
  • Threaded batch fetch (with MAX_THREADS)
  • Rate limiter for TMDB/Discogs API pacing
  • File + logging utilities shared by all pipeline steps
"""

import os
import re
import time
import json
import unicodedata
import logging
import concurrent.futures
import threading
from pathlib import Path
from tqdm import tqdm
import requests
from typing import List, Dict, Any, Iterable, Optional

from config import (
    API_TIMEOUT,
    RETRY_BACKOFF,
    LOG_LEVEL,
    MAX_THREADS,
    SAVE_RAW_JSON,
    DATA_DIR,
)

# ---------------------------------------------------------------------------
# 🪶 Logging setup (per-module consistency)
# ---------------------------------------------------------------------------
logger = logging.getLogger("Utils")
logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

# ---------------------------------------------------------------------------
# 🕒 Rate Limiter (thread-safe API pacing)
# ---------------------------------------------------------------------------
class RateLimiter:
    """
    Thread-safe rate limiter to prevent exceeding API request quotas.

    Parameters
    ----------
    calls_per_second : float
        Maximum allowed request rate. Example: 3.0 means ≤3 API calls/sec.
    """

    def __init__(self, calls_per_second: float = 3.0):
        self.lock = threading.Lock()
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0.0

    def wait(self):
        """Block until it’s safe to make another API call."""
        with self.lock:
            now = time.perf_counter()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.perf_counter()


# ---------------------------------------------------------------------------
# 🎞 Title Normalization & Cleaning
# ---------------------------------------------------------------------------
def normalize_title_for_matching(text: str) -> str:
    """Normalize soundtrack or movie titles for robust fuzzy matching."""
    if not isinstance(text, str):
        return ""

    text = text.lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[\(\[\{].*?[\)\]\}]", " ", text)

    noise_patterns = [
        r"original motion picture soundtrack",
        r"original soundtrack",
        r"motion picture soundtrack",
        r"complete motion picture score",
        r"deluxe edition",
        r"expanded edition",
        r"\bost\b",
        r"\bsoundtrack\b",
        r"\bscore\b",
    ]
    for pat in noise_patterns:
        text = re.sub(pat, " ", text, flags=re.IGNORECASE)

    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    tokens = [t for t in text.split() if len(t) > 1]
    return " ".join(tokens)


def normalize_for_matching_extended(text: str) -> str:
    """Extended normalization with roman numeral & franchise cleanup."""
    ROMAN_MAP = {
        " i ": " 1 ", " ii ": " 2 ", " iii ": " 3 ", " iv ": " 4 ", " v ": " 5 ",
        " vi ": " 6 ", " vii ": " 7 ", " viii ": " 8 ", " ix ": " 9 ", " x ": " 10 ",
    }
    ARTICLES = {"the", "a", "an"}
    FRANCHISE_WORDS = {"part", "episode", "chapter", "vol", "volume"}

    base = normalize_title_for_matching(text)
    if not base:
        return ""

    s = f" {base} "
    for k, v in ROMAN_MAP.items():
        s = s.replace(k, v)
    s = s.strip()

    toks = s.split()
    while toks and toks[0] in ARTICLES:
        toks = toks[1:]
    s = " ".join(toks)
    s = re.sub(rf"\b({'|'.join(FRANCHISE_WORDS)})\s+\d+\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s


def clean_title(text: str) -> str:
    """Lightweight cleaner for display/logging."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"\(.*?\)|\[.*?\]", "", text)
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def is_mostly_digits(s: str, threshold: float = 0.7) -> bool:
    """Return True if a string is mostly digits (catalog IDs, etc.)."""
    if not s:
        return False
    digits = sum(c.isdigit() for c in s)
    return digits / max(len(s), 1) > threshold


# ---------------------------------------------------------------------------
# 🧩 Progress Bar Factory
# ---------------------------------------------------------------------------
def make_progress_bar(iterable=None, desc=None, total=None, leave=True, **kwargs):
    """Unified tqdm wrapper for iterable or numeric progress."""
    desc = (desc or "Working")[:40]
    if iterable is not None:
        return tqdm(iterable, desc=desc, leave=leave, **kwargs)
    return tqdm(total=total, desc=desc, leave=leave, **kwargs)


# ---------------------------------------------------------------------------
# 🌐 Safe Request Helper (integrated)
# ---------------------------------------------------------------------------
def safe_request(
    url: str,
    params: dict = None,
    headers: dict = None,
    retries: int = 3,
    timeout: float = API_TIMEOUT,
    backoff: float = RETRY_BACKOFF,
) -> Optional[Dict[str, Any]]:
    """HTTP GET request with retry and backoff logic."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            logger.warning(f"⏳ Timeout ({timeout}s) on attempt {attempt}/{retries} for {url}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ Attempt {attempt}/{retries} failed: {e}")
        if attempt < retries:
            sleep = backoff * attempt
            logger.info(f"Retrying in {sleep:.1f}s...")
            time.sleep(sleep)
    logger.error(f"❌ Exhausted retries ({retries}) for {url}")
    return None


# ---------------------------------------------------------------------------
# ⚡ Parallel Batch Fetch (Threaded)
# ---------------------------------------------------------------------------
def batch_fetch(
    urls: Iterable[str],
    params_list: Optional[List[Dict[str, Any]]] = None,
    headers: Optional[Dict[str, str]] = None,
    desc: str = "Fetching batch",
    max_threads: int = MAX_THREADS,
) -> List[Dict[str, Any]]:
    """
    Perform multiple GET requests concurrently (bounded threads).
    Each element in params_list corresponds to a URL.
    """
    results = []
    urls = list(urls)
    params_list = params_list or [{} for _ in urls]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for i, url in enumerate(urls):
            p = params_list[i] if i < len(params_list) else {}
            futures.append(executor.submit(safe_request, url, p, headers))
        for f in make_progress_bar(
            concurrent.futures.as_completed(futures), desc=desc, total=len(futures)
        ):
            result = f.result()
            if result:
                results.append(result)
    return results


# ---------------------------------------------------------------------------
# 💾 File Helpers
# ---------------------------------------------------------------------------
def save_json(data: dict, path: Path):
    """Save a JSON file safely (atomic write)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)
    logger.info(f"💾 Wrote {path.name} ({len(json.dumps(data)):,} bytes)")


def read_json(path: Path) -> Optional[dict]:
    """Load JSON safely, returning None on error."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"⚠️ Could not read {path.name}: {e}")
        return None


# ---------------------------------------------------------------------------
# 🗃️  Request Cache (to avoid redundant API hits)
# ---------------------------------------------------------------------------
def get_cache_path(url: str, params: dict, cache_dir: Path = DATA_DIR / "cache") -> Path:
    """Return deterministic cache filename for a given URL+params pair."""
    from hashlib import md5
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = f"{url}?{json.dumps(params, sort_keys=True)}".encode("utf-8")
    return cache_dir / f"{md5(key).hexdigest()}.json"


def cached_request(
    url: str,
    params: dict = None,
    headers: dict = None,
    retries: int = 3,
    timeout: float = API_TIMEOUT,
    backoff: float = RETRY_BACKOFF,
    use_cache: bool = True,
) -> Optional[dict]:
    """Wrapper around safe_request() that stores results locally."""
    cache_path = get_cache_path(url, params or {})
    if use_cache and cache_path.exists():
        return read_json(cache_path)

    result = safe_request(url, params, headers, retries, timeout, backoff)
    if result and use_cache:
        save_json(result, cache_path)
    return result
