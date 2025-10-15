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

import os, re, time, json, hashlib, unicodedata, logging, threading, concurrent.futures
from pathlib import Path
from tqdm import tqdm
from types import SimpleNamespace
from typing import List, Dict, Any, Iterable, Optional
import requests

from scripts.config import (
    TMDB_API_KEY,
    API_TIMEOUT,
    RETRY_BACKOFF,
    LOG_LEVEL,
    MAX_THREADS,
    DATA_DIR,
    DISCOGS_USER_AGENT,
    DISCOGS_SLEEP_SEC,
)

try:
    from scripts.config import AZURE_SAS_TOKEN
except ImportError:
    AZURE_SAS_TOKEN = None


# ---------------------------------------------------------------------------
# 🪶 Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("Utils")
logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))


# ---------------------------------------------------------------------------
# 🕒 Rate Limiter
# ---------------------------------------------------------------------------
class RateLimiter:
    """Simple thread-safe rate limiter."""
    def __init__(self, rate_per_sec: float = 3.0):
        self.min_interval = 1.0 / float(rate_per_sec or 3.0)
        self.last_call = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            elapsed = time.time() - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

    def __enter__(self):
        self.wait(); return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


# ---------------------------------------------------------------------------
# 🎞 Title Normalization
# ---------------------------------------------------------------------------
def normalize_title_for_matching(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFKD", text.lower().strip())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[\(\[\{].*?[\)\]\}]", " ", text)
    noise = [
        r"original motion picture soundtrack", r"original soundtrack",
        r"motion picture soundtrack", r"complete motion picture score",
        r"deluxe edition", r"expanded edition", r"\bost\b",
        r"\bsoundtrack\b", r"\bscore\b",
    ]
    for pat in noise:
        text = re.sub(pat, " ", text, flags=re.I)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return " ".join(t for t in text.split() if len(t) > 1)


def normalize_for_matching_extended(text: str) -> str:
    ROMAN_MAP = {
        " i ": " 1 ", " ii ": " 2 ", " iii ": " 3 ", " iv ": " 4 ",
        " v ": " 5 ", " vi ": " 6 ", " vii ": " 7 ", " viii ": " 8 ",
        " ix ": " 9 ", " x ": " 10 ",
    }
    ARTICLES = {"the", "a", "an"}
    base = normalize_title_for_matching(text)
    if not base:
        return ""
    s = f" {base} "
    for k, v in ROMAN_MAP.items():
        s = s.replace(k, v)
    toks = s.strip().split()
    while toks and toks[0] in ARTICLES:
        toks = toks[1:]
    s = " ".join(toks)
    s = re.sub(r"\b(part|episode|chapter|vol|volume)\s+\d+\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def clean_title(text: str) -> str:
    if not isinstance(text, str): return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", "", text.lower())).strip()


# ---------------------------------------------------------------------------
# 🧩 Progress Bar
# ---------------------------------------------------------------------------
def make_progress_bar(iterable=None, desc=None, total=None, leave=True, **kw):
    desc = (desc or "Working")[:40]
    return tqdm(iterable, desc=desc, leave=leave, total=total, **kw) if iterable is not None \
        else tqdm(total=total, desc=desc, leave=leave, **kw)


# ---------------------------------------------------------------------------
# 💾 File Helpers
# ---------------------------------------------------------------------------
def save_json(data: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)
    logger.debug(f"💾 Saved cache: {path.name} ({len(json.dumps(data)):,} bytes)")


def read_json(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.debug(f"⚠️ Cache read failed ({path.name}): {e}")
        return None


def get_cache_path(url: str, params: dict, cache_dir: Path = DATA_DIR / "cache") -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = f"{url}?{json.dumps(params, sort_keys=True)}".encode("utf-8")
    return cache_dir / f"{hashlib.md5(key).hexdigest()}.json"


def safe_filename(name: str, max_len: int = 120) -> str:
    """Return a cross-platform, filesystem-safe filename."""
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    s = re.sub(r'[\\/*?:"<>|]+', "_", s)     # illegal chars (Win/macOS/Linux)
    s = re.sub(r"\s+", " ", s).strip()       # collapse whitespace
    s = s.replace(" ", "_")                  # spaces → underscores
    s = s[:max_len] if max_len > 0 else s
    return s or "file"


# ---------------------------------------------------------------------------
# 🌐 Safe Request + Cache Wrapper
# ---------------------------------------------------------------------------
def cached_request(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    retries: int = 3,
    timeout: float = API_TIMEOUT,
    backoff: float = RETRY_BACKOFF,
    use_cache: bool = True,
    rate_limiter: Optional[RateLimiter] = None,
) -> SimpleNamespace:
    """
    Unified GET wrapper:
      - Discogs/TMDB auth injection
      - Retry with exponential backoff
      - File-based caching
      - Returns Response-like object (status_code, json())
    """
    log = logging.getLogger("utils.cached_request")
    params, headers = params or {}, headers or {}

    # 🔐 Inject API keys
    if "discogs.com" in url:
        params.setdefault("token", os.getenv("DISCOGS_TOKEN"))
        headers.setdefault("User-Agent", DISCOGS_USER_AGENT or "UnguidedCapstone/1.0")
    elif "themoviedb.org" in url:
        params.setdefault("api_key", TMDB_API_KEY)
    elif "blob.core.windows.net" in url and AZURE_SAS_TOKEN:
        params.setdefault("sv", AZURE_SAS_TOKEN)

    cache_path = get_cache_path(url, params)
    if use_cache and cache_path.exists():
        cached_data = read_json(cache_path)
        if cached_data:
            return SimpleNamespace(status_code=200, json=lambda: cached_data)

    for attempt in range(1, retries + 1):
        try:
            if rate_limiter:
                rate_limiter.wait()
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            code = resp.status_code

            if code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                log.warning(f"⚠️ 429 Too Many Requests. Sleeping {wait}s...")
                time.sleep(wait)
                continue
            if code >= 500:
                log.warning(f"⚠️ Server error {code}. Retrying in {backoff * attempt:.1f}s...")
                time.sleep(backoff * attempt)
                continue

            resp.raise_for_status()
            data = resp.json()
            if use_cache:
                save_json(data, cache_path)
            time.sleep(DISCOGS_SLEEP_SEC)
            return resp

        except requests.RequestException as e:
            log.warning(f"⚠️ Attempt {attempt}/{retries} failed: {e}")
            time.sleep(backoff * attempt)

    log.error(f"❌ Exhausted retries for {url}")
    return SimpleNamespace(status_code=500, json=lambda: {"error": "Request failed"})


# ---------------------------------------------------------------------------
# ⚡ Threaded Batch Fetch
# ---------------------------------------------------------------------------
def batch_fetch(
    urls: Iterable[str],
    params_list: Optional[List[Dict[str, Any]]] = None,
    headers: Optional[Dict[str, str]] = None,
    desc: str = "Fetching batch",
    max_threads: int = MAX_THREADS,
    rate_limiter: Optional[RateLimiter] = None,
) -> List[dict]:
    results = []
    urls = list(urls)
    params_list = params_list or [{} for _ in urls]
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as ex:
        futures = [
            ex.submit(cached_request, urls[i], params_list[i], headers, rate_limiter=rate_limiter)
            for i in range(len(urls))
        ]
        for f in make_progress_bar(concurrent.futures.as_completed(futures),
                                   desc=desc, total=len(futures)):
            r = f.result()
            if r and getattr(r, "status_code", 500) == 200:
                results.append(r.json())
    return results
