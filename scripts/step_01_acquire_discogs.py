"""
step_01_acquire_discogs.py
---------------------------------
Purpose:
    Acquire Discogs release data for enrichment with TMDB.
    Refactored for config.py v2 compatibility.

Version:
    v3 – Oct 2025

Author:
    Mark Holahan
"""

import os
import time
import json
import logging
import requests
from pathlib import Path

from config import (
    DISCOGS_API_URL,
    DISCOGS_CONSUMER_KEY,
    DISCOGS_CONSUMER_SECRET,
    DISCOGS_USER_AGENT,
    DISCOGS_PER_PAGE,
    DISCOGS_SLEEP_SEC,
    DISCOGS_MAX_RETRIES,
    API_TIMEOUT,
    RETRY_BACKOFF,
    LOG_LEVEL,
    SAVE_RAW_JSON,
    DISCOGS_RAW_DIR,
    print_config_summary,
    GOLDEN_TITLES,
    get_active_title_list
)

MOVIE_TITLES = get_active_title_list()
print(f"[Info] Loaded {len(MOVIE_TITLES)} titles for this run.")


# === 1. Logging setup ===
from config import LOG_DIR

LOG_PATH = LOG_DIR / "discogs_step01.log"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH,
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.info("=== Starting Step 01: Acquire Discogs data ===")


print_config_summary()


# === 2. Credential validation ===
if not DISCOGS_CONSUMER_KEY or not DISCOGS_CONSUMER_SECRET:
    raise EnvironmentError("❌ Missing Discogs API credentials. Run setup_env.ps1 first.")

HEADERS = {"User-Agent": DISCOGS_USER_AGENT}


# === 3. Core fetch helper ===
def safe_request(url, params, retries=DISCOGS_MAX_RETRIES):
    """Request wrapper with retry logic and exponential backoff."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=API_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logging.warning(f"Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(RETRY_BACKOFF * attempt)
            else:
                logging.error(f"Final failure for params={params}: {e}")
                return None


# === 4. Discogs search function ===
def search_discogs(title, mode="plain"):
    """
    Query Discogs API for soundtrack or plain title variations.
    Saves raw responses conditionally per config.py.
    """
    query = title if mode == "plain" else f"{title} soundtrack"
    params = {
        "q": query,
        "type": "release",
        "per_page": DISCOGS_PER_PAGE,
        "key": DISCOGS_CONSUMER_KEY,
        "secret": DISCOGS_CONSUMER_SECRET,
    }

    data = safe_request(DISCOGS_API_URL, params)
    if not data:
        return 0

    # Save raw JSON if enabled
    if SAVE_RAW_JSON:
        out_dir = DISCOGS_RAW_DIR / mode
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{title.replace(' ', '_')}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logging.info(f"📁 Saved {mode} JSON: {title}")

    # Filter results for soundtrack-like entries
    results = data.get("results", [])
    keywords = ["soundtrack", "score", "stage & screen", "ost", "original motion picture"]
    matches = [
        item for item in results
        if any(
            kw in str(
                (item.get("genre") or [])
                + (item.get("style") or [])
                + [item.get("title", "")]
            ).lower()
            for kw in keywords
        )
    ]

    time.sleep(DISCOGS_SLEEP_SEC)
    return len(matches)


# === 5. Test set (temporary — replace with Discogs batch loader later) ===
MOVIE_TITLES = GOLDEN_TITLES


# === 6. Run search comparison ===
coverage = []
for title in MOVIE_TITLES:
    plain_hits = search_discogs(title, "plain")
    soundtrack_hits = search_discogs(title, "soundtrack")
    coverage.append({
        "movie": title,
        "plain_hits": plain_hits,
        "soundtrack_hits": soundtrack_hits,
    })
    print(f"{title:35} | Plain: {plain_hits:<2} | Soundtrack: {soundtrack_hits:<2}")


# === 7. Summary metrics ===
total = len(MOVIE_TITLES)
plain_total = sum(c["plain_hits"] > 0 for c in coverage)
soundtrack_total = sum(c["soundtrack_hits"] > 0 for c in coverage)

print("\n🎯 Coverage Summary")
print(f"Plain queries:      {plain_total}/{total} ({plain_total / total:.1%})")
print(f"'Soundtrack' queries: {soundtrack_total}/{total} ({soundtrack_total / total:.1%})")

logging.info("✅ Step 01 completed successfully.")
