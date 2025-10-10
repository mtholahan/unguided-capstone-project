"""
Step 02 – Fetch TMDB Data
---------------------------------
Purpose:
    Retrieve movie metadata from The Movie Database (TMDB) for each
    Discogs-acquired title produced in Step 01. This step enriches
    the soundtrack dataset with canonical TMDB identifiers, release
    years, and popularity metrics for downstream matching (Steps 03–04).

Modes of Operation:
    1️⃣  USE_GOLDEN_LIST = True
        • Reads the in-code GOLDEN_TITLES list defined in config.py.
        • Ignores DISCOG_MAX_TITLES.
        • Supports reproducible, small-batch validation runs.

    2️⃣  USE_GOLDEN_LIST = False
        • Loads titles from TITLE_LIST_PATH on local disk
          (for example: data/movie_titles_200.txt or titles_active.csv).
        • Applies DISCOG_MAX_TITLES as an upper cap.
        • Enables large-scale tests (up to 200 titles).

Automation Highlights:
    • Parallelized API calls with thread-safe RateLimiter to respect
      TMDB_RATE_LIMIT and API quotas.
    • Caching layer writes JSON payloads under data/raw/tmdb_raw/,
      allowing deterministic re-runs in RUN_LOCAL mode.
    • Centralized configuration (no manual code edits needed).
    • Metrics automatically written to data/metrics/tmdb_fetch_metrics.json.

Deliverables:
    • Raw TMDB JSON files (per title)
    • Aggregated metrics JSON with success, cache, and error counts
    • Log entries summarizing runtime, rate limit, and coverage

Dependencies:
    • Step 01 must complete successfully to supply Discogs-based title inputs.
    • Requires TMDB_API_KEY environment variable to be set.

Author:
    Mark Holahan
Version:
    v6.0 – Oct 2025 (refactored for dual-mode title sourcing)
"""

import time
import re
import concurrent.futures
from pathlib import Path
from base_step import BaseStep
from utils import cached_request, RateLimiter
from config import (
    TMDB_SEARCH_URL,
    TMDB_RATE_LIMIT,
    TMDB_RAW_DIR,
    DISCOG_MAX_TITLES,
    RUN_LOCAL,
    SAVE_RAW_JSON,
    USE_GOLDEN_LIST,
    GOLDEN_TITLES,
    get_active_title_list,
    get_safe_workers,
)

# ===============================================================
# 🔤 Safe filename helper
# ===============================================================
def safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_\-\.]+", "_", name)


# ===============================================================
# 🧩 JSON extraction helper
# ===============================================================
def extract_json(resp):
    """Return consistent JSON from either Response-like or dict."""
    if resp is None:
        return {}
    if isinstance(resp, dict):
        return resp
    if hasattr(resp, "json"):
        try:
            return resp.json() or {}
        except Exception:
            return {}
    return {}


# ===============================================================
# 🎬 Step 02 – Fetch TMDB data
# ===============================================================
class Step02FetchTMDB(BaseStep):
    """Parallel TMDB acquisition with caching, rate limiting, and metrics."""

    def __init__(self):
        super().__init__("step_02_fetch_tmdb")
        self.rate_limiter = RateLimiter(TMDB_RATE_LIMIT)
        self.raw_dir = Path(TMDB_RAW_DIR)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = get_safe_workers("tmdb")

        # --- Title source logic
        if USE_GOLDEN_LIST:
            titles = GOLDEN_TITLES
            source = "GOLDEN"
        else:
            titles = get_active_title_list()
            source = "ACTIVE"

        self.movie_titles = titles[:DISCOG_MAX_TITLES]
        self.source = source

        self.logger.info(
            f"🎬 TMDB acquisition initialized — Source={source} | "
            f"{len(self.movie_titles)} titles (limit={DISCOG_MAX_TITLES}) | "
            f"{self.max_workers} workers (rate={TMDB_RATE_LIMIT}/s)."
        )

    # --------------------------
    # TMDB worker
    # --------------------------
    def fetch_tmdb_data(self, title: str) -> dict:
        """Fetch and cache TMDB results for a single movie title."""
        safe_title = safe_filename(title)
        cache_file = self.raw_dir / f"{safe_title}.json"

        # ✅ Use cache if available
        if cache_file.exists():
            self.logger.debug(f"⏩ Using cached TMDB file for {safe_title}")
            return {"title": title, "results_count": 0, "cached": True}

        if RUN_LOCAL:
            self.logger.warning(f"🌐 Skipping TMDB API (RUN_LOCAL=True) — no cache for {title}")
            return {"title": title, "results_count": 0, "cached": False}

        try:
            with self.rate_limiter:
                resp = cached_request(TMDB_SEARCH_URL, params={"query": title})

            data = extract_json(resp)
            results = data.get("results", []) if isinstance(data, dict) else []

            if results:
                payload = [
                    {
                        "query_title": title,
                        "tmdb_id": r.get("id"),
                        "original_title": r.get("original_title"),
                        "release_date": r.get("release_date"),
                        "vote_average": r.get("vote_average"),
                        "popularity": r.get("popularity"),
                    }
                    for r in results
                ]
                if SAVE_RAW_JSON:
                    self.atomic_write(cache_file, payload)
                self.logger.info(f"🎬 {title}: {len(results)} TMDB results fetched.")
                return {"title": title, "results_count": len(results)}

            # handle empty/no results
            self.logger.debug(f"{title}: No TMDB results found.")
            return {"title": title, "results_count": 0}

        except Exception as e:
            self.logger.error(f"{title}: TMDB fetch failed → {e}")
            return {"title": title, "results_count": 0, "error": str(e)}

    # --------------------------
    # Main run sequence
    # --------------------------
    def run(self):
        self.logger.info(f"🎥 Starting Step 02: Fetch TMDB data [{self.source}]")
        t0 = time.time()
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {
                executor.submit(self.fetch_tmdb_data, title): title
                for title in self.movie_titles
            }
            for future in concurrent.futures.as_completed(future_map):
                title = future_map[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    self.logger.error(f"{title}: thread failed → {e}")

        duration = round(time.time() - t0, 2)
        total = len(results)
        successes = sum(r["results_count"] > 0 for r in results)
        cached = sum(r.get("cached", False) for r in results)
        errors = sum(1 for r in results if r.get("error"))

        summary = {
            "titles_total": total,
            "successful_fetches": successes,
            "cached_skipped": cached,
            "errors": errors,
            "duration_sec": duration,
            "max_workers": self.max_workers,
            "run_local": RUN_LOCAL,
            "source": self.source,
        }

        self.save_metrics("tmdb_fetch_metrics.json", {"summary": summary, "details": results})
        self.write_metrics(summary)
        self.logger.info(
            f"✅ Step 02 completed in {duration:.2f}s | "
            f"{successes}/{total} successes, {cached} cached skips, {errors} errors."
        )


# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step02FetchTMDB().run()