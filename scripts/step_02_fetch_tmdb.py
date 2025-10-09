"""
step_02_fetch_tmdb.py
---------------------------------
Purpose:
    Fetch TMDB movie data for Discogs-enriched titles.
    Parallelized, cached, and rate-limited for stable, repeatable ETL runs.

Version:
    v4.4 – Oct 2025

Author:
    Mark Holahan
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
    GOLDEN_TITLES_TEST,
    get_safe_workers,
)

# ===============================================================
# 🔤 Safe filename helper
# ===============================================================
def safe_filename(name: str) -> str:
    """Return a filesystem-safe version of a string (Windows-compatible)."""
    return re.sub(r"[^A-Za-z0-9_\-\.]+", "_", name)


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

        # ✅ Unified concurrency logic (consistent with Step 01)
        self.max_workers = get_safe_workers("tmdb")
        self.movie_titles = GOLDEN_TITLES_TEST
        self.logger.info(
            f"Loaded {len(self.movie_titles)} titles | Using {self.max_workers} workers "
            f"(rate={TMDB_RATE_LIMIT}/s)."
        )

    # --------------------------
    # TMDB worker
    # --------------------------
    def fetch_tmdb_data(self, title: str) -> dict:
        """Fetch and cache TMDB results for a single movie title."""
        safe_title = safe_filename(title)
        cache_file = self.raw_dir / f"{safe_title}.json"

        # ✅ Skip cached response if JSON already exists
        if cache_file.exists():
            self.logger.debug(f"⏩ Skipping cached TMDB file for {safe_title}")
            return {"title": title, "results_count": 0, "cached": True}

        try:
            # Enforce API rate limiting
            with self.rate_limiter:
                response = cached_request(TMDB_SEARCH_URL, params={"query": title})
            if not response:
                return {"title": title, "results_count": 0}

            results = response.get("results", [])
            if results:
                # Save raw JSON for harmonization
                self.atomic_write(cache_file, response)
                self.logger.info(f"🎬 {title}: {len(results)} results fetched.")

            return {"title": title, "results_count": len(results)}

        except Exception as e:
            self.logger.error(f"{title}: TMDB fetch failed → {e}")
            return {"title": title, "results_count": 0, "error": str(e)}

    # --------------------------
    # Main run sequence
    # --------------------------
    def run(self):
        """Main step execution logic."""
        self.logger.info("🎬 Starting Step 02: Fetch TMDB data")
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
