"""
step_01_acquire_discogs.py
---------------------------------
Purpose:
    Acquire Discogs release data for enrichment with TMDB.
    Parallelized + cached + BaseStep-integrated with config-driven concurrency.

Version:
    v4.4 – Oct 2025
"""

import time
import re
import concurrent.futures
from pathlib import Path
from base_step import BaseStep
from utils import cached_request
from config import (
    DISCOGS_API_URL,
    DISCOGS_PER_PAGE,
    DISCOGS_SLEEP_SEC,
    SAVE_RAW_JSON,
    DISCOGS_RAW_DIR,
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
# 🎵 Step 01 – Acquire Discogs data
# ===============================================================
class Step01AcquireDiscogs(BaseStep):
    """Parallel Discogs acquisition with caching + metrics."""

    def __init__(self):
        super().__init__("step_01_acquire_discogs")
        self.movie_titles = get_active_title_list() or GOLDEN_TITLES
        self.raw_dir = Path(DISCOGS_RAW_DIR)
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # ✅ Unified concurrency (auto-tuned)
        self.max_workers = get_safe_workers("discogs")
        self.logger.info(
            f"Loaded {len(self.movie_titles)} titles | Using {self.max_workers} workers."
        )

    # --------------------------
    # Discogs worker
    # --------------------------
    def fetch_discogs_for_title(self, title: str) -> dict:
        result = {"movie": title, "plain_hits": 0, "soundtrack_hits": 0}
        keywords = [
            "soundtrack", "score", "stage & screen", "ost",
            "original motion picture", "banda sonora", "film", "película"
        ]

        for mode in ("plain", "soundtrack"):
            query = title if mode == "plain" else f"{title} soundtrack"
            params = {"q": query, "type": "release", "per_page": DISCOGS_PER_PAGE}
            safe_title = safe_filename(title)
            out_dir = self.raw_dir / mode
            out_path = out_dir / f"{safe_title}.json"

            # ✅ Skip cached responses
            if SAVE_RAW_JSON and out_path.exists():
                self.logger.debug(f"⏩ Skipping cached {mode} file for {safe_title}")
                continue

            try:
                data = cached_request(DISCOGS_API_URL, params=params)
                if not data:
                    continue

                if SAVE_RAW_JSON:
                    out_dir.mkdir(parents=True, exist_ok=True)
                    self.atomic_write(out_path, data)

                results = data.get("results", [])
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
                if mode == "plain":
                    result["plain_hits"] = len(matches)
                else:
                    result["soundtrack_hits"] = len(matches)

                time.sleep(DISCOGS_SLEEP_SEC)
            except Exception as e:
                self.logger.error(f"{title} ({mode}) failed → {e}")

        return result


    # --------------------------
    # Run step
    # --------------------------
    def run(self):
        self.logger.info("🎵 Starting Step 01: Acquire Discogs data")
        t0 = time.time()
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {
                executor.submit(self.fetch_discogs_for_title, title): title
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
        plain_total = sum(r["plain_hits"] > 0 for r in results)
        soundtrack_total = sum(r["soundtrack_hits"] > 0 for r in results)

        summary = {
            "titles_total": total,
            "plain_coverage": plain_total / total if total else 0,
            "soundtrack_coverage": soundtrack_total / total if total else 0,
            "duration_sec": duration,
            "max_workers": self.max_workers,
        }

        self.save_metrics("discogs_coverage.json", {"summary": summary, "details": results})
        self.write_metrics(summary)
        self.logger.info(
            f"🎯 Coverage: {plain_total}/{total} plain, "
            f"{soundtrack_total}/{total} soundtrack | {duration:.2f}s total."
        )
        self.logger.info("✅ Step 01 completed successfully.")
        

# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step01AcquireDiscogs().run()
