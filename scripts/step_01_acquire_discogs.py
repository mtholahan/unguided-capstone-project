"""
Step 01 – Acquire Discogs Data
---------------------------------
Purpose:
    Entry point for the Discogs→TMDB prototype pipeline.
    Retrieves soundtrack release metadata from the Discogs API
    and prepares it for downstream TMDB enrichment (Steps 02–04).

Modes of Operation:
    1️⃣  USE_GOLDEN_LIST = True
        • Uses the hard-coded GOLDEN_TITLES list in config.py.
        • Ignores DISCOG_MAX_TITLES.
        • Deterministic, small-scale test runs for mentor validation.

    2️⃣  USE_GOLDEN_LIST = False
        • Reads movie titles from TITLE_LIST_PATH on local disk
          (e.g., data/movie_titles_200.txt or titles_active.csv).
        • Applies DISCOG_MAX_TITLES as an upper cap.
        • Enables large-scale testing and coverage analysis.

Automation Highlights:
    • Fully parameterized via config.py (no manual edits required).
    • Parallelized Discogs API calls with safe worker management.
    • Local caching, offline (RUN_LOCAL) mode, and metrics logging.
    • Generates discogs_coverage.json summary for analysis.

Deliverables:
    • Raw JSONs cached under data/raw/discogs_raw/
    • Metrics JSON under data/metrics/
    • Updated README and slide-deck section documenting
      mode behavior, automation, and test coverage.

Author:
    Mark Holahan
Version:
    v5.0 – Oct 2025 
"""

import time
import re
import requests
import concurrent.futures
from pathlib import Path
from base_step import BaseStep
from utils import cached_request
from config import (
    DISCOGS_API_URL,
    DISCOGS_RAW_DIR,
    DISCOGS_PER_PAGE,
    DISCOGS_SLEEP_SEC,
    RATE_LIMIT_SLEEP_SEC,
    DISCOG_MAX_TITLES,
    SAVE_RAW_JSON,
    USE_GOLDEN_LIST,
    GOLDEN_TITLES,
    RUN_LOCAL,
    get_active_title_list,
    get_safe_workers,
    print_mode_summary,
)


# ===============================================================
# 🔤 Filename helpers
# ===============================================================
def safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_\-\.]+", "_", name)

def clean_title_for_query(title: str) -> str:
    """Normalize movie titles for Discogs search queries."""
    title_clean = re.sub(r"\s*\([^)]*\)", "", title)
    title_clean = re.sub(r"\s{2,}", " ", title_clean)
    title_clean = title_clean.strip().strip(" -:")
    return title_clean

# ===============================================================
# 🎵 Step 01 – Acquire Discogs data
# ===============================================================
class Step01AcquireDiscogs(BaseStep):
    """Parallel Discogs acquisition with caching + metrics."""

    def __init__(self):
        super().__init__("step_01_acquire_discogs")
        self.raw_dir = Path(DISCOGS_RAW_DIR)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = get_safe_workers("discogs")

        # --- Title source resolution ---
        print_mode_summary()

        if USE_GOLDEN_LIST:
            # Case A: Hard-coded titles, deterministic mode
            titles = GOLDEN_TITLES
            self.source = "GOLDEN"
            self.logger.info(
                f"🎬 Using GOLDEN_TITLES ({len(titles)} items) — DISCOG_MAX_TITLES ignored."
            )
        else:
            # Case B: External title file, capped by DISCOG_MAX_TITLES
            titles = get_active_title_list()
            if not titles:
                raise RuntimeError(
                    "USE_GOLDEN_LIST=False but no active title list found — "
                    "ensure TITLE_LIST_PATH exists and is non-empty."
                )

            if DISCOG_MAX_TITLES and len(titles) > DISCOG_MAX_TITLES:
                self.logger.info(
                    f"Truncating title list from {len(titles)} → {DISCOG_MAX_TITLES} (config cap)."
                )
                titles = titles[:DISCOG_MAX_TITLES]

            self.source = f"ACTIVE_FILE({len(titles)})"

        self.movie_titles = titles

        self.logger.info(
            f"🎬 Discogs acquisition initialized — Source={self.source} | "
            f"{len(self.movie_titles)} titles | {self.max_workers} workers."
        )

    # ------------------------------------------------------------
    # Local JSON loader
    # ------------------------------------------------------------
    def read_json(self, path: Path):
        import json
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to read {path.name}: {e}")
            return None

    # ------------------------------------------------------------
    # Discogs API worker
    # ------------------------------------------------------------
    def fetch_discogs_for_title(self, title: str) -> dict:
        """Fetch Discogs data (plain + soundtrack), skipping API if cached."""
        result = {"movie": title, "plain_hits": 0, "soundtrack_hits": 0}
        keywords = [
            "soundtrack", "score", "stage & screen", "ost",
            "original motion picture", "banda sonora", "film", "película"
        ]

        cleaned_title = clean_title_for_query(title)
        if cleaned_title != title:
            self.logger.debug(f"🎬 Cleaned title: '{title}' → '{cleaned_title}'")

        for mode in ("plain", "soundtrack"):
            query = cleaned_title if mode == "plain" else f"{cleaned_title} soundtrack"
            params = {"q": query, "type": "release", "per_page": DISCOGS_PER_PAGE}
            safe_title = safe_filename(title)
            out_dir = self.raw_dir / mode
            out_path = out_dir / f"{safe_title}.json"

            # 🗂️ Cached data
            if out_path.exists():
                self.logger.debug(f"⏩ Using cached {mode} file for {safe_title}")
                data = self.read_json(out_path)
                continue

            if RUN_LOCAL:
                self.logger.warning(
                    f"🌐 Skipping API (RUN_LOCAL=True) and no cache for {safe_title} ({mode})"
                )
                continue

            # 🌐 Make request with rate-limit awareness
            data = None
            attempt = 0
            while data is None and attempt < 3:
                attempt += 1
                try:
                    response = cached_request(DISCOGS_API_URL, params=params)
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", RATE_LIMIT_SLEEP_SEC))
                        self.logger.warning(
                            f"⚠️ Discogs rate limit reached. Sleeping {retry_after} seconds..."
                        )
                        time.sleep(retry_after)
                        continue  # retry same query after cooldown
                    elif response.status_code != 200:
                        self.logger.error(
                            f"❌ Discogs API error {response.status_code} for '{query}'"
                        )
                        break
                    else:
                        data = response.json()
                        if SAVE_RAW_JSON and data:
                            out_dir.mkdir(parents=True, exist_ok=True)
                            self.atomic_write(out_path, data)
                        time.sleep(DISCOGS_SLEEP_SEC)
                except requests.RequestException as e:
                    self.logger.error(f"💥 Request failed for {query}: {e}")
                    time.sleep(5)  # small retry pause
                    continue

            if not data:
                continue

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
            result[f"{mode}_hits"] = len(matches)

        return result

    # ------------------------------------------------------------
    # Run step
    # ------------------------------------------------------------
    def run(self):
        self.logger.info(f"🎵 Starting Step 01: Acquire Discogs data [{self.source}]")
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
            "discog_max_titles": DISCOG_MAX_TITLES,
            "plain_coverage": plain_total / total if total else 0,
            "soundtrack_coverage": soundtrack_total / total if total else 0,
            "duration_sec": duration,
            "max_workers": self.max_workers,
            "run_local": RUN_LOCAL,
            "source": self.source,
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
