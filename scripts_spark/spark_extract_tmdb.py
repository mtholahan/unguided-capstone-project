"""
🎬 Step 01: Acquire TMDB Metadata
Unguided Capstone Project – Unified Environment-Aware Version
"""

import os
import json
import time
from pathlib import Path
from scripts.utils.base_step import BaseStep
from scripts.config_env import load_and_validate_env
from scripts.utils.pipeline_helpers import RateLimiter, save_json
from scripts.utils.env import load_env
from scripts.utils.io_utils import safe_filename, write_json_cache, read_json_cache

env = load_env()

# ===============================================================
# 🎯 Step 01 – Acquire TMDB Metadata
# ===============================================================
class Step01AcquireTMDB(BaseStep):
    """Fetch movie metadata from TMDB API and store JSON results locally."""

    def __init__(self, spark=None):
        super().__init__(name="step_01_acquire_tmdb")
        self.spark = spark

        # ✅ Environment-aware directories
        root_path = Path(env.get("ROOT") or env.get("root", ".")).resolve()

        self.output_dir = root_path / "data" / "intermediate"
        self.metrics_dir = root_path / "data" / "metrics"
        self.raw_dir = self.output_dir / "tmdb_raw"

        # Ensure the raw dir exists (idempotent)
        self.raw_dir.mkdir(parents=True, exist_ok=True)


        # ✅ Environment variabless
        load_and_validate_env()
        self.tmdb_api_key = env.get("TMDB_API_KEY")

        if not self.tmdb_api_key:
            raise EnvironmentError("❌ TMDB_API_KEY missing in environment.")

        # ✅ Rate limiter
        self.rate_limiter = RateLimiter(rate_per_sec=3)  # ~3 calls/sec

    # ---------------------------------------------------------------
    def load_title_list(self) -> list[str]:
        """Load movie titles from local source file."""
        titles_file = Path("data/movie_titles_200.txt")
        if not titles_file.exists():
            self.logger.error(f"❌ Titles file not found: {titles_file.resolve()}")
            return []

        titles = [t.strip() for t in titles_file.read_text(encoding="utf-8").splitlines() if t.strip()]
        self.logger.info(f"📄 Loaded {len(titles)} titles from {titles_file}")
        return titles

    # ---------------------------------------------------------------

    def fetch_movie(self, title: str) -> bool:
        """Fetch metadata for a single title and save results locally, using JSON cache."""
        safe_title = safe_filename(title)
        output_file = self.raw_dir / f"{safe_title}.json"
        checkpoint_file = self.raw_dir / "_checkpoint.json"

        # --- Check cache first
        cached = read_json_cache(output_file)
        if cached:
            self.logger.debug(f"🌀 Using cached TMDB JSON for {output_file.name}")
            return True

        url = "https://api.themoviedb.org/3/search/movie"
        params = {"query": title, "api_key": self.tmdb_api_key}

        # --- Apply rate limiting before API call
        self.rate_limiter.wait_for_slot()

        try:
            import requests
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self.logger.warning(f"⚠️ TMDB fetch failed for '{title}': {e}")
            return False

        # --- Write fresh response to cache
        write_json_cache(data, output_file)
        self.logger.info(f"✅ Cached TMDB JSON for '{title}' → {output_file.name}")

        # --- Update checkpoint safely
        try:
            completed = []
            if checkpoint_file.exists():
                completed = json.loads(checkpoint_file.read_text())
            completed.append(title)
            save_json(sorted(set(completed)), checkpoint_file)
        except Exception as e:
            self.logger.warning(f"⚠️ Checkpoint update failed for '{title}': {e}")

        return True


    # ---------------------------------------------------------------
    def run(self):
        """Acquire TMDB metadata for all target titles."""
        start_time = time.time()
        checkpoint_file = self.raw_dir / "_checkpoint.json"
        skip_titles = set()

        if checkpoint_file.exists():
            try:
                skip_titles = set(json.loads(checkpoint_file.read_text()))
                self.logger.info(f"⏩ Resuming from checkpoint ({len(skip_titles)} done)")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not read checkpoint file: {e}")

        # --- Determine title list
        titles = self.load_title_list()
        remaining = [t for t in titles if t not in skip_titles]
        self.logger.info(f"🎬 Starting Step 01 | {len(remaining)} remaining / {len(titles)} total")
        self.logger.info(f"🕒 Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        # --- Save title list for downstream use
        titles_path = self.output_dir / "titles_to_process.json"
        save_json(titles, titles_path)
        self.logger.info(f"💾 Saved {len(titles)} titles → {titles_path.name} for downstream use")

        # --- Fetch all titles
        success_count = 0
        for i, title in enumerate(remaining, start=1):
            if self.fetch_movie(title):
                success_count += 1
            if i % 25 == 0 or i == len(remaining):
                self.logger.info(f"Progress: {i}/{len(remaining)} titles processed")

        # --- Metrics ---
        duration = round(time.time() - start_time, 2)
        total_titles = len(titles)
        completed = success_count + len(skip_titles)
        success_rate = round((completed / total_titles * 100), 2) if total_titles > 0 else 0.0

        metrics = {
            "titles_total": total_titles,
            "titles_downloaded": completed,
            "success_rate": success_rate,
            "duration_sec": duration,
            "direction": "TMDB→Discogs",
        }

        self.write_metrics(metrics, name="step01_acquire_tmdb_metrics")
        self.logger.info(f"✅ Completed Step 01 | {metrics['success_rate']}% success in {duration:.2f}s")

# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step01AcquireTMDB(None).run()
