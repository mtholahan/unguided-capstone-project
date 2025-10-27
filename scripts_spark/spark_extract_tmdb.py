"""
🎬 Step 01: Acquire TMDB Metadata
Unguided Capstone Project – Unified Environment-Aware Version
"""

import os
import json
import time
from pathlib import Path
from scripts.base_step import BaseStep
from scripts.config_env import load_and_validate_env
from scripts.utils import cached_request, RateLimiter, save_json, safe_filename

# ===============================================================
# 🎯 Step 01 – Acquire TMDB Metadata
# ===============================================================
class Step01AcquireTMDB(BaseStep):
    """Fetch movie metadata from TMDB API and store JSON results locally."""

    def __init__(self, spark=None):
        super().__init__(name="step_01_acquire_tmdb")
        self.spark = spark

        # ✅ Environment-aware directories
        self.output_dir = Path(os.getenv("PIPELINE_OUTPUT_DIR", "data/intermediate")).resolve()
        self.metrics_dir = Path(os.getenv("PIPELINE_METRICS_DIR", "data/metrics")).resolve()
        self.raw_dir = self.output_dir / "tmdb_raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # ✅ Environment variables
        load_and_validate_env()
        self.tmdb_api_key = os.getenv("TMDB_API_KEY")
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
        """Fetch metadata for a single title and save results locally."""
        safe_title = safe_filename(title)
        output_file = self.raw_dir / f"{safe_title}.json"
        checkpoint_file = self.raw_dir / "_checkpoint.json"

        # Skip if cached
        if output_file.exists():
            self.logger.debug(f"⏭️ Skipping cached {output_file.name}")
            return True

        url = "https://api.themoviedb.org/3/search/movie"
        params = {"query": title, "api_key": self.tmdb_api_key}

        resp = cached_request(url, params=params, rate_limiter=self.rate_limiter)
        if resp.status_code != 200:
            self.logger.warning(f"⚠️ TMDB fetch failed for '{title}' ({resp.status_code})")
            return False

        data = resp.json()
        save_json(data, output_file)
        self.logger.info(f"✅ Saved TMDB JSON for '{title}'")

        # --- Update checkpoint
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
        metrics = {
            "titles_total": len(titles),
            "titles_downloaded": success_count + len(skip_titles),
            "success_rate": round((success_count + len(skip_titles)) / len(titles) * 100, 2),
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
