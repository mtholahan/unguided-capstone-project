"""
Step 01: Acquire TMDB Metadata
Unguided Capstone Project – TMDB→Discogs Directional Refactor (Sprint A)

This module fetches metadata for a curated list of movies from TMDB API,
stores each movie's JSON response in data/raw/tmdb_raw/, 
and maintains an incremental checkpoint for resume safety.
"""

import json
import time
import requests
from pathlib import Path
from urllib.parse import quote
from datetime import datetime

from scripts.base_step import BaseStep
from scripts.config import TMDB_API_KEY, TMDB_REQUEST_DELAY_SEC, DATA_DIR


class Step01AcquireTMDB(BaseStep):
    """Fetch movie metadata from TMDB and store JSON results locally."""

    def __init__(self):
        super().__init__(name="step_01_acquire_tmdb")

        # ✅ Pull API key and data directory from conf.py
        self.tmdb_api_key = TMDB_API_KEY
        if not self.tmdb_api_key:
            raise EnvironmentError("TMDB_API_KEY missing in conf.py or setup_env.ps1")

        self.data_dir = Path(DATA_DIR)
        self.raw_dir = self.data_dir / "raw" / "tmdb_raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self.throttle_sec =  TMDB_REQUEST_DELAY_SEC # API safety throttle

    # -------------------------------------------------------------------------
    def load_title_list(self) -> list[str]:
        """Load movie titles from local text file."""
        titles_file = Path("data/movie_titles_200.txt")
        if not titles_file.exists():
            self.logger.error(f"❌ Titles file not found: {titles_file.resolve()}")
            return []

        with open(titles_file, "r", encoding="utf-8") as f:
            titles = [line.strip() for line in f if line.strip()]
        self.logger.info(f"📄 Loaded {len(titles)} movie titles from {titles_file}")
        return titles

    # -------------------------------------------------------------------------
    def fetch_movie(self, title: str) -> bool:
        """
        Fetch metadata for a single movie from TMDB API and save it as JSON.
        Idempotent: skips if file already exists or record checkpointed.
        """
        safe_title = "".join(c if c.isalnum() or c in " _-" else "_" for c in title)
        output_file = self.raw_dir / f"{safe_title}.json"
        checkpoint_file = self.raw_dir / "_checkpoint.json"

        # Skip if already exists
        if output_file.exists():
            self.logger.debug(f"⏭️ Skipping existing file: {output_file.name}")
            return True

        url = (
            f"https://api.themoviedb.org/3/search/movie?"
            f"api_key={self.tmdb_api_key}&query={quote(title)}"
        )

        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                self.logger.warning(f"⚠️ TMDB fetch failed for '{title}' (HTTP {resp.status_code})")
                return False

            data = resp.json()
            output_file.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
                errors="ignore"
            )

            self.logger.info(f"✅ Saved TMDB JSON for '{title}'")

            # Update checkpoint
            try:
                completed = []
                if checkpoint_file.exists():
                    completed = json.loads(checkpoint_file.read_text())
                completed.append(title)
                checkpoint_file.write_text(json.dumps(sorted(set(completed)), indent=2))
            except Exception as e:
                self.logger.warning(f"⚠️ Could not update checkpoint for '{title}': {e}")

            return True

        except Exception as e:
            self.logger.error(f"❌ Exception fetching '{title}': {e}")
            return False

    # -------------------------------------------------------------------------
    def run(self):
        """Iterate over titles, fetch TMDB metadata, and record metrics."""
        start_time = time.time()
        checkpoint_file = self.raw_dir / "_checkpoint.json"
        skip_titles = set()

        # Load checkpoint if it exists
        if checkpoint_file.exists():
            try:
                skip_titles = set(json.loads(checkpoint_file.read_text()))
                self.logger.info(f"⏩ Resuming from checkpoint — {len(skip_titles)} titles already completed.")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not read checkpoint file: {e}")

        titles = self.load_title_list()
        remaining_titles = [t for t in titles if t not in skip_titles]
        self.logger.info(f"🎬 Processing {len(remaining_titles)} remaining titles from list of {len(titles)}")

        success_count = 0
        completed = list(skip_titles)

        for i, title in enumerate(remaining_titles, start=1):
            ok = self.fetch_movie(title)
            if ok:
                success_count += 1
                completed.append(title)

            time.sleep(self.throttle_sec)

            if i % 50 == 0 or i == len(remaining_titles):
                self.logger.info(f"Progress: {i}/{len(remaining_titles)} titles processed")

        # --- Metrics logging ---
        metrics = {
            "titles_total": len(titles),
            "titles_downloaded": len(completed),
            "duration_sec": round(time.time() - start_time, 2),
            "success_rate": round(len(completed) / len(titles) * 100, 2),
        }

        self.write_metrics(metrics, name="step01_acquire_tmdb_metrics")
        self.logger.info(f"✅ Completed Step 01 | {len(completed)}/{len(titles)} titles processed")


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    Step01AcquireTMDB().run()
