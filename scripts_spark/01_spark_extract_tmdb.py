"""
Step 01: Acquire TMDB Metadata
Unguided Capstone Project – TMDB→Discogs Directional Refactor (Sprint A)

Refactored to leverage utils.py primitives:
  - cached_request(): retry/backoff + caching
  - save_json(): atomic writes
  - safe_filename(): portable file naming
  - RateLimiter(): thread-safe pacing
"""

import os, sys
import json
import time
from pathlib import Path

# 🧭 Fix path before importing scripts
project_root = Path(__file__).resolve().parents[1]
os.chdir(project_root)
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# ----------------------------------------------------------------------
# 🧭 Environment verification (runs before imports that depend on scripts/)
# ----------------------------------------------------------------------
from scripts.config_env import load_and_validate_env

# Load .env and populate environment variables
load_and_validate_env()

tmdb_key = os.getenv("TMDB_API_KEY")
local_mode = os.getenv("LOCAL_MODE", "false").lower() == "true"

if tmdb_key:
    key_status = "✅ TMDB key detected"
    key_suffix = f"(len={len(tmdb_key)})"
else:
    key_status = "🚫 TMDB key NOT found"
    key_suffix = ""

mode_status = "🌐 ONLINE mode" if not local_mode else "⚙️ OFFLINE mode"

print(
    f"\n{'='*60}\n"
    f"🔧 Environment Loaded\n"
    f"{key_status} {key_suffix}\n"
    f"{mode_status}\n"
    f"Project Root: {os.getcwd()}\n"
    f"{'='*60}\n"
)

# ----------------------------------------------------------------------
# ✅ Safe to import project modules now
# ----------------------------------------------------------------------
from scripts.base_step import BaseStep
from scripts.config import (
    TMDB_API_KEY,
    DATA_DIR,
    USE_GOLDEN_LIST,
    GOLDEN_TITLES_TEST,
    INTERMEDIATE_DIR,
)
from scripts.utils import cached_request, RateLimiter, save_json, safe_filename


class Step01AcquireTMDB(BaseStep):
    """Fetch movie metadata from TMDB API and store JSON results locally."""

    def __init__(self):
        super().__init__(name="step_01_acquire_tmdb")

        self.tmdb_api_key = TMDB_API_KEY
        if not self.tmdb_api_key:
            raise EnvironmentError("TMDB_API_KEY missing in environment or config.py")

        self.data_dir = Path(DATA_DIR)
        self.raw_dir = self.data_dir / "raw" / "tmdb_raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self.rate_limiter = RateLimiter(rate_per_sec=3)  # ~3 calls/sec

    # ------------------------------------------------------------------
    def load_title_list(self) -> list[str]:
        """Load movie titles from local file."""
        titles_file = Path("data/movie_titles_200.txt")
        if not titles_file.exists():
            self.logger.error(f"❌ Titles file not found: {titles_file.resolve()}")
            return []

        with open(titles_file, "r", encoding="utf-8") as f:
            titles = [line.strip() for line in f if line.strip()]
        self.logger.info(f"📄 Loaded {len(titles)} titles from {titles_file}")
        return titles

    # ------------------------------------------------------------------
    def fetch_movie(self, title: str) -> bool:
        """Fetch metadata for one movie, with caching and checkpoint safety."""
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

        # Update checkpoint
        try:
            completed = []
            if checkpoint_file.exists():
                completed = json.loads(checkpoint_file.read_text())
            completed.append(title)
            checkpoint_file.write_text(json.dumps(sorted(set(completed)), indent=2))
        except Exception as e:
            self.logger.warning(f"⚠️ Checkpoint update failed for '{title}': {e}")

        return True

    # ------------------------------------------------------------------
    def run(self):
        """Acquire TMDB metadata for all target titles (GOLDEN or AUTO)."""
        start_time = time.time()
        checkpoint_file = self.raw_dir / "_checkpoint.json"
        skip_titles = set()

        # --- Load checkpoint if available ---
        if checkpoint_file.exists():
            try:
                skip_titles = set(json.loads(checkpoint_file.read_text()))
                self.logger.info(f"⏩ Resuming from checkpoint ({len(skip_titles)} done)")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not read checkpoint file: {e}")

        # --- Determine which titles to process ---
        if USE_GOLDEN_LIST:
            titles = GOLDEN_TITLES_TEST
            mode = "GOLDEN"
        else:
            titles = self.load_title_list()
            mode = f"AUTO ({len(titles)} titles)"

        # --- Persist the active title list for downstream steps ---
        try:
            titles_path = Path(INTERMEDIATE_DIR) / "titles_to_process.json"
            Path(INTERMEDIATE_DIR).mkdir(parents=True, exist_ok=True)
            titles_path.write_text(json.dumps(titles, indent=2, ensure_ascii=False), encoding="utf-8")
            self.logger.info(f"💾 Saved {len(titles)} titles → {titles_path.name} for downstream use")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not save shared title list: {e}")

        # --- Filter out checkpointed titles ---
        remaining = [t for t in titles if t not in skip_titles]
        self.logger.info(f"🎬 Starting Step 01 ({mode}) | {len(remaining)} remaining / {len(titles)} total")

        success_count = 0
        for i, title in enumerate(remaining, start=1):
            if self.fetch_movie(title):
                success_count += 1
            if i % 25 == 0 or i == len(remaining):
                self.logger.info(f"Progress: {i}/{len(remaining)} titles processed")

        # --- Metrics ---
        duration = round(time.time() - start_time, 2)
        metrics = {
            "mode": mode,
            "titles_total": len(titles),
            "titles_downloaded": success_count + len(skip_titles),
            "success_rate": round((success_count + len(skip_titles)) / len(titles) * 100, 2),
            "duration_sec": duration,
            "direction": "TMDB→Discogs",
            "branch": "step6-dev",
        }

        self.write_metrics(metrics, name="step01_acquire_tmdb_metrics")
        self.logger.info(f"✅ Completed Step 01 ({mode}) | {metrics['success_rate']}% success in {duration:.2f}s")


if __name__ == "__main__":
    Step01AcquireTMDB().run()
