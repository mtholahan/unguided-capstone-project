"""
Environment Verification Header
Added for consistent .env loading and mode detection across steps.
"""

import os, sys
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

"""
Step 02 – Query Discogs API for Soundtrack Metadata
Unguided Capstone Project – TMDB → Discogs Direction (step6-dev)

Uses utils.py:
    • cached_request() – retry/backoff/caching
    • save_json() – atomic writes
    • safe_filename() – cross-platform filenames
    • RateLimiter() – API pacing
"""

import json
import time
from pathlib import Path

from scripts.base_step import BaseStep
from scripts.config import (
    DATA_DIR,
    DISCOGS_API_URL,
    DISCOGS_CONSUMER_KEY,
    DISCOGS_CONSUMER_SECRET,
    DISCOGS_USER_AGENT,
    INTERMEDIATE_DIR,
)
from scripts.utils import cached_request, RateLimiter, save_json, safe_filename


class Step02QueryDiscogs(BaseStep):
    """Fetch Discogs metadata for TMDB-derived titles."""

    def __init__(self):
        super().__init__(name="step_02_query_discogs")

        self.data_dir = Path(DATA_DIR)
        self.raw_dir = Path(DATA_DIR) / "raw" / "discogs_raw"
        self.tmdb_raw_dir = Path(DATA_DIR) / "raw" / "tmdb_raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self.api_url = DISCOGS_API_URL
        self.user_agent = DISCOGS_USER_AGENT
        self.rate_limiter = RateLimiter(rate_per_sec=2)
        self.keywords = ["soundtrack", "score", "ost", "motion picture"]

    # ---------------------------------------------------------------
    def clean_title_for_query(self, title: str) -> str:
        title = title.strip()
        if "(" in title:
            title = title.split("(")[0].strip()
        return title

    # ---------------------------------------------------------------
    def fetch_discogs_release(self, title: str) -> bool:
        """Query Discogs for a movie title and save results locally."""
        safe_title = safe_filename(title)
        output_file = self.raw_dir / f"{safe_title}.json"
        checkpoint_file = self.raw_dir / "_checkpoint.json"

        if output_file.exists():
            self.logger.debug(f"⏭️ Skipping cached file {output_file.name}")
            return True

        query = self.clean_title_for_query(title)
        params = {
            "q": title,
            "type": "release",
            "per_page": 5,
            "key": DISCOGS_CONSUMER_KEY,
            "secret": DISCOGS_CONSUMER_SECRET,
        }

        resp = cached_request(
            DISCOGS_API_URL,
            params=params,
            headers={"User-Agent": DISCOGS_USER_AGENT},
            rate_limiter=self.rate_limiter,
        )

        if resp.status_code != 200:
            self.logger.warning(f"⚠️ Discogs fetch failed for '{title}' ({resp.status_code})")
            return False

        data = resp.json()
        if not data.get("results"):
            self.logger.info(f"🚫 No results for '{title}'")
            return False

        filtered = [
            r
            for r in data["results"]
            if query.lower() in r.get("title", "").lower()
            or any(k in r.get("title", "").lower() for k in self.keywords)
        ]
        data["filtered_results"] = filtered
        save_json(data, output_file)
        self.logger.info(f"✅ Saved Discogs JSON for '{title}' ({len(filtered)} filtered hits)")

        # --- Update checkpoint file
        try:
            completed = []
            if checkpoint_file.exists():
                completed = json.loads(checkpoint_file.read_text())
            completed.append(title)
            checkpoint_file.write_text(json.dumps(sorted(set(completed)), indent=2))
        except Exception as e:
            self.logger.warning(f"⚠️ Could not update checkpoint for '{title}': {e}")

        return True

    # ---------------------------------------------------------------
    def run(self):
        """
        Query Discogs API for TMDB titles (shared title list aware).

        Respects:
            • USE_GOLDEN_LIST propagated from Step 01 via titles_to_process.json
            • Checkpoint resume (_checkpoint.json)
            • Defensive retry & rate limiting via utils.cached_request()
        """
        start_time = time.time()
        checkpoint_file = self.raw_dir / "_checkpoint.json"
        skip_titles = set()

        # --- Load checkpoint if available
        if checkpoint_file.exists():
            try:
                skip_titles = set(json.loads(checkpoint_file.read_text()))
                self.logger.info(f"⏩ Resuming from checkpoint ({len(skip_titles)} done)")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not read checkpoint file: {e}")

        # --- Load shared title list from Step 01
        titles_path = Path(INTERMEDIATE_DIR) / "titles_to_process.json"
        if titles_path.exists():
            titles = json.loads(titles_path.read_text(encoding="utf-8"))
            mode = f"SHARED ({len(titles)} titles)"
            self.logger.info(f"📄 Loaded {len(titles)} titles from {titles_path.name}")
        else:
            tmdb_files = list(self.tmdb_raw_dir.glob("*.json"))
            titles = [f.stem.replace("_", " ") for f in tmdb_files]
            mode = f"AUTO ({len(titles)} titles)"
            self.logger.warning("⚠️ No shared title list found; reverting to AUTO mode")

        # --- Filter checkpointed titles
        remaining = [t for t in titles if t not in skip_titles]
        self.logger.info(f"🎧 Starting Step 02 ({mode}) | {len(remaining)} remaining / {len(titles)} total")

        success_count = 0
        for i, title in enumerate(remaining, start=1):
            if self.fetch_discogs_release(title):
                success_count += 1
            if i % 25 == 0 or i == len(remaining):
                self.logger.info(f"Progress: {i}/{len(remaining)} titles processed")

        # --- Metrics
        duration = round(time.time() - start_time, 2)
        metrics = {
            "mode": mode,
            "titles_total": len(titles),
            "titles_queried": success_count + len(skip_titles),
            "success_rate": round((success_count + len(skip_titles)) / len(titles) * 100, 2),
            "duration_sec": duration,
            "direction": "TMDB→Discogs",
            "branch": "step6-dev",
        }

        self.write_metrics(metrics, name="step02_query_discogs_metrics")
        self.logger.info(f"✅ Completed Step 02 ({mode}) | {metrics['success_rate']}% success in {duration:.2f}s")


if __name__ == "__main__":
    Step02QueryDiscogs().run()
