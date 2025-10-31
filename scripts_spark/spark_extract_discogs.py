import os
import json
import time
from pathlib import Path
from scripts.utils.base_step import BaseStep
from scripts.config_env import load_and_validate_env
from scripts.utils.pipeline_helpers import cached_request, RateLimiter, save_json, safe_filename
from scripts.utils.env import load_env

env = load_env()

# ===============================================================
# 🎯 Step 02 – Query Discogs API for Soundtrack Metadata
# ===============================================================
class Step02QueryDiscogs(BaseStep):
    """Fetch Discogs metadata for TMDB-derived titles."""

    def __init__(self, spark=None):
        super().__init__(name="step_02_query_discogs")
        self.spark = spark

        # ✅ Environment-aware directories
        from pathlib import Path

        root_path = Path(env.get("ROOT") or env.get("root", ".")).resolve()

        self.output_dir = root_path / "data" / "intermediate"
        self.metrics_dir = root_path / "data" / "metrics"
        self.raw_dir = self.output_dir / "discogs_raw"

        # Ensure directory exists
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self.raw_dir = self.output_dir / "discogs_raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # ✅ API + rate limiting
        load_and_validate_env()
        self.api_url = env.get("DISCOGS_API_URL", "https://api.discogs.com/database/search")
        self.user_agent = env.get("DISCOGS_USER_AGENT", f"UnguidedCapstone/1.0 +https://github.com/{os.getenv('USER','localuser')}")
        self.key = env.get("DISCOGS_CONSUMER_KEY")
        self.secret = env.get("DISCOGS_CONSUMER_SECRET")
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
        safe_title = safe_filename(title)
        output_file = self.raw_dir / f"{safe_title}.json"
        checkpoint_file = self.raw_dir / "_checkpoint.json"

        # Skip if cached
        if output_file.exists():
            self.logger.debug(f"💾 Skipping cached {output_file.name}")
            return True

        # --- Construct query ---
        params = {
            "q": title,
            "type": "release",
            "per_page": 5,
        }

        # --- Flexible authentication ---
        headers = {"User-Agent": self.user_agent}
        if self.key and not self.secret:
            # Personal token mode
            headers["Authorization"] = f"Discogs token={self.key}"
        elif self.key and self.secret:
            # OAuth key/secret mode → must be query params
            params["key"] = self.key
            params["secret"] = self.secret

        # print("DEBUG URL:", self.api_url)
        # print("DEBUG PARAMS:", params)
        # print("DEBUG HEADERS:", headers)

        data = cached_request(
            self.api_url,
            output_file,
            params=params,
            headers=headers,
            rate_limiter=self.rate_limiter,
        )

        if not data:
            self.logger.warning(f"⚠️ Discogs fetch failed for '{title}'")
            return False

        save_json(data, output_file)
        self.logger.info(f"✅ Saved Discogs JSON for '{title}'")

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
        """Query Discogs API for TMDB titles."""
        start_time = time.time()
        checkpoint_file = self.raw_dir / "_checkpoint.json"
        skip_titles = set()

        if checkpoint_file.exists():
            try:
                skip_titles = set(json.loads(checkpoint_file.read_text()))
                self.logger.info(f"⏩ Resuming from checkpoint ({len(skip_titles)} done)")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not read checkpoint file: {e}")

        # --- Load shared title list
        titles_path = self.output_dir / "titles_to_process.json"
        if titles_path.exists():
            titles = json.loads(titles_path.read_text(encoding="utf-8"))
            mode = f"SHARED ({len(titles)} titles)"
        else:
            tmdb_files = list((self.output_dir / "tmdb_raw").glob("*.json"))
            titles = [f.stem.replace("_", " ") for f in tmdb_files]
            mode = f"AUTO ({len(titles)} titles)"
            self.logger.warning("⚠️ No shared title list found; reverting to AUTO mode")

        remaining = [t for t in titles if t not in skip_titles]
        self.logger.info(f"🎧 Starting Step 02 ({mode}) | {len(remaining)} remaining / {len(titles)} total")
        self.logger.info(f"🕒 Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        success_count = 0
        for i, title in enumerate(remaining, start=1):
            if self.fetch_discogs_release(title):
                success_count += 1
            if i % 25 == 0 or i == len(remaining):
                self.logger.info(f"Progress: {i}/{len(remaining)} titles processed")

        # --- Metrics ---
        duration = round(time.time() - start_time, 2)
        total_titles = len(titles)
        completed = success_count + len(skip_titles)

        # Handle case when there are no titles to avoid division by zero
        if total_titles > 0:
            success_rate = round((completed / total_titles) * 100, 2)
        else:
            success_rate = 0.0

        metrics = {
            "mode": mode,
            "titles_total": total_titles,
            "titles_queried": completed,
            "success_rate": success_rate,
            "duration_sec": duration,
            "direction": "TMDB→Discogs",
        }

        self.write_metrics(metrics, name="step02_query_discogs_metrics")
        self.logger.info(
            f"✅ Completed Step 02 ({mode}) | {metrics['success_rate']}% success in {duration:.2f}s"
        )


# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step02QueryDiscogs(None).run()
