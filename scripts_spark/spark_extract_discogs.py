import os
import json
import time
from pathlib import Path
from scripts.base_step import BaseStep
from scripts.config_env import load_and_validate_env
from scripts.utils import cached_request, RateLimiter, save_json, safe_filename

# ===============================================================
# 🎯 Step 02 – Query Discogs API for Soundtrack Metadata
# ===============================================================
class Step02QueryDiscogs(BaseStep):
    """Fetch Discogs metadata for TMDB-derived titles."""

    def __init__(self, spark=None):
        super().__init__(name="step_02_query_discogs")
        self.spark = spark

        # ✅ Environment-aware directories
        self.output_dir = Path(os.getenv("PIPELINE_OUTPUT_DIR", "data/intermediate")).resolve()
        self.metrics_dir = Path(os.getenv("PIPELINE_METRICS_DIR", "data/metrics")).resolve()
        self.raw_dir = self.output_dir / "discogs_raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # ✅ API + rate limiting
        load_and_validate_env()
        self.api_url = os.getenv("DISCOGS_API_URL", "https://api.discogs.com/database/search")
        self.user_agent = os.getenv("DISCOGS_USER_AGENT", "unguided-capstone-bot/1.0")
        self.key = os.getenv("DISCOGS_CONSUMER_KEY")
        self.secret = os.getenv("DISCOGS_CONSUMER_SECRET")
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
            "key": self.key,
            "secret": self.secret,
        }

        resp = cached_request(self.api_url, params=params,
                              headers={"User-Agent": self.user_agent},
                              rate_limiter=self.rate_limiter)

        if resp.status_code != 200:
            self.logger.warning(f"⚠️ Discogs fetch failed for '{title}' ({resp.status_code})")
            return False

        data = resp.json()
        if not data.get("results"):
            self.logger.info(f"🚫 No results for '{title}'")
            return False

        filtered = [
            r for r in data["results"]
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
            save_json(sorted(set(completed)), checkpoint_file)
        except Exception as e:
            self.logger.warning(f"⚠️ Could not update checkpoint for '{title}': {e}")

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

        duration = round(time.time() - start_time, 2)
        metrics = {
            "mode": mode,
            "titles_total": len(titles),
            "titles_queried": success_count + len(skip_titles),
            "success_rate": round((success_count + len(skip_titles)) / len(titles) * 100, 2),
            "duration_sec": duration,
            "direction": "TMDB→Discogs",
        }
        self.write_metrics(metrics, name="step02_query_discogs_metrics")
        self.logger.info(f"✅ Completed Step 02 ({mode}) | {metrics['success_rate']}% success in {duration:.2f}s")


# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step02QueryDiscogs(None).run()
