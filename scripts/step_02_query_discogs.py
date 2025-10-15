"""
Step 02: Query Discogs API for Soundtrack Metadata
Unguided Capstone Project – TMDB→Discogs Directional Refactor (Sprint A)

Sequential, checkpoint-safe Discogs query process that builds on Step 01 outputs.
Uses Discogs consumer key/secret authentication pattern validated in test_discogs_auth.py.
"""

import json
import time
import requests
from pathlib import Path
from urllib.parse import quote
from datetime import datetime

from scripts.base_step import BaseStep
from scripts.config import (
    DATA_DIR,
    DISCOGS_API_URL,
    DISCOGS_TOKEN,
    DISCOGS_USER_AGENT,
    DISCOGS_RAW_DIR,
    DISCOGS_MAX_RETRIES,
    DISCOGS_SLEEP_SEC,
    RATE_LIMIT_SLEEP_SEC,
    RETRY_BACKOFF,
    API_TIMEOUT,
)


class Step02QueryDiscogs(BaseStep):
    """Fetch Discogs metadata for TMDB-derived titles and store raw JSON."""

    def __init__(self):
        super().__init__(name="step_02_query_discogs")

        # --- Directory setup ---
        self.data_dir = Path(DATA_DIR)
        self.tmdb_raw = self.data_dir / "raw" / "tmdb_raw"
        self.discogs_raw = DISCOGS_RAW_DIR
        self.discogs_raw.mkdir(parents=True, exist_ok=True)

        # --- Config constants ---
        self.api_url = DISCOGS_API_URL
        self.token = DISCOGS_TOKEN
        self.user_agent = DISCOGS_USER_AGENT
        self.max_retries = DISCOGS_MAX_RETRIES
        self.throttle_sec = DISCOGS_SLEEP_SEC
        self.retry_wait = RETRY_BACKOFF
        self.timeout = API_TIMEOUT
        self.rate_limit_wait = RATE_LIMIT_SLEEP_SEC

        self.keywords = ["soundtrack", "score", "ost", "motion picture"]

    # ------------------------------------------------------------------
    def clean_title_for_query(self, title: str) -> str:
        """Normalize TMDB title for Discogs search."""
        title = title.strip()
        if "(" in title:
            title = title.split("(")[0].strip()
        return title

    # ------------------------------------------------------------------
    def load_titles_from_tmdb(self) -> list[str]:
        """Extract unique movie titles from TMDB JSON outputs."""
        titles = []
        for f in self.tmdb_raw.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                if "results" in data and data["results"]:
                    tmdb_title = data["results"][0].get("title") or f.stem
                else:
                    tmdb_title = f.stem
                titles.append(tmdb_title.strip())
            except Exception:
                titles.append(f.stem)
        titles = sorted(set(titles))
        self.logger.info(f"📄 Loaded {len(titles)} TMDB titles for Discogs queries")
        return titles

    # ------------------------------------------------------------------
    def fetch_discogs_release(self, title: str) -> bool:
        """Query Discogs API for a soundtrack release and save JSON."""
        safe_title = "".join(c if c.isalnum() or c in " _-" else "_" for c in title)
        output_file = self.discogs_raw / f"{safe_title}.json"
        checkpoint_file = self.discogs_raw / "_checkpoint.json"

        if output_file.exists():
            self.logger.debug(f"⏭️ Skipping existing file {output_file.name}")
            return True

        query = self.clean_title_for_query(title)
        params = {
            "q": f"{query} soundtrack",
            "token": self.token,
            "per_page": 5,
            "page": 1
        }


        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.get(
                    self.api_url,
                    params=params,
                    headers={"User-Agent": self.user_agent},
                    timeout=self.timeout,
                )

                # --- Rate-limit and retry handling ---
                if resp.status_code == 429:
                    wait_for = int(resp.headers.get("Retry-After", self.rate_limit_wait))
                    self.logger.warning(f"⏳ Rate-limited. Sleeping {wait_for}s…")
                    time.sleep(wait_for)
                    continue
                elif resp.status_code >= 500:
                    self.logger.warning(f"⚠️ Discogs server error (attempt {attempt})")
                    time.sleep(self.retry_wait)
                    continue
                elif resp.status_code != 200:
                    self.logger.warning(
                        f"⚠️ Discogs fetch failed for '{title}' (HTTP {resp.status_code})"
                    )
                    return False

                data = resp.json()
                if not data.get("results"):
                    self.logger.info(f"🚫 No results for '{title}'")
                    return False

                filtered = [
                    r for r in data["results"]
                    if query.lower() in r.get("title", "").lower()
                    or "soundtrack" in r.get("title", "").lower()
                ]

                data["filtered_results"] = filtered

                output_file.write_text(
                    json.dumps(data, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                self.logger.info(
                    f"✅ Saved Discogs JSON for '{title}' ({len(filtered)} filtered hits)"
                )

                # --- Checkpoint update ---
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
                self.logger.error(f"❌ Exception fetching '{title}' (attempt {attempt}): {e}")
                time.sleep(self.retry_wait)

        self.logger.error(f"❌ All retries failed for '{title}'")
        return False

    # ------------------------------------------------------------------
    def run(self):
        """Iterate over TMDB titles and query Discogs metadata."""
        start_time = time.time()
        checkpoint_file = self.discogs_raw / "_checkpoint.json"
        skip_titles = set()

        if checkpoint_file.exists():
            try:
                skip_titles = set(json.loads(checkpoint_file.read_text()))
                self.logger.info(f"⏩ Resuming from checkpoint — {len(skip_titles)} titles already completed.")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not read checkpoint file: {e}")

        titles = self.load_titles_from_tmdb()

        # titles = ["Blade Runner", "Amélie", "Inception"] # For testing purposes

        remaining = [t for t in titles if t not in skip_titles]
        self.logger.info(f"🎧 Processing {len(remaining)} remaining titles out of {len(titles)}")

        success_count = 0
        completed = list(skip_titles)

        for i, title in enumerate(remaining, start=1):
            ok = self.fetch_discogs_release(title)
            if ok:
                success_count += 1
                completed.append(title)

            time.sleep(self.throttle_sec)
            if i % 10 == 0 or i == len(remaining):
                self.logger.info(f"Progress: {i}/{len(remaining)} titles processed")

        metrics = {
            "titles_total": len(titles),
            "titles_downloaded": len(completed),
            "duration_sec": round(time.time() - start_time, 2),
            "success_rate": round(len(completed) / len(titles) * 100, 2),
        }

        self.write_metrics(metrics, name="step02_query_discogs_metrics")
        self.logger.info(f"✅ Completed Step 02 | {len(completed)}/{len(titles)} titles processed")


if __name__ == "__main__":
    Step02QueryDiscogs().run()
