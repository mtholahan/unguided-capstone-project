"""Step 10: Enrich TMDb (Enhanced)
Enriches matched titles with additional metadata (genres, runtime, alt titles).
Now gracefully skips missing tmdb_id values and logs skipped rows for audit.
"""

from base_step import BaseStep
import pandas as pd
import requests
import time
import os
from tqdm import tqdm
from config import TMDB_DIR, TMDB_API_KEY, ROW_LIMIT, DEBUG_MODE, TMDB_PAGE_LIMIT


class Step10EnrichMatches(BaseStep):
    def __init__(self, name: str = "Step 10: Enrich TMDb Matches (Enhanced)"):
        super().__init__(name="Step 10: Enrich TMDb Matches (Enhanced)")
        self.input_matches = TMDB_DIR / "tmdb_match_results_enhanced.csv"
        self.fallback_matches = TMDB_DIR / "tmdb_match_results.csv"
        self.output_file = TMDB_DIR / "tmdb_enriched_matches.csv"
        self.audit_file = TMDB_DIR / "tmdb_enrich_audit.csv"
        self.api_base = "https://api.themoviedb.org/3/movie"
        self.sleep_time = 0.25  # throttle requests

    def clean_text(self, text: str) -> str:
        """Normalize text to UTF-8, strip whitespace."""
        if pd.isna(text):
            return ""
        return (
            str(text)
            .encode("latin1", errors="ignore")
            .decode("utf-8", errors="ignore")
            .strip()
        )

    def run(self):
        # 1️⃣ Load matches (prefer enhanced)
        matches_path = self.input_matches if self.input_matches.exists() else self.fallback_matches
        self.logger.info(f"📥 Loading matches from {matches_path.name} …")

        if not matches_path.exists() or os.path.getsize(matches_path) == 0:
            self.logger.warning(f"🪫 No matches available in {matches_path}; skipping Step 10.")
            return

        try:
            matches = pd.read_csv(matches_path, dtype=str)
        except pd.errors.EmptyDataError:
            self.logger.warning(f"🪫 {matches_path} is empty; skipping Step 10.")
            return

        if matches.empty:
            self.logger.warning(f"🪫 {matches_path} contains 0 rows; skipping Step 10.")
            return

        total = len(matches)
        enriched_rows, audit_rows = [], []

        self.logger.info("🎯 Enriching matched records with TMDb metadata…")
        for idx, row in tqdm(enumerate(matches.itertuples(index=False), start=1), total=total, desc="Enriching"):
            tmdb_id = getattr(row, "tmdb_id", None)
            tmdb_title = self.clean_text(getattr(row, "tmdb_title", ""))
            matched_title = self.clean_text(getattr(row, "matched_title", ""))

            # --- Guard against missing or malformed IDs ---
            if not tmdb_id or str(tmdb_id).lower() in ("nan", "none", "", "0"):
                self.logger.warning(f"⚠️ Skipping enrichment for {tmdb_title} — invalid tmdb_id: {tmdb_id}")
                audit_rows.append({
                    "tmdb_id": tmdb_id,
                    "tmdb_title": tmdb_title,
                    "status": "skipped_invalid_id"
                })
                continue

            result = {
                "tmdb_id": tmdb_id,
                "tmdb_title": tmdb_title,
                "matched_title": matched_title,
                "release_year": getattr(row, "tmdb_year", None),
                "match_score": getattr(row, "match_score", None),
                "release_group_id": getattr(row, "release_group_id", None),
                "runtime": None,
                "genres": "",
                "overview": "",
                "alt_titles": "",
            }

            success = True

            # --- Movie Details ---
            try:
                resp = requests.get(
                    f"{self.api_base}/{tmdb_id}",
                    params={"api_key": TMDB_API_KEY, "language": "en-US"},
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                result["runtime"] = data.get("runtime")
                result["genres"] = ", ".join(g["name"] for g in data.get("genres", []))
                result["overview"] = self.clean_text(data.get("overview", ""))
                time.sleep(self.sleep_time)
            except Exception as e:
                success = False
                self.logger.warning(f"❌ Failed movie details for {tmdb_title} [{tmdb_id}]: {e}")

            # --- Alt Titles ---
            try:
                alt_resp = requests.get(
                    f"{self.api_base}/{tmdb_id}/alternative_titles",
                    params={"api_key": TMDB_API_KEY},
                    timeout=10,
                )
                alt_resp.raise_for_status()
                alt_titles = [self.clean_text(a["title"]) for a in alt_resp.json().get("titles", []) if "title" in a]
                result["alt_titles"] = ", ".join(alt_titles)
                time.sleep(self.sleep_time)
            except Exception as e:
                success = False
                self.logger.warning(f"❌ Failed alt titles for {tmdb_title} [{tmdb_id}]: {e}")

            enriched_rows.append(result)
            audit_rows.append({
                "tmdb_id": tmdb_id,
                "tmdb_title": tmdb_title,
                "status": "success" if success else "partial_or_failed"
            })

        # 3️⃣ Write results
        enriched_df = pd.DataFrame(enriched_rows)
        audit_df = pd.DataFrame(audit_rows)
        enriched_df.to_csv(self.output_file, index=False, encoding="utf-8")
        audit_df.to_csv(self.audit_file, index=False, encoding="utf-8")

        self.logger.info(f"✅ Saved {len(enriched_df)} enriched rows to {self.output_file.name}")
        self.logger.info(f"🧾 Enrichment audit written to {self.audit_file.name}")

if __name__ == "__main__":
    step = Step10EnrichMatches()
    step.run()