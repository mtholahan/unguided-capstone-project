"""Step 10: Enrich TMDb
Enriches matched titles with additional metadata (genres, IDs, release years).
Writes tmdb_enriched_matches.csv to TMDB_DIR.
"""

from base_step import BaseStep
import pandas as pd
import requests
import time
from tqdm import tqdm
from config import TMDB_DIR, TMDB_API_KEY

class Step10EnrichMatches(BaseStep):
    def __init__(self, name: str = "Step 10: Enrich TMDb Matches"):
        super().__init__(name)
        # Read “enhanced” matches (after rescues); if not found, fallback to raw matches
        self.input_matches = TMDB_DIR / "tmdb_match_results_enhanced.csv"
        self.fallback_matches = TMDB_DIR / "tmdb_match_results.csv"
        self.output_file = TMDB_DIR / "tmdb_enriched_matches.csv"
        self.api_base = "https://api.themoviedb.org/3/movie"
        self.sleep_time = 0.25  # ~4 requests/sec

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
        # 1) Load matches (prefer the enhanced file if it exists)
        matches_path = self.input_matches if self.input_matches.exists() else self.fallback_matches
        self.logger.info(f"📥 Loading matches from {matches_path.name} …")
        matches = pd.read_csv(matches_path, dtype=str)

        enriched_rows = []
        total = len(matches)

        # 2) Loop over matches and call TMDb for each tmdb_id
        self.logger.info("🎯 Enriching matched records with TMDb metadata…")
        for idx, row in enumerate(matches.itertuples(index=False), start=1):
            if idx % 50 == 0 or idx == total:
                print(f"➤ Enriched {idx}/{total}", flush=True)

            tmdb_id = row.tmdb_id
            result = {
                "tmdb_id":         tmdb_id,
                "tmdb_title":      self.clean_text(row.tmdb_title),
                "matched_title":   self.clean_text(row.matched_title),
                "release_year":    row.release_year if "release_year" in row._fields else None,
                "match_score":     row.match_score if "match_score" in row._fields else None,
                "release_group_id": row.release_group_id,
                "runtime":         None,
                "genres":          "",
                "overview":        "",
                "alt_titles":      "",
            }

            # 2a) Fetch movie details (runtime, genres, overview)
            try:
                resp = requests.get(
                    f"{self.api_base}/{tmdb_id}",
                    params={"api_key": TMDB_API_KEY, "language": "en-US"},
                    timeout=10
                )
                resp.raise_for_status()
                data = resp.json()

                result["runtime"] = data.get("runtime")
                # Join genre names with commas
                result["genres"] = ", ".join([g["name"] for g in data.get("genres", [])])
                result["overview"] = self.clean_text(data.get("overview", ""))
                time.sleep(self.sleep_time)
            except Exception as e:
                self.logger.warning(f"❌ Error fetching details for {tmdb_id}: {e}")

            # 2b) Fetch alternative titles
            try:
                alt_resp = requests.get(
                    f"{self.api_base}/{tmdb_id}/alternative_titles",
                    params={"api_key": TMDB_API_KEY},
                    timeout=10
                )
                alt_resp.raise_for_status()
                alt_data = alt_resp.json().get("titles", [])
                alt_list = [self.clean_text(a["title"]) for a in alt_data if "title" in a]
                result["alt_titles"] = ", ".join(alt_list)
                time.sleep(self.sleep_time)
            except Exception as e:
                self.logger.warning(f"❌ Error fetching alt titles for {tmdb_id}: {e}")

            enriched_rows.append(result)

        # 3) Write enriched DataFrame to CSV
        enriched_df = pd.DataFrame(enriched_rows)
        enriched_df.to_csv(self.output_file, index=False, encoding="utf-8")
        self.logger.info(f"✅ Saved {len(enriched_df)} enriched rows to {self.output_file.name}")
