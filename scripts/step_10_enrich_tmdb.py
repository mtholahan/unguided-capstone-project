"""Step 10: Enrich TMDb (Refactored & Enhanced)
-----------------------------------------------
Enriches matched titles with additional metadata (genres, runtime, alt titles).
Gracefully skips missing IDs and logs skipped rows for audit.

Inputs : TMDB_DIR/tmdb_match_results_enhanced.csv (fallback: tmdb_match_results.csv)
Outputs: TMDB_DIR/tmdb_enriched_matches.csv
         TMDB_DIR/tmdb_enrich_audit.csv
"""

from base_step import BaseStep
import pandas as pd
import requests
import time
import os
from config import TMDB_DIR, TMDB_API_KEY, ROW_LIMIT, DEBUG_MODE


class Step10EnrichMatches(BaseStep):
    def __init__(self, name="Step 10: Enrich TMDb Matches (Refactored)"):
        super().__init__(name=name)
        self.input_matches = TMDB_DIR / "tmdb_match_results_enhanced.csv"
        self.fallback_matches = TMDB_DIR / "tmdb_match_results.csv"
        self.output_file = TMDB_DIR / "tmdb_enriched_matches.csv"
        self.audit_file = TMDB_DIR / "tmdb_enrich_audit.csv"
        self.api_base = "https://api.themoviedb.org/3/movie"
        self.sleep_time = 0.25  # polite throttle between API calls

    # -------------------------------------------------------------
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

    # -------------------------------------------------------------
    def _safe_get(self, url: str, params: dict, retries: int = 3, backoff: float = 1.5):
        """Simple retry wrapper for TMDb API requests."""
        for attempt in range(1, retries + 1):
            try:
                r = requests.get(url, params=params, timeout=10)
                r.raise_for_status()
                return r
            except Exception as e:
                if attempt == retries:
                    self.logger.warning(f"❌ TMDb GET failed after {retries} attempts → {e}")
                    return None
                wait = backoff * attempt
                self.logger.warning(f"⚠️ TMDb GET error (attempt {attempt}/{retries}) → retrying in {wait:.1f}s")
                time.sleep(wait)

    # -------------------------------------------------------------
    def run(self):
        self.setup_logger()
        self.logger.info("🚀 Starting Step 10: Enrich TMDb (Refactored & Enhanced)")

        # --- Load match input ---
        matches_path = self.input_matches if self.input_matches.exists() else self.fallback_matches
        self.logger.info(f"📥 Loading matches from {matches_path.name}")

        if not matches_path.exists() or os.path.getsize(matches_path) == 0:
            self.logger.warning(f"🪫 No matches available at {matches_path}; skipping enrichment.")
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
        self.logger.info(f"🎯 Enriching {total:,} matched records with TMDb metadata…")

        for row in self.progress_iter(matches.itertuples(index=False), desc="Enriching TMDb"):
            tmdb_id = getattr(row, "tmdb_id", None)
            tmdb_title = self.clean_text(getattr(row, "tmdb_title", ""))
            matched_title = self.clean_text(getattr(row, "matched_title", ""))

            # --- Skip invalid IDs ---
            if not tmdb_id or str(tmdb_id).lower() in ("nan", "none", "", "0"):
                self.logger.debug(f"⏭️ Skipping invalid tmdb_id for {tmdb_title}")
                audit_rows.append(
                    {"tmdb_id": tmdb_id, "tmdb_title": tmdb_title, "status": "skipped_invalid_id"}
                )
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

            # --- Movie details ---
            r = self._safe_get(
                f"{self.api_base}/{tmdb_id}",
                {"api_key": TMDB_API_KEY, "language": "en-US"},
            )
            if r:
                try:
                    data = r.json()
                    result["runtime"] = data.get("runtime")
                    result["genres"] = ", ".join(g["name"] for g in data.get("genres", []))
                    result["overview"] = self.clean_text(data.get("overview", ""))
                except Exception as e:
                    success = False
                    self.logger.warning(f"❌ Parse error for movie details {tmdb_title} [{tmdb_id}]: {e}")
            else:
                success = False

            # --- Alternative titles ---
            alt_r = self._safe_get(f"{self.api_base}/{tmdb_id}/alternative_titles", {"api_key": TMDB_API_KEY})
            if alt_r:
                try:
                    alt_titles = [
                        self.clean_text(a.get("title", ""))
                        for a in alt_r.json().get("titles", [])
                        if a.get("title")
                    ]
                    result["alt_titles"] = ", ".join(alt_titles)
                except Exception as e:
                    success = False
                    self.logger.warning(f"❌ Parse error for alt titles {tmdb_title} [{tmdb_id}]: {e}")
            else:
                success = False

            enriched_rows.append(result)
            audit_rows.append(
                {
                    "tmdb_id": tmdb_id,
                    "tmdb_title": tmdb_title,
                    "status": "success" if success else "partial_or_failed",
                }
            )
            time.sleep(self.sleep_time)

        # --- Write outputs ---
        enriched_df = pd.DataFrame(enriched_rows)
        audit_df = pd.DataFrame(audit_rows)
        enriched_df.to_csv(self.output_file, index=False, encoding="utf-8")
        audit_df.to_csv(self.audit_file, index=False, encoding="utf-8")

        self.logger.info(f"✅ Saved {len(enriched_df):,} enriched rows → {self.output_file.name}")
        self.logger.info(f"🧾 Enrichment audit written → {self.audit_file.name}")

        # --- Metrics ---
        metrics = {
            "rows_input": total,
            "rows_enriched": len(enriched_df),
            "rows_skipped": audit_df["status"].value_counts().get("skipped_invalid_id", 0),
            "rows_partial_failed": audit_df["status"].value_counts().get("partial_or_failed", 0),
            "api_key_present": bool(TMDB_API_KEY),
        }
        self.write_metrics("step10_enrich_tmdb", metrics)
        self.logger.info(f"📊 Metrics logged: {metrics}")
        self.logger.info("✅ [DONE] Step 10 completed successfully.")


if __name__ == "__main__":
    Step10EnrichMatches().run()
