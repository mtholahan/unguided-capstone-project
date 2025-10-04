"""Step 09: Apply Rescues
Applies manual overrides (“rescues”) to fix known false negatives.
Merges rescue data into tmdb_matches.csv and writes enhanced results.
"""

from base_step import BaseStep
import pandas as pd
from config import TMDB_DIR
import os


class Step09ApplyRescues(BaseStep):
    def __init__(
        self,
        name: str = "Step 09: Apply Manual Rescues",
        threshold: float = 90.0,
    ):
        super().__init__(name)
        self.threshold = threshold

        # Input/Output files
        self.match_file        = TMDB_DIR / "tmdb_match_results.csv"
        self.rescue_file       = TMDB_DIR / "rescues.csv"
        self.output_enhanced   = TMDB_DIR / "tmdb_match_results_enhanced.csv"

    def run(self):
        self.logger.info("🎬 Loading fuzzy-match results for enhancement…")

        if not self.match_file.exists() or os.path.getsize(self.match_file) == 0:
            self.logger.warning(f"🪫 No matches found in {self.match_file}; skipping Step 09.")
            return

        try:
            df = pd.read_csv(self.match_file, dtype=str)
        except pd.errors.EmptyDataError:
            self.logger.warning(f"🪫 {self.match_file} is empty; skipping Step 09.")
            return

        if df.empty:
            self.logger.warning(f"🪫 {self.match_file} has 0 rows; skipping Step 09.")
            return

        # Normalize: rename 'score' → 'match_score'
        if "score" in df.columns:
            df.rename(columns={"score": "match_score"}, inplace=True)

        # Threshold filter first
        df["match_score"] = df["match_score"].astype(float)
        final_df = df[df["match_score"] >= self.threshold].copy()

        # === NEW: Apply rescues ===
        rescued_count, skipped_count, overridden_count = 0, 0, 0

        if self.rescue_file.exists():
            self.logger.info(f"🛟 Loading rescues from {self.rescue_file.name}")
            rescues = pd.read_csv(self.rescue_file, dtype=str)
            rescues = rescues.rename(columns={c: c.lower().strip() for c in rescues.columns})

            for _, row in rescues.iterrows():
                title = row.get("golden_title", "").strip()
                year = row.get("expected_year", "").strip()
                artist = row.get("expected_artist", "").strip()
                override = str(row.get("override", "False")).lower() in ("true", "1", "yes")

                exists = (
                    (final_df["title"].str.lower() == title.lower())
                    & (final_df["year"].astype(str) == year)
                )

                if exists.any():
                    if override:
                        overridden_count += 1
                        self.logger.info(f"🔄 Overriding existing match for {title} ({year})")
                        final_df = final_df[~exists]
                    else:
                        skipped_count += 1
                        self.logger.info(f"✅ Skipping rescue for {title} ({year}) — already matched")
                        continue

                rescue_entry = {
                    "title": title,
                    "year": year,
                    "artist": artist,
                    "match_source": "rescue",
                    "match_score": 100.0,
                }
                final_df = pd.concat([final_df, pd.DataFrame([rescue_entry])], ignore_index=True)
                rescued_count += 1
                self.logger.info(f"🛟 Injected rescue match for {title} ({year})")

        else:
            self.logger.info("📭 No rescues.csv file found; skipping manual rescues")

        # === Summary block ===
        self.logger.info("📊 Rescue Summary")
        self.logger.info(f"   Injected:   {rescued_count}")
        self.logger.info(f"   Skipped:    {skipped_count}")
        self.logger.info(f"   Overridden: {overridden_count}")
        self.logger.info(f"   Final rows: {len(final_df)}")

        # Write out enhanced matches
        final_df.to_csv(self.output_enhanced, index=False)
        self.logger.info(f"💾 Enhanced matches saved to {self.output_enhanced.name} ({len(final_df)} rows)")
