"""Step 09: Apply Rescues
Applies manual overrides (“rescues”) to fix known false negatives.
Merges rescue data into tmdb_matches.csv.
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
        boost_per_genre: float = 5.0,
        vector_threshold: float = 0.6,
        enable_vector_matching: bool = False,
    ):
        super().__init__(name)
        self.threshold = threshold
        self.boost_per_genre = boost_per_genre
        self.vector_threshold = vector_threshold
        self.enable_vector_matching = enable_vector_matching

        # Input/Output files
        self.match_file        = TMDB_DIR / "tmdb_match_results.csv"
        self.manual_rescue     = TMDB_DIR / "manual_rescue.csv"
        self.enriched_movies   = TMDB_DIR / "enriched_top_1000.csv"
        self.candidates_file   = TMDB_DIR / "tmdb_input_candidates_clean.csv"
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

        # Rename 'score' → 'match_score' if present
        if "score" in df.columns:
            df.rename(columns={"score": "match_score"}, inplace=True)

        # (genre boost / rescues could be re-added here later if needed)

        # Final threshold filter and write
        final_df = df[df["match_score"].astype(float) >= self.threshold]
        final_df.to_csv(self.output_enhanced, index=False)
        self.logger.info(f"💾 Enhanced matches saved to {self.output_enhanced.name} ({len(final_df)} rows)")
