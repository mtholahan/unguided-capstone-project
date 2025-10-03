"""Step 07: Prepare TMDb Input
Prepares TMDb dataset for matching by normalizing fields (title, year).
Writes tmdb_input.csv to TMDB_DIR.
"""

import pandas as pd
from base_step import BaseStep
from config import DATA_DIR
from utils import normalize_title_for_matching, is_mostly_digits

class Step07PrepareTMDbInput(BaseStep):
    def __init__(self, name="Step 07: Prepare TMDb Input"):
        super().__init__(name)
        self.input_tsv  = DATA_DIR / "soundtracks.tsv"
        self.output_csv = DATA_DIR / "tmdb" / "tmdb_input_candidates_clean.csv"

    def run(self):
        self.logger.info("🔍 Loading soundtracks (no header)...")
        # Read the raw TSV (no column names)
        df = pd.read_csv(self.input_tsv, sep="\t", header=None, dtype=str, engine="python")

        # 1) Extract the three needed columns by index:
        #    [2] → title
        #    [4] → release_group_id
        #   [18] → release_date (YYYY-MM-DD…)
        working = pd.DataFrame({
            "title":            df.iloc[:, 2],
            "release_group_id": df.iloc[:, 4],
            "release_year":     df.iloc[:, 18].str.slice(0, 4)
        })

        # 2) Normalize titles
        self.logger.info("🔧 Normalizing titles...")
        working["normalized_title"] = working["title"].apply(normalize_title_for_matching)

        # 3) Filter out invalid rows (short titles, numeric titles, bad year)
        initial_count = len(working)
        self.logger.info(f"ℹ️ Initial row count: {initial_count}")

        def is_valid(row):
            nt = row["normalized_title"]
            if len(nt) < 3:
                return False
            if is_mostly_digits(nt):
                return False
            try:
                yr = int(row["release_year"])
            except:
                return False
            return (1900 <= yr <= 2025)

        filtered = working[working.apply(is_valid, axis=1)]
        removed = initial_count - len(filtered)
        self.logger.info(f"🧹 Removed {removed} invalid or out-of-range titles")

        # 4) Drop duplicates on (normalized_title, release_year)
        self.logger.info("🧼 Dropping duplicates...")
        filtered = filtered.drop_duplicates(subset=["normalized_title", "release_year"])

        # 5) Build final output with exactly four columns
        out_df = pd.DataFrame({
            "normalized_title":  filtered["normalized_title"],
            "title":             filtered["title"],
            "release_group_id":  filtered["release_group_id"],
            "year":              filtered["release_year"].astype(int)
        })

        final_count = len(out_df)
        out_df.to_csv(self.output_csv, index=False)
        self.logger.info(f"✅ Final output row count: {final_count}")
        self.logger.info(f"✅ Saved to {self.output_csv}")
