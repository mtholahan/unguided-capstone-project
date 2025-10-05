"""
Step 07: Prepare TMDb Input (DRY + utils-integrated)
----------------------------------------------------
Normalizes both MusicBrainz (MB) and TMDb titles using utils.normalize_for_matching_extended().
Computes deterministic (exact) title/year overlaps as a pre-fuzzy baseline.

Inputs:
    DATA_DIR/soundtracks.parquet          ← from Step 05
    TMDB_DIR/enriched_top_1000.csv        ← from Step 06
Outputs:
    TMDB_DIR/tmdb_input_candidates_clean.csv
    TMDB_DIR/tmdb_movies_normalized.parquet
    TMDB_DIR/tmdb_deterministic_matches_sample.csv
Metrics:
    deterministic_match_pct logged to pipeline_metrics.csv
"""

from base_step import BaseStep
from config import DATA_DIR, TMDB_DIR, DEBUG_MODE
from utils import normalize_for_matching_extended as normalize, is_mostly_digits
import pandas as pd
from tqdm import tqdm


class Step07PrepareTMDbInput(BaseStep):
    def __init__(self, name="Step 07: Prepare TMDb Input (DRY)"):
        super().__init__(name=name)
        self.input_parquet = DATA_DIR / "soundtracks.parquet"
        self.tmdb_input_csv = TMDB_DIR / "enriched_top_1000.csv"
        self.output_csv = TMDB_DIR / "tmdb_input_candidates_clean.csv"

    def run(self):
        self.logger.info("🔍 Loading MB soundtracks (Parquet)...")
        df = pd.read_parquet(self.input_parquet)

        # Extract title from raw_row
        def extract_title(raw):
            try:
                for part in str(raw).split("|"):
                    if part.strip():
                        return part.strip()
            except Exception:
                pass
            return None

        df["title"] = [extract_title(r) for r in tqdm(df["raw_row"], desc="Extracting titles")]
        df["normalized_title"] = [normalize(t) for t in tqdm(df["title"], desc="Normalizing MB titles")]

        # Filter invalid
        def is_valid(row):
            nt = row["normalized_title"]
            if not nt or len(nt) < 3 or is_mostly_digits(nt):
                return False
            try:
                yr = int(row["release_year"])
            except Exception:
                return False
            return 1900 <= yr <= 2025

        df = df[df.apply(is_valid, axis=1)].drop_duplicates(subset=["normalized_title", "release_year"])
        out_df = df.rename(columns={"release_year": "year"})
        out_df = out_df[["normalized_title", "title", "release_group_id", "year", "release_group_secondary_type"]]
        out_df.to_csv(self.output_csv, index=False)
        self.logger.info(f"✅ Saved MB candidates → {self.output_csv} ({len(out_df):,} rows)")

        # ---- TMDb side ----
        try:
            df_tmdb = pd.read_csv(self.tmdb_input_csv, dtype=str)
            df_tmdb["normalized_title"] = [normalize(t) for t in df_tmdb["title"].fillna("")]
            df_tmdb["year"] = pd.to_numeric(df_tmdb["release_year"], errors="coerce")
            df_tmdb = df_tmdb[(df_tmdb["year"].between(1900, 2025)) & (df_tmdb["normalized_title"].str.len() > 2)]
            df_tmdb = df_tmdb.drop_duplicates(subset=["normalized_title", "year"])
            tmdb_norm_path = TMDB_DIR / "tmdb_movies_normalized.parquet"
            df_tmdb.to_parquet(tmdb_norm_path, index=False)
            self.logger.info(f"💾 Saved normalized TMDb dataset → {tmdb_norm_path}")

            merged = out_df.merge(df_tmdb[["normalized_title", "year", "tmdb_id"]],
                                  on=["normalized_title", "year"], how="inner")
            coverage = (len(merged) / len(out_df)) * 100 if len(out_df) else 0
            self.logger.info(f"📊 Deterministic overlap: {coverage:.2f}% ({len(merged)}/{len(out_df)})")

            diag = TMDB_DIR / "tmdb_deterministic_matches_sample.csv"
            merged.head(100).to_csv(diag, index=False)
            self.logger.info(f"🔎 Sample deterministic matches → {diag}")

            metrics = {
                "rows_mb_candidates": len(out_df),
                "rows_tmdb": len(df_tmdb),
                "rows_deterministic_match": len(merged),
                "deterministic_match_pct": round(coverage, 2),
            }
            self.write_metrics("step07_prepare_tmdb_input", metrics)

        except FileNotFoundError:
            self.logger.warning("⚠️ TMDB file not found — skipping overlap metric")

        if DEBUG_MODE:
            self.logger.info("🔎 Debug preview (first 5 MB rows):")
            self.logger.info(out_df.head().to_string())

        self.logger.info("✅ Step 07 completed successfully.")


if __name__ == "__main__":
    Step07PrepareTMDbInput().run()
