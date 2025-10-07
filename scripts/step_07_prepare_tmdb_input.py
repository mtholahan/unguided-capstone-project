"""
Step 07: Prepare TMDb Input (DRY + utils-integrated)
----------------------------------------------------
Normalizes both MusicBrainz (MB) and TMDb titles using utils.normalize_for_matching_extended().
Computes deterministic (exact) title/year overlaps as a pre-fuzzy baseline.
"""

from base_step import BaseStep
from config import DATA_DIR, TMDB_DIR, ROW_LIMIT, DEBUG_MODE
from utils import normalize_for_matching_extended as normalize, is_mostly_digits, make_progress_bar
import pandas as pd


class Step07PrepareTMDbInput(BaseStep):
    def __init__(self, name="Step 07: Prepare TMDb Input (DRY)"):
        super().__init__(name=name)
        self.input_parquet = DATA_DIR / "soundtracks.parquet"
        self.tmdb_input_csv = TMDB_DIR / "enriched_top_1000.csv"
        self.output_csv = TMDB_DIR / "tmdb_input_candidates_clean.csv"

    def run(self):
        self.logger.info("🔍 Loading MB soundtracks (Parquet)...")
        df = pd.read_parquet(self.input_parquet)

        # --- Extract title from raw_row ---
        def extract_title(raw):
            """Extract the soundtrack title from the MusicBrainz pipe-delimited raw_row."""
            try:
                parts = str(raw).split("|")
                if len(parts) > 1:
                    title = parts[1].strip()
                    if len(title) > 5 and not all(c in "0123456789abcdef-" for c in title.lower()):
                        return title
                for part in parts[2:4]:
                    if len(part.strip()) > 3 and not all(c in "0123456789abcdef-" for c in part.lower()):
                        return part.strip()
                return None
            except Exception:
                return None

        # --- Replace tqdm with unified progress bar ---
        titles = []
        with make_progress_bar(df["raw_row"], desc="Extracting titles", unit="row") as bar:
            for r in bar:
                titles.append(extract_title(r))
        df["title"] = titles

        normalized_titles = []
        with make_progress_bar(df["title"], desc="Normalizing MB titles", unit="title") as bar:
            for t in bar:
                normalized_titles.append(normalize(t))
        df["normalized_title"] = normalized_titles

        print("\n=== Normalized Title Diagnostic Sample ===")
        sample = df.sample(10, random_state=42)
        print(sample[["title", "normalized_title"]])
        print("Unique normalized examples:", df["normalized_title"].dropna().unique()[:10])
        print("==========================================\n")

        # --- Row Rejection Diagnostics ---
        reasons = {"short_title": 0, "digits_only": 0, "bad_year": 0, "bad_type": 0, "passed": 0}

        def is_valid(row):
            nt = str(row.get("normalized_title") or "").strip()
            if not nt or len(nt) < 3:
                reasons["short_title"] += 1
                return False
            if nt.isdigit():
                reasons["digits_only"] += 1
                return False
            try:
                yr = int(float(row.get("release_year") or 0))
            except Exception:
                reasons["bad_year"] += 1
                return False
            if not (1900 <= yr <= 2025):
                reasons["bad_year"] += 1
                return False
            rgt = str(row.get("release_group_secondary_type") or "").strip().lower()
            if rgt and "soundtrack" not in rgt:
                reasons["bad_type"] += 1
                return False
            reasons["passed"] += 1
            return True

        df_filtered = df[df.apply(is_valid, axis=1)].drop_duplicates(subset=["normalized_title", "release_year"])

        # --- Prepare output ---
        out_df = df_filtered.rename(columns={"release_year": "year"})
        out_df = out_df[["normalized_title", "title", "release_group_id", "year", "release_group_secondary_type"]]
        out_df.to_csv(self.output_csv, index=False)
        self.logger.info(f"✅ Saved MB candidates → {self.output_csv} ({len(out_df):,} rows)")

        # --- TMDb side ---
        try:
            df_tmdb = pd.read_csv(self.tmdb_input_csv, dtype=str)
            normalized_tmdb = []
            with make_progress_bar(df_tmdb["title"].fillna(""), desc="Normalizing TMDb titles", unit="title") as bar:
                for t in bar:
                    normalized_tmdb.append(normalize(t))
            df_tmdb["normalized_title"] = normalized_tmdb

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
