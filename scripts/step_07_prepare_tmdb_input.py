"""Step 07: Prepare TMDb Input (Refactored)
-------------------------------------------
Normalizes both MusicBrainz (MB) and TMDb titles using
utils.normalize_for_matching_extended() and computes deterministic
title/year overlaps as a pre-fuzzy baseline.
"""

from base_step import BaseStep
from config import DATA_DIR, TMDB_DIR, DEBUG_MODE
from utils import normalize_for_matching_extended as normalize
import pandas as pd


class Step07PrepareTMDbInput(BaseStep):
    def __init__(self, name="Step 07: Prepare TMDb Input (Refactored)"):
        super().__init__(name=name)
        self.input_parquet = DATA_DIR / "soundtracks.parquet"
        self.tmdb_input_csv = TMDB_DIR / "enriched_top_1000.csv"
        self.output_csv = TMDB_DIR / "tmdb_input_candidates_clean.csv"

    # -------------------------------------------------------------
    def extract_title(self, raw):
        """Extract soundtrack title from MusicBrainz raw_row field."""
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

    # -------------------------------------------------------------
    def run(self):
        self.setup_logger()
        self.logger.info("🚀 Starting Step 07: Prepare TMDb Input (Refactored)")

        # --- Load MusicBrainz soundtracks ---
        if not self.input_parquet.exists():
            self.fail(f"Missing input file: {self.input_parquet}")
            return

        self.logger.info(f"📥 Loading MB soundtracks from {self.input_parquet}")
        df = pd.read_parquet(self.input_parquet)

        # --- Extract & normalize MB titles ---
        titles = []
        for raw in self.progress_iter(df["raw_row"], desc="Extracting MB titles"):
            titles.append(self.extract_title(raw))
        df["title"] = titles

        normalized_titles = []
        for t in self.progress_iter(df["title"], desc="Normalizing MB titles"):
            normalized_titles.append(normalize(t))
        df["normalized_title"] = normalized_titles

        # --- Row validation ---
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

        df_filtered = df[df.apply(is_valid, axis=1)].drop_duplicates(
            subset=["normalized_title", "release_year"]
        )
        out_df = df_filtered.rename(columns={"release_year": "year"})
        out_df = out_df[
            ["normalized_title", "title", "release_group_id", "year", "release_group_secondary_type"]
        ]

        # --- Save MB candidates ---
        out_df.to_csv(self.output_csv, index=False)
        self.logger.info(f"✅ Saved MB candidates → {self.output_csv.name} ({len(out_df):,} rows)")

        # --- Process TMDb side ---
        merged = pd.DataFrame()
        try:
            if not self.tmdb_input_csv.exists():
                self.logger.warning("⚠️ TMDb file not found — skipping overlap metric")
            else:
                df_tmdb = pd.read_csv(self.tmdb_input_csv, dtype=str)
                normalized_tmdb = []
                for t in self.progress_iter(df_tmdb["title"].fillna(""), desc="Normalizing TMDb titles"):
                    normalized_tmdb.append(normalize(t))
                df_tmdb["normalized_title"] = normalized_tmdb

                df_tmdb["year"] = pd.to_numeric(df_tmdb["release_year"], errors="coerce")
                df_tmdb = df_tmdb[
                    (df_tmdb["year"].between(1900, 2025))
                    & (df_tmdb["normalized_title"].str.len() > 2)
                ]
                df_tmdb = df_tmdb.drop_duplicates(subset=["normalized_title", "year"])

                tmdb_norm_path = TMDB_DIR / "tmdb_movies_normalized.parquet"
                df_tmdb.to_parquet(tmdb_norm_path, index=False)
                self.logger.info(f"💾 Saved normalized TMDb dataset → {tmdb_norm_path.name}")

                merged = out_df.merge(
                    df_tmdb[["normalized_title", "year", "tmdb_id"]],
                    on=["normalized_title", "year"],
                    how="inner",
                )
                coverage = (len(merged) / len(out_df) * 100) if len(out_df) else 0
                self.logger.info(f"📊 Deterministic overlap: {coverage:.2f}% ({len(merged)}/{len(out_df)})")

                diag = TMDB_DIR / "tmdb_deterministic_matches_sample.csv"
                merged.head(100).to_csv(diag, index=False)
                self.logger.info(f"🔎 Sample deterministic matches → {diag.name}")

        except Exception as e:
            self.logger.error(f"❌ Error processing TMDb side: {e}")

        # --- Metrics ---
        metrics = {
            "rows_mb_candidates": len(out_df),
            "rows_tmdb": len(merged) if not merged.empty else 0,
            "rows_deterministic_match": len(merged),
            "deterministic_match_pct": round(
                (len(merged) / max(len(out_df), 1)) * 100, 2
            ),
            "validation_summary": reasons,
        }
        self.write_metrics("step07_prepare_tmdb_input", metrics)
        self.logger.info(f"📈 Metrics logged: {metrics}")

        # --- Debug mode preview ---
        if DEBUG_MODE:
            self.logger.info("🔍 Debug preview (first 5 MB rows):")
            self.logger.info(out_df.head().to_string())

        self.logger.info("✅ [DONE] Step 07 completed successfully.")


if __name__ == "__main__":
    Step07PrepareTMDbInput().run()
