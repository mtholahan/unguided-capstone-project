"""
🎬 Step 05 – Match and Enrich (Phase 2)
Enhanced fuzzy matching between TMDB and Discogs datasets.
Unguided Capstone Project | Unified Environment-Aware Version
"""

import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from rapidfuzz import fuzz
from scripts.base_step import BaseStep
from scripts.config_env import load_and_validate_env
from scripts.utils import normalize_for_matching_extended as normalize_title


# ===============================================================
# 🎯 Core Class
# ===============================================================
class Step05MatchAndEnrichV2(BaseStep):
    """Enhanced fuzzy matching with year-bounded logic."""

    def __init__(self, spark=None):
        super().__init__(name="step_05_match_and_enrich")
        self.spark = spark

        # ✅ Environment-aware directories
        self.output_dir = Path(os.getenv("PIPELINE_OUTPUT_DIR", "data/intermediate")).resolve()
        self.metrics_dir = Path(os.getenv("PIPELINE_METRICS_DIR", "data/metrics")).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

        # ✅ Input / output files
        self.candidates_path = self.output_dir / "tmdb_discogs_candidates_extended.csv"
        self.output_path = self.output_dir / "tmdb_discogs_matches_v2.csv"
        self.metrics_path = self.metrics_dir / "step05_matching_metrics.json"
        self.histogram_path = self.metrics_dir / "step05_score_distribution.png"

        # ✅ Validate environment once per step
        load_and_validate_env()

    # -----------------------------------------------------------
    def _compute_hybrid_score(self, t1: str, t2: str) -> tuple[float, float, float]:
        """Blend token_sort and partial_ratio for robust fuzzy match."""
        token_score = fuzz.token_sort_ratio(t1, t2)
        partial_score = fuzz.partial_ratio(t1, t2)
        hybrid = round(0.7 * token_score + 0.3 * partial_score, 2)
        return hybrid, token_score, partial_score

    # -----------------------------------------------------------
    def _save_histogram(self, df: pd.DataFrame):
        """Save histogram of match score distribution."""
        if df.empty:
            self.logger.warning("⚠️ No data available for histogram plotting.")
            return
        plt.figure(figsize=(8, 5))
        plt.hist(df["hybrid_score"], bins=20, edgecolor="black")
        plt.title("Fuzzy Match Score Distribution (Hybrid)")
        plt.xlabel("Hybrid Score")
        plt.ylabel("Frequency")
        plt.tight_layout()
        plt.savefig(self.histogram_path)
        plt.close()
        self.logger.info(f"📊 Histogram saved → {self.histogram_path}")

    # -----------------------------------------------------------
    def run(self):
        """Perform fuzzy matching between TMDB and Discogs titles."""
        t0 = time.time()
        self.logger.info("🚀 Starting Step 05 | Enhanced Fuzzy Matching")
        self.logger.info(f"🕒 Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        # --- Input validation ---
        if not self.candidates_path.exists():
            self.logger.error(f"❌ Missing candidates file: {self.candidates_path}")
            return
        df = pd.read_csv(self.candidates_path)
        if df.empty:
            self.logger.warning("⚠️ Candidate file is empty; skipping match computation.")
            return

        # --- Normalize and prepare ---
        df["tmdb_title_norm"] = df["tmdb_title_norm"].fillna("").map(normalize_title)
        df["discogs_title_norm"] = df["discogs_title_norm"].fillna("").map(normalize_title)
        for col in ["tmdb_year", "discogs_year"]:
            df[col] = pd.to_numeric(df.get(col), errors="coerce")

        # --- Compute matches ---
        matches = []
        for _, row in df.iterrows():
            t1, t2 = row["tmdb_title_norm"], row["discogs_title_norm"]
            if not t1 or not t2:
                continue

            year_diff = None
            if not pd.isna(row.tmdb_year) and not pd.isna(row.discogs_year):
                year_diff = abs(row.tmdb_year - row.discogs_year)
                if year_diff > 1:
                    continue

            hybrid_score, token_score, partial_score = self._compute_hybrid_score(t1, t2)
            matches.append({
                "tmdb_title": t1,
                "discogs_title": t2,
                "tmdb_year": row.get("tmdb_year"),
                "discogs_year": row.get("discogs_year"),
                "token_score": token_score,
                "partial_score": partial_score,
                "hybrid_score": hybrid_score,
                "year_diff": year_diff,
            })

        # --- Save results ---
        out_df = pd.DataFrame(matches)
        out_df.to_csv(self.output_path, index=False)
        self.logger.info(f"💾 Saved {len(out_df):,} matched rows → {self.output_path}")

        self._save_histogram(out_df)

        # --- Metrics ---
        metrics = {
            "total_candidates": len(df),
            "total_matches": len(out_df),
            "avg_hybrid_score": round(out_df["hybrid_score"].mean(), 2) if not out_df.empty else 0.0,
            "median_score": round(out_df["hybrid_score"].median(), 2) if not out_df.empty else 0.0,
            "stddev_score": round(out_df["hybrid_score"].std(), 2) if not out_df.empty else 0.0,
            "runtime_sec": round(time.time() - t0, 2),
        }

        self.write_metrics(metrics, name="step05_match_metrics")
        self.logger.info(f"✅ Step 05 completed successfully in {metrics['runtime_sec']}s")
        self.logger.info(f"📂 Outputs: {self.output_dir}")
        self.logger.info(f"📊 Metrics: {self.metrics_dir}")


# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step05MatchAndEnrichV2(None).run()
