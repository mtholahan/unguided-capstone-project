import pandas as pd
from pathlib import Path
from rapidfuzz import fuzz
import logging
import json
import time
import matplotlib.pyplot as plt
from scripts.base_step import BaseStep
from scripts.config import INTERMEDIATE_DIR, METRICS_DIR
from scripts.utils import normalize_for_matching_extended as normalize_title


class Step05MatchAndEnrichV2(BaseStep):
    """Step 05 – Phase 2 Rescue Plan: Enhanced Fuzzy Matching with Year-Bound Logic."""

    def __init__(self):
        super().__init__("step_05_match_and_enrich_v2")
        self.candidates_path = INTERMEDIATE_DIR / "tmdb_discogs_candidates_extended.csv"
        self.output_path = INTERMEDIATE_DIR / "tmdb_discogs_matches_v2.csv"
        self.metrics_path = METRICS_DIR / "step05_matching_metrics.json"
        self.histogram_path = METRICS_DIR / "step05_score_distribution.png"

    # --------------------------------------------------------------
    def run(self):
        t0 = time.time()
        self.logger.info("Starting Step 05 | Phase 2 Rescue Plan")

        if not self.candidates_path.exists():
            self.logger.error(f"Missing candidates file: {self.candidates_path}")
            return

        df = pd.read_csv(self.candidates_path)
        self.logger.info(f"Loaded {len(df):,} candidate rows")

        # Normalize titles
        df["tmdb_title_norm"] = df["tmdb_title_norm"].fillna("").map(normalize_title)
        df["discogs_title_norm"] = df["discogs_title_norm"].fillna("").map(normalize_title)

        # Normalize release years (if present)
        for col in ["tmdb_year", "discogs_year"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = None

        matches = []

        for _, row in df.iterrows():
            t1, t2 = row["tmdb_title_norm"], row["discogs_title_norm"]
            if not t1 or not t2:
                continue

            year_diff = None
            if not pd.isna(row.tmdb_year) and not pd.isna(row.discogs_year):
                year_diff = abs(row.tmdb_year - row.discogs_year)

            # Apply year-bounded matching (±1 year)
            if year_diff is not None and year_diff > 1:
                continue

            token_score = fuzz.token_sort_ratio(t1, t2)
            partial_score = fuzz.partial_ratio(t1, t2)
            hybrid_score = round(0.7 * token_score + 0.3 * partial_score, 2)

            matches.append({
                "tmdb_id": row.get("tmdb_id"),
                "discogs_id": row.get("discogs_id"),
                "tmdb_title": t1,
                "discogs_title": t2,
                "tmdb_year": row.get("tmdb_year"),
                "discogs_year": row.get("discogs_year"),
                "token_score": token_score,
                "partial_score": partial_score,
                "hybrid_score": hybrid_score,
                "year_diff": year_diff,
            })

        out_df = pd.DataFrame(matches)
        out_df.to_csv(self.output_path, index=False)
        self.logger.info(f"Saved {len(out_df):,} matched rows → {self.output_path}")

        # --------------------------------------------------------------
        # Histogram Visualization
        # --------------------------------------------------------------
        if not out_df.empty:
            plt.figure(figsize=(8, 5))
            plt.hist(out_df["hybrid_score"], bins=20, edgecolor='black')
            plt.title("Fuzzy Match Score Distribution (Hybrid)")
            plt.xlabel("Hybrid Score")
            plt.ylabel("Frequency")
            METRICS_DIR.mkdir(exist_ok=True, parents=True)
            plt.tight_layout()
            plt.savefig(self.histogram_path)
            plt.close()
            self.logger.info(f"📊 Saved score distribution histogram → {self.histogram_path}")

        # --------------------------------------------------------------
        # Metrics summary
        # --------------------------------------------------------------
        metrics = {
            "total_candidates": len(df),
            "total_matches": len(out_df),
            "avg_hybrid_score": round(out_df["hybrid_score"].mean(), 2) if not out_df.empty else 0.0,
            "avg_token_score": round(out_df["token_score"].mean(), 2) if not out_df.empty else 0.0,
            "avg_partial_score": round(out_df["partial_score"].mean(), 2) if not out_df.empty else 0.0,
            "high_confidence": int((out_df["hybrid_score"] >= 90).sum()),
            "mid_confidence": int(((out_df["hybrid_score"] >= 70) & (out_df["hybrid_score"] < 90)).sum()),
            "low_confidence": int((out_df["hybrid_score"] < 70).sum()),
            "median_score": round(out_df["hybrid_score"].median(), 2) if not out_df.empty else 0.0,
            "stddev_score": round(out_df["hybrid_score"].std(), 2) if not out_df.empty else 0.0,
            "step_runtime_sec": round(time.time() - t0, 2),
        }

        with open(self.metrics_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)

        self.logger.info(f"📈 Saved metrics JSON → {self.metrics_path}")
        self.logger.info(f"✅ Step 05 Phase 2 complete | Runtime {metrics['step_runtime_sec']}s")


if __name__ == "__main__":
    Step05MatchAndEnrichV2().run()