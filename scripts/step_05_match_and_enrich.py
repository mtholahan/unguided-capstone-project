"""
Step 05 – TMDB ↔ Discogs Matching & Enrichment
-----------------------------------------------
Performs deterministic + fuzzy matching between TMDB and Discogs titles.
Outputs:
    data/intermediate/tmdb_discogs_matches.csv
    metrics/step05_matching_metrics.json
"""

import pandas as pd
from pathlib import Path
from rapidfuzz import fuzz
import logging
import json
import time

# 🧩 Project imports
from scripts.base_step import BaseStep
from scripts.config import INTERMEDIATE_DIR, METRICS_DIR, LOG_DIR
from scripts.utils import normalize_for_matching_extended as normalize_title


class Step05MatchAndEnrich(BaseStep):
    """Step 05 – Hybrid TMDB↔Discogs Matching."""

    def __init__(self):
        super().__init__("step_05_match_and_enrich")

        self.candidates_path = INTERMEDIATE_DIR / "tmdb_discogs_candidates_extended.csv"
        self.output_path = INTERMEDIATE_DIR / "tmdb_discogs_matches.csv"
        self.metrics_path = METRICS_DIR / "step05_matching_metrics.json"

    # ------------------------------------------------------------------
    def run(self):
        t0 = time.time()
        self.logger.info("Starting Step 05 | Hybrid TMDB ↔ Discogs matching")

        # --------------------------------------------------------------
        # Load candidate pairs
        # --------------------------------------------------------------
        if not self.candidates_path.exists():
            self.logger.error(f"Missing candidates file: {self.candidates_path}")
            return

        df = pd.read_csv(self.candidates_path)
        self.logger.info(f"Loaded {len(df):,} candidate rows")

        # ------------------------------------------------------------------
        # Rescue-pass: ID re-hydration + deeper normalization
        # ------------------------------------------------------------------
        from scripts.utils_schema import load_tmdb_fullscan, load_discogs_fullscan
        from scripts.config import TMDB_RAW_DIR, DISCOGS_RAW_DIR

        self.logger.info("Running rescue-pass enrichment...")

        # 1️⃣ Load raw sources from Step 04
        tmdb_df = load_tmdb_fullscan(TMDB_RAW_DIR, self.logger)
        discogs_df = load_discogs_fullscan(DISCOGS_RAW_DIR, self.logger)

        # keep only id + title
        tmdb_df = tmdb_df[["tmdb_id", "title"]].dropna().drop_duplicates()
        discogs_df = discogs_df[["discogs_id", "title"]].dropna().drop_duplicates()

        # 2️⃣ Normalize & strip parentheses / OST noise
        def clean_title(t: str) -> str:
            if not isinstance(t, str):
                return ""
            import re
            t = normalize_title(t)
            t = re.sub(r"\(.*?\)", "", t)                     # remove parentheticals
            for stop in ["ost", "score", "deluxe", "expanded", "edition"]:
                t = t.replace(stop, "")
            return re.sub(r"\s+", " ", t).strip()

        tmdb_df["title_clean"] = tmdb_df["title"].map(clean_title)
        discogs_df["title_clean"] = discogs_df["title"].map(clean_title)

        # 3️⃣ Merge IDs into candidates by normalized title
        df["tmdb_title_norm"] = df["tmdb_title_norm"].fillna("").map(clean_title)
        df["discogs_title_norm"] = df["discogs_title_norm"].fillna("").map(clean_title)

        df = df.merge(tmdb_df[["tmdb_id", "title_clean"]],
                    left_on="tmdb_title_norm", right_on="title_clean", how="left")
        df = df.merge(discogs_df[["discogs_id", "title_clean"]],
                    left_on="discogs_title_norm", right_on="title_clean", how="left")

        # Drop helper columns
        df.drop(columns=["title_clean_x", "title_clean_y"], inplace=True, errors="ignore")

        self.logger.info(f"Re-hydrated IDs | tmdb_ids={df['tmdb_id'].notna().sum():,} | "
                        f"discogs_ids={df['discogs_id'].notna().sum():,}")


        # --------------------------------------------------------------
        # Normalize titles
        # --------------------------------------------------------------
        self.logger.info("Normalizing titles for fuzzy matching...")
        df["tmdb_title_norm"] = df["tmdb_title_norm"].fillna("").map(normalize_title)
        df["discogs_title_norm"] = df["discogs_title_norm"].fillna("").map(normalize_title)

        # --------------------------------------------------------------
        # Score similarity
        # --------------------------------------------------------------
        matches = []
        for _, row in df.iterrows():
            t1, t2 = row["tmdb_title_norm"], row["discogs_title_norm"]
            if not t1 or not t2:
                continue
            score = fuzz.token_sort_ratio(t1, t2)
            match_type = "deterministic" if score == 100 else "fuzzy"
            matches.append({
                "tmdb_id": row.get("tmdb_id"),
                "discogs_id": row.get("discogs_id"),
                "tmdb_title": row.get("tmdb_title_norm"),
                "discogs_title": row.get("discogs_title_norm"),
                "score": score,
                "match_type": match_type,
            })

        out_df = pd.DataFrame(matches)
        out_df.to_csv(self.output_path, index=False)
        self.logger.info(f"Saved {len(out_df):,} matched rows → {self.output_path}")

        # --------------------------------------------------------------
        # Metrics summary
        # --------------------------------------------------------------
        metrics = {
            "total_candidates": len(df),
            "total_matches": len(out_df),
            "avg_score": round(out_df["score"].mean(), 2) if not out_df.empty else 0.0,
            "high_confidence": int((out_df["score"] >= 90).sum()),
            "mid_confidence": int(((out_df["score"] >= 70) & (out_df["score"] < 90)).sum()),
            "low_confidence": int((out_df["score"] < 70).sum()),
            "step_runtime_sec": round(time.time() - t0, 2),
        }

        METRICS_DIR.mkdir(exist_ok=True, parents=True)
        with open(self.metrics_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)

        self.logger.info(f"📈 Saved metrics JSON → {self.metrics_path}")
        self.logger.info(f"✅ Step 05 matching complete | Runtime {metrics['step_runtime_sec']}s")


# ----------------------------------------------------------------------
if __name__ == "__main__":
    Step05MatchAndEnrich().run()
