# ================================================================
#  match_and_enrich.py — v3.2 (mount-less, config-driven)
#  ---------------------------------------------------------------
#  Purpose : Fuzzy-match TMDB and Discogs candidate titles,
#            produce enriched match table + metrics
# ================================================================

import scripts.bootstrap  # Ensures package discovery on Databricks/local
from scripts import config
from scripts.base_step import BaseStep
from pyspark.sql import functions as F, types as T
import os, time, json, requests

import pandas as pd
from rapidfuzz import fuzz, process
import fsspec


FUZZ_THRESHOLD = config.FUZZ_THRESHOLD
YEAR_VARIANCE = config.YEAR_VARIANCE

class Step05MatchAndEnrichDBX(BaseStep):
    """Step 05 – Fuzzy match & enrich TMDB↔Discogs pairs."""

    def __init__(self):
        super().__init__("step_05_match_and_enrich")
        self.intermediate_dir = config.INTERMEDIATE_DIR
        self.metrics_dir = config.METRICS_DIR
        self.logger.info("✅ Step05 initialized (config-driven)")

    # ------------------------------------------------------------
    def run(self) -> pd.DataFrame:
        t0 = time.time()
        self.logger.info("🚀 Running Step05")

        storage_opts = {"account_name": config.STORAGE_ACCOUNT, "anon": False}
        fs = fsspec.filesystem("abfss", **storage_opts)

        input_path = f"{self.intermediate_dir}/tmdb_discogs_candidates/tmdb_discogs_candidates.parquet"
        df = pd.read_parquet(input_path, storage_options=storage_opts)
        self.logger.info(f"📦 Loaded {len(df):,} candidate pairs")

        # 1️⃣ Compute fuzzy similarity
        df["match_score"] = df.apply(
            lambda r: fuzz.token_set_ratio(str(r["tmdb_title"]), str(r["discogs_title"])),
            axis=1,
        )

        # 2️⃣ Filter & flag strong matches
        df["year_diff"] = abs(df["tmdb_year"] - df["discogs_year"])
        df["is_match"] = (df["match_score"] >= FUZZ_THRESHOLD) & (df["year_diff"] <= YEAR_VARIANCE)
        df_matches = df[df["is_match"]].reset_index(drop=True)

        # 3️⃣ Write enriched results
        output_dir = f"{self.intermediate_dir}/enriched"
        out_path = f"{output_dir}/tmdb_discogs_matches.parquet"
        with fs.open(out_path, "wb") as f:
            df_matches.to_parquet(f, index=False)

        # 4️⃣ Metrics
        metrics = {
            "total_candidates": len(df),
            "strong_matches": len(df_matches),
            "fuzz_threshold": FUZZ_THRESHOLD,
            "year_variance": YEAR_VARIANCE,
            "duration_sec": round(time.time() - t0, 1),
            "output_path": out_path,
        }
        self.write_metrics(metrics, name="step05_match_metrics", metrics_dir=self.metrics_dir)
        self.logger.info(f"✅ Step05 completed in {metrics['duration_sec']} s")

        return df_matches
