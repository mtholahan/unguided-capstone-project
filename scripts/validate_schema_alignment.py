# ================================================================
#  validate_schema_alignment.py — v3.2 (mount-less)
# ================================================================

import scripts.bootstrap  # Ensures package discovery on Databricks/local
from scripts import config
from scripts.base_step import BaseStep
from pyspark.sql import functions as F, types as T
import os, time, json, requests

import pandas as pd
import fsspec
from scripts.utils_schema import infer_schema, build_integrity_summary


class Step04ValidateSchemaAlignment(BaseStep):
    """Step 04 – Schema validation & alignment (Pandas-only)."""

    def __init__(self):
        super().__init__("step_04_validate_schema_alignment")
        self.intermediate_dir = config.INTERMEDIATE_DIR
        self.metrics_dir = config.METRICS_DIR
        self.logger.info("✅ Step04 initialized (config-driven)")

    def run(self) -> pd.DataFrame:
        t0 = time.time()
        self.logger.info("🚀 Running Step04")

        storage_opts = {"account_name": config.STORAGE_ACCOUNT, "anon": False}
        fs = fsspec.filesystem("abfss", **storage_opts)

        tmdb_df = pd.read_parquet(f"{self.intermediate_dir}/tmdb", storage_options=storage_opts)
        discogs_df = pd.read_parquet(f"{self.intermediate_dir}/discogs", storage_options=storage_opts)
        candidates_df = pd.read_parquet(
            f"{self.intermediate_dir}/tmdb_discogs_candidates/tmdb_discogs_candidates.parquet",
            storage_options=storage_opts)

        tmdb_schema = infer_schema(tmdb_df)
        discogs_schema = infer_schema(discogs_df)
        integrity_df = build_integrity_summary(tmdb_df, discogs_df, candidates_df, self.logger)

        output_dir = f"{self.intermediate_dir}/validation/schema_alignment"
        for name, df in {
            "schema_tmdb.csv": tmdb_schema,
            "schema_discogs.csv": discogs_schema,
            "integrity_summary.csv": integrity_df,
        }.items():
            with fs.open(f"{output_dir}/{name}", "wb") as f:
                df.to_csv(f, index=False)

        metrics = {
            "tmdb_rows": len(tmdb_df),
            "discogs_rows": len(discogs_df),
            "tmdb_columns": tmdb_df.shape[1],
            "discogs_columns": discogs_df.shape[1],
            "duration_sec": round(time.time() - t0, 1),
        }
        self.write_metrics(metrics, name="step04_schema_validation_metrics", metrics_dir=self.metrics_dir)
        self.logger.info(f"✅ Step04 completed in {metrics['duration_sec']} s")

        return integrity_df
