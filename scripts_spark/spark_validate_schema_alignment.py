"""
🎯 Step 04 – Schema Validation & Alignment
Unguided Capstone Project | Unified Environment-Aware Pattern
"""

import os
import time
import pandas as pd
from pathlib import Path
from scripts.utils.base_step import BaseStep
from scripts.config_env import load_and_validate_env
from scripts.utils_schema import (
    setup_file_logger,
    load_tmdb_fullscan,
    load_discogs_fullscan,
    infer_schema,
    build_integrity_summary,
)
from scripts.utils.env import load_env
from scripts.utils.io_utils import ensure_dirs

env = load_env()

# ===============================================================
# 🎯 Core Class
# ===============================================================
class Step04ValidateSchemaAlignment(BaseStep):
    """Validate and align TMDB vs Discogs schemas before enrichment."""

    def __init__(
        self,
        spark=None,
        tmdb_raw_dir: Path | None = None,
        discogs_raw_dir: Path | None = None,
        candidates_file: Path | None = None,
    ):
        super().__init__(name="step_04_validate_schema_alignment")
        self.spark = spark

        # ✅ Environment-aware directories
        root_path = Path(env.get("ROOT") or env.get("root", ".")).resolve()

        self.output_dir = root_path / "data" / "intermediate"
        self.metrics_dir = root_path / "data" / "metrics"
        self.validation_dir = root_path / "data" / "validation"
        self.validation_log_dir = self.validation_dir / "logs"

        # Ensure output dirs exist
        ensure_dirs([
            self.output_dir,
            self.metrics_dir,
            self.validation_dir,
            self.validation_log_dir,
        ])

        # ✅ Input sources (env override or fallback to defaults)
        self.tmdb_raw_dir = Path(env.get("TMDB_RAW_DIR", root_path / "data" / "raw" / "tmdb")).resolve()
        self.discogs_raw_dir = Path(env.get("DISCOGS_RAW_DIR", root_path / "data" / "raw" / "discogs")).resolve()

        self.candidates_file = (
            Path(env.get("CANDIDATES_FILE", "")) if env.get("CANDIDATES_FILE")
            else self.output_dir / "tmdb_discogs_candidates_extended.csv"
        )


        # ✅ Load .env
        load_and_validate_env()
        setup_file_logger(self.logger, self.validation_log_dir / "validation.log")

    # ---------------------------------------------------------------
    def run(self):
        """Validate and align schemas."""
        self.logger.info(f"🚀 Starting Step 04 | Schema Validation & Alignment")
        self.logger.info(f"🕒 Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"📁 TMDB source: {self.tmdb_raw_dir}")
        self.logger.info(f"📁 Discogs source: {self.discogs_raw_dir}")

        # --- Load datasets ---
        tmdb_df = load_tmdb_fullscan(self.tmdb_raw_dir, self.logger)
        discogs_df = load_discogs_fullscan(self.discogs_raw_dir, self.logger)
        self.logger.info(f"📊 TMDB rows={len(tmdb_df)}, cols={tmdb_df.shape[1]}")
        self.logger.info(f"📊 Discogs rows={len(discogs_df)}, cols={discogs_df.shape[1]}")

        # --- Infer schemas ---
        tmdb_schema = infer_schema(tmdb_df)
        discogs_schema = infer_schema(discogs_df)

        # --- Integrity summary ---
        integrity_df = build_integrity_summary(
            tmdb_df, discogs_df, self.candidates_file, self.logger
        )

        # --- Write outputs ---
        tmdb_out = self.output_dir / "schema_comparison_tmdb.csv"
        discogs_out = self.output_dir / "schema_comparison_discogs.csv"
        integ_out = self.output_dir / "integrity_summary.csv"

        tmdb_schema.to_csv(tmdb_out, index=False)
        discogs_schema.to_csv(discogs_out, index=False)
        integrity_df.to_csv(integ_out, index=False)

        # --- Metrics ---
        metrics = {
            "tmdb_rows": len(tmdb_df),
            "discogs_rows": len(discogs_df),
            "tmdb_columns": tmdb_df.shape[1],
            "discogs_columns": discogs_df.shape[1],
            "tmdb_schema_path": str(tmdb_out),
            "discogs_schema_path": str(discogs_out),
            "integrity_summary_path": str(integ_out),
            "duration_sec": round(time.time() - time.mktime(time.localtime()), 2),
        }

        self.write_metrics(metrics, name="step04_schema_validation_metrics")
        self.logger.info("✅ Step 04 validation complete.")
        self.logger.info(f"📂 Outputs written to: {self.output_dir}")


# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step04ValidateSchemaAlignment(None).run()
