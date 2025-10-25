"""
Environment Verification Header
Added for consistent .env loading and mode detection across steps.
"""
from __future__ import annotations
import os, sys
from pathlib import Path

# 🧭 Fix path before importing scripts
project_root = Path(__file__).resolve().parents[1]
os.chdir(project_root)
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# ----------------------------------------------------------------------
# 🧭 Environment verification (runs before imports that depend on scripts/)
# ----------------------------------------------------------------------
from scripts.config_env import load_and_validate_env

# Load .env and populate environment variables
load_and_validate_env()

tmdb_key = os.getenv("TMDB_API_KEY")
local_mode = os.getenv("LOCAL_MODE", "false").lower() == "true"

if tmdb_key:
    key_status = "✅ TMDB key detected"
    key_suffix = f"(len={len(tmdb_key)})"
else:
    key_status = "🚫 TMDB key NOT found"
    key_suffix = ""

mode_status = "🌐 ONLINE mode" if not local_mode else "⚙️ OFFLINE mode"

print(
    f"\n{'='*60}\n"
    f"🔧 Environment Loaded\n"
    f"{key_status} {key_suffix}\n"
    f"{mode_status}\n"
    f"Project Root: {os.getcwd()}\n"
    f"{'='*60}\n"
)

"""
Step 04 – Harmonized Data Validation & Schema Alignment (Full Scan, Lean Version)
------------------------------------------------------------------
Purpose:
    Validate and align schemas for TMDB (movies) and Discogs (soundtracks)
    across all raw JSON files prior to enrichment/matching.

This version delegates common helper functions to scripts/utils_schema.py
for readability and maintainability.
"""
import sys
from typing import Any, Dict

import pandas as pd

# Repo-relative imports (ensure VS Code root = project folder)
from scripts.base_step import BaseStep
from scripts.config import (
    INTERMEDIATE_DIR,
    METRICS_DIR,
    LOG_DIR,
    TMDB_RAW_DIR,
    DISCOGS_RAW_DIR,
)
from scripts.utils_schema import (
    setup_file_logger,
    ensure_dirs,
    load_tmdb_fullscan,
    load_discogs_fullscan,
    infer_schema,
    build_integrity_summary,
)

# ---------------------------
# Defaults
# ---------------------------
CANDIDATES_FILE_DEFAULT = Path(INTERMEDIATE_DIR) / "tmdb_discogs_candidates_extended.csv"
VALIDATION_DIR = Path("data/validation")
VALIDATION_LOG_DIR = Path(LOG_DIR) / "validation"
TMDB_RAW_DIR_DEFAULT = TMDB_RAW_DIR
DISCOGS_RAW_DIR_DEFAULT = DISCOGS_RAW_DIR

class Step04ValidateSchemaAlignment(BaseStep):
    def __init__(
        self,
        tmdb_raw_dir: Path | None = None,
        discogs_raw_dir: Path | None = None,
        candidates_file: Path | None = None,
    ):
        super().__init__("step_04_validate_schema_alignment")
        self.tmdb_raw_dir = Path(tmdb_raw_dir) if tmdb_raw_dir else TMDB_RAW_DIR_DEFAULT
        self.discogs_raw_dir = Path(discogs_raw_dir) if discogs_raw_dir else DISCOGS_RAW_DIR_DEFAULT
        self.candidates_file = Path(candidates_file) if candidates_file else CANDIDATES_FILE_DEFAULT

        ensure_dirs([VALIDATION_DIR, VALIDATION_LOG_DIR, METRICS_DIR])
        setup_file_logger(self.logger, VALIDATION_LOG_DIR / "validation.log")
        self.logger.info(
            f"Initialized Step 04 | TMDB={self.tmdb_raw_dir} | DISCOGS={self.discogs_raw_dir}"
        )

    def run(self) -> None:
        tmdb_df = load_tmdb_fullscan(self.tmdb_raw_dir, self.logger)
        discogs_df = load_discogs_fullscan(self.discogs_raw_dir, self.logger)

        self.logger.info(f"TMDB DF shape: {tmdb_df.shape}")
        self.logger.info(f"Discogs DF shape: {discogs_df.shape}")

        tmdb_schema = infer_schema(tmdb_df)
        discogs_schema = infer_schema(discogs_df)

        integrity_df = build_integrity_summary(
            tmdb_df, discogs_df, self.candidates_file, self.logger
        )

        tmdb_out = VALIDATION_DIR / "schema_comparison_tmdb.csv"
        discogs_out = VALIDATION_DIR / "schema_comparison_discogs.csv"
        integ_out = VALIDATION_DIR / "integrity_summary.csv"

        tmdb_schema.to_csv(tmdb_out, index=False)
        discogs_schema.to_csv(discogs_out, index=False)
        integrity_df.to_csv(integ_out, index=False)

        metrics = {
            "tmdb_rows": len(tmdb_df),
            "discogs_rows": len(discogs_df),
            "tmdb_columns": tmdb_df.shape[1],
            "discogs_columns": discogs_df.shape[1],
            "tmdb_schema_path": str(tmdb_out),
            "discogs_schema_path": str(discogs_out),
            "integrity_summary_path": str(integ_out),
        }
        self.save_metrics("step04_schema_validation_metrics.json", {"summary": metrics})
        self.write_metrics(metrics)

        self.logger.info("✅ Step 04 validation complete.")
        self.logger.info(f"Outputs → {VALIDATION_DIR}")


if __name__ == "__main__":
    Step04ValidateSchemaAlignment().run()
