"""
base_step.py — Unified Base Class for Pipeline Steps
----------------------------------------------------
Provides shared logging, metrics tracking, and optional Azure upload.
All steps (01–07) inherit from BaseStep for consistent behavior.

Version:
    v4 — Oct 2025

Author:
    Mark Holahan
"""

import os
import sys
import csv
import json
import logging
import subprocess
from pathlib import Path
from datetime import datetime
import pandas as pd
from pyspark.sql import functions as F
from scripts.config import (
    DATA_DIR,
    LOG_DIR,
    LOG_LEVEL,
)

# Optional Azure dependency
try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None


# ============================================================
# 🪶 Shared Logger Factory (used by BaseStep + main.py)
# ============================================================
def setup_logger(name="Pipeline", log_dir=LOG_DIR):
    """Initialize a unified logger writing to logs/pipeline.log and console."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / "pipeline.log"

    # Avoid reconfiguring multiple times
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file, mode="a", encoding="utf-8"),
                logging.StreamHandler(sys.stdout),
            ],
        )

    logger = logging.getLogger(name)
    logger.info(f"Initialized logger for {name}")
    return logger


# ============================================================
# 🧩 Base Class for All Steps
# ============================================================
class BaseStep:
    """Parent class providing consistent setup, logging, and metrics."""

    def __init__(self, name: str):
        self.name = name
        self.logger = setup_logger(name)
        self.data_dir = Path(DATA_DIR)
        self.log_dir = Path(LOG_DIR)
        self.metrics_dir = self.data_dir / "metrics"
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

        # Git metadata for reproducibility
        try:
            branch = subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                stderr=subprocess.DEVNULL,
            ).decode("utf-8").strip()
            commit = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                stderr=subprocess.DEVNULL,
            ).decode("utf-8").strip()
        except Exception:
            branch, commit = "unknown", "unknown"

        self.logger.info(f"Initialized {name} [branch={branch}, commit={commit}]")


    # ============================================================
    # 💾 Metrics Handling — CSV + JSON with Runtime Rollup
    # ============================================================
    def write_metrics(
        self,
        metrics: dict,
        name: str | None = None,
        metrics_dir: str | Path | None = None,
    ):
        """
        Persist step metrics in two forms:
        1️⃣ Append to consolidated pipeline_metrics.csv (local)
        2️⃣ Write individual JSON snapshot for the step (local + optional ADLS)

        Backward compatible with older calls that only passed (metrics, name).
        """
        import csv, json, time, os
        from datetime import datetime
        from pathlib import Path
        from scripts.config import DATA_DIR

        # --- pick metrics dir (arg > instance > default) ---
        if metrics_dir is None:
            metrics_dir = getattr(self, "metrics_dir", None) or (Path(DATA_DIR) / "metrics")
        metrics_dir = Path(metrics_dir)
        metrics_dir.mkdir(parents=True, exist_ok=True)

        # --- Track timing info ---
        now = time.time()
        if not hasattr(self, "_pipeline_start"):
            self._pipeline_start = now
            self._last_step_time = now
            pipeline_runtime = 0.0
        else:
            pipeline_runtime = round(now - self._pipeline_start, 2)

        step_runtime = (
            metrics.get("duration_sec")
            if "duration_sec" in metrics
            else round(now - getattr(self, "_last_step_time", now), 2)
        )
        self._last_step_time = now

        # --- Augment metrics with metadata ---
        metrics = metrics.copy()
        metrics["step_name"] = name or getattr(self, "name", "unknown_step")
        metrics["timestamp"] = datetime.now().isoformat(timespec="seconds")
        metrics["step_runtime_sec"] = step_runtime
        metrics["pipeline_runtime_sec"] = pipeline_runtime

        # --- 1️⃣ Append to pipeline_metrics.csv ---
        metrics_file = metrics_dir / "pipeline_metrics.csv"
        write_header = not metrics_file.exists()
        with open(metrics_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=metrics.keys())
            if write_header:
                writer.writeheader()
            writer.writerow(metrics)

        # --- 2️⃣ Write individual JSON snapshot ---
        json_file = metrics_dir / f"{metrics['step_name']}.json"
        json_file.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")

        # --- 3️⃣ Optionally copy metrics to ADLS if dbutils available ---
        #     (keep your original behavior)
        if hasattr(self, "dbutils"):
            try:
                adls_target = os.path.join(str(metrics_dir), f"{metrics['step_name']}.json")
                self.dbutils.fs.cp(f"file:{json_file}", adls_target, recurse=True)
                self.logger.info(f"📤 Metrics copied to ADLS: {adls_target}")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not copy metrics to ADLS: {e}")

        # --- Log summary ---
        self.logger.info(f"📊 Logged metrics for {metrics['step_name']}: {metrics}")
        self.logger.debug(
            f"🪶 Metrics written to: {metrics_file.name}, {json_file.name} | "
            f"Step runtime={step_runtime:.2f}s | Pipeline runtime={pipeline_runtime:.2f}s"
        )
 


    # ============================================================
    # 💾 Metrics Handling — JSON (per-step summary)
    # ============================================================
    def save_metrics(self, filename: str, data: dict):
        """
        Save per-step metrics or outputs as a JSON file.
        Compatible with main.py’s rollup_metrics() aggregator.
        """
        out_path = self.metrics_dir / filename
        tmp = out_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, out_path)
        self.logger.info(f"📈 Saved metrics JSON → {out_path.name}")

    # ============================================================
    # 🧱 Atomic Write Helper (for any JSON-like file)
    # ============================================================
    def atomic_write(self, path: Path, data):
        """Safely write JSON data atomically."""
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
        self.logger.info(f"💾 Atomic write complete → {path.name}")

    # ============================================================
    # ☁️ Azure Upload (Optional)
    # ============================================================
    def upload_to_blob(self, local_path: Path, conn_str=None, container=None):
        """Upload a local file to Azure Blob Storage."""
        if not BlobServiceClient or not conn_str or not container:
            self.logger.info("Skipping Azure upload (missing config or SDK).")
            return
        try:
            blob_service = BlobServiceClient.from_connection_string(conn_str)
            blob_client = blob_service.get_blob_client(
                container=container, blob=f"outputs/{local_path.name}"
            )
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            self.logger.info(f"☁️ Uploaded {local_path.name} → blob:{container}/outputs/")
        except Exception as e:
            self.logger.warning(f"⚠️ Azure upload failed: {e}")

    # ============================================================
    # 🧮 Safe CSV Write Helper
    # ============================================================
    def safe_overwrite(self, df: pd.DataFrame, path: Path):
        """Write CSV atomically to avoid partial writes."""
        tmp = path.with_suffix(".tmp")
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)
        self.logger.info(f"💾 Wrote {len(df):,} rows → {path.name}")

    # ============================================================
    # Common schema normalization helper
    # ============================================================
    def normalize_schema(self, df, required_cols):
        """
        Ensure all required columns exist in the DataFrame.
        Adds missing columns as nulls and reorders schema for consistency.
        """
        existing = df.columns
        missing = [c for c in required_cols if c not in existing]

        if missing:
            self.logger.warning(f"⚠️ Adding missing columns to schema: {missing}")

        for c in missing:
            df = df.withColumn(c, F.lit(None))

        # Maintain order: required first, then any extras
        final_cols = required_cols + [c for c in existing if c not in required_cols]
        return df.select(*final_cols)
    
    # ============================================================
    # 🚀 Abstract run() method
    # ============================================================
    def run(self):
        """Each pipeline step must implement this method."""
        raise NotImplementedError("Each pipeline step must implement its own run() method.")
