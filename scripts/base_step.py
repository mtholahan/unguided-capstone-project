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
from config import (
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
    # 💾 Metrics Handling — CSV
    # ============================================================
    def write_metrics(self, metrics: dict):
        """Append metrics to pipeline_metrics.csv with automatic headers."""
        metrics_file = self.metrics_dir / "pipeline_metrics.csv"
        metrics = metrics.copy()
        metrics["step_name"] = self.name
        metrics["timestamp"] = datetime.now().isoformat(timespec="seconds")

        write_header = not metrics_file.exists()
        fieldnames = metrics.keys()

        with open(metrics_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(metrics)

        self.logger.info(f"📊 Logged metrics for {self.name}: {metrics}")

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
    # 🚀 Abstract run() method
    # ============================================================
    def run(self):
        """Each pipeline step must implement this method."""
        raise NotImplementedError("Each pipeline step must implement its own run() method.")
