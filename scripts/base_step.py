# base_step.py

import logging
from pathlib import Path
import sys
from tqdm import tqdm
from config import DEBUG_MODE, Config, DATA_DIR
import subprocess
import os
import pandas as pd
try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None
from config import AZURE_CONN_STR, BLOB_CONTAINER
from utils import make_progress_bar


class BaseStep:
    # ============================================================
    # 🔧 Constructor with auto-Config fallback + Git metadata logging
    # ============================================================
    def __init__(self, name, config=None):
        """
        Initialize the base pipeline step.
        Ensures logger and config are ready before run().
        Logs DATA_DIR, ROW_LIMIT, and Git metadata for reproducibility.
        """
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.config = config if config is not None else Config()

        self.metrics_dir = os.path.join(DATA_DIR, "metrics")
        os.makedirs(self.metrics_dir, exist_ok=True)

        # --- Try to capture Git metadata (optional) ---
        branch, commit = "unknown", "unknown"
        try:
            branch = subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL
            ).decode("utf-8").strip()
            commit = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
            ).decode("utf-8").strip()
        except Exception:
            pass

        try:
            self.logger.info(
                f"Initialized {self.name} "
                f"[branch={branch}, commit={commit}] "
                f"with DATA_DIR={self.config.DATA_DIR}, ROW_LIMIT={self.config.ROW_LIMIT}"
            )
        except Exception as e:
            print(f"[Init] {self.name}: config/logging check failed ({e})")

    def fail(self, msg: str):
        self.logger.error(msg)
        sys.stdout.flush()
        raise RuntimeError(msg)

    # ============================================================
    # 🪶 Logging setup (now PowerShell-safe with live console output)
    # ============================================================
    def setup_logger(self):
        """
        Configure logging once per pipeline run.
        - Console (UTF-8 safe)
        - Global pipeline.log (appends all steps)
        """
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            import io
            sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding="utf-8")

        # Support either dict-like config or Config object with attributes
        try:
            log_dir_value = getattr(self.config, "log_dir", None)
            if log_dir_value is None and isinstance(self.config, dict):
                log_dir_value = self.config.get("log_dir")
            log_dir = Path(log_dir_value or "./logs")
        except Exception:
            log_dir = Path("./logs")

        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "pipeline.log"

        # Root logger only initialized once
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                handlers=[
                    logging.FileHandler(log_file, mode="w", encoding="utf-8"),
                    logging.StreamHandler(sys.stdout),  # ensure console output
                ],
            )

        self.logger = logging.getLogger(self.name)

        # --- Force immediate flush for PowerShell ---
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.flush = lambda: sys.stdout.flush()

        # --- Add an explicit console handler if missing ---
        if not any(isinstance(h, logging.StreamHandler) for h in self.logger.handlers):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(logging.Formatter("%(message)s"))
            self.logger.addHandler(console_handler)

        self.logger.info(f"Initialized step: {self.name} (DEBUG_MODE={DEBUG_MODE})")

    # ============================================================
    # ⏳ Unified progress iterator
    # ============================================================
    def progress_iter(self, iterable, desc=None, unit="item", leave=False, total=None):
        """Unified progress iterator using make_progress_bar."""
        with make_progress_bar(iterable, desc=desc, unit=unit, leave=leave, total=total) as bar:
            for item in bar:
                yield item
            bar.close()
        sys.stdout.flush()

    # ============================================================
    # ☁️ Azure upload helpers
    # ============================================================
    def upload_to_blob(self, local_path: Path):
        """Upload a local file to Azure Blob Storage."""
        if not AZURE_CONN_STR or BlobServiceClient is None:
            self.logger.info("Skipping Azure upload (no Azure SDK or connection string).")
            sys.stdout.flush()
            return
        try:
            blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
            blob_client = blob_service.get_blob_client(
                container=BLOB_CONTAINER, blob=f"outputs/{local_path.name}"
            )
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            self.logger.info(f"☁️ Uploaded {local_path.name} → blob:{BLOB_CONTAINER}/outputs/")
        except Exception as e:
            self.logger.warning(f"⚠️ Azure upload skipped or failed: {e}")
        sys.stdout.flush()

    # ============================================================
    # 💾 Safe overwrite & metrics utilities
    # ============================================================
    def safe_overwrite(self, df: pd.DataFrame, path: Path, upload_to_blob=False):
        """Write CSV atomically, then optionally push to Azure Blob."""
        tmp = Path(f"{path}.tmp")
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)
        self.logger.info(f"💾 Wrote {len(df):,} rows → {path.name}")
        sys.stdout.flush()

        if upload_to_blob:
            self.upload_to_blob(path)

    def write_metrics(self, step_name, metrics: dict):
        """
        Append metrics to pipeline_metrics.csv.
        Automatically adds new columns if unseen.
        """
        import csv, os
        from datetime import datetime

        metrics_file = os.path.join(self.metrics_dir, "pipeline_metrics.csv")
        metrics = metrics.copy()
        metrics["step_name"] = step_name
        metrics["timestamp"] = datetime.now().isoformat(timespec="seconds")

        if os.path.exists(metrics_file):
            with open(metrics_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                existing_headers = reader.fieldnames or []
        else:
            existing_headers = []

        new_headers = [k for k in metrics.keys() if k not in existing_headers]
        fieldnames = existing_headers + new_headers

        write_header = not os.path.exists(metrics_file)
        if new_headers and not write_header:
            with open(metrics_file, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            with open(metrics_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)

        with open(metrics_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(metrics)
        sys.stdout.flush()

    def run(self):
        raise NotImplementedError("Subclasses must implement a run() method.")
