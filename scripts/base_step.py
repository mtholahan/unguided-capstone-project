# base_step.py

import logging
from pathlib import Path
import sys
from tqdm import tqdm
from config import DEBUG_MODE, Config, DATA_DIR # Global debug toggle
import subprocess
import os

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

        # --- Always create a logger early ---
        self.logger = logging.getLogger(self.name)

        # --- Auto-config fallback ---
        self.config = config if config is not None else Config()

        self.metrics_dir = os.path.join(DATA_DIR, "metrics")
        os.makedirs(self.metrics_dir, exist_ok=True)

        # --- Try to capture Git metadata (optional) ---
        branch, commit = "unknown", "unknown"
        try:
            branch = (
                subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL)
                .decode("utf-8")
                .strip()
            )
            commit = (
                subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL)
                .decode("utf-8")
                .strip()
            )
        except Exception:
            pass

        # --- Safe logging (logger now guaranteed) ---
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
        raise RuntimeError(msg)

    def setup_logger(self):
        """
        Configure logging once per pipeline run.
        - Console (UTF-8 safe)
        - Global pipeline.log (appends all steps)
        """
        try:
            sys.stdout.reconfigure(encoding="utf-8")  # Ensure stdout supports UTF-8
        except AttributeError:
            # Fallback for Python < 3.7
            import io
            sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding="utf-8")

        log_dir = Path(self.config.get("log_dir", "./logs"))
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "pipeline.log"  # single unified log

        # Root logger (only configured once)
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                handlers=[
                    logging.FileHandler(log_file, mode="w", encoding="utf-8"),
                    logging.StreamHandler(sys.stdout),
                ],
            )

        self.logger = logging.getLogger(self.name)
        self.logger.info(f"Initialized step: {self.name} (DEBUG_MODE={DEBUG_MODE})")

    def progress_iter(self, iterable, **kwargs):
        """Wrapper for tqdm with default description set to step name."""
        if "desc" not in kwargs:
            kwargs["desc"] = self.name
        return tqdm(iterable, **kwargs)
    
    def write_metrics(self, step_name, metrics: dict):
        """
        Dynamically appends metrics to pipeline_metrics.csv.
        Adds new columns automatically if unseen before.
        """
        import csv, os
        from datetime import datetime

        metrics_file = os.path.join(self.metrics_dir, "pipeline_metrics.csv")
        metrics = metrics.copy()
        metrics["step_name"] = step_name
        metrics["timestamp"] = datetime.now().isoformat(timespec="seconds")

        # Merge existing and new headers
        if os.path.exists(metrics_file):
            with open(metrics_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                existing_headers = reader.fieldnames or []
        else:
            existing_headers = []

        new_headers = [k for k in metrics.keys() if k not in existing_headers]
        fieldnames = existing_headers + new_headers

        # If file doesn't exist, write header
        write_header = not os.path.exists(metrics_file)
        # If new headers appeared, rewrite file with updated header
        if new_headers and not write_header:
            with open(metrics_file, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            with open(metrics_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)

        # Append new record
        with open(metrics_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(metrics)

    def run(self):
        raise NotImplementedError("Subclasses must implement a run() method.")
