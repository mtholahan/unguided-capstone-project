# base_step.py

import logging
from pathlib import Path
import sys
from tqdm import tqdm
from config import DEBUG_MODE, Config # Global debug toggle
import subprocess

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
    
    # ============================================================
    # 📊 Metrics Logging Utility (with duration + Git provenance)
    # ============================================================
    def write_metrics(self, step_name: str, metrics: dict, start_time=None):
        """
        Write standardized pipeline metrics to CSV for cross-step tracking.
        Includes Git branch + commit for reproducibility.

        Args:
            step_name (str): Identifier of the current pipeline step.
            metrics (dict): Dictionary with at least:
                - rows_total
                - rows_matched
                - rows_skipped
                - match_pct
            start_time (float, optional): time.time() at step start for duration tracking.
        """
        import datetime as dt
        import csv
        import time
        import subprocess
        from pathlib import Path

        try:
            # --- Git metadata ---
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
                pass  # fine if not a Git repo

            # --- File path & metadata ---
            data_dir = getattr(self.config, "DATA_DIR", Path("data"))
            metrics_file = Path(data_dir) / "metrics" / "pipeline_metrics.csv"
            metrics_file.parent.mkdir(exist_ok=True)

            # --- Compose final metrics row ---
            metrics.update({
                "step_name": step_name,
                "run_timestamp": dt.datetime.utcnow().isoformat(timespec="seconds"),
                "git_branch": branch,
                "git_commit": commit,
            })

            if start_time is not None:
                metrics["duration_sec"] = round(time.time() - start_time, 2)
            else:
                metrics["duration_sec"] = None

            header = [
                "step_name",
                "rows_total",
                "rows_matched",
                "rows_skipped",
                "match_pct",
                "duration_sec",
                "git_branch",
                "git_commit",
                "run_timestamp",
            ]

            write_header = not metrics_file.exists()

            with open(metrics_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=header)
                if write_header:
                    writer.writeheader()
                writer.writerow(metrics)

            self.logger.info(f"📊 Metrics logged to {metrics_file} → {metrics}")

        except Exception as e:
            self.logger.warning(f"⚠️ Failed to write metrics for {step_name}: {e}")


    def run(self):
        raise NotImplementedError("Subclasses must implement a run() method.")
