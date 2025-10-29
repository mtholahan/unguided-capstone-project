"""
main.py — TMDB → Discogs Unified Pipeline Orchestrator
------------------------------------------------------------
Environment-aware, pytest-friendly, and Azure-ready.
"""

import argparse
import json
import os
import sys
import time
import traceback
import pandas as pd
from datetime import datetime
from pathlib import Path
from importlib import import_module
from pyspark.sql import SparkSession

# --- Safe path bootstrap so imports like `from scripts...` work
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from scripts.base_step import setup_logger
from scripts.config import LOG_LEVEL, LOG_DIR
from scripts.config_env import (
    configure_spark_from_env,
    load_and_validate_env,
    ensure_local_env_defaults,
)

# ================================================================
# 🧩 CLI Argument Parsing
# ================================================================
parser = argparse.ArgumentParser(description="Unguided Capstone Unified Pipeline")
parser.add_argument("--resume", type=str, help="Resume pipeline from step name", default=None)
parser.add_argument("--output-dir", type=str, help="Optional override for output directory", default=None)
args = parser.parse_args()

# ================================================================
# Environment Setup
# ================================================================
# ✅ Determine output dirs dynamically (pytest-aware)
if args.output_dir:
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    os.environ["PIPELINE_OUTPUT_DIR"] = str(output_dir)
    print(f"📦 Output directory overridden → {output_dir}")
else:
    output_dir = Path(os.getenv("PIPELINE_OUTPUT_DIR", "data/intermediate")).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    os.environ["PIPELINE_OUTPUT_DIR"] = str(output_dir)

metrics_dir = Path(__file__).resolve().parents[2] / "data" / "metrics"
metrics_dir.mkdir(parents=True, exist_ok=True)
os.environ["PIPELINE_METRICS_DIR"] = str(metrics_dir)

if os.getenv("DEBUG_ENV_SNAPSHOT") == "1":
    print("\n=== DEBUG ENVIRONMENT SNAPSHOT ===")
    for key in ["PIPELINE_OUTPUT_DIR", "PIPELINE_METRICS_DIR", "PWD"]:
        print(f"{key} = {os.getenv(key)}")
    print(f"cwd = {os.getcwd()}")
    print("===============================\n")

# ================================================================
# Environment & Spark Initialization
# ================================================================
print("🔧 Loading environment configuration...")
ensure_local_env_defaults()
env = load_and_validate_env()

print("🚀 Initializing Spark session...")
spark = (
    SparkSession.builder
    .appName("UnguidedCapstone_Pipeline_Step8")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
configure_spark_from_env(spark)
spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark session active.\n")

# ================================================================
# Pipeline Configuration
# ================================================================
STEPS = [
    "scripts_spark.spark_extract_tmdb",
    "scripts_spark.spark_extract_discogs",
    "scripts_spark.spark_prepare_tmdb_input",
    "scripts_spark.spark_validate_schema_alignment",
    "scripts_spark.spark_match_and_enrich",
]

CHECKPOINT_FILE = output_dir / "pipeline_checkpoint.json"

# ================================================================
# Utility Functions
# ================================================================
def build_steps():
    """Dynamically import each step module and instantiate its Step class."""
    step_objs = []
    for mod_name in STEPS:
        module = import_module(mod_name)
        step_class = next(
            (getattr(module, c) for c in dir(module) if c.lower().startswith("step")),
            None,
        )
        if not step_class:
            print(f"⚠️ No Step class found in {mod_name}")
            continue
        try:
            obj = step_class(spark)
        except TypeError:
            obj = step_class()
        step_objs.append(obj)
    return step_objs


def save_checkpoint(step_name: str, success: bool = True):
    payload = {
        "last_step": step_name,
        "timestamp": datetime.now().isoformat(),
        "success": success,
    }
    CHECKPOINT_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def rollup_metrics(logger, step_times: dict):
    """Aggregate per-step metrics JSONs into one CSV."""
    metrics_files = list(metrics_dir.glob("*.json"))
    if not metrics_files:
        logger.warning("⚠️ No metrics JSONs found to roll up.")
        return

    rows = []
    for file in metrics_files:
        try:
            data = json.loads(file.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                name = data.get("step_name", file.stem)
                dur = step_times.get(name, data.get("duration_sec"))
                data["duration_sec"] = dur
                data.setdefault("step_runtime_sec", dur)
                rows.append(data)
        except Exception as e:
            logger.warning(f"⚠️ Skipping {file.name}: {e}")

    if not rows:
        return

    df = pd.DataFrame(rows)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.sort_values("timestamp")
    if "step_runtime_sec" in df.columns:
        df["pipeline_runtime_sec"] = df["step_runtime_sec"].cumsum()

    out_csv = metrics_dir / "pipeline_metrics.csv"
    df.to_csv(out_csv, index=False)
    logger.info(f"📊 Consolidated {len(df)} step metrics → {out_csv.name}")

# ================================================================
# Orchestration
# ================================================================
def main(resume_from: str | None = None):
    logger = setup_logger("Pipeline", log_dir=LOG_DIR)
    start_pipeline = time.time()

    logger.info("🚀 Starting TMDB → Discogs pipeline")
    logger.info(f"🔈 Log level: {LOG_LEVEL}")

    steps = build_steps()
    step_times = {}

    resume_mode = bool(resume_from)
    resume_triggered = not resume_mode
    resume_from_norm = resume_from.lower().strip() if resume_from else None

    for step in steps:
        step_name = step.__class__.__name__
        short_name = step_name.replace("Step", "").lower()

        if not resume_triggered:
            if resume_from_norm in step_name.lower() or resume_from_norm in short_name:
                resume_triggered = True
                logger.info(f"🔁 Resuming from {step_name}")
            else:
                logger.info(f"⏭️ Skipping {step_name} (before resume point)")
                continue

        logger.info(f"🚩 Running {step_name}")
        t0 = time.time()
        try:
            step.run()
            dur = round(time.time() - t0, 2)
            step_times[step_name] = dur
            logger.info(f"✅ Completed {step_name} in {dur:.2f}s")
            save_checkpoint(step_name, True)
        except Exception as e:
            dur = round(time.time() - t0, 2)
            step_times[step_name] = dur
            logger.error(f"❌ {step_name} failed after {dur:.2f}s: {e}")
            logger.debug(traceback.format_exc())
            save_checkpoint(step_name, False)
            logger.info("🛑 Pipeline halted due to failure.")
            break

    total_runtime = round(time.time() - start_pipeline, 2)
    logger.info(f"🏁 Total Runtime: {total_runtime:.2f}s")

    rollup_metrics(logger, step_times)
    logger.info("✅ Pipeline execution completed\n" + "-" * 60)

    try:
        spark.stop()
        logger.info("🧹 Spark session stopped.")
    except Exception:
        logger.warning("⚠️ Spark shutdown failed.")


# ================================================================
# CLI Entrypoint
# ================================================================
if __name__ == "__main__":
    main(resume_from=args.resume)
