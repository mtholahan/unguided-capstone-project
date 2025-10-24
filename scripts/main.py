"""
main.py — Discogs → TMDB Unified Pipeline Orchestrator (v5)
------------------------------------------------------------
Step 8: Deploy for Testing

Orchestrates Spark-based pipeline steps with checkpointing,
logging, and metrics roll-up.  Integrates .env and Azure
configuration through scripts.config_env.

Author:  Mark Holahan
Mentor:  Akhil
Version: v5  (Oct 2025)
"""

import json
import time
import traceback
import pandas as pd
from datetime import datetime
from pathlib import Path
from importlib import import_module
from pyspark.sql import SparkSession

from scripts.base_step import setup_logger
from scripts.config import LOG_LEVEL, DATA_DIR, LOG_DIR
from scripts.config_env import configure_spark_from_env, load_and_validate_env

# ================================================================
# Environment & Spark Initialization
# ================================================================
print("🔧 Loading environment configuration...")
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
print("✅ Spark session active.\n")

# ================================================================
# Pipeline Configuration
# ================================================================
STEPS = [
    # --- Spark refactored modules ---
    "scripts_spark.extract_spark_tmdb",
    "scripts_spark.extract_spark_discogs",
    "scripts_spark.prepare_spark_tmdb_input",

    # --- Legacy Python steps (still valid) ---
    #"scripts.step_04_validate_schema_alignment",
    #"scripts.step_05_match_and_enrich",
]

CHECKPOINT_FILE = Path(DATA_DIR) / "pipeline_checkpoint.json"
METRICS_DIR = Path(DATA_DIR) / "metrics"
METRICS_DIR.mkdir(parents=True, exist_ok=True)

# ================================================================
# Utility Functions
# ================================================================
def build_steps():
    """
    Dynamically import each step module and instantiate its BaseStep subclass.
    Pass SparkSession if constructor accepts it.
    """
    step_objs = []
    for mod_name in STEPS:
        module = import_module(mod_name)
        step_class = next(
            (getattr(module, c) for c in dir(module)
             if c.lower().startswith("step")), None
        )
        if not step_class:
            print(f"⚠️ No Step class found in {mod_name}")
            continue

        # Inspect constructor args to decide whether to pass Spark
        try:
            obj = step_class(spark)
        except TypeError:
            obj = step_class()
        step_objs.append(obj)
    return step_objs


def save_checkpoint(step_name: str, success: bool = True):
    """Write a checkpoint after each step."""
    payload = {
        "last_step": step_name,
        "timestamp": datetime.now().isoformat(),
        "success": success,
    }
    CHECKPOINT_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ================================================================
# Metrics Roll-Up (unchanged)
# ================================================================
def rollup_metrics(logger, step_times: dict):
    import json
    from pathlib import Path
    from scripts.config import DATA_DIR

    metrics_dir = Path(DATA_DIR) / "metrics"
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
            logger.warning(f"⚠️ Skipping metrics file {file.name}: {e}")

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

    logger.info("🚀 Starting Spark-enabled Discogs → TMDB pipeline")
    logger.info(f"🔧 Environment branch: {env.get('BRANCH', 'unknown')}")
    logger.info(f"📂 Data directory: {DATA_DIR}")
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

    spark.stop()
    logger.info("🧹 Spark session stopped.")

# ================================================================
# CLI Entrypoint
# ================================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the Discogs→TMDB Spark pipeline")
    parser.add_argument("--resume", type=str, help="Resume from specified step name")
    args = parser.parse_args()
    main(resume_from=args.resume)
