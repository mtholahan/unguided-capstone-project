"""
main.py — Discogs → TMDB Unified Pipeline Orchestrator (v4)
------------------------------------------------------------
Executes all BaseStep subclasses sequentially with checkpointing, 
logging, and metric rollup. Compatible with new class-based 
step modules (e.g., Step01AcquireDiscogs, Step02FetchTMDB).

Version:
    v4 – Oct 2025

Author:
    Mark Holahan
"""

import json
import time
import traceback
import pandas as pd
from datetime import datetime
from pathlib import Path
from importlib import import_module
from dotenv import load_dotenv

from base_step import setup_logger
from config import LOG_LEVEL, DATA_DIR, LOG_DIR

# ================================================================
# Environment Initialization
# ================================================================
load_dotenv()

# ================================================================
# Pipeline Configuration
# ================================================================
STEPS = [
    "step_01_acquire_discogs",
    "step_02_fetch_tmdb",
    "step_03_prepare_tmdb_input",
    "step_04_match_discogs_tmdb",
     # extend when ready
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
    Assumes exactly one class per module beginning with 'Step'.
    """
    step_objs = []
    for mod_name in STEPS:
        module = import_module(mod_name)
        step_class = next(
            (getattr(module, c) for c in dir(module)
             if c.lower().startswith("step")), None
        )
        if step_class:
            step_objs.append(step_class())
        else:
            print(f"⚠️ No Step class found in module: {mod_name}")
    return step_objs


def save_checkpoint(step_name: str, success: bool = True):
    """Write a checkpoint after each step (resumable pipeline)."""
    payload = {
        "last_step": step_name,
        "timestamp": datetime.now().isoformat(),
        "success": success,
    }
    CHECKPOINT_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def rollup_metrics(logger, step_times: dict):
    """Aggregate JSON metrics from each step into a single CSV summary."""
    rows = []
    for file in METRICS_DIR.glob("*.json"):
        try:
            with open(file, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                data["step"] = file.stem
                data["duration_sec"] = step_times.get(data["step"], None)
                rows.append(data)
        except Exception as e:
            logger.warning(f"⚠️ Skipping metrics file {file.name}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        out_csv = METRICS_DIR / "pipeline_metrics.csv"
        df.to_csv(out_csv, index=False)
        logger.info(f"📊 Consolidated {len(df)} step metrics → {out_csv.name}")


# ================================================================
# Orchestration Logic
# ================================================================
def main(resume_from: str | None = None):
    logger = setup_logger("Pipeline", log_dir=LOG_DIR)
    start_pipeline = time.time()

    logger.info("🚀 Starting Discogs → TMDB ETL Pipeline")
    logger.info(f"📂 Data Directory: {DATA_DIR}")
    logger.info(f"🔧 Log Level: {LOG_LEVEL}")

    steps = build_steps()
    step_times = {}

    resume_mode = bool(resume_from)
    resume_triggered = not resume_mode
    resume_from_norm = resume_from.lower().strip() if resume_from else None

    for step in steps:
        step_name = step.__class__.__name__
        short_name = step_name.replace("Step", "").lower()

        # Resume control
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
            duration = round(time.time() - t0, 2)
            step_times[step_name] = duration
            logger.info(f"✅ Completed {step_name} in {duration:.2f}s")
            save_checkpoint(step_name, success=True)
        except Exception as e:
            duration = round(time.time() - t0, 2)
            step_times[step_name] = duration
            logger.error(f"❌ {step_name} failed after {duration:.2f}s: {e}")
            logger.debug(traceback.format_exc())
            save_checkpoint(step_name, success=False)
            logger.info("🛑 Pipeline halted due to failure.")
            break

    # Summary Report
    total_runtime = round(time.time() - start_pipeline, 2)
    logger.info("🧾 Pipeline Summary:")
    for step, duration in step_times.items():
        logger.info(f"   ⏱ {step:<35} {duration:>6.2f}s")
    logger.info(f"🏁 Total Runtime: {total_runtime:.2f}s")

    rollup_metrics(logger, step_times)
    logger.info("✅ Pipeline execution completed")
    logger.info("-" * 60)


# ================================================================
# CLI Entrypoint
# ================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the Discogs→TMDB pipeline")
    parser.add_argument("--resume", type=str, help="Resume from specified step name")
    args = parser.parse_args()

    main(resume_from=args.resume)
