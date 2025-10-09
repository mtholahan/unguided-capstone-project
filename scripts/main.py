"""
main.py — Discogs → TMDB Pipeline Orchestrator (v3)
---------------------------------------------------
Executes all pipeline steps sequentially.
Supports resume, metrics, and checkpointing.
"""

import time
import json
import traceback
from datetime import datetime
from pathlib import Path
from importlib import import_module
from dotenv import load_dotenv
import pandas as pd
import os

from base_step import setup_logger
from config import LOG_LEVEL, DATA_DIR

# -------------------------------------------------------------------------
# Load .env automatically
# -------------------------------------------------------------------------
load_dotenv()

# -------------------------------------------------------------------------
# Pipeline configuration
# -------------------------------------------------------------------------
STEPS = [
    "step_01_acquire_discogs",
    # "step_02_fetch_tmdb",
    # "step_03_prepare_tmdb_input",
    # "step_04_match_instrumented",
    # "step_05_apply_rescues",
    # "step_06_enrich_tmdb",
    # "step_07_coverage_audit",
]

CHECKPOINT_FILE = Path(DATA_DIR) / "pipeline_checkpoint.json"
METRICS_DIR = Path(DATA_DIR) / "metrics"
METRICS_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------------------
# Dynamic step loader
# -------------------------------------------------------------------------
def build_steps():
    """Import all modules dynamically and instantiate step classes."""
    step_objs = []
    for mod_name in STEPS:
        module = import_module(mod_name)
        step_class = next(
            (getattr(module, c) for c in dir(module) if c.lower().startswith("step")), None
        )
        if step_class:
            step_objs.append(step_class())
    return step_objs


# -------------------------------------------------------------------------
# Checkpoint & metrics helpers
# -------------------------------------------------------------------------
def save_checkpoint(step_name, success=True):
    data = {"last_step": step_name, "timestamp": datetime.now().isoformat(), "success": success}
    CHECKPOINT_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def rollup_metrics(logger, step_times):
    """Consolidate per-step metrics into one CSV."""
    rows = []
    for f in METRICS_DIR.glob("step*.json"):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            data["step"] = f.stem
            data["duration_sec"] = step_times.get(f.stem, None)
            rows.append(data)
        except Exception as e:
            logger.warning(f"⚠️ Skipping metrics file {f.name}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        out_path = METRICS_DIR / "pipeline_metrics.csv"
        df.to_csv(out_path, index=False)
        logger.info(f"📊 Consolidated {len(df)} step metrics → {out_path.name}")


# -------------------------------------------------------------------------
# Orchestration Entry Point
# -------------------------------------------------------------------------
def main(resume_from=None):
    logger = setup_logger("Pipeline")
    start_all = time.time()

    logger.info("🚀 Starting Discogs→TMDB Pipeline")
    logger.info(f"📂 Data dir: {DATA_DIR}")
    logger.info(f"🔧 Log level: {LOG_LEVEL}")

    step_times = {}
    steps = build_steps()

    resume_mode = bool(resume_from)
    resume_from_norm = resume_from.lower().strip() if resume_from else None
    resume_triggered = not resume_mode  # start from beginning if no resume flag

    for step in steps:
        step_name = step.__class__.__name__
        short_name = step_name.replace("Step", "").lower()

        # Resume control
        if not resume_triggered:
            if resume_from_norm in step_name.lower() or resume_from_norm in short_name:
                resume_triggered = True
                logger.info(f"🔁 Resuming from: {step_name}")
            else:
                logger.info(f"⏭️ Skipping {step_name} (before resume point)")
                continue

        # Run step
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
            logger.error(f"❌ Step {step_name} failed after {duration:.2f}s: {e}")
            logger.debug(traceback.format_exc())
            save_checkpoint(step_name, success=False)
            break

    # Summary
    total_runtime = round(time.time() - start_all, 2)
    logger.info("🧾 Pipeline Summary")
    for step, duration in step_times.items():
        logger.info(f"   ⏱ {step:<35} {duration:>6.2f}s")
    logger.info(f"🏁 Total Runtime: {total_runtime:.2f}s")

    rollup_metrics(logger, step_times)
    logger.info("✅ Pipeline completed successfully")
    logger.info("-" * 60)


# -------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the Discogs→TMDB ETL pipeline")
    parser.add_argument("--resume", type=str, default=None, help="Step name to resume from")
    args = parser.parse_args()

    main(resume_from=args.resume)
