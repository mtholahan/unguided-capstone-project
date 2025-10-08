"""
main.py — Pipeline Orchestrator (Refactored & Polished)
-------------------------------------------------------
Executes all pipeline Steps sequentially (03B → 10B).
Handles logging, timing, resume capability, and final
metrics rollup into pipeline_metrics.csv.
"""

import time, json, traceback
from datetime import datetime
from pathlib import Path
import pandas as pd
from config import DEBUG_MODE, ROW_LIMIT
from base_step import setup_logger
from importlib import import_module

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------
STEPS = [
    "step_03b_rehydrate_guids",
    "step_04_mb_full_join",
    "step_05_filter_soundtracks_enhanced",
    "step_06_fetch_tmdb",
    "step_07_prepare_tmdb_input",
    "step_08_match_instrumented",
    "step_09_apply_rescues",
    "step_10_enrich_tmdb",
    "step_10b_coverage_audit",
]
CHECKPOINT_FILE = Path("D:/Capstone_Staging/pipeline_checkpoint.json")
METRICS_DIR = Path("D:/Capstone_Staging/metrics")
METRICS_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------------------
# Build step objects dynamically
# -------------------------------------------------------------------------
def build_steps():
    step_objs = []
    for mod_name in STEPS:
        module = import_module(mod_name)
        cls_name = "".join([s.capitalize() for s in mod_name.split("_")[1:]])
        step_class = next(
            (getattr(module, c) for c in dir(module) if c.lower().startswith("step")), None
        )
        if step_class:
            step_objs.append(step_class())
    return step_objs


# -------------------------------------------------------------------------
# Helper: checkpoint + metrics rollup
# -------------------------------------------------------------------------
def save_checkpoint(step_name, success=True):
    data = {"last_step": step_name, "timestamp": datetime.now().isoformat(), "success": success}
    CHECKPOINT_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def rollup_metrics(logger, step_times):
    """Aggregate all step JSON metrics into pipeline_metrics.csv"""
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
# Main orchestration
# -------------------------------------------------------------------------
def main(resume_from=None):
    logger = setup_logger("Pipeline")
    start_all = time.time()

    logger.info("🚀 Starting Full Pipeline Run")
    logger.info(f"🧭 Debug Mode: {DEBUG_MODE}")
    logger.info(f"📏 Row Limit: {ROW_LIMIT or '∞'}")
    logger.info(f"📂 Metrics Directory: {METRICS_DIR}")
    if resume_from:
        logger.info(f"🔁 Resuming from step: {resume_from}")

    step_times = {}
    steps = build_steps()

    resume_mode = bool(resume_from)
    resume_from_norm = resume_from.lower().strip() if resume_from else None
    resume_triggered = False

    for step in steps:
        step_name = step.__class__.__name__
        short_name = step_name.replace("Step", "").strip().lower()

        # Resume logic
        if resume_mode and not resume_triggered:
            if resume_from_norm in step_name.lower() or resume_from_norm in short_name:
                resume_triggered = True
                logger.info(f"🔁 Resuming pipeline from: {step_name}")
            else:
                logger.info(f"⏭️ Skipping {step_name} (before resume point)")
                continue

        # Execute step
        logger.info(f"🚩 Running {step_name}")
        t0 = time.time()
        try:
            step.run()
            duration = round(time.time() - t0, 2)
            step_times[f"step{step_name.split(':')[0][-2:]}"] = duration
            logger.info(f"✅ Completed {step_name} in {duration:.2f}s")
            save_checkpoint(step_name, success=True)
        except Exception as e:
            duration = round(time.time() - t0, 2)
            logger.error(f"❌ Step {step_name} failed after {duration:.2f}s: {e}")
            logger.debug(traceback.format_exc())
            save_checkpoint(step_name, success=False)
            break

    # ---------------------------------------------------------------------
    # Summary + Metrics rollup
    # ---------------------------------------------------------------------
    total_runtime = round(time.time() - start_all, 2)
    logger.info("🧾 Pipeline Summary")
    for step, duration in step_times.items():
        logger.info(f"   ⏱ {step:<25} {duration:>6.2f}s")
    logger.info(f"🏁 Total Runtime: {total_runtime:.2f}s")

    rollup_metrics(logger, step_times)
    logger.info("✅ Pipeline completed successfully (refactored main.py)")
    logger.info("-" * 60)


# -------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the ETL pipeline orchestrator")
    parser.add_argument("--resume", type=str, default=None, help="Step name to resume from")
    args = parser.parse_args()

    main(resume_from=args.resume)
