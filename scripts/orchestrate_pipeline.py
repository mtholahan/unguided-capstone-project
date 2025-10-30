#!/usr/bin/env python3
"""
Unguided Capstone Orchestrator
Runs Spark-based pipeline steps sequentially or selectively.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

# ================================================================
# 0️⃣ Load .env once for all steps
# ================================================================
from scripts.config_env import load_and_validate_env
load_and_validate_env()

# ================================================================
# 1️⃣ Step registry (ordered)
# ================================================================
PIPELINE_STEPS = [
    ("step_01_acquire_tmdb",        "scripts_spark/spark_extract_tmdb.py"),
    ("step_02_acquire_discogs",     "scripts_spark/spark_extract_discogs.py"),
    ("step_03_prepare_tmdb_input",  "scripts_spark/spark_prepare_tmdb_input.py"),
    ("step_04_validate_alignment",  "scripts_spark/spark_validate_schema_alignment.py"),
    ("step_05_match_and_enrich",    "scripts_spark/spark_match_and_enrich.py"),
]

# ================================================================
# 2️⃣ Utility
# ================================================================
def run_step(step_name: str, script_path: str):
    """Run one pipeline step in a subprocess."""
    print(f"\n🚀 Running {step_name} ...")
    try:
        subprocess.run([sys.executable, script_path], check=True)
        print(f"✅ Completed {step_name}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Step failed: {step_name}")
        sys.exit(e.returncode)

# ================================================================
# 3️⃣ CLI Argument Parsing
# ================================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Unguided Capstone Unified Orchestrator")
    parser.add_argument("--resume", type=str, help="Resume from this step onward", default=None)
    parser.add_argument("--run-once", type=str, help="Run a single step in isolation", default=None)
    return parser.parse_args()

# ================================================================
# 4️⃣ Main execution
# ================================================================
def main():
    args = parse_args()
    steps_to_run = PIPELINE_STEPS

    if args.run_once:
        steps_to_run = [s for s in PIPELINE_STEPS if s[0] == args.run_once]
        if not steps_to_run:
            print(f"⚠️ Unknown step: {args.run_once}")
            sys.exit(1)

    elif args.resume:
        found = False
        for i, (name, _) in enumerate(PIPELINE_STEPS):
            if name == args.resume:
                steps_to_run = PIPELINE_STEPS[i:]
                found = True
                break
        if not found:
            print(f"⚠️ Unknown resume point: {args.resume}")
            sys.exit(1)

    # Run steps sequentially
    for step_name, script_path in steps_to_run:
        run_step(step_name, script_path)

    print("\n🎉 Pipeline complete.\n")


if __name__ == "__main__":
    main()
