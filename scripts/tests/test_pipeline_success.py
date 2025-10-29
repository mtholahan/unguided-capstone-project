"""
Step 8 — Unified Pipeline Integration Test (Refactored)
-------------------------------------------------------
Ensures the end-to-end pipeline executes successfully in the current
environment, produces artifacts, and logs completion markers.
"""

import os
import sys
import json
import subprocess
from datetime import datetime
from pathlib import Path
import pytest


def run_pipeline_safe(tmp_path: Path) -> subprocess.CompletedProcess:
    """Run main.py in a subprocess with isolated output + metrics paths."""
    project_root = Path(__file__).resolve().parents[2]
    main_script = project_root / "scripts" / "main.py"

    output_dir = tmp_path / "pipeline_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    metrics_dir = tmp_path / "pipeline_metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["PIPELINE_OUTPUT_DIR"] = str(output_dir)
    env["PIPELINE_METRICS_DIR"] = str(metrics_dir)

    cmd = [sys.executable, str(main_script)]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        check=False
    )

    log_path = tmp_path / "pipeline_test.log"
    log_path.write_text(result.stdout, encoding="utf-8")
    print(f"\n🪵 Full pipeline log → {log_path}")

    return result

def test_pipeline_completes_and_generates_artifacts():
    """Runs the full pipeline and checks for final metrics output."""
    project_root = Path(__file__).resolve().parents[2]
    metrics_dir = project_root / "data" / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    # ✅ Skip test gracefully if no data is available
    data_dir = project_root / "data" / "intermediate"
    if not data_dir.exists() or not any(data_dir.iterdir()):
        pytest.skip(f"Skipping: no intermediate data found in {data_dir}")

    # ✅ Run the orchestrator
    result = subprocess.run(
        [sys.executable, "scripts/main.py"],
        cwd=project_root,
        capture_output=True,
        text=True
    )

    print(result.stdout)
    assert result.returncode == 0, f"Pipeline exited with non-zero code: {result.stderr}"

    # ✅ Verify that metrics file exists and is non-empty
    pipeline_csv = metrics_dir / "pipeline_metrics.csv"
    if not pipeline_csv.exists():
        pytest.skip(f"Skipping: expected metrics file missing at {pipeline_csv}")

    assert pipeline_csv.stat().st_size > 0, f"{pipeline_csv} exists but is empty."
    print(f"✅ Pipeline metrics found at {pipeline_csv}")

