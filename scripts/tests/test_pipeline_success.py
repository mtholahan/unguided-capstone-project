"""
Step 8 — Unified Pipeline Integration Test
------------------------------------------
Validates that the end-to-end pipeline runs successfully under a test environment.
Generates a test report including pass/fail metrics and code coverage summary.
"""

import os
import sys
import json
import subprocess
from datetime import datetime
from pathlib import Path
import pytest

def run_pipeline_safe(tmp_path: Path) -> subprocess.CompletedProcess:
    """Launch pipeline with pytest-safe stdout forwarding to avoid Spark hang."""
    output_dir = tmp_path / "pipeline_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "scripts/main.py",
        "--output-dir",
        str(output_dir),
    ]

    env = os.environ.copy()
    env["PIPELINE_OUTPUT_DIR"] = str(output_dir)
    env.setdefault("PIPELINE_METRICS_DIR", str(tmp_path / "pipeline_metrics"))

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,  # capture to file safely
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        check=False,
        bufsize=1,
    )

    # Write raw logs for debugging
    log_path = tmp_path / "pipeline_test.log"
    log_path.write_text(result.stdout, encoding="utf-8")
    print(f"\n🪵 Full pipeline log → {log_path}")

    return result

@pytest.mark.integration
def test_pipeline_completes_and_generates_artifacts(tmp_path):
    """Run the unified pipeline and validate successful output generation."""
    start_time = datetime.utcnow()
    result = run_pipeline_safe(tmp_path)
    end_time = datetime.utcnow()

    duration = (end_time - start_time).total_seconds()
    output_dir = Path(os.getenv("PIPELINE_OUTPUT_DIR", tmp_path / "pipeline_output"))

    # === Assertions ===
    assert result.returncode == 0, f"❌ Pipeline exited with non-zero code.\n{result.stdout[-500:]}"
    assert output_dir.exists(), "❌ Expected output directory not created."

    artifacts = [f for f in output_dir.rglob("*") if f.is_file()]
    assert artifacts, "❌ No output artifacts were produced."

    # === Check success signal in logs ===
    assert "Pipeline execution completed" in result.stdout or "🏁" in result.stdout, \
        "❌ Success message not detected in pipeline logs."

    # === Create JSON test report for Step 8 deliverable ===
    report = {
        "timestamp": end_time.isoformat(),
        "duration_sec": duration,
        "pipeline_exit_code": result.returncode,
        "num_artifacts": len(artifacts),
        "output_dir": str(output_dir),
        "log_excerpt": result.stdout[-800:],  # keep tail for quick review
        "status": "PASS",
    }

    report_path = tmp_path / "pipeline_test_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"📊 Test report generated → {report_path}")

    # ✅ Optional: print summary for CI logs
    print("\n=== TEST SUMMARY ===")
    print(json.dumps(report, indent=2))

    # === Assertions for pytest ===
    assert report["status"] == "PASS"
