"""
Step 8 — Environment Validation Test (Refactored)
-------------------------------------------------
Confirms that verify_env.py executes successfully and confirms
key environment markers (Python, Spark, PATH integrity).
"""

import subprocess
import sys
from pathlib import Path
import pytest


@pytest.mark.smoke
def test_env_validation_runs_successfully():
    """Runs verify_env.py and asserts exit code 0 + confirmation text."""
    project_root = Path(__file__).resolve().parents[2]
    verify_script = project_root / "scripts" / "verify_env.py"

    result = subprocess.run(
        [sys.executable, str(verify_script)],
        capture_output=True,
        text=True
    )

    print(result.stdout)
    assert result.returncode == 0, f"verify_env failed:\n{result.stderr}"
    assert any(k in result.stdout for k in ["Environment verified", "✅", "All checks passed"]), \
        "Expected environment success marker not found."
