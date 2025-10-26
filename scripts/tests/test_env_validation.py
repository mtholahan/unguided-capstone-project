import subprocess
import sys
import pytest

def test_env_validation_runs_successfully():
    """Runs verify_env.py and asserts exit code 0."""
    result = subprocess.run(
        [sys.executable, "scripts/verify_env.py"],
        capture_output=True, text=True
    )
    print(result.stdout)
    assert result.returncode == 0, f"verify_env failed: {result.stderr}"
    # optional sanity check for known keywords
    assert "Python" in result.stdout or "packages" in result.stdout
