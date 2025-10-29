import subprocess
import sys
import os
from pathlib import Path

def test_env_validation_runs_successfully():
    """Runs verify_env.py and asserts environment variables are valid."""
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"

    # Ensure .env exists before test
    assert env_path.exists(), f".env file not found at {env_path}"

    result = subprocess.run(
        [sys.executable, "scripts/verify_env.py"],
        cwd=project_root,
        capture_output=True,
        text=True
    )

    print(result.stdout)
    assert result.returncode == 0, f"verify_env failed: {result.stderr}"
    assert "Environment verified" in result.stdout or "✅" in result.stdout
