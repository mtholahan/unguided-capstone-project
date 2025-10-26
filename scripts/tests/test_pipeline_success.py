import subprocess
import sys
import pytest
from pathlib import Path

@pytest.mark.integration
def test_pipeline_completes_and_outputs(tmp_path):
    """Runs the main pipeline and checks output artifact creation."""
    result = subprocess.run(
        [sys.executable, "scripts/main.py", "--output-dir", str(tmp_path)],
        capture_output=True, text=True
    )
    print(result.stdout)
    assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
    outputs = list(tmp_path.glob("**/*"))
    assert any(p.suffix in (".csv", ".parquet", ".json") for p in outputs), \
        "No output data files created"
