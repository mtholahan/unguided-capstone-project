import json
from pathlib import Path
from scripts_spark.spark_match_and_enrich import Step05MatchAndEnrichV2

def test_match_and_enrich_runs_successfully(monkeypatch, tmp_path):
    """Runs Step05MatchAndEnrich end-to-end."""
    step = Step05MatchAndEnrichV2()

    project_root = Path(__file__).resolve().parents[2]
    metrics_dir = project_root / "data" / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    # Use actual input data or skip if not present
    candidates_file = project_root / "data" / "intermediate" / "matched_candidates.csv"
    if not candidates_file.exists():
        pytest.skip(f"Skipping: required candidates file not found at {candidates_file}")

    step.candidates_path = candidates_file
    step.output_path = metrics_dir / "step05_output.csv"
    step.metrics_path = metrics_dir / "step05_metrics.json"
    step.histogram_path = metrics_dir / "step05_histogram.png"

    step.run()

    assert step.output_path.exists(), f"Expected output CSV not created at {step.output_path}"
    assert step.metrics_path.exists(), f"Expected metrics JSON not created at {step.metrics_path}"

    with open(step.metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    assert "total_candidates" in metrics
    assert metrics["total_matches"] <= metrics["total_candidates"]
    assert metrics["step_runtime_sec"] > 0
