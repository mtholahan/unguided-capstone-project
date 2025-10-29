import json
from pathlib import Path
from scripts_spark.spark_match_and_enrich import Step05MatchAndEnrichV2

def test_match_and_enrich_runs_successfully(monkeypatch, tmp_path, mock_candidates):
    """Runs Step05MatchAndEnrich end-to-end on mock data."""
    step = Step05MatchAndEnrichV2()

    project_root = Path(__file__).resolve().parents[2]
    metrics_dir = project_root / "data" / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    # Redirect file outputs to project metrics dir
    step.candidates_path = mock_candidates
    step.output_path = metrics_dir / "step05_output.csv"
    step.metrics_path = metrics_dir / "step05_metrics.json"
    step.histogram_path = metrics_dir / "step05_histogram.png"

    step.run()

    assert step.output_path.exists(), f"Expected output CSV not created at {step.output_path}"
    assert step.metrics_path.exists(), f"Expected metrics JSON not created at {step.metrics_path}"

    with open(step.metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    assert "total_candidates" in metrics
    assert metrics["total_candidates"] == 3
    assert "total_matches" in metrics
    assert metrics["total_matches"] <= 3
    assert "step_runtime_sec" in metrics and metrics["step_runtime_sec"] > 0
