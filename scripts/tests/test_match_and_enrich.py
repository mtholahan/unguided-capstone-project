import pytest
from pathlib import Path
from scripts_spark.spark_match_and_enrich import Step05MatchAndEnrichV2

@pytest.fixture
def mock_candidates(tmp_path):
    """Creates a small CSV with synthetic candidate pairs for matching."""
    import pandas as pd

    df = pd.DataFrame([
        {"tmdb_id": 1, "discogs_id": "A", "tmdb_title_norm": "The Matrix", "discogs_title_norm": "Matrix", "tmdb_year": 1999, "discogs_year": 1999},
        {"tmdb_id": 2, "discogs_id": "B", "tmdb_title_norm": "Inception", "discogs_title_norm": "Incepton", "tmdb_year": 2010, "discogs_year": 2011},
        {"tmdb_id": 3, "discogs_id": "C", "tmdb_title_norm": "Interstellar", "discogs_title_norm": "Interstellar", "tmdb_year": 2014, "discogs_year": 2019},
    ])
    path = tmp_path / "tmdb_discogs_candidates_extended.csv"
    df.to_csv(path, index=False)
    return path

def test_match_and_enrich_runs_successfully(monkeypatch, tmp_path, mock_candidates):
    """Runs Step05MatchAndEnrichV2 end-to-end on mock data."""
    step = Step05MatchAndEnrichV2()

    # Redirect all file outputs to tmp_path
    step.candidates_path = mock_candidates
    step.output_path = tmp_path / "output.csv"
    step.metrics_path = tmp_path / "metrics.json"
    step.histogram_path = tmp_path / "histogram.png"

    # Execute
    step.run()

    # Assertions
    assert step.output_path.exists(), "Expected output CSV not created."
    assert step.metrics_path.exists(), "Expected metrics JSON not created."

    import json
    with open(step.metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    # Basic metrics integrity check
    assert "total_candidates" in metrics
    assert metrics["total_candidates"] == 3
    assert metrics["total_matches"] <= 3
    assert metrics["step_runtime_sec"] > 0
