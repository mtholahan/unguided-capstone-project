import pytest
from scripts.spark.step05_match_and_enrich import match_and_enrich_records

@pytest.fixture
def sample_data():
    return [
        {"id": 1, "name": "John", "city": "NY"},
        {"id": 1, "name": "Jon", "city": "NYC"}
    ]

def test_string_matching_behavior(sample_data):
    """Checks fuzzy/standard match logic on sample input."""
    enriched = match_and_enrich_records(sample_data)
    assert isinstance(enriched, list)
    assert all("match_score" in rec for rec in enriched)
    # Validate edge behavior
    scores = [rec["match_score"] for rec in enriched]
    assert min(scores) >= 0 and max(scores) <= 1
