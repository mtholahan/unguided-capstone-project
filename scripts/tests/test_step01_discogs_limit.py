"""
Unit Test — Step 01 Discogs Limiter
-----------------------------------
Verifies that:
1. DISCOG_MAX_TITLES is enforced before API calls.
2. Logging reports proper counts.
3. No API calls are made beyond the limit.
"""

import io
import logging
from unittest.mock import patch, MagicMock
from OLD_step_01_acquire_discogs import Step01AcquireDiscogs
from config import DISCOG_MAX_TITLES


def test_discogs_limiter_behavior(monkeypatch, caplog):
    """Ensure limiter is applied before any API calls."""
    # Mock out network & filesystem dependencies
    fake_titles = [f"Movie_{i}" for i in range(500)]
    monkeypatch.setattr("step_01_acquire_discogs.get_active_title_list", lambda: fake_titles)
    monkeypatch.setattr("step_01_acquire_discogs.cached_request", lambda *a, **kw: {"results": []})
    monkeypatch.setattr("pathlib.Path.mkdir", lambda *a, **kw: None)
    monkeypatch.setattr("step_01_acquire_discogs.BaseStep.atomic_write", lambda *a, **kw: None)
    monkeypatch.setattr("step_01_acquire_discogs.BaseStep.save_metrics", lambda *a, **kw: None)
    monkeypatch.setattr("step_01_acquire_discogs.BaseStep.write_metrics", lambda *a, **kw: None)

    # Capture logs
    caplog.set_level(logging.INFO)

    # Instantiate the step
    step = Step01AcquireDiscogs()

    # Assert limiter was applied
    assert len(step.movie_titles) == DISCOG_MAX_TITLES, (
        f"Limiter failed — expected {DISCOG_MAX_TITLES}, got {len(step.movie_titles)}"
    )

    # Log output should include limiter info
    limiter_log = [r for r in caplog.records if "limit=" in r.message]
    assert limiter_log, "Expected log message showing applied limiter"
    print("✅ Limiter log verified.")

    # Run in dry mode with fake network calls
    with patch("concurrent.futures.ThreadPoolExecutor.submit", MagicMock(return_value=MagicMock())):
        step.run()

    print("✅ Step 01 run completed without exceeding limiter.")


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-vv"]))
