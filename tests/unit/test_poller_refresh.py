"""Unit tests for explicit poller refresh behavior."""

import pytest

from server.poller import Poller


@pytest.mark.unit
def test_poll_now_updates_status_and_marks_change(mock_cluster, monkeypatch):
    poller = Poller()
    snapshots = iter([
        ("ok", "2026-04-13T00:00:00", frozenset()),
        ("ok", "2026-04-13T00:00:05", frozenset({("1", "RUNNING")})),
    ])

    monkeypatch.setattr(poller, "_snapshot_ids", lambda name: next(snapshots))
    monkeypatch.setattr(
        "server.jobs.poll_cluster",
        lambda cluster: {"status": "ok", "cluster": cluster, "updated": "2026-04-13T00:00:05"},
    )

    result = poller.poll_now(mock_cluster)
    status = poller.get_status()[mock_cluster]

    assert result["status"] == "ok"
    assert result["changed"] is True
    assert status["failure_count"] == 0
    assert status["last_duration_ms"] is not None
    assert status["view_state"] == "live"
