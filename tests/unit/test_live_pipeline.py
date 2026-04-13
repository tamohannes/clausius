"""Unit tests for the simplified live polling pipeline."""

import pytest

from server.jobs import poll_cluster


@pytest.mark.unit
def test_poll_cluster_writes_live_then_queues_bookkeeping(mock_cluster, monkeypatch):
    order = []
    queued = {}

    def fake_fetch(cluster):
        assert cluster == mock_cluster
        return {
            "status": "ok",
            "jobs": [{
                "jobid": "1",
                "name": "demo-job",
                "state": "RUNNING",
                "dependency": "",
                "submitted": "2026-04-13T00:00:00",
            }],
            "updated": "2026-04-13T00:00:01",
        }

    def fake_queue(cluster, context):
        order.append("queue")
        queued["cluster"] = cluster
        queued["context"] = context

    monkeypatch.setattr("server.jobs.fetch_cluster_data", fake_fetch)
    monkeypatch.setattr("server.jobs._enrich_missing_gres", lambda cluster, jobs: order.append("enrich"))
    monkeypatch.setattr("server.jobs.replace_live_jobs", lambda cluster, jobs: order.append("replace"))
    monkeypatch.setattr("server.jobs.set_cluster_state", lambda cluster, status, updated, last_error=None: order.append("state"))
    monkeypatch.setattr("server.jobs._schedule_cluster_bookkeeping", fake_queue)
    monkeypatch.setattr("server.jobs._schedule_softfail_migration", lambda: order.append("softfail"))

    result = poll_cluster(mock_cluster)

    assert result["status"] == "ok"
    assert result["bookkeeping"] == "queued"
    assert queued["cluster"] == mock_cluster
    assert queued["context"]["current_ids"] == {"1"}
    assert queued["context"]["prev_ids"] == set()
    assert order == ["enrich", "replace", "state", "queue"]


@pytest.mark.unit
def test_poll_cluster_keeps_live_snapshot_and_reports_error(mock_cluster, monkeypatch):
    from server import config

    with config._cache_lock:
        config._cache[mock_cluster] = {
            "status": "ok",
            "jobs": [{"jobid": "1", "name": "demo-job", "state": "RUNNING"}],
            "updated": "2026-04-13T00:00:00",
        }

    calls = []
    monkeypatch.setattr(
        "server.jobs.fetch_cluster_data",
        lambda cluster: {
            "status": "error",
            "error": "ssh failed",
            "jobs": [],
            "updated": "2026-04-13T00:00:30",
        },
    )
    monkeypatch.setattr(
        "server.jobs.set_cluster_state",
        lambda cluster, status, updated, last_error=None: calls.append(
            (cluster, status, updated, last_error)
        ),
    )
    monkeypatch.setattr("server.jobs._schedule_cluster_bookkeeping", lambda *args, **kwargs: pytest.fail("bookkeeping should not run on fetch error"))

    result = poll_cluster(mock_cluster)

    assert result["status"] == "error"
    assert result["error"] == "ssh failed"
    assert calls == [(mock_cluster, "ok", "2026-04-13T00:00:30", "ssh failed")]
