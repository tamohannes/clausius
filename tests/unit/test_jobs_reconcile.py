"""Unit tests for stale active-row reconciliation in server/jobs.py."""

import pytest

from server.db import get_db
from server.jobs import _reconcile_stale_pinned_active_rows


def _insert_stale_pinned_row(cluster, job_id="123", state="PENDING"):
    con = get_db()
    con.execute(
        """INSERT INTO job_history
           (cluster, job_id, job_name, state, board_visible, submitted)
           VALUES (?, ?, ?, ?, 1, ?)""",
        (cluster, job_id, "stale-job", state, "2026-04-12T22:00:00"),
    )
    con.commit()
    con.close()


@pytest.mark.unit
def test_reconcile_stale_pinned_row_finalizes_from_sacct(db_path, mock_cluster, monkeypatch):
    _insert_stale_pinned_row(mock_cluster)
    monkeypatch.setattr(
        "server.jobs.sacct_final_batch",
        lambda cluster, job_ids: {
            "123": {
                "jobid": "123",
                "name": "stale-job",
                "state": "CANCELLED",
                "ended_at": "2026-04-12T22:53:27",
            }
        },
    )

    _reconcile_stale_pinned_active_rows(mock_cluster, set())

    con = get_db()
    row = con.execute(
        "SELECT state, board_visible, ended_at FROM job_history WHERE cluster=? AND job_id='123'",
        (mock_cluster,),
    ).fetchone()
    con.close()
    assert row["state"] == "CANCELLED"
    assert row["board_visible"] == 1
    assert row["ended_at"] == "2026-04-12T22:53:27"


@pytest.mark.unit
def test_reconcile_stale_pinned_row_hides_when_sacct_still_active(db_path, mock_cluster, monkeypatch):
    _insert_stale_pinned_row(mock_cluster)
    monkeypatch.setattr(
        "server.jobs.sacct_final_batch",
        lambda cluster, job_ids: {
            "123": {
                "jobid": "123",
                "name": "stale-job",
                "state": "PENDING",
            }
        },
    )

    _reconcile_stale_pinned_active_rows(mock_cluster, set())

    con = get_db()
    row = con.execute(
        "SELECT state, board_visible FROM job_history WHERE cluster=? AND job_id='123'",
        (mock_cluster,),
    ).fetchone()
    con.close()
    assert row["state"] == "PENDING"
    assert row["board_visible"] == 0
