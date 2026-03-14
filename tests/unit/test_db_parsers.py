"""Unit tests for server/db.py parser and normalization functions."""

import pytest
from datetime import datetime, timedelta

from server.db import (
    parse_slurm_elapsed_seconds,
    parse_dt_maybe,
    normalize_job_times_local,
    _infer_parent_from_name,
)


# ── parse_slurm_elapsed_seconds ─────────────────────────────────────────────

class TestParseSlurmElapsedSeconds:
    @pytest.mark.unit
    @pytest.mark.parametrize("elapsed, expected", [
        ("05:30", 330),
        ("00:00", 0),
        ("59:59", 3599),
        ("1:05:30", 3930),
        ("10:00:00", 36000),
        ("0:00:01", 1),
        ("1-00:00:00", 86400),
        ("2-12:30:45", 2 * 86400 + 12 * 3600 + 30 * 60 + 45),
        ("0-00:05:00", 300),
    ])
    def test_valid_formats(self, elapsed, expected):
        assert parse_slurm_elapsed_seconds(elapsed) == expected

    @pytest.mark.unit
    @pytest.mark.parametrize("elapsed", [
        None, "", "—", "N/A", "Unknown",
    ])
    def test_sentinel_values_return_none(self, elapsed):
        assert parse_slurm_elapsed_seconds(elapsed) is None

    @pytest.mark.unit
    @pytest.mark.parametrize("elapsed", [
        "abc", "12", ":", "1:2:3:4", "foo-bar",
    ])
    def test_malformed_returns_none(self, elapsed):
        assert parse_slurm_elapsed_seconds(elapsed) is None

    @pytest.mark.unit
    def test_whitespace_stripped(self):
        assert parse_slurm_elapsed_seconds("  05:30  ") == 330


# ── parse_dt_maybe ──────────────────────────────────────────────────────────

class TestParseDtMaybe:
    @pytest.mark.unit
    def test_iso_format_with_t(self):
        result = parse_dt_maybe("2026-03-09T13:22:55")
        assert result == datetime(2026, 3, 9, 13, 22, 55)

    @pytest.mark.unit
    def test_iso_format_with_space(self):
        result = parse_dt_maybe("2026-03-09 13:22:55")
        assert result == datetime(2026, 3, 9, 13, 22, 55)

    @pytest.mark.unit
    @pytest.mark.parametrize("value", [
        None, "", "Unknown", "N/A", "—", "None",
    ])
    def test_sentinel_values_return_none(self, value):
        assert parse_dt_maybe(value) is None

    @pytest.mark.unit
    def test_garbage_returns_none(self):
        assert parse_dt_maybe("not-a-date") is None

    @pytest.mark.unit
    def test_numeric_passthrough(self):
        assert parse_dt_maybe(12345) is None


# ── normalize_job_times_local ────────────────────────────────────────────────

class TestNormalizeJobTimesLocal:
    @pytest.mark.unit
    def test_pending_uses_submitted(self):
        job = {"state": "PENDING", "submitted": "2026-03-09T10:00:00"}
        result = normalize_job_times_local(job)
        assert result["started_local"] == "2026-03-09T10:00:00"
        assert result["ended_local"] == ""

    @pytest.mark.unit
    def test_pending_no_submitted(self):
        result = normalize_job_times_local({"state": "PENDING"})
        assert result["started_local"] == ""
        assert result["ended_local"] == ""

    @pytest.mark.unit
    def test_running_with_started_raw(self):
        job = {"state": "RUNNING", "started": "2026-03-09T12:00:00"}
        result = normalize_job_times_local(job)
        assert result["started_local"] == "2026-03-09T12:00:00"
        assert result["ended_local"] == ""

    @pytest.mark.unit
    def test_running_derives_from_elapsed(self):
        job = {"state": "RUNNING", "elapsed": "01:00:00"}
        result = normalize_job_times_local(job)
        assert result["started_local"] != ""
        assert result["ended_local"] == ""

    @pytest.mark.unit
    def test_completing_treated_like_running(self):
        job = {"state": "COMPLETING", "started": "2026-03-09T11:00:00"}
        result = normalize_job_times_local(job)
        assert result["started_local"] == "2026-03-09T11:00:00"
        assert result["ended_local"] == ""

    @pytest.mark.unit
    def test_terminal_with_ended_at(self):
        job = {
            "state": "FAILED",
            "elapsed": "00:30:00",
            "ended_at": "2026-03-09T13:00:00",
        }
        result = normalize_job_times_local(job)
        assert result["ended_local"] == "2026-03-09T13:00:00"
        assert result["started_local"] == "2026-03-09T12:30:00"

    @pytest.mark.unit
    def test_original_dict_not_mutated(self):
        job = {"state": "RUNNING", "started": "2026-03-09T12:00:00"}
        result = normalize_job_times_local(job)
        assert "started_local" not in job
        assert result is not job


# ── _infer_parent_from_name ──────────────────────────────────────────────────

class TestInferParentFromName:
    @pytest.mark.unit
    def test_judge_infers_base_eval(self):
        by_name = {"eval-math": "100"}
        id_set = {"100", "200"}
        job = {"job_id": "200"}
        result = _infer_parent_from_name("eval-math-judge", by_name, id_set, job)
        assert result == "100"

    @pytest.mark.unit
    def test_judge_rs0_infers_base_eval(self):
        by_name = {"eval-math": "100"}
        id_set = {"100", "300"}
        job = {"job_id": "300"}
        result = _infer_parent_from_name("eval-math-judge-rs0", by_name, id_set, job)
        assert result == "100"

    @pytest.mark.unit
    def test_summarize_results_infers_judge(self):
        by_name = {"eval-math-judge-rs0": "200", "eval-math": "100"}
        id_set = {"100", "200", "300"}
        job = {"job_id": "300"}
        result = _infer_parent_from_name("eval-math-summarize-results", by_name, id_set, job)
        assert result == "200"

    @pytest.mark.unit
    def test_no_match_returns_none(self):
        result = _infer_parent_from_name("random-job", {}, set(), {"job_id": "1"})
        assert result is None

    @pytest.mark.unit
    def test_self_reference_guard(self):
        by_name = {"eval-math": "100"}
        id_set = {"100"}
        job = {"job_id": "100"}
        result = _infer_parent_from_name("eval-math-judge", by_name, id_set, job)
        assert result is None or result != "100"

    @pytest.mark.unit
    def test_empty_name(self):
        assert _infer_parent_from_name("", {}, set(), {"job_id": "1"}) is None
