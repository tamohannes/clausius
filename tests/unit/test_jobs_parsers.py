"""Unit tests for server/jobs.py parsing and dependency logic."""

import time
import pytest

from server.jobs import parse_dependency, parse_squeue_output


class TestParseDependency:
    @pytest.mark.unit
    def test_single_afterok(self):
        result = parse_dependency("afterok:12345")
        assert result == [{"type": "afterok", "job_id": "12345"}]

    @pytest.mark.unit
    def test_multiple_deps(self):
        result = parse_dependency("afterok:123,afterany:456")
        assert len(result) == 2
        assert result[0] == {"type": "afterok", "job_id": "123"}
        assert result[1] == {"type": "afterany", "job_id": "456"}

    @pytest.mark.unit
    @pytest.mark.parametrize("raw", [
        None, "", "(null)", "  ", "  (null)  ",
    ])
    def test_empty_and_null(self, raw):
        assert parse_dependency(raw) == []

    @pytest.mark.unit
    def test_afternotok(self):
        result = parse_dependency("afternotok:789")
        assert result == [{"type": "afternotok", "job_id": "789"}]

    @pytest.mark.unit
    def test_no_match(self):
        assert parse_dependency("singleton") == []


class TestParseSqueueOutput:
    HEADER = "jobid|name|state|reason|elapsed|timelimit|nodes|cpus|gres|partition|submitted|started|dependency"

    @pytest.mark.unit
    def test_normal_output(self):
        line = "100|eval-math|RUNNING|None|01:00:00|2-00:00:00|1|8|gpu:8|batch|2026-03-09T10:00:00|2026-03-09T10:01:00|"
        jobs = parse_squeue_output(line)
        assert len(jobs) == 1
        j = jobs[0]
        assert j["jobid"] == "100"
        assert j["name"] == "eval-math"
        assert j["state"] == "RUNNING"

    @pytest.mark.unit
    def test_empty_output(self):
        assert parse_squeue_output("") == []
        assert parse_squeue_output("   \n  \n") == []

    @pytest.mark.unit
    def test_short_line_padded(self):
        jobs = parse_squeue_output("100|job|RUNNING")
        assert len(jobs) == 1
        assert jobs[0]["jobid"] == "100"
        assert jobs[0]["dependency"] == ""

    @pytest.mark.unit
    def test_dependency_wiring(self):
        lines = (
            "100|parent|RUNNING||||1|8|gpu:8|batch|2026-03-09T10:00:00|2026-03-09T10:01:00|\n"
            "200|child|PENDING||||1|8|gpu:8|batch|2026-03-09T10:00:00||afterok:100"
        )
        jobs = parse_squeue_output(lines)
        by_id = {j["jobid"]: j for j in jobs}
        assert "100" in by_id["200"]["depends_on"]
        assert "200" in by_id["100"]["dependents"]

    @pytest.mark.unit
    def test_sort_order_running_first(self):
        lines = (
            "200|job-b|PENDING||||1|8|||2026-03-09T10:00:00||\n"
            "100|job-a|RUNNING||||1|8|||2026-03-09T10:00:00|2026-03-09T10:01:00|"
        )
        jobs = parse_squeue_output(lines)
        assert jobs[0]["state"] == "RUNNING"
        assert jobs[1]["state"] == "PENDING"

    @pytest.mark.unit
    def test_off_screen_dep_not_in_depends_on(self):
        line = "200|child|PENDING||||1|8|||2026-03-09T10:00:00||afterok:999"
        jobs = parse_squeue_output(line)
        assert jobs[0]["depends_on"] == []
        assert jobs[0]["dep_details"] == [{"type": "afterok", "job_id": "999"}]
