"""MCP tool/resource contract tests with mocked HTTP transport."""

import json
import pytest
from unittest.mock import patch, MagicMock

import mcp_server
from mcp_server import (
    list_jobs, list_log_files, get_job_log,
    get_job_stats, get_history, cancel_job, cancel_jobs,
    list_projects, get_project_jobs,
    cleanup_history, jobs_summary, _slim_job, _api_get,
)


def _mock_api_get(response):
    return patch.object(mcp_server, "_api_get", return_value=response)


def _mock_api_post(response):
    return patch.object(mcp_server, "_api_post", return_value=response)


# ── _slim_job ────────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestSlimJob:
    def test_includes_cluster(self):
        result = _slim_job("cluster-a", {"jobid": "1", "name": "eval"})
        assert result["cluster"] == "cluster-a"
        assert result["jobid"] == "1"

    def test_strips_empty_fields(self):
        result = _slim_job("c", {"jobid": "1", "name": "", "reason": None, "depends_on": []})
        assert "name" not in result
        assert "reason" not in result
        assert "depends_on" not in result

    def test_keeps_nonempty_fields(self):
        result = _slim_job("c", {"jobid": "1", "progress": 45, "state": "RUNNING"})
        assert result["progress"] == 45
        assert result["state"] == "RUNNING"


# ── list_jobs ────────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestListJobs:
    def test_all_clusters_flattened(self):
        api_resp = {
            "c1": {"status": "ok", "jobs": [{"jobid": "1", "state": "RUNNING"}]},
            "c2": {"status": "ok", "jobs": [{"jobid": "2", "state": "PENDING"}]},
        }
        with _mock_api_get(api_resp):
            result = list_jobs()
        assert len(result) == 2
        clusters = {r["cluster"] for r in result}
        assert clusters == {"c1", "c2"}

    def test_single_cluster(self):
        api_resp = {"status": "ok", "jobs": [{"jobid": "1", "state": "RUNNING"}]}
        with _mock_api_get(api_resp):
            result = list_jobs(cluster="c1")
        assert len(result) == 1
        assert result[0]["cluster"] == "c1"

    def test_error_returns_error_list(self):
        with _mock_api_get({"status": "error", "error": "unreachable"}):
            result = list_jobs(cluster="bad")
        assert len(result) == 1
        assert "error" in result[0]

    def test_global_error(self):
        with _mock_api_get({"status": "error", "error": "down"}):
            result = list_jobs()
        assert len(result) == 1
        assert "error" in result[0]

    def test_returns_list(self):
        with _mock_api_get({"c1": {"status": "ok", "jobs": []}}):
            result = list_jobs()
        assert isinstance(result, list)


# ── list_log_files ───────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestListLogFiles:
    def test_passthrough(self):
        api_resp = {"status": "ok", "files": [{"label": "main", "path": "/x"}], "dirs": []}
        with _mock_api_get(api_resp):
            result = list_log_files("c1", "123")
        assert result["status"] == "ok"
        assert len(result["files"]) == 1


# ── get_job_log ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetJobLog:
    def test_success_returns_content(self):
        with _mock_api_get({"status": "ok", "content": "log line 1\nlog line 2"}):
            result = get_job_log("c1", "123")
        assert isinstance(result, str)
        assert "log line 1" in result

    def test_error_returns_error_string(self):
        with _mock_api_get({"status": "error", "error": "not found"}):
            result = get_job_log("c1", "123")
        assert result.startswith("Error:")

    def test_empty_content(self):
        with _mock_api_get({"status": "ok", "content": ""}):
            result = get_job_log("c1", "123")
        # Empty string content is returned as-is (not wrapped)
        assert result == "" or result == "(empty)"

    def test_with_path_and_lines(self):
        with patch.object(mcp_server, "_api_get") as mock:
            mock.return_value = {"status": "ok", "content": "x"}
            get_job_log("c1", "1", path="/a/b.log", lines=50)
            url = mock.call_args[0][0]
            assert "path=" in url
            assert "lines=50" in url


# ── get_job_stats ────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetJobStats:
    def test_returns_dict(self):
        resp = {"status": "ok", "job_id": "1", "state": "RUNNING", "gpus": []}
        with _mock_api_get(resp):
            result = get_job_stats("c1", "1")
        assert isinstance(result, dict)
        assert result["status"] == "ok"


# ── get_history ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetHistory:
    def test_returns_list_on_list_response(self):
        with _mock_api_get([{"job_id": "1"}, {"job_id": "2"}]):
            result = get_history()
        assert isinstance(result, list)
        assert len(result) == 2

    def test_wraps_non_list_in_list(self):
        with _mock_api_get({"status": "error", "error": "fail"}):
            result = get_history()
        assert isinstance(result, list)
        assert len(result) == 1

    def test_with_cluster_and_limit(self):
        with patch.object(mcp_server, "_api_get") as mock:
            mock.return_value = []
            get_history(cluster="c1", limit=10)
            url = mock.call_args[0][0]
            assert "cluster=c1" in url
            assert "limit=10" in url

    def test_with_project_filter(self):
        with patch.object(mcp_server, "_api_get") as mock:
            mock.return_value = []
            get_history(project="artsiv", limit=20)
            url = mock.call_args[0][0]
            assert "project=artsiv" in url


# ── list_projects ────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestListProjects:
    def test_returns_list(self):
        with _mock_api_get([{"project": "artsiv", "job_count": 5, "color": "#e8f4fd"}]):
            result = list_projects()
        assert isinstance(result, list)
        assert result[0]["project"] == "artsiv"

    def test_wraps_error_in_list(self):
        with _mock_api_get({"status": "error", "error": "fail"}):
            result = list_projects()
        assert isinstance(result, list)


# ── get_project_jobs ─────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetProjectJobs:
    def test_combines_live_and_history(self):
        def _mock_get(path):
            if "/api/history" in path:
                return [{"cluster": "c1", "job_id": "1", "job_name": "artsiv_eval", "state": "COMPLETED", "project": "artsiv"}]
            return {"c1": {"status": "ok", "jobs": [
                {"jobid": "2", "name": "artsiv_train", "state": "RUNNING", "project": "artsiv"}
            ]}}
        with patch.object(mcp_server, "_api_get", side_effect=_mock_get):
            result = get_project_jobs("artsiv")
        assert isinstance(result, list)
        assert len(result) == 2

    def test_filters_by_project(self):
        def _mock_get(path):
            if "/api/history" in path:
                return []
            return {"c1": {"status": "ok", "jobs": [
                {"jobid": "1", "name": "artsiv_eval", "state": "RUNNING", "project": "artsiv"},
                {"jobid": "2", "name": "other_eval", "state": "RUNNING", "project": "other"},
            ]}}
        with patch.object(mcp_server, "_api_get", side_effect=_mock_get):
            result = get_project_jobs("artsiv")
        live = [r for r in result if r.get("state") == "RUNNING"]
        assert all(r.get("cluster") for r in live)
        assert len(live) == 1


# ── cancel_job ───────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestCancelJob:
    def test_returns_dict(self):
        with _mock_api_post({"status": "ok"}):
            result = cancel_job("c1", "123")
        assert result["status"] == "ok"

    def test_error_propagated(self):
        with _mock_api_post({"status": "error", "error": "not found"}):
            result = cancel_job("c1", "bad")
        assert result["status"] == "error"


# ── cancel_jobs ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestCancelJobs:
    def test_all_succeed(self):
        with _mock_api_post({"status": "ok"}):
            result = cancel_jobs("c1", ["100", "200", "300"])
        assert result["status"] == "ok"
        assert result["cancelled"] == 3
        assert result["failed"] == 0

    def test_partial_failure(self):
        call_count = [0]
        def _mock_post(path):
            call_count[0] += 1
            if "200" in path:
                return {"status": "error", "error": "not found"}
            return {"status": "ok"}
        with patch.object(mcp_server, "_api_post", side_effect=_mock_post):
            result = cancel_jobs("c1", ["100", "200", "300"])
        assert result["status"] == "partial"
        assert result["cancelled"] == 2
        assert result["failed"] == 1
        assert result["details"]["200"]["status"] == "error"

    def test_empty_list(self):
        result = cancel_jobs("c1", [])
        assert result["status"] == "ok"
        assert result["cancelled"] == 0


# ── cleanup_history ──────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestCleanupHistory:
    def test_dry_run(self):
        resp_mock = MagicMock()
        resp_mock.read.return_value = json.dumps({"status": "ok", "dry_run": True}).encode()
        resp_mock.__enter__ = lambda s: s
        resp_mock.__exit__ = MagicMock(return_value=False)
        with patch("urllib.request.urlopen", return_value=resp_mock):
            result = cleanup_history(days=30, dry_run=True)
        assert result["status"] == "ok"
        assert result["dry_run"] is True


# ── jobs_summary ─────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestJobsSummary:
    def test_summary_format(self):
        api_resp = {
            "c1": {"status": "ok", "jobs": [
                {"state": "RUNNING"}, {"state": "PENDING"},
            ]},
            "c2": {"status": "error"},
        }
        with _mock_api_get(api_resp):
            result = jobs_summary()
        assert isinstance(result, str)
        assert "Total:" in result
        assert "1 running" in result
        assert "1 pending" in result
        assert "c2: unreachable" in result

    def test_all_idle(self):
        with _mock_api_get({"c1": {"status": "ok", "jobs": []}}):
            result = jobs_summary()
        assert "idle" in result
        assert "Total: 0 running" in result

    def test_global_error(self):
        with _mock_api_get({"status": "error", "error": "down"}):
            result = jobs_summary()
        assert "Error" in result
