"""MCP edge case tests for boundary inputs and error handling (HTTP proxy architecture)."""

import pytest
from unittest.mock import patch

from mcp_server import list_jobs, get_job_log, get_history, jobs_summary, mount_cluster, clear_failed, clear_completed


@pytest.mark.mcp
class TestBoundaryParameters:
    def test_zero_lines_log(self):
        with patch("mcp_server._api", return_value={"status": "ok", "content": ""}):
            result = get_job_log("dfw", "1", lines=0)
        assert isinstance(result, str)

    def test_negative_history_limit(self):
        with patch("mcp_server._api", return_value=[]):
            result = get_history(limit=-1)
        assert isinstance(result, list)

    def test_large_history_limit(self):
        with patch("mcp_server._api", return_value=[]):
            result = get_history(limit=999999)
        assert isinstance(result, list)


@pytest.mark.mcp
class TestEmptyData:
    def test_list_jobs_empty_cluster(self):
        with patch("mcp_server._api", return_value={"status": "ok", "jobs": []}):
            result = list_jobs(cluster="dfw")
        assert result == []

    def test_list_jobs_all_empty(self):
        with patch("mcp_server._api", return_value={"dfw": {"status": "ok", "jobs": []}}):
            result = list_jobs()
        assert result == []

    def test_summary_with_unreachable_only(self):
        with patch("mcp_server._api", return_value={"status": "ok", "summary": "Total: 0 running, 0 pending, 0 failed\nc1: unreachable"}):
            result = jobs_summary()
        assert "unreachable" in result
        assert "Total: 0" in result


@pytest.mark.mcp
class TestMountEdgeCases:
    def test_invalid_action_no_api_call(self):
        result = mount_cluster("c1", "bad")
        assert result["status"] == "error"

    def test_valid_mount_calls_api(self):
        with patch("mcp_server._api", return_value={"status": "ok", "message": "OK", "mounts": {}}) as mock:
            result = mount_cluster("c1", "mount")
        assert result["status"] == "ok"
        mock.assert_called_once()


@pytest.mark.mcp
class TestClearEdgeCases:
    def test_clear_failed_calls_api(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            result = clear_failed("c1")
        assert result["status"] == "ok"
        mock.assert_called_once()

    def test_clear_completed_calls_api(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            result = clear_completed("c1")
        assert result["status"] == "ok"
        mock.assert_called_once()
