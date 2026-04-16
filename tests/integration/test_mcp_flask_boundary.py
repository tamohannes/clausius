"""MCP HTTP-proxy integration tests.

Verifies MCP tools correctly translate arguments to HTTP calls and parse
responses. Uses _api mocking since the MCP server is now a proxy.
"""

import pytest
from unittest.mock import patch

from mcp_server import (
    health_check, list_jobs, get_history,
    cancel_job, jobs_summary,
    list_logbook_entries, create_logbook_entry,
    read_logbook_entry, delete_logbook_entry,
)


@pytest.mark.integration
@pytest.mark.mcp
class TestMcpHttpProxy:
    def test_health_check(self):
        with patch("mcp_server._api", return_value={"status": "ok", "board_version": 1}):
            result = health_check()
        assert result["status"] == "ok"

    def test_list_jobs_returns_list(self):
        snapshot = {"c1": {"status": "ok", "jobs": [{"jobid": "1", "state": "RUNNING"}]}}
        with patch("mcp_server._api", return_value=snapshot):
            result = list_jobs()
        assert isinstance(result, list)

    def test_list_jobs_unknown_cluster_error(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "Unknown cluster"}):
            result = list_jobs(cluster="nonexistent")
        assert len(result) == 1
        assert "error" in result[0]

    def test_get_history_returns_list(self):
        with patch("mcp_server._api", return_value=[{"job_id": "1"}]):
            result = get_history(limit=5)
        assert isinstance(result, list)

    def test_jobs_summary_string(self):
        with patch("mcp_server._api", return_value={"status": "ok", "summary": "Total: 1 running, 0 pending, 0 failed\nc1: 1 running"}):
            result = jobs_summary()
        assert isinstance(result, str)
        assert "Total:" in result

    def test_cancel_returns_result(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "not found"}):
            result = cancel_job("local", "99999999")
        assert result["status"] == "error"


@pytest.mark.integration
@pytest.mark.mcp
class TestMcpLogbookIntegration:
    def test_create_and_read(self):
        with patch("mcp_server._api", side_effect=[
            {"status": "ok", "id": 1, "created_at": "2026-04-16"},
            {"id": 1, "title": "Integration note", "body": "body text"},
        ]):
            created = create_logbook_entry("test-proj", "Integration note", "body text")
            assert created["status"] == "ok"
            full = read_logbook_entry("test-proj", created["id"])
            assert full["title"] == "Integration note"
            assert full["body"] == "body text"

    def test_list_entries(self):
        entries = [{"id": 1, "title": "A"}, {"id": 2, "title": "B"}]
        with patch("mcp_server._api", return_value=entries):
            result = list_logbook_entries("test-proj")
        assert isinstance(result, list)
        assert len(result) == 2

    def test_delete_entry(self):
        with patch("mcp_server._api", side_effect=[
            {"status": "ok"},
            [],
        ]):
            result = delete_logbook_entry("test-proj", 1)
            assert result["status"] == "ok"
            remaining = list_logbook_entries("test-proj")
            assert len(remaining) == 0
