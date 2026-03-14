"""MCP-to-Flask boundary integration tests.

Starts the Flask app in a background thread, patches mcp_server.API_BASE
to point at it, and calls MCP tool functions directly to validate that
MCP outputs match route contracts.
"""

import json
import threading
import time
import pytest

import mcp_server
from mcp_server import (
    list_jobs, list_log_files, get_job_log,
    get_job_stats, get_history, cancel_job,
    cleanup_history, jobs_summary,
)


@pytest.fixture()
def live_app(app, mock_ssh, db_path, monkeypatch):
    """Start Flask on a random port and point MCP at it."""
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    server_thread = threading.Thread(
        target=lambda: app.run(host="127.0.0.1", port=port, use_reloader=False),
        daemon=True,
    )
    server_thread.start()

    base = f"http://127.0.0.1:{port}"
    monkeypatch.setattr(mcp_server, "API_BASE", base)

    # Wait for server to be ready
    import urllib.request
    for _ in range(50):
        try:
            urllib.request.urlopen(f"{base}/api/mounts", timeout=1)
            break
        except Exception:
            time.sleep(0.1)

    yield base


@pytest.mark.integration
@pytest.mark.mcp
class TestMcpFlaskBoundary:
    def test_list_jobs_returns_list(self, live_app):
        result = list_jobs()
        assert isinstance(result, list)

    def test_list_jobs_single_cluster(self, live_app):
        result = list_jobs(cluster="local")
        assert isinstance(result, list)

    def test_list_jobs_unknown_cluster_error(self, live_app):
        result = list_jobs(cluster="nonexistent")
        assert len(result) == 1
        assert "error" in result[0]

    def test_get_history_returns_list(self, live_app):
        result = get_history(limit=5)
        assert isinstance(result, list)

    def test_get_job_stats_local_error(self, live_app):
        result = get_job_stats("local", "999")
        assert result["status"] == "error"

    def test_get_job_log_no_files(self, live_app):
        result = get_job_log("local", "999999")
        assert isinstance(result, str)

    def test_list_log_files_local(self, live_app):
        result = list_log_files("local", "999999")
        assert "files" in result or "error" in result

    def test_jobs_summary_string(self, live_app):
        result = jobs_summary()
        assert isinstance(result, str)
        assert "Total:" in result

    def test_cancel_job_bad_pid(self, live_app):
        result = cancel_job("local", "99999999")
        assert result["status"] == "error"

    def test_cleanup_dry_run(self, live_app):
        result = cleanup_history(days=365, dry_run=True)
        assert result.get("status") == "ok"
        # When no records match, response omits dry_run field
        assert result.get("dry_run") is True or result.get("deleted_records") == 0
