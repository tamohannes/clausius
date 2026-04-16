"""MCP server import and health check smoke tests (HTTP proxy architecture)."""

import pytest
from unittest.mock import patch

from mcp_server import health_check


@pytest.mark.mcp
class TestMcpImport:
    def test_module_imports(self):
        import mcp_server
        assert hasattr(mcp_server, "mcp")
        assert hasattr(mcp_server, "health_check")

    def test_health_check_returns_ok(self):
        with patch("mcp_server._api", return_value={"status": "ok", "board_version": 42}):
            result = health_check()
        assert result["status"] == "ok"
        assert result["service"] == "connected"

    def test_health_check_service_down(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "unreachable"}):
            result = health_check()
        assert result["status"] == "ok"
        assert result["service"] == "unreachable"
