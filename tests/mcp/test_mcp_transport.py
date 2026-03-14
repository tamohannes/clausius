"""MCP transport error handling tests."""

import json
import pytest
from unittest.mock import patch, MagicMock
import urllib.error

from mcp_server import _api_get, _api_post


@pytest.mark.mcp
class TestApiGetErrors:
    def test_url_error(self):
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("refused")):
            result = _api_get("/api/jobs")
        assert result["status"] == "error"
        assert "unreachable" in result["error"]

    def test_timeout(self):
        with patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
            result = _api_get("/api/jobs")
        assert result["status"] == "error"

    def test_malformed_json(self):
        resp_mock = MagicMock()
        resp_mock.read.return_value = b"not json{{{"
        resp_mock.__enter__ = lambda s: s
        resp_mock.__exit__ = MagicMock(return_value=False)
        with patch("urllib.request.urlopen", return_value=resp_mock):
            result = _api_get("/api/jobs")
        assert result["status"] == "error"

    def test_http_error(self):
        err = urllib.error.HTTPError("/api/jobs", 500, "Internal", {}, None)
        with patch("urllib.request.urlopen", side_effect=err):
            result = _api_get("/api/jobs")
        assert result["status"] == "error"

    def test_generic_exception(self):
        with patch("urllib.request.urlopen", side_effect=RuntimeError("boom")):
            result = _api_get("/api/jobs")
        assert result["status"] == "error"
        assert "boom" in result["error"]


@pytest.mark.mcp
class TestApiPostErrors:
    def test_url_error(self):
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("refused")):
            result = _api_post("/api/cancel/c1/123")
        assert result["status"] == "error"
        assert "unreachable" in result["error"]

    def test_generic_exception(self):
        with patch("urllib.request.urlopen", side_effect=RuntimeError("boom")):
            result = _api_post("/api/cancel/c1/123")
        assert result["status"] == "error"
