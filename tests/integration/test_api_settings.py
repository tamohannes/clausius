"""Integration tests for /api/settings routes."""

import json
import pytest


@pytest.mark.integration
class TestApiSettingsGet:
    def test_returns_config(self, client, mock_ssh):
        resp = client.get("/api/settings")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "port" in data
        assert "ssh_timeout" in data
        assert "clusters" in data

    def test_contains_cache_fresh(self, client, mock_ssh):
        data = client.get("/api/settings").get_json()
        assert "cache_fresh_sec" in data


@pytest.mark.integration
class TestApiSettingsPost:
    def test_partial_patch(self, client, mock_ssh, tmp_path, monkeypatch):
        monkeypatch.setattr("server.config.CONFIG_PATH", str(tmp_path / "config.json"))
        # write initial config so reload_config can write to it
        import json as _json
        initial = client.get("/api/settings").get_json()
        (tmp_path / "config.json").write_text(_json.dumps(initial))

        resp = client.post("/api/settings",
                           data=json.dumps({"ssh_timeout": 15}),
                           content_type="application/json")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["settings"]["ssh_timeout"] == 15

    def test_invalid_body_400(self, client, mock_ssh):
        resp = client.post("/api/settings",
                           data="not json",
                           content_type="application/json")
        assert resp.status_code == 400

    def test_empty_body_400(self, client, mock_ssh):
        resp = client.post("/api/settings",
                           data=json.dumps(None),
                           content_type="application/json")
        assert resp.status_code == 400
