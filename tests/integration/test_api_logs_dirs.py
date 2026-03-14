"""Integration tests for log, ls, log_full, jsonl_index, jsonl_record routes."""

import json
import os
import pytest


@pytest.mark.integration
class TestApiLogFiles:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/log_files/nonexistent/123")
        assert resp.status_code == 404

    def test_local_cluster(self, client, mock_ssh):
        resp = client.get("/api/log_files/local/99999")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "files" in data
        assert "dirs" in data


@pytest.mark.integration
class TestApiLs:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/ls/nonexistent?path=/tmp")
        assert resp.status_code == 404

    def test_no_path_400(self, client, mock_ssh):
        resp = client.get("/api/ls/local")
        assert resp.status_code == 400

    def test_local_dir_listing(self, client, mock_ssh, tmp_path):
        (tmp_path / "file.txt").write_text("x")
        resp = client.get(f"/api/ls/local?path={tmp_path}")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["source"] == "local"
        names = [e["name"] for e in data["entries"]]
        assert "file.txt" in names

    def test_force_bypasses_cache(self, client, mock_ssh, tmp_path):
        (tmp_path / "a.txt").write_text("x")
        client.get(f"/api/ls/local?path={tmp_path}")
        (tmp_path / "b.txt").write_text("y")
        resp = client.get(f"/api/ls/local?path={tmp_path}&force=1")
        names = [e["name"] for e in resp.get_json()["entries"]]
        assert "b.txt" in names


@pytest.mark.integration
class TestApiLog:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/log/nonexistent/123")
        assert resp.status_code == 404

    def test_local_file_read(self, client, mock_ssh, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("line1\nline2\nline3\n")
        resp = client.get(f"/api/log/local/1?path={f}&lines=10")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "line1" in data["content"]
        assert data["source"] == "local"

    def test_cache_hit(self, client, mock_ssh, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("cached content")
        client.get(f"/api/log/local/1?path={f}")
        resp = client.get(f"/api/log/local/1?path={f}")
        data = resp.get_json()
        assert data["source"] == "cache"

    def test_force_bypasses_cache(self, client, mock_ssh, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("original")
        client.get(f"/api/log/local/1?path={f}")
        f.write_text("updated")
        resp = client.get(f"/api/log/local/1?path={f}&force=1")
        data = resp.get_json()
        assert data["source"] == "local"


@pytest.mark.integration
class TestApiLogFull:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/log_full/nonexistent/1?path=/x")
        assert resp.status_code == 404

    def test_no_path_400(self, client, mock_ssh):
        resp = client.get("/api/log_full/local/1")
        assert resp.status_code == 400

    def test_local_pagination(self, client, mock_ssh, tmp_path):
        f = tmp_path / "big.log"
        f.write_text("\n".join(f"line {i}" for i in range(1000)))
        resp = client.get(f"/api/log_full/local/1?path={f}&page=0&page_size=100")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["total_pages"] == 10
        assert data["page"] == 0
        assert "line 0" in data["content"]


@pytest.mark.integration
class TestApiJsonlIndex:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/jsonl_index/nonexistent/1?path=/x")
        assert resp.status_code == 404

    def test_no_path_400(self, client, mock_ssh):
        resp = client.get("/api/jsonl_index/local/1")
        assert resp.status_code == 400

    def test_local_jsonl_index(self, client, mock_ssh, tmp_path):
        f = tmp_path / "data.jsonl"
        f.write_text('{"id": 1}\n{"id": 2}\n')
        resp = client.get(f"/api/jsonl_index/local/1?path={f}&mode=all")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["count"] == 2


@pytest.mark.integration
class TestApiJsonlRecord:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/jsonl_record/nonexistent/1?path=/x&line=0")
        assert resp.status_code == 404

    def test_no_path_400(self, client, mock_ssh):
        resp = client.get("/api/jsonl_record/local/1?line=0")
        assert resp.status_code == 400

    def test_local_record_fetch(self, client, mock_ssh, tmp_path):
        f = tmp_path / "data.jsonl"
        f.write_text('{"id": 0}\n{"id": 1}\n')
        resp = client.get(f"/api/jsonl_record/local/1?path={f}&line=1")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert '"id": 1' in data["content"]
