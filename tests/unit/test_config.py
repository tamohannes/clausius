"""Unit tests for server/config.py helpers."""

import json
import os
import time
import pytest

from server.config import _cache_get, _cache_set, _warm_lock, _load_mount_map, settings_response


class TestCacheGetSet:
    @pytest.mark.unit
    def test_set_and_get(self):
        store = {}
        _cache_set(store, "k1", "val1")
        assert _cache_get(store, "k1", ttl_sec=60) == "val1"

    @pytest.mark.unit
    def test_expired_returns_none(self):
        store = {}
        with _warm_lock:
            store["k1"] = {"ts": time.monotonic() - 100, "value": "old"}
        assert _cache_get(store, "k1", ttl_sec=10) is None

    @pytest.mark.unit
    def test_missing_key_returns_none(self):
        assert _cache_get({}, "nope", ttl_sec=60) is None

    @pytest.mark.unit
    def test_overwrite(self):
        store = {}
        _cache_set(store, "k1", "v1")
        _cache_set(store, "k1", "v2")
        assert _cache_get(store, "k1", ttl_sec=60) == "v2"


class TestLoadMountMap:
    @pytest.mark.unit
    def test_default_when_env_empty(self, monkeypatch):
        monkeypatch.delenv("JOB_MONITOR_MOUNT_MAP", raising=False)
        result = _load_mount_map()
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_valid_json_env(self, monkeypatch, mock_cluster):
        monkeypatch.setenv("JOB_MONITOR_MOUNT_MAP",
                           json.dumps({mock_cluster: ["~/.jm/mounts/test"]}))
        result = _load_mount_map()
        assert mock_cluster in result

    @pytest.mark.unit
    def test_malformed_json_falls_back(self, monkeypatch):
        monkeypatch.setenv("JOB_MONITOR_MOUNT_MAP", "not json{{{")
        result = _load_mount_map()
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_string_root_wrapped_in_list(self, monkeypatch, mock_cluster):
        monkeypatch.setenv("JOB_MONITOR_MOUNT_MAP",
                           json.dumps({mock_cluster: "/single/path"}))
        result = _load_mount_map()
        if mock_cluster in result:
            assert isinstance(result[mock_cluster], list)


class TestSettingsResponse:
    @pytest.mark.unit
    def test_contains_expected_keys(self):
        resp = settings_response()
        assert "port" in resp
        assert "ssh_timeout" in resp
        assert "cache_fresh_sec" in resp
