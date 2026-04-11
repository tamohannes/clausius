"""Unit tests for server/aihub.py — AI Hub OpenSearch integration."""

import json
import time
import pytest

from server.aihub import (
    _friendly_cluster,
    _os_cluster_names,
    _pick_best_accounts,
    CLUSTER_NAME_MAP,
    CLUSTER_NAME_REV,
)
from server.jobs import _parse_gres_gpu_count
from server.config import _cache_get, _cache_set


class TestClusterNameMapping:
    @pytest.mark.unit
    def test_forward_mapping(self):
        assert CLUSTER_NAME_MAP["eos"] == "eos"
        assert CLUSTER_NAME_MAP["dfw"] == "cw-dfw-cs-001"
        assert CLUSTER_NAME_MAP["aws-dfw"] == "aws-dfw-cs-001"
        assert CLUSTER_NAME_MAP["hsg"] == "oci-hsg-cs-001"

    @pytest.mark.unit
    def test_reverse_mapping(self):
        assert CLUSTER_NAME_REV["eos"] == "eos"
        assert CLUSTER_NAME_REV["cw-dfw-cs-001"] == "dfw"
        assert CLUSTER_NAME_REV["aws-dfw-cs-001"] == "aws-dfw"
        assert CLUSTER_NAME_REV["oci-hsg-cs-001"] == "hsg"

    @pytest.mark.unit
    def test_dfw_and_aws_dfw_are_distinct(self):
        assert CLUSTER_NAME_MAP["dfw"] != CLUSTER_NAME_MAP["aws-dfw"]
        assert CLUSTER_NAME_REV["cw-dfw-cs-001"] == "dfw"
        assert CLUSTER_NAME_REV["aws-dfw-cs-001"] == "aws-dfw"

    @pytest.mark.unit
    def test_friendly_cluster_known(self):
        assert _friendly_cluster("cw-dfw-cs-001") == "dfw"
        assert _friendly_cluster("draco-oci-iad") == "iad"

    @pytest.mark.unit
    def test_friendly_cluster_unknown_passes_through(self):
        assert _friendly_cluster("unknown-cluster") == "unknown-cluster"

    @pytest.mark.unit
    def test_os_cluster_names_specific(self):
        result = _os_cluster_names(["eos", "dfw"])
        assert "eos" in result
        assert "cw-dfw-cs-001" in result
        assert len(result) == 2

    @pytest.mark.unit
    def test_os_cluster_names_all(self):
        result = _os_cluster_names(None)
        assert len(result) == len(CLUSTER_NAME_MAP)

    @pytest.mark.unit
    def test_os_cluster_names_filters_unknown(self):
        result = _os_cluster_names(["eos", "nonexistent"])
        assert len(result) == 1
        assert result[0] == "eos"


class TestPickBestAccounts:
    @pytest.mark.unit
    def test_picks_highest_level_fs_for_priority(self):
        cd = {"accounts": {
            "acct_a": {"level_fs": 1.5, "headroom": 100, "gpus_allocated": 500},
            "acct_b": {"level_fs": 3.0, "headroom": 50, "gpus_allocated": 100},
        }}
        _pick_best_accounts(cd)
        assert cd["best_priority"]["account"] == "acct_b"
        assert cd["best_priority"]["level_fs"] == 3.0

    @pytest.mark.unit
    def test_picks_highest_headroom_for_capacity(self):
        cd = {"accounts": {
            "acct_a": {"level_fs": 1.5, "headroom": 200, "gpus_allocated": 500},
            "acct_b": {"level_fs": 3.0, "headroom": 50, "gpus_allocated": 100},
        }}
        _pick_best_accounts(cd)
        assert cd["best_capacity"]["account"] == "acct_a"
        assert cd["best_capacity"]["headroom"] == 200

    @pytest.mark.unit
    def test_same_account_for_both(self):
        cd = {"accounts": {
            "acct_a": {"level_fs": 5.0, "headroom": 300, "gpus_allocated": 400},
        }}
        _pick_best_accounts(cd)
        assert cd["best_priority"]["account"] == "acct_a"
        assert cd["best_capacity"]["account"] == "acct_a"

    @pytest.mark.unit
    def test_empty_accounts(self):
        cd = {"accounts": {}}
        _pick_best_accounts(cd)
        assert cd["best_priority"] is None
        assert cd["best_capacity"] is None


class TestParseGresGpuCount:
    @pytest.mark.unit
    def test_standard_gres(self):
        assert _parse_gres_gpu_count("gpu:8") == 8

    @pytest.mark.unit
    def test_typed_gres(self):
        assert _parse_gres_gpu_count("gpu:a100:4") == 4

    @pytest.mark.unit
    def test_empty_gres(self):
        assert _parse_gres_gpu_count("") == 0
        assert _parse_gres_gpu_count("N/A") == 0
        assert _parse_gres_gpu_count("(null)") == 0

    @pytest.mark.unit
    def test_multi_gres(self):
        assert _parse_gres_gpu_count("gpu:4,shard:2") == 4

    @pytest.mark.unit
    def test_gres_prefix(self):
        assert _parse_gres_gpu_count("gres/gpu:4") == 4
        assert _parse_gres_gpu_count("gres/gpu:b200:4") == 4

    @pytest.mark.unit
    def test_gres_prefix_with_socket(self):
        assert _parse_gres_gpu_count("gres/gpu:4(S:0-1)") == 4
        assert _parse_gres_gpu_count("gres/gpu:b200:4(S:0-1)") == 4


class TestAihubCaching:
    @pytest.mark.unit
    def test_cache_stores_and_retrieves(self):
        store = {}
        _cache_set(store, "test_key", {"data": 42})
        result = _cache_get(store, "test_key", 300)
        assert result == {"data": 42}

    @pytest.mark.unit
    def test_cache_expires(self):
        from server.config import _warm_lock
        store = {}
        with _warm_lock:
            store["old"] = {"ts": time.monotonic() - 600, "value": "stale"}
        result = _cache_get(store, "old", 300)
        assert result is None
