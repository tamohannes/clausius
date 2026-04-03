"""Unit tests for server/recommendations.py — fairshare-aware scoring."""

import pytest

from server.recommendations import (
    _time_to_sec,
    _is_cpu_partition,
    _is_eligible,
    _generate_tip,
    _pick_best_account,
)


class TestTimeToSec:
    @pytest.mark.unit
    def test_hhmmss(self):
        assert _time_to_sec("4:00:00") == 14400

    @pytest.mark.unit
    def test_integer(self):
        assert _time_to_sec(7200) == 7200

    @pytest.mark.unit
    def test_days(self):
        result = _time_to_sec("1-00:00:00")
        assert result == 86400

    @pytest.mark.unit
    def test_default_on_bad_input(self):
        result = _time_to_sec("garbage")
        assert result == 14400


class TestIsCpuPartition:
    @pytest.mark.unit
    def test_cpu_prefix(self):
        assert _is_cpu_partition("cpu") is True
        assert _is_cpu_partition("cpu_long") is True
        assert _is_cpu_partition("cpu_datamover") is True

    @pytest.mark.unit
    def test_gpu_partition(self):
        assert _is_cpu_partition("batch") is False
        assert _is_cpu_partition("interactive") is False
        assert _is_cpu_partition("batch_long") is False


class TestIsEligible:
    def _part(self, **overrides):
        base = {
            "name": "batch", "state": "UP",
            "max_time_sec": 14400, "max_time": "4:00:00",
            "min_nodes": 0, "max_nodes": None,
            "preempt_mode": "OFF", "allow_accounts": "ALL",
        }
        base.update(overrides)
        return base

    @pytest.mark.unit
    def test_up_partition_eligible(self):
        ok, _ = _is_eligible(self._part(), 1, 3600, "", False)
        assert ok is True

    @pytest.mark.unit
    def test_down_partition_rejected(self):
        ok, reason = _is_eligible(self._part(state="DOWN"), 1, 3600, "", False)
        assert ok is False
        assert "DOWN" in reason

    @pytest.mark.unit
    def test_time_limit_exceeded(self):
        ok, reason = _is_eligible(self._part(max_time_sec=3600), 1, 7200, "", False)
        assert ok is False
        assert "time" in reason.lower()

    @pytest.mark.unit
    def test_min_nodes_not_met(self):
        ok, reason = _is_eligible(self._part(min_nodes=4), 1, 3600, "", False)
        assert ok is False
        assert "min" in reason.lower()

    @pytest.mark.unit
    def test_preemptable_rejected_when_not_allowed(self):
        ok, _ = _is_eligible(self._part(preempt_mode="REQUEUE"), 1, 3600, "", False)
        assert ok is False

    @pytest.mark.unit
    def test_preemptable_accepted_when_allowed(self):
        ok, _ = _is_eligible(self._part(preempt_mode="REQUEUE"), 1, 3600, "", True)
        assert ok is True

    @pytest.mark.unit
    def test_account_not_allowed(self):
        ok, _ = _is_eligible(
            self._part(allow_accounts="admin,special"), 1, 3600, "other_acct", False
        )
        assert ok is False

    @pytest.mark.unit
    def test_account_allowed(self):
        ok, _ = _is_eligible(
            self._part(allow_accounts="admin,my_acct"), 1, 3600, "my_acct", False
        )
        assert ok is True

    @pytest.mark.unit
    def test_skip_partitions(self):
        ok, _ = _is_eligible(self._part(name="defq"), 1, 3600, "", False)
        assert ok is False
        ok, _ = _is_eligible(self._part(name="fake"), 1, 3600, "", False)
        assert ok is False

    @pytest.mark.unit
    def test_cpu_partition_rejected(self):
        ok, _ = _is_eligible(self._part(name="cpu_long"), 1, 3600, "", False)
        assert ok is False


class TestPickBestAccount:
    @pytest.mark.unit
    def test_picks_highest_fs(self):
        fs = {
            "acct_a": {"level_fs": 1.0},
            "acct_b": {"level_fs": 2.5},
        }
        acct, fs_val = _pick_best_account(fs, ["acct_a", "acct_b"])
        assert acct == "acct_b"
        assert fs_val == 2.5

    @pytest.mark.unit
    def test_empty_accounts(self):
        acct, fs_val = _pick_best_account({}, [])
        assert acct is None
        assert fs_val == -1

    @pytest.mark.unit
    def test_missing_account_skipped(self):
        fs = {"acct_a": {"level_fs": 1.5}}
        acct, fs_val = _pick_best_account(fs, ["acct_a", "nonexistent"])
        assert acct == "acct_a"


class TestGenerateTip:
    @pytest.mark.unit
    def test_default_partition_tip(self):
        part = {"name": "batch", "is_default": True, "priority_tier": 5,
                "preempt_mode": "OFF", "idle_nodes": 10, "pending_jobs": 0}
        tip = _generate_tip(part, "eos", [part], 0)
        assert "default" in tip

    @pytest.mark.unit
    def test_idle_nodes_in_tip(self):
        part = {"name": "batch", "is_default": False, "priority_tier": 1,
                "preempt_mode": "OFF", "idle_nodes": 42, "pending_jobs": 5}
        tip = _generate_tip(part, "eos", [part], 0)
        assert "42 idle" in tip

    @pytest.mark.unit
    def test_no_pending_in_tip(self):
        part = {"name": "batch", "is_default": False, "priority_tier": 1,
                "preempt_mode": "OFF", "idle_nodes": 0, "pending_jobs": 0}
        tip = _generate_tip(part, "eos", [part], 0)
        assert "no pending" in tip
