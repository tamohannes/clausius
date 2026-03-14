"""Helpers for live cluster tests: submit/cleanup throwaway Slurm jobs.

The target cluster is resolved in order:
  1. TEST_CLUSTER env var (explicit override)
  2. First non-local cluster from config.json
"""

import subprocess
import time
import os


def _resolve_live_cluster():
    """Return the cluster name to use for live tests."""
    explicit = os.environ.get("TEST_CLUSTER", "").strip()
    if explicit:
        return explicit
    try:
        from server.config import CLUSTERS
        for name, cfg in CLUSTERS.items():
            if name != "local" and cfg.get("host"):
                return name
    except Exception:
        pass
    return None


LIVE_CLUSTER = _resolve_live_cluster()
APP_BASE = os.environ.get("TEST_APP_BASE", "http://localhost:7272")


def ssh_cmd(cluster_cfg, command):
    """Run a command on the cluster via SSH and return stdout."""
    from server.config import CLUSTERS
    cfg = CLUSTERS[cluster_cfg]
    ssh_args = [
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        "-p", str(cfg["port"]),
        "-i", cfg["key"],
        f"{cfg['user']}@{cfg['host']}",
        command,
    ]
    result = subprocess.run(ssh_args, capture_output=True, text=True, timeout=30)
    return result.stdout.strip(), result.stderr.strip()


def submit_throwaway_job(cluster, duration_sec=60):
    """Submit a short sleep job and return the job ID."""
    script = f"sbatch --job-name=jm-test-{int(time.time())} --wrap='sleep {duration_sec}' --time=0:05:00 -N1 -n1"
    out, err = ssh_cmd(cluster, script)
    for word in out.split():
        if word.isdigit():
            return word
    raise RuntimeError(f"Failed to submit job: {out} {err}")


def cancel_throwaway_job(cluster, job_id):
    """Cancel a job (cleanup finalizer)."""
    try:
        ssh_cmd(cluster, f"scancel {job_id}")
    except Exception:
        pass


def wait_for_job_state(cluster, job_id, target_states, timeout=60):
    """Poll until job reaches one of target_states."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out, _ = ssh_cmd(cluster, f"squeue -j {job_id} -h -o '%T' 2>/dev/null")
        state = out.strip().upper()
        if state in target_states:
            return state
        if not state:
            return "GONE"
        time.sleep(3)
    return "TIMEOUT"
