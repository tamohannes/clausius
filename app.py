import json
import os
import random
import re
import shlex
import select
import sqlite3
import subprocess
import threading
import time
from glob import glob
from collections import deque
from datetime import datetime, timedelta

import paramiko
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)
APP_ROOT = os.path.dirname(__file__)
DEFAULT_USER = os.environ.get("JOB_MONITOR_SSH_USER") or os.environ.get("USER") or "user"
DEFAULT_SSH_KEY = os.path.expanduser(os.environ.get("JOB_MONITOR_SSH_KEY", "~/.ssh/id_ed25519"))
DB_PATH = os.path.join(APP_ROOT, "history.db")
SSH_TIMEOUT = 8
POLL_ACTIVE_SEC = 120
POLL_IDLE_SEC = 900
POLL_UNREACHABLE_SEC = 1800

CONFIG_PATH = os.path.join(APP_ROOT, "config.json")
if not os.path.isfile(CONFIG_PATH):
    raise SystemExit(
        f"Config file not found: {CONFIG_PATH}\n"
        "Copy config.example.json to config.json and fill in your cluster details."
    )
with open(CONFIG_PATH) as _cf:
    _CONFIG = json.load(_cf)

APP_PORT = _CONFIG.get("port", 7272)
LOG_SEARCH_BASES = _CONFIG.get("log_search_bases", [])
NEMO_RUN_BASES = _CONFIG.get("nemo_run_bases", [])
MOUNT_LUSTRE_PREFIXES = _CONFIG.get("mount_lustre_prefixes", [])
_proc_filters = _CONFIG.get("local_process_filters", {})
LOCAL_PROC_INCLUDE = _proc_filters.get("include", [])
LOCAL_PROC_EXCLUDE = _proc_filters.get("exclude", [])

CLUSTERS = {}
for _name, _cfg in _CONFIG.get("clusters", {}).items():
    CLUSTERS[_name] = {
        "host": _cfg["host"],
        "user": _cfg.get("user", DEFAULT_USER),
        "key": os.path.expanduser(_cfg.get("key", DEFAULT_SSH_KEY)),
        "port": _cfg.get("port", 22),
        "gpu_type": _cfg.get("gpu_type", ""),
    }
CLUSTERS["local"] = {
    "host": None, "user": None, "key": None,
    "port": None, "gpu_type": "local",
}


def _load_mount_map():
    """
    Cluster -> list of local mount roots used for sshfs-mounted paths.
    Override via env var JOB_MONITOR_MOUNT_MAP (JSON object), e.g.:
      {"my-cluster":["~/.job-monitor/mounts/my-cluster"]}
    """
    home = os.path.expanduser("~")
    base = os.path.join(home, ".job-monitor", "mounts")
    defaults = {
        name: [os.path.join(base, name)]
        for name in CLUSTERS if name != "local"
    }
    raw = os.environ.get("JOB_MONITOR_MOUNT_MAP", "").strip()
    if not raw:
        return defaults
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            return defaults
        out = {}
        for name, roots in parsed.items():
            if name not in CLUSTERS or name == "local":
                continue
            if isinstance(roots, str):
                roots = [roots]
            if not isinstance(roots, list):
                continue
            norm = []
            for r in roots:
                if not isinstance(r, str):
                    continue
                p = os.path.abspath(os.path.expanduser(r.strip()))
                if p:
                    norm.append(p)
            if norm:
                out[name] = norm
        return out or defaults
    except Exception:
        return defaults


MOUNT_MAP = _load_mount_map()
MOUNT_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "scripts", "sshfs_logs.sh")

STATE_ORDER = {"RUNNING": 0, "COMPLETING": 1, "PENDING": 2, "FAILED": 3, "CANCELLED": 4}
SQUEUE_FMT = "%i|%j|%T|%r|%M|%l|%D|%C|%b|%P|%V|%S"
SQUEUE_HDR = ["jobid", "name", "state", "reason", "elapsed", "timelimit", "nodes", "cpus", "gres", "partition", "submitted", "started"]

# In-memory cache
_cache_lock = threading.Lock()
_cache = {}       # cluster -> {status, jobs, updated}
_seen_jobs = {}   # cluster -> set of job_ids currently live in squeue
_last_polled = {} # cluster -> monotonic timestamp of last poll

# Reused SSH sessions (to reduce reconnect churn on cluster login nodes)
_ssh_pool_lock = threading.Lock()
_ssh_pool = {}              # cluster -> {"client": SSHClient, "last_used": monotonic}
_ssh_cluster_locks = {}     # cluster -> Lock (serialize exec_command per cluster)
SSH_IDLE_TTL_SEC = 180

# Warm caches for fast UX on click
_warm_lock = threading.Lock()
_log_index_cache = {}     # (cluster, job_id) -> {"ts":..., "value": {...}}
_log_content_cache = {}   # (cluster, job_id, path) -> {"ts":..., "value": "..."}
_stats_cache = {}         # (cluster, job_id) -> {"ts":..., "value": {...}}
_dir_list_cache = {}      # (cluster, path) -> {"ts":..., "value": {...}}
_progress_cache = {}      # (cluster, job_id) -> {"ts":..., "value": int|None}
_prefetch_last = {}       # (cluster, job_id) -> monotonic ts
LOG_INDEX_TTL_SEC = 120
LOG_CONTENT_TTL_SEC = 45
STATS_TTL_SEC = 15
DIR_LIST_TTL_SEC = 20
PROGRESS_TTL_SEC = 60
PREFETCH_MIN_GAP_SEC = 120

TERMINAL_STATES = {"FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL"}
PINNABLE_TERMINAL_STATES = TERMINAL_STATES | {"COMPLETED"}


# ─── Database ───────────────────────────────────────────────────────────────


def parse_slurm_elapsed_seconds(elapsed):
    """Parse Slurm elapsed formats: MM:SS, HH:MM:SS, D-HH:MM:SS."""
    if not elapsed or elapsed in {"—", "N/A", "Unknown"}:
        return None
    try:
        s = elapsed.strip()
        days = 0
        if "-" in s:
            d, s = s.split("-", 1)
            days = int(d)
        parts = [int(x) for x in s.split(":")]
        if len(parts) == 2:
            h, m, sec = 0, parts[0], parts[1]
        elif len(parts) == 3:
            h, m, sec = parts
        else:
            return None
        return days * 86400 + h * 3600 + m * 60 + sec
    except Exception:
        return None


def parse_dt_maybe(value):
    if not value:
        return None
    text = str(value).strip()
    if not text or text in {"Unknown", "N/A", "—", "None"}:
        return None
    try:
        return datetime.fromisoformat(text.replace(" ", "T"))
    except Exception:
        return None


def normalize_job_times_local(job):
    """
    Ensure displayed times use THIS machine's clock.
    - Running jobs: start_local = now - elapsed
    - Finished jobs: if ended_at exists and elapsed exists, derive start_local from those.
    """
    j = dict(job)
    state = str(j.get("state", "")).upper()
    elapsed_s = parse_slurm_elapsed_seconds(j.get("elapsed"))
    now = datetime.now()

    submitted = parse_dt_maybe(j.get("submitted"))
    started_raw = parse_dt_maybe(j.get("started") or j.get("start"))

    # Pending: "start" should show queue submit time, not current time.
    if state == "PENDING":
        j["started_local"] = submitted.isoformat(timespec="seconds") if submitted else ""
        j["ended_local"] = ""
        return j

    # Running: prefer explicit started timestamp; fall back to elapsed-based estimate.
    if state in {"RUNNING", "COMPLETING"}:
        if started_raw:
            j["started_local"] = started_raw.isoformat(timespec="seconds")
        elif elapsed_s is not None:
            j["started_local"] = (now - timedelta(seconds=elapsed_s)).isoformat(timespec="seconds")
        elif submitted:
            j["started_local"] = submitted.isoformat(timespec="seconds")
        else:
            j["started_local"] = ""
        j["ended_local"] = ""
        return j

    # For terminal/pinned jobs, keep ended_at but derive start if possible
    ended = parse_dt_maybe(j.get("ended_at"))
    if ended:
        j["ended_local"] = ended.isoformat(timespec="seconds")
        if started_raw:
            j["started_local"] = started_raw.isoformat(timespec="seconds")
        elif elapsed_s is not None:
            j["started_local"] = (ended - timedelta(seconds=elapsed_s)).isoformat(timespec="seconds")
        elif submitted:
            j["started_local"] = submitted.isoformat(timespec="seconds")
    else:
        j["ended_local"] = ""
        if started_raw:
            j["started_local"] = started_raw.isoformat(timespec="seconds")
        elif elapsed_s is not None:
            j["started_local"] = (now - timedelta(seconds=elapsed_s)).isoformat(timespec="seconds")
        elif submitted:
            j["started_local"] = submitted.isoformat(timespec="seconds")

    return j


def _cache_get(store, key, ttl_sec):
    with _warm_lock:
        rec = store.get(key)
    if not rec:
        return None
    if time.monotonic() - rec["ts"] > ttl_sec:
        return None
    return rec["value"]


def _cache_set(store, key, value):
    with _warm_lock:
        store[key] = {"ts": time.monotonic(), "value": value}


_PROGRESS_RE = re.compile(r'(\d{1,3})%\|')

def extract_progress(content):
    """Extract the last tqdm-style progress percentage from log content."""
    if not content:
        return None
    matches = _PROGRESS_RE.findall(content[-4096:])
    if matches:
        pct = int(matches[-1])
        if 0 <= pct <= 100:
            return pct
    return None


def _tail_local_file(path, lines):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return "".join(deque(fh, maxlen=max(1, int(lines))))
    except Exception as e:
        return f"Could not read local mounted file: {e}"


def _extract_arg_value(tokens, key):
    """Return CLI value for both '--key value' and '--key=value' forms."""
    for idx, token in enumerate(tokens):
        if token == key and idx + 1 < len(tokens):
            return tokens[idx + 1]
        prefix = f"{key}="
        if token.startswith(prefix):
            return token[len(prefix):]
    return ""


def _safe_proc_readlink(path):
    try:
        return os.readlink(path)
    except Exception:
        return ""


def _is_regular_local_file_target(target):
    if not target:
        return False
    # Ignore non-file stream targets.
    if target.startswith(("pipe:", "socket:", "anon_inode:")):
        return False
    if target.startswith("/dev/"):
        return False
    return os.path.isfile(target)


def _local_child_pids(pid):
    """Return direct child PIDs for a process, best effort."""
    try:
        with open(f"/proc/{pid}/task/{pid}/children", "r", encoding="utf-8", errors="replace") as fh:
            raw = fh.read().strip()
        if not raw:
            return []
        return [p for p in raw.split() if p.isdigit()]
    except Exception:
        return []


def _collect_file_logs_from_pid(pid, seen_files):
    """Collect file-backed stdout/stderr from one local pid."""
    out = []
    proc_dir = f"/proc/{pid}"
    for fd, label in (("1", "stdout"), ("2", "stderr")):
        target = _safe_proc_readlink(f"{proc_dir}/fd/{fd}")
        if _is_regular_local_file_target(target) and target not in seen_files:
            seen_files.add(target)
            out.append({"label": label, "path": target})
    return out


def _read_local_procfd_snapshot(pid, fd_num, lines=200):
    """Best-effort non-blocking read from /proc/<pid>/fd/<n> stream."""
    path = f"/proc/{pid}/fd/{fd_num}"
    if not os.path.exists(path):
        return f"Process stream not found: {path}"
    fdesc = None
    chunks = []
    total = 0
    max_bytes = 256 * 1024
    try:
        fdesc = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        # Short snapshot window: enough for buffered output, no UI stalls.
        deadline = time.monotonic() + 0.35
        while time.monotonic() < deadline and total < max_bytes:
            r, _, _ = select.select([fdesc], [], [], 0.05)
            if not r:
                break
            try:
                data = os.read(fdesc, 8192)
            except BlockingIOError:
                break
            if not data:
                break
            chunks.append(data)
            total += len(data)
    except Exception as e:
        return f"Could not read process stream: {e}"
    finally:
        if fdesc is not None:
            try:
                os.close(fdesc)
            except Exception:
                pass
    text = b"".join(chunks).decode("utf-8", errors="replace")
    if not text:
        return "(no buffered output captured from live process stream)"
    return "\n".join(text.splitlines()[-max(1, int(lines)):])


def _collect_recent_local_files(root, max_files=40):
    allowed_suffixes = (".log", ".out", ".err", ".txt", ".json", ".jsonl", ".jsonl-async", ".md")
    out = []
    if not root or not os.path.isdir(root):
        return out
    # Keep search shallow for responsiveness.
    for cur, _, files in os.walk(root):
        rel = os.path.relpath(cur, root)
        depth = 0 if rel == "." else rel.count(os.sep) + 1
        if depth > 2:
            continue
        for name in files:
            lower = name.lower()
            if not lower.endswith(allowed_suffixes):
                continue
            full = os.path.join(cur, name)
            try:
                mtime = os.path.getmtime(full)
            except Exception:
                mtime = 0.0
            out.append((mtime, full))
    out.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in out[:max_files]]


def _local_job_log_files(job_id):
    """
    Best-effort log discovery for local process jobs.
    Sources:
    - process fd stdout/stderr targets if they are regular files
    - output directories parsed from CLI args (output_dir/output_file)
    """
    if not str(job_id).isdigit():
        return {"files": [], "dirs": [], "error": "Local job id must be a PID."}
    pid = str(job_id)
    proc_dir = f"/proc/{pid}"
    if not os.path.isdir(proc_dir):
        return {"files": [], "dirs": [], "error": f"Local process {pid} is not running."}

    cmdline_bytes = b""
    try:
        with open(f"{proc_dir}/cmdline", "rb") as fh:
            cmdline_bytes = fh.read()
    except Exception:
        pass
    tokens = []
    if cmdline_bytes:
        tokens = [t.decode("utf-8", errors="replace") for t in cmdline_bytes.split(b"\x00") if t]
    else:
        # Fallback from ps if cmdline is unavailable.
        ps = subprocess.run(["ps", "-p", pid, "-o", "args="], capture_output=True, text=True, timeout=3)
        raw = (ps.stdout or "").strip()
        if raw:
            try:
                tokens = shlex.split(raw)
            except Exception:
                tokens = raw.split()

    cwd = _safe_proc_readlink(f"{proc_dir}/cwd")
    discovered_dirs = []

    # Parse output_dir/output_file from CLI args.
    output_dir = _extract_arg_value(tokens, "--output_dir") or _extract_arg_value(tokens, "++output_dir")
    output_file = _extract_arg_value(tokens, "--output_file") or _extract_arg_value(tokens, "++output_file")

    if output_dir:
        if not output_dir.startswith("/") and cwd:
            output_dir = os.path.normpath(os.path.join(cwd, output_dir))
        discovered_dirs.append(output_dir)
    if output_file:
        if not output_file.startswith("/") and cwd:
            output_file = os.path.normpath(os.path.join(cwd, output_file))
        discovered_dirs.append(os.path.dirname(output_file))

    # Include common NeMo logs/results subdirs when output_dir is known.
    expanded_dirs = []
    for d in discovered_dirs:
        expanded_dirs.extend([
            d,
            os.path.join(d, "eval-logs"),
            os.path.join(d, "eval-results"),
        ])

    seen_dirs = set()
    dirs = []
    for d in expanded_dirs:
        if not d:
            continue
        nd = os.path.normpath(d)
        if nd in seen_dirs:
            continue
        seen_dirs.add(nd)
        if os.path.isdir(nd):
            label = "eval-logs" if nd.endswith("/eval-logs") else ("eval-results" if nd.endswith("/eval-results") else "output")
            dirs.append({"label": label, "path": nd})

    files = []
    seen_files = set()

    # fd-based discovery (works when redirected to real files).
    files.extend(_collect_file_logs_from_pid(pid, seen_files))

    # If wrapper process has no file-backed logs, borrow from direct children.
    if not files:
        for cpid in _local_child_pids(pid):
            files.extend(_collect_file_logs_from_pid(cpid, seen_files))

    # Directory scan for recent log/result files.
    for d in [x["path"] for x in dirs]:
        for p in _collect_recent_local_files(d):
            if p in seen_files:
                continue
            seen_files.add(p)
            files.append({"label": _label_log(os.path.basename(p)), "path": p})

    # Last resort: pipe stream snapshot for interactive/wrapper processes.
    if not files:
        for fd, label in (("1", "stdout"), ("2", "stderr")):
            target = _safe_proc_readlink(f"{proc_dir}/fd/{fd}")
            if target.startswith("pipe:"):
                files.append({"label": f"{label} stream", "path": f"procfd://{pid}/{fd}"})

    err = ""
    if not files and not dirs:
        err = (
            "No local log files auto-discovered. "
            "This process may be writing to a terminal/pipe instead of file-backed stdout/stderr."
        )
    return {"files": files, "dirs": dirs, "error": err}


def _local_candidates_for_remote_path(cluster_name, remote_path):
    roots = MOUNT_MAP.get(cluster_name, [])
    if not roots or not remote_path:
        return []
    rp = str(remote_path).strip()
    if not rp.startswith("/"):
        return []
    out = []
    seen = set()
    suffixes = [rp.lstrip("/")]
    if "/lustre/" in rp:
        suffixes.append("lustre/" + rp.split("/lustre/", 1)[1])
    for root in roots:
        for suf in suffixes:
            cand = os.path.normpath(os.path.join(root, suf))
            if cand not in seen:
                seen.add(cand)
                out.append(cand)
    return out


def _resolve_mounted_path(cluster_name, remote_path, want_dir=False):
    checker = os.path.isdir if want_dir else os.path.isfile
    for cand in _local_candidates_for_remote_path(cluster_name, remote_path):
        if checker(cand):
            return cand
    return ""


def _list_local_dir(path):
    entries = []
    for name in sorted(os.listdir(path)):
        full = os.path.join(path, name)
        entries.append({
            "name": name,
            "path": full,
            "is_dir": os.path.isdir(full),
            "size": os.path.getsize(full) if os.path.isfile(full) else None,
        })
    return entries


def _prefetch_nested_dir_cache_local(cluster, request_path, local_base_path, entries, limit=8):
    """
    Warm cache for immediate child directories to speed up tree expansion.
    Applies only to local filesystem paths (cluster=local or mounted dirs).
    """
    try:
        warmed = 0
        for e in entries:
            if warmed >= limit:
                break
            if not e.get("is_dir"):
                continue
            name = e.get("name", "")
            if not name:
                continue
            child_req_path = request_path.rstrip("/") + "/" + name
            child_local_path = os.path.join(local_base_path, name)
            if not os.path.isdir(child_local_path):
                continue
            child_entries = _list_local_dir(child_local_path)
            payload = {
                "status": "ok",
                "path": child_req_path,
                "entries": child_entries,
                "source": "local" if cluster == "local" else "mount",
                "resolved_path": child_local_path,
            }
            _cache_set(_dir_list_cache, (cluster, child_req_path), payload)
            warmed += 1
    except Exception:
        # Best-effort optimization.
        pass


def _cluster_mount_status(cluster_name):
    roots = MOUNT_MAP.get(cluster_name, [])
    mounted_root = ""
    for r in roots:
        p = os.path.abspath(os.path.expanduser(r))
        if os.path.ismount(p):
            mounted_root = p
            break
    return {
        "cluster": cluster_name,
        "mounted": bool(mounted_root),
        "root": mounted_root or (os.path.abspath(os.path.expanduser(roots[0])) if roots else ""),
        "roots": [os.path.abspath(os.path.expanduser(r)) for r in roots],
    }


def _all_mount_status():
    return {
        name: _cluster_mount_status(name)
        for name in CLUSTERS
        if name != "local"
    }


def _mounted_root(cluster_name):
    """Return active mount root for cluster, else empty string."""
    for r in MOUNT_MAP.get(cluster_name, []):
        p = os.path.abspath(os.path.expanduser(r))
        if os.path.ismount(p):
            return p
    return ""


def _remote_path_from_mounted(cluster_name, local_path):
    root = _mounted_root(cluster_name)
    if not root:
        return ""
    lp = os.path.abspath(local_path)
    try:
        rel = os.path.relpath(lp, root)
    except Exception:
        return ""
    if rel == ".":
        return "/"
    return "/" + rel.lstrip("/")


def _discover_job_logs_from_mount(cluster_name, job_id):
    """
    Fast local discovery for mounted clusters.
    Returns {"files":[...], "dirs":[...]} or None when no mount/no hit.
    """
    root = _mounted_root(cluster_name)
    if not root:
        return None
    user = CLUSTERS[cluster_name]["user"]
    allowed_suffixes = (".log", ".out", ".err", ".txt", ".json", ".jsonl", ".jsonl-async", ".md")
    paths = []
    bases = [
        os.path.join(root, prefix, user)
        for prefix in MOUNT_LUSTRE_PREFIXES
    ]
    for base in bases:
        if not os.path.isdir(base):
            continue
        # Targeted patterns avoid deep full-tree scans.
        pats = [
            os.path.join(base, "nemo-run", "**", "eval-logs", f"*{job_id}*"),
            os.path.join(base, "**", "eval-logs", f"*{job_id}*"),
        ]
        for pat in pats:
            for p in glob(pat, recursive=True):
                if not os.path.isfile(p):
                    continue
                if not p.lower().endswith(allowed_suffixes):
                    continue
                paths.append(p)
                if len(paths) >= 80:
                    break
            if len(paths) >= 80:
                break
        if paths:
            break
    if not paths:
        return None

    uniq = []
    seen = set()
    for p in paths:
        rp = _remote_path_from_mounted(cluster_name, p)
        if not rp or rp in seen:
            continue
        seen.add(rp)
        uniq.append(rp)

    files = _label_and_sort_files(uniq)
    dirs = []
    if files:
        log_dir = os.path.dirname(files[0]["path"])
        output_dir = os.path.dirname(log_dir)
        for dname in ["eval-logs", "eval-results"]:
            dpath = output_dir.rstrip("/") + "/" + dname
            # Only include if the mounted directory exists.
            if _resolve_mounted_path(cluster_name, dpath, want_dir=True):
                dirs.append({"label": dname, "path": dpath})
    return {"files": files, "dirs": dirs}


def _run_mount_script(action, cluster="all"):
    if action not in {"mount", "unmount"}:
        return False, "Invalid action."
    if cluster != "all" and (cluster not in CLUSTERS or cluster == "local"):
        return False, "Unknown cluster."
    script = os.path.abspath(MOUNT_SCRIPT_PATH)
    if not os.path.isfile(script):
        return False, f"Mount script not found: {script}"
    cmd = [script, action]
    if cluster and cluster != "all":
        cmd.append(cluster)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        if proc.returncode != 0:
            msg = "\n".join(x for x in [out, err] if x).strip() or f"{action} failed"
            return False, msg
        return True, out or f"{action} completed"
    except Exception as e:
        return False, str(e)


def _schedule_prefetch(cluster, job_id):
    """Throttle background prefetch per job."""
    k = (cluster, str(job_id))
    now = time.monotonic()
    with _warm_lock:
        last = _prefetch_last.get(k, 0.0)
        if now - last < PREFETCH_MIN_GAP_SEC:
            return
        _prefetch_last[k] = now
    t = threading.Thread(target=_prefetch_job_data, args=(cluster, str(job_id)), daemon=True)
    t.start()


def _prefetch_job_data(cluster, job_id):
    """Warm log index, first log content, stats, and progress."""
    try:
        log_result = get_job_log_files(cluster, job_id)
        _cache_set(_log_index_cache, (cluster, job_id), log_result)
        files = log_result.get("files", [])
        if files:
            first = files[0]["path"]
            content = fetch_log_tail(cluster, first, lines=220)
            _cache_set(_log_content_cache, (cluster, job_id, first), content)
            pct = extract_progress(content)
            if pct is not None:
                _cache_set(_progress_cache, (cluster, job_id), pct)
    except Exception:
        pass
    try:
        stats = get_job_stats(cluster, job_id)
        _cache_set(_stats_cache, (cluster, job_id), stats)
    except Exception:
        pass


def _label_and_sort_files(paths):
    ORDER = {"main output": 0, "server output": 1, "sandbox output": 2, "sbatch log": 3, "sbatch stderr": 4}
    files = [{"label": _label_log(os.path.basename(p)), "path": p} for p in paths]
    files.sort(key=lambda f: ORDER.get(f["label"], 10))
    return files


def prefetch_cluster_bulk(cluster, job_ids):
    """
    One SSH round-trip per cluster to warm:
      - stats cache for all job_ids (from one squeue call)
      - log index cache for all job_ids (from one log-dir scan)
    """
    if cluster == "local" or not job_ids:
        return
    ids = [str(j) for j in job_ids if j]
    ids_csv = ",".join(ids)
    user = CLUSTERS[cluster]["user"]
    script = f"""#!/bin/sh
IDS="{ids_csv}"
USER="{user}"

# Stats in one go
squeue -h -j "$IDS" -o "%i|%T|%D|%C|%b|%N|%M" | sed 's/^/STAT:/'

# Best-effort logdir from latest sbatch script
LOGDIR=""
for BASE in {" ".join(NEMO_RUN_BASES)}; do
  [ -d "$BASE" ] || continue
  SB=$(find "$BASE" -maxdepth 5 -name "*sbatch.sh" -type f 2>/dev/null | xargs ls -1t 2>/dev/null | head -1)
  if [ -n "$SB" ]; then
    OUT=$(grep '#SBATCH --output=' "$SB" | head -1 | sed 's/.*--output=//' | tr -d ' ')
    if [ -n "$OUT" ]; then
      LOGDIR=$(dirname "$OUT")
      break
    fi
  fi
done
echo "LOGDIR:$LOGDIR"
if [ -n "$LOGDIR" ] && [ -d "$LOGDIR" ]; then
  find "$LOGDIR" -maxdepth 1 -type f 2>/dev/null | sed 's/^/FILE:/'
fi
"""
    try:
        out, _ = ssh_run_with_timeout(cluster, script, timeout_sec=20)
    except Exception:
        return

    stat_map = {}
    logdir = ""
    all_files = []
    for line in out.splitlines():
        if line.startswith("STAT:"):
            parts = line[len("STAT:"):].split("|")
            if len(parts) >= 7:
                jid = parts[0].strip()
                stat_map[jid] = {
                    "status": "ok",
                    "job_id": jid,
                    "state": parts[1].strip(),
                    "nodes": parts[2].strip(),
                    "cpus": parts[3].strip(),
                    "gres": parts[4].strip(),
                    "node_list": parts[5].strip(),
                    "elapsed": parts[6].strip(),
                    "gpus": [],
                    "ave_cpu": "",
                    "ave_rss": "",
                    "max_rss": "",
                    "max_vmsize": "",
                    "_partial": True,
                }
        elif line.startswith("LOGDIR:"):
            logdir = line[len("LOGDIR:"):].strip()
        elif line.startswith("FILE:"):
            fp = line[len("FILE:"):].strip()
            if fp:
                all_files.append(fp)

    # Warm stats cache
    for jid in ids:
        if jid in stat_map:
            _cache_set(_stats_cache, (cluster, jid), stat_map[jid])

    # Warm log index + first content cache per job
    for jid in ids:
        matched = [p for p in all_files if jid in os.path.basename(p)]
        if matched:
            files = _label_and_sort_files(matched)
            dirs = []
            if logdir:
                outdir = os.path.dirname(logdir)
                dirs = [{"label": "eval-logs", "path": outdir.rstrip("/") + "/eval-logs"},
                        {"label": "eval-results", "path": outdir.rstrip("/") + "/eval-results"}]
            result = {"files": files, "dirs": dirs}
            _cache_set(_log_index_cache, (cluster, jid), result)
            # Warm first file content + progress
            first = files[0]["path"]
            content = fetch_log_tail(cluster, first, lines=220)
            _cache_set(_log_content_cache, (cluster, jid, first), content)
            pct = extract_progress(content)
            if pct is not None:
                _cache_set(_progress_cache, (cluster, jid), pct)

def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    con = get_db()
    con.execute("""
        CREATE TABLE IF NOT EXISTS job_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster       TEXT NOT NULL,
            job_id        TEXT NOT NULL,
            job_name      TEXT,
            state         TEXT,
            exit_code     TEXT,
            reason        TEXT,
            elapsed       TEXT,
            nodes         TEXT,
            gres          TEXT,
            partition     TEXT,
            submitted     TEXT,
            ended_at      TEXT,
            log_path      TEXT,
            board_visible INTEGER DEFAULT 0,
            UNIQUE(cluster, job_id)
        )
    """)
    # Migrate: add board_visible if it doesn't exist yet
    try:
        con.execute("ALTER TABLE job_history ADD COLUMN board_visible INTEGER DEFAULT 0")
    except Exception:
        pass
    con.commit()
    con.close()


def upsert_job(cluster, job, terminal=False, set_board_visible=None):
    """
    Upsert a job record. 
    - terminal=True: job has finished; set board_visible=1 unless already dismissed
    - set_board_visible: explicitly set board_visible (for dismiss)
    """
    con = get_db()

    # Check current board_visible to avoid overwriting a dismiss
    row = con.execute(
        "SELECT board_visible FROM job_history WHERE cluster=? AND job_id=?",
        (cluster, job["jobid"])
    ).fetchone()
    current_visible = row["board_visible"] if row else None

    if set_board_visible is not None:
        bv = set_board_visible
    elif terminal:
        # Only make visible if not already explicitly dismissed (0 from user action)
        bv = 1 if current_visible != 0 else 0
    else:
        bv = current_visible if current_visible is not None else 0

    # Add started column if missing (migration)
    try:
        con.execute("ALTER TABLE job_history ADD COLUMN started TEXT")
        con.commit()
    except Exception:
        pass

    con.execute("""
        INSERT INTO job_history
            (cluster, job_id, job_name, state, exit_code, reason, elapsed,
             nodes, gres, partition, submitted, started, ended_at, log_path, board_visible)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(cluster, job_id) DO UPDATE SET
            job_name    = COALESCE(excluded.job_name, job_name),
            state       = excluded.state,
            exit_code   = COALESCE(excluded.exit_code, exit_code),
            reason      = COALESCE(excluded.reason, reason),
            elapsed     = COALESCE(excluded.elapsed, elapsed),
            nodes       = COALESCE(excluded.nodes, nodes),
            gres        = COALESCE(excluded.gres, gres),
            partition   = COALESCE(excluded.partition, partition),
            submitted   = COALESCE(excluded.submitted, submitted),
            started     = COALESCE(excluded.started, started),
            ended_at    = COALESCE(excluded.ended_at, ended_at),
            board_visible = excluded.board_visible
    """, (
        cluster, job["jobid"],
        job.get("name") or job.get("job_name"),
        job.get("state"),
        job.get("exit_code"), job.get("reason"), job.get("elapsed"),
        job.get("nodes"), job.get("gres"), job.get("partition"),
        job.get("submitted"), job.get("started"),
        job.get("ended_at"), job.get("log_path"),
        bv,
    ))
    con.commit()
    con.close()


# Keep old name as alias for compatibility
def upsert_history(cluster, job):
    upsert_job(cluster, job)


def get_board_pinned(cluster=None):
    """Return jobs that are terminal but board_visible=1 (not yet dismissed)."""
    con = get_db()
    if cluster:
        rows = con.execute(
            "SELECT * FROM job_history WHERE cluster=? AND board_visible=1 ORDER BY id DESC",
            (cluster,)
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT * FROM job_history WHERE board_visible=1 ORDER BY id DESC"
        ).fetchall()
    con.close()
    return [normalize_job_times_local(dict(r)) for r in rows]


def dismiss_job(cluster, job_id):
    con = get_db()
    con.execute(
        "UPDATE job_history SET board_visible=0 WHERE cluster=? AND job_id=?",
        (cluster, job_id)
    )
    con.commit()
    con.close()


def dismiss_all(cluster):
    con = get_db()
    con.execute(
        "UPDATE job_history SET board_visible=0 WHERE cluster=?",
        (cluster,)
    )
    con.commit()
    con.close()


def dismiss_by_state_prefix(cluster, prefixes):
    con = get_db()
    if not prefixes:
        con.close()
        return
    where = " OR ".join(["state LIKE ?"] * len(prefixes))
    args = [cluster] + [f"{p}%" for p in prefixes]
    con.execute(
        f"UPDATE job_history SET board_visible=0 WHERE cluster=? AND ({where})",
        args
    )
    con.commit()
    con.close()


def get_history(cluster=None, limit=200):
    con = get_db()
    if cluster and cluster != "all":
        rows = con.execute(
            "SELECT * FROM job_history WHERE cluster=? ORDER BY id DESC LIMIT ?",
            (cluster, limit)
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT * FROM job_history ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
    con.close()
    return [dict(r) for r in rows]


# ─── SSH helpers ────────────────────────────────────────────────────────────

def _ssh_client(cluster_name):
    cfg = CLUSTERS[cluster_name]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        cfg["host"], port=cfg["port"], username=cfg["user"],
        key_filename=cfg["key"],
        timeout=SSH_TIMEOUT, banner_timeout=SSH_TIMEOUT, auth_timeout=SSH_TIMEOUT,
    )
    # Keepalive helps long-lived reused connections remain healthy through NAT/firewalls.
    try:
        client.get_transport().set_keepalive(30)
    except Exception:
        pass
    return client


def _get_cluster_lock(cluster_name):
    with _ssh_pool_lock:
        if cluster_name not in _ssh_cluster_locks:
            _ssh_cluster_locks[cluster_name] = threading.Lock()
        return _ssh_cluster_locks[cluster_name]


def _get_pooled_client(cluster_name, force_new=False):
    now = time.monotonic()
    with _ssh_pool_lock:
        if not force_new:
            rec = _ssh_pool.get(cluster_name)
            if rec:
                client = rec["client"]
                try:
                    tr = client.get_transport()
                    if tr and tr.is_active():
                        rec["last_used"] = now
                        return client
                except Exception:
                    pass
                # stale/broken client
                try:
                    client.close()
                except Exception:
                    pass
                _ssh_pool.pop(cluster_name, None)

        client = _ssh_client(cluster_name)
        _ssh_pool[cluster_name] = {"client": client, "last_used": now}
        return client


def _close_cluster_client(cluster_name):
    with _ssh_pool_lock:
        rec = _ssh_pool.pop(cluster_name, None)
    if rec:
        try:
            rec["client"].close()
        except Exception:
            pass


def _ssh_pool_gc_loop():
    while True:
        now = time.monotonic()
        stale = []
        with _ssh_pool_lock:
            for cluster, rec in list(_ssh_pool.items()):
                if now - rec.get("last_used", 0) > SSH_IDLE_TTL_SEC:
                    stale.append(cluster)
        for cluster in stale:
            _close_cluster_client(cluster)
        time.sleep(30)


def ssh_run(cluster_name, command):
    lock = _get_cluster_lock(cluster_name)
    with lock:
        # Retry once with a fresh client if the pooled one fails.
        for attempt in (1, 2):
            client = _get_pooled_client(cluster_name, force_new=(attempt == 2))
            try:
                _, stdout, stderr = client.exec_command(command, timeout=SSH_TIMEOUT)
                out = stdout.read().decode().strip()
                err = stderr.read().decode().strip()
                with _ssh_pool_lock:
                    rec = _ssh_pool.get(cluster_name)
                    if rec:
                        rec["last_used"] = time.monotonic()
                return out, err
            except Exception:
                _close_cluster_client(cluster_name)
                if attempt == 2:
                    raise


def ssh_run_with_timeout(cluster_name, command, timeout_sec=20):
    lock = _get_cluster_lock(cluster_name)
    with lock:
        for attempt in (1, 2):
            client = _get_pooled_client(cluster_name, force_new=(attempt == 2))
            try:
                _, stdout, stderr = client.exec_command(command, timeout=timeout_sec)
                out = stdout.read().decode().strip()
                err = stderr.read().decode().strip()
                with _ssh_pool_lock:
                    rec = _ssh_pool.get(cluster_name)
                    if rec:
                        rec["last_used"] = time.monotonic()
                return out, err
            except Exception:
                _close_cluster_client(cluster_name)
                if attempt == 2:
                    raise


def get_job_stats(cluster, job_id):
    """Best-effort job resource stats for popup."""
    if cluster == "local":
        return {"status": "error", "error": "Stats popup is supported for Slurm clusters only."}

    try:
        # Allocation + node list
        sq, _ = ssh_run_with_timeout(
            cluster,
            f"squeue -j {job_id} -h -o '%T|%D|%C|%b|%N|%M'",
            timeout_sec=10,
        )
        if not sq:
            # Fallback: scontrol for jobs not shown in squeue output window.
            sctl, _ = ssh_run_with_timeout(cluster, f"scontrol show job {job_id} 2>/dev/null", timeout_sec=10)
            if not sctl:
                return {"status": "error", "error": "Job not in queue anymore. Check history/logs."}
            tokens = sctl.replace("\n", " ").split()
            kv = {}
            for t in tokens:
                if "=" in t:
                    k, v = t.split("=", 1)
                    kv[k] = v
            state = kv.get("JobState", "")
            nodes = kv.get("NumNodes", "")
            cpus = kv.get("NumCPUs", "")
            gres = kv.get("TresPerNode", kv.get("Gres", ""))
            node_list = kv.get("NodeList", "")
            elapsed = kv.get("RunTime", "")
        else:
            state, nodes, cpus, gres, node_list, elapsed = (sq.split("|") + [""] * 6)[:6]

        # Accounting stats (can be empty on some clusters)
        sstat_out, _ = ssh_run_with_timeout(
            cluster,
            f"sstat -j {job_id}.batch --noheader -P --format=AveCPU,AveRSS,MaxRSS,MaxVMSize 2>/dev/null | head -1",
            timeout_sec=10,
        )
        ave_cpu, ave_rss, max_rss, max_vms = (sstat_out.split("|") + ["", "", "", ""])[:4] if sstat_out else ("", "", "", "")

        # TRES usage fallback (often available even when direct nvidia-smi probing isn't)
        tres_ave, _ = ssh_run_with_timeout(
            cluster,
            f"sstat -j {job_id}.batch --noheader -P --format=TresUsageInAve,TresUsageInMax 2>/dev/null | head -1",
            timeout_sec=10,
        )
        tres_usage_text = tres_ave.strip()

        def _extract_tres_value(text, key):
            # e.g. "...gres/gpuutil=72.5,gres/gpumem=12345M,..."
            if not text:
                return ""
            for token in text.replace(" ", "").split(","):
                if token.startswith(key + "="):
                    return token.split("=", 1)[1]
            return ""

        gpuutil_ave = _extract_tres_value(tres_usage_text, "gres/gpuutil")
        gpumem_ave = _extract_tres_value(tres_usage_text, "gres/gpumem")

        # GPU live utilization (best-effort; only for running jobs with GPUs)
        gpu_rows = []
        gpu_probe_error = ""
        if "gpu" in (gres or "").lower() and state in ("RUNNING", "COMPLETING"):
            gpu_cmd = (
                f"srun --jobid {job_id} -N1 -n1 --overlap "
                "bash -lc \"nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total "
                "--format=csv,noheader,nounits\" 2>/dev/null | head -16"
            )
            gpu_out, gpu_err = ssh_run_with_timeout(cluster, gpu_cmd, timeout_sec=20)
            for line in gpu_out.splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 5:
                    gpu_rows.append(
                        {
                            "index": parts[0],
                            "name": parts[1],
                            "util": parts[2] + "%",
                            "mem": f"{parts[3]}/{parts[4]} MiB",
                        }
                    )
            if not gpu_rows and gpu_err:
                gpu_probe_error = gpu_err

        # If per-GPU rows unavailable, provide aggregated fallback from TRES.
        gpu_summary = ""
        if not gpu_rows:
            if gpuutil_ave or gpumem_ave:
                gpu_summary = f"Ave GPU util: {gpuutil_ave or 'n/a'} | Ave GPU mem: {gpumem_ave or 'n/a'}"
            elif "gpu" in (gres or "").lower() and state in ("RUNNING", "COMPLETING"):
                gpu_summary = "Per-GPU probe unavailable on this cluster/job (srun/nvidia-smi restricted)."
            elif "gpu" in (gres or "").lower():
                gpu_summary = "GPU job is not currently running; live per-GPU stats unavailable."

        return {
            "status": "ok",
            "job_id": job_id,
            "state": state,
            "elapsed": elapsed,
            "nodes": nodes,
            "cpus": cpus,
            "gres": gres,
            "node_list": node_list,
            "ave_cpu": ave_cpu,
            "ave_rss": ave_rss,
            "max_rss": max_rss,
            "max_vmsize": max_vms,
            "gpuutil_ave": gpuutil_ave,
            "gpumem_ave": gpumem_ave,
            "gpu_summary": gpu_summary,
            "gpu_probe_error": gpu_probe_error,
            "gpus": gpu_rows,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def fetch_log_tail(cluster_name, log_path, lines=150):
    """Read from sshfs mount first, then SSH fallback."""
    try:
        if cluster_name == "local":
            if str(log_path).startswith("procfd://"):
                try:
                    tail = str(log_path)[len("procfd://"):]
                    pid, fd = tail.split("/", 1)
                    return _read_local_procfd_snapshot(pid, fd, lines=lines)
                except Exception:
                    return "Invalid local process stream path."
            result = subprocess.run(["tail", f"-n{lines}", log_path],
                                    capture_output=True, text=True, timeout=5)
            return result.stdout or result.stderr or "(empty file)"
        mounted = _resolve_mounted_path(cluster_name, log_path, want_dir=False)
        if mounted:
            return _tail_local_file(mounted, lines)
        cmd = f"[ -f '{log_path}' ] && tail -n {lines} '{log_path}' || echo '__NOT_FOUND__'"
        out, _ = ssh_run(cluster_name, cmd)
        if "__NOT_FOUND__" in out:
            return f"File not found on cluster:\n{log_path}"
        return out or "(empty file)"
    except Exception as e:
        return f"Could not read log: {e}"


def _label_log(name):
    n = name.lower()
    if "main" in n and "srun" in n:    return "main output"
    if "server" in n and "srun" in n:  return "server output"
    if "sandbox" in n and "srun" in n: return "sandbox output"
    if "sbatch" in n:                  return "sbatch log"
    if n.endswith(".out"):             return "stdout"
    if n.endswith(".err"):             return "stderr"
    return name


def get_job_log_files(cluster_name, job_id):
    """
    Discover log files for a job in a single SSH round-trip.

    nemo-run log layout (confirmed):
      sbatch scripts: <nemo_run_base>/eval/eval_<id>/nemo-run*_sbatch.sh
      log files declared in sbatch as:
        #SBATCH --output=<output_dir>/eval-logs/<name>_<job_id>_sbatch.log
        srun --output <output_dir>/eval-logs/{main,server,sandbox}_<name>_<job_id>_srun.log

    Strategy:
      1. scontrol (live jobs) → get StdOut path → derive log dir
      2. grep nemo-run sbatch scripts for job_id → get log dir
      3. list all *job_id* files in that log dir
    """
    if cluster_name == "local":
        return _local_job_log_files(job_id)

    # Mount-first path: avoid SSH roundtrip when filesystem is mounted.
    mount_result = _discover_job_logs_from_mount(cluster_name, str(job_id))
    if mount_result and mount_result.get("files"):
        return mount_result

    user = CLUSTERS[cluster_name]["user"]

    script = f"""#!/bin/sh
JOB={job_id}
USER={user}

emit() {{ echo "FILE:$1:$2"; }}

LOGDIR=""

# ── 1. scontrol (live/recent jobs) ────────────────────────────────────────
SCTL=$(scontrol show job "$JOB" 2>/dev/null)
if [ -n "$SCTL" ]; then
  STDOUT=$(echo "$SCTL" | tr ' ' '\\n' | grep '^StdOut=' | cut -d= -f2- | sed "s/%j/$JOB/g")
  if [ -n "$STDOUT" ]; then
    LOGDIR=$(dirname "$STDOUT")
  fi
fi

# ── 2. Grep nemo-run sbatch scripts for this job_id ──────────────────────
# The sbatch scripts contain the output path in a #SBATCH --output= line.
# We search the most-recently-modified sbatch scripts first.
if [ -z "$LOGDIR" ]; then
  for NEMO_BASE in {" ".join(NEMO_RUN_BASES)}; do
    [ -d "$NEMO_BASE" ] || continue
    # Find sbatch scripts that contain this job_id (means the job ran from that experiment)
    SBATCH=$(find "$NEMO_BASE" -maxdepth 5 -name "*sbatch.sh" 2>/dev/null \\
             | xargs grep -l "$JOB" 2>/dev/null | head -1)
    if [ -z "$SBATCH" ]; then
      # Fallback: newest sbatch scripts (job_id won't be in them but output path is consistent)
      SBATCH=$(find "$NEMO_BASE" -maxdepth 5 -name "*sbatch.sh" 2>/dev/null \\
               | sort -t_ -k1 -r | head -1)
    fi
    if [ -n "$SBATCH" ]; then
      OUT_LINE=$(grep '#SBATCH --output=' "$SBATCH" | head -1)
      OUT_PATH=$(echo "$OUT_LINE" | sed 's/.*--output=//' | sed "s/%j/$JOB/g" | tr -d ' ')
      [ -n "$OUT_PATH" ] && LOGDIR=$(dirname "$OUT_PATH")
      break
    fi
  done
fi

# ── 3. List matching files in log dir ─────────────────────────────────────
if [ -n "$LOGDIR" ] && [ -d "$LOGDIR" ]; then
  find "$LOGDIR" -maxdepth 1 -type f -name "*$JOB*" 2>/dev/null | sort | while read F; do
    emit "$(basename "$F")" "$F"
  done
else
  # Last resort: search user root for *job_id*.log files
  for ROOT in {" ".join(LOG_SEARCH_BASES)}; do
    [ -d "$ROOT" ] || continue
    find "$ROOT" -maxdepth 8 -type f -name "*$JOB*.log" 2>/dev/null | head -20 | while read F; do
      emit "$(basename "$F")" "$F"
    done
    break
  done
fi
"""

    try:
        client = _ssh_client(cluster_name)
        _, stdout, _ = client.exec_command(script, timeout=25)
        out = stdout.read().decode().strip()
        client.close()
    except Exception as e:
        return {"files": [], "dirs": [], "error": f"SSH error: {e}"}

    seen = set()
    files = []
    ORDER = {"main output": 0, "server output": 1, "sandbox output": 2,
             "sbatch log": 3, "sbatch stderr": 4}

    allowed_suffixes = (".log", ".out", ".err", ".txt", ".json", ".jsonl", ".jsonl-async", ".md")
    for line in out.splitlines():
        if not line.startswith("FILE:"):
            continue
        parts = line[5:].split(":", 1)
        if len(parts) != 2:
            continue
        raw_label, path = parts[0].strip(), parts[1].strip()
        if not path or path in seen:
            continue
        # Strict filter: only real log/result-like files
        lower_path = path.lower()
        if not lower_path.endswith(allowed_suffixes):
            continue
        seen.add(path)
        files.append({"label": _label_log(raw_label), "path": path})

    files.sort(key=lambda f: ORDER.get(f["label"], 10))

    # Also return the root output dirs for the file explorer
    # Derive output_dir from first file path (parent of eval-logs/)
    dirs = []
    if files:
        log_dir = os.path.dirname(files[0]["path"])
        output_dir = os.path.dirname(log_dir)
        # Check if eval-logs and eval-results exist
        for dname in ["eval-logs", "eval-results"]:
            dpath = output_dir.rstrip("/") + "/" + dname
            dirs.append({"label": dname, "path": dpath})

    return {"files": files, "dirs": dirs}


def get_job_log_files_cached(cluster_name, job_id, force=False):
    key = (cluster_name, str(job_id))
    if not force:
        cached = _cache_get(_log_index_cache, key, LOG_INDEX_TTL_SEC)
        if cached is not None:
            return cached
    value = get_job_log_files(cluster_name, str(job_id))
    _cache_set(_log_index_cache, key, value)
    return value


def get_job_stats_cached(cluster, job_id, force=False):
    key = (cluster, str(job_id))
    if not force:
        cached = _cache_get(_stats_cache, key, STATS_TTL_SEC)
        # If cached payload is partial (from bulk prefetch), refresh on demand.
        if cached is not None and not cached.get("_partial"):
            return cached
    value = get_job_stats(cluster, str(job_id))
    _cache_set(_stats_cache, key, value)
    return value


# ─── Job fetching ────────────────────────────────────────────────────────────

def parse_squeue_output(out):
    jobs = []
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < len(SQUEUE_HDR):
            parts += [""] * (len(SQUEUE_HDR) - len(parts))
        jobs.append(dict(zip(SQUEUE_HDR, parts)))
    jobs.sort(key=lambda j: STATE_ORDER.get(j.get("state", "").upper(), 99))
    return jobs


def fetch_jobs_remote(cluster_name):
    out, _ = ssh_run(cluster_name, f"squeue -u $USER --noheader -o '{SQUEUE_FMT}'")
    return parse_squeue_output(out)


def fetch_jobs_local():
    """Fetch local slurm jobs; fall back to relevant local processes."""
    # Try local squeue first
    try:
        result = subprocess.run(
            ["squeue", "-u", os.environ.get("USER", DEFAULT_USER),
             "--noheader", f"-o{SQUEUE_FMT}"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            return parse_squeue_output(result.stdout.strip())
    except FileNotFoundError:
        pass

    # Fall back: show only NeMo-related local processes
    result = subprocess.run(
        ["ps", "aux", "--sort=-%cpu"],
        capture_output=True, text=True, timeout=5
    )
    jobs = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split(None, 10)
        if len(parts) < 11:
            continue
        cmd = parts[10]
        cmd_l = cmd.lower()
        include_tokens = LOCAL_PROC_INCLUDE
        exclude_tokens = [APP_ROOT.lower()] + LOCAL_PROC_EXCLUDE
        if any(t in cmd_l for t in include_tokens) and not any(t in cmd_l for t in exclude_tokens):
            jobs.append({
                "jobid": parts[1],  # PID
                "name": cmd[:60],
                "state": "RUNNING",
                "reason": "",
                "elapsed": parts[9],
                "timelimit": "—",
                "nodes": "1",
                "cpus": parts[1],
                "gres": "local",
                "partition": "local",
                "submitted": "—",
                "log": "",
            })
    return jobs[:20]


def fetch_cluster_data(cluster_name):
    try:
        if cluster_name == "local":
            jobs = fetch_jobs_local()
        else:
            jobs = fetch_jobs_remote(cluster_name)
        return {"status": "ok", "jobs": jobs, "updated": datetime.now().isoformat()}
    except Exception as e:
        return {"status": "error", "error": str(e), "jobs": [], "updated": datetime.now().isoformat()}


def sacct_final(cluster_name, job_id):
    """Get final job info from sacct after it leaves squeue."""
    try:
        fmt = "JobID,JobName,State,ExitCode,Elapsed,Start,End"
        if cluster_name == "local":
            result = subprocess.run(
                ["sacct", "-j", job_id, f"--format={fmt}", "--noheader", "-P"],
                capture_output=True, text=True, timeout=5
            )
            out = result.stdout.strip()
        else:
            out, _ = ssh_run(cluster_name, f"sacct -j {job_id} --format={fmt} --noheader -P 2>/dev/null | head -1")
        if not out:
            return {}
        parts = out.split("|")
        keys = ["jobid", "name", "state", "exit_code", "elapsed", "started", "ended_at"]
        return dict(zip(keys, parts + [""] * len(keys)))
    except Exception:
        return {}


# ─── Background poller ───────────────────────────────────────────────────────

def _dithered(base_sec):
    """Apply ±20% random jitter so clusters don't all poll at the same instant."""
    return base_sec * random.uniform(0.8, 1.2)


def poll_loop():
    # Adaptive polling with dither to stay friendly to login nodes:
    #   active jobs  ~2 min   |  idle  ~15 min  |  unreachable  ~30 min
    for name in CLUSTERS:
        _last_polled.setdefault(name, 0.0)

    while True:
        now = time.monotonic()
        to_poll = []

        with _cache_lock:
            snapshot = {k: dict(v) for k, v in _cache.items()}

        for name in CLUSTERS:
            data = snapshot.get(name, {})
            status = data.get("status")
            jobs = data.get("jobs", [])
            has_active = any(
                (j.get("state") in ("RUNNING", "COMPLETING", "PENDING")) and not j.get("_pinned")
                for j in jobs
            )

            if not data:
                interval = _dithered(10)
            elif status != "ok":
                interval = _dithered(POLL_UNREACHABLE_SEC)
            elif has_active:
                interval = _dithered(POLL_ACTIVE_SEC)
            else:
                interval = _dithered(POLL_IDLE_SEC)

            if now - _last_polled.get(name, 0.0) >= interval:
                to_poll.append(name)
                _last_polled[name] = now

        if to_poll:
            threads = []
            for name in to_poll:
                t = threading.Thread(target=poll_cluster, args=(name,), daemon=True)
                threads.append(t)
                t.start()
            for t in threads:
                t.join(timeout=SSH_TIMEOUT + 5)

        time.sleep(5)


def poll_cluster(name):
    data = fetch_cluster_data(name)
    current_ids = {j["jobid"] for j in data.get("jobs", [])}

    with _cache_lock:
        prev_ids = _seen_jobs.get(name, set())
        prev_jobs = {j["jobid"]: j for j in _cache.get(name, {}).get("jobs", [])}
        _cache[name] = data
        _seen_jobs[name] = current_ids

    # Jobs that just disappeared from squeue
    gone_ids = prev_ids - current_ids
    for job_id in gone_ids:
        prev_job = prev_jobs.get(job_id, {})
        prev_state = prev_job.get("state", "").upper()
        final = sacct_final(name, job_id)
        final_state = (final.get("state", "") or prev_state).upper().split()[0]

        record = final if final else {
            "jobid": job_id,
            "name": prev_job.get("name", ""),
            "state": final_state or "COMPLETED",
            "elapsed": prev_job.get("elapsed", ""),
            "nodes": prev_job.get("nodes", ""),
            "gres": prev_job.get("gres", ""),
            "partition": prev_job.get("partition", ""),
            "submitted": prev_job.get("submitted", ""),
            "ended_at": datetime.now().isoformat(),
        }

        is_terminal = any(final_state.startswith(s) for s in PINNABLE_TERMINAL_STATES)
        upsert_job(name, record, terminal=is_terminal)

    # Keep DB up-to-date for currently live jobs (skip local — PIDs are noise)
    if name != "local":
        for job in data.get("jobs", []):
            upsert_job(name, job, terminal=False)


# ─── Flask routes ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", clusters=CLUSTERS)


@app.route("/api/jobs")
def api_jobs():
    with _cache_lock:
        snapshot = {k: dict(v) for k, v in _cache.items()}

    # For each cluster, merge in DB-pinned terminal jobs
    all_pinned = get_board_pinned()  # jobs with board_visible=1
    pinned_by_cluster = {}
    for row in all_pinned:
        c = row["cluster"]
        pinned_by_cluster.setdefault(c, []).append(row)

    for name in list(CLUSTERS.keys()):
        if name not in snapshot:
            snapshot[name] = {"status": "ok", "jobs": [], "updated": None}
        data = snapshot[name]
        if data.get("status") != "ok":
            continue
        live_ids = {j["jobid"] for j in data.get("jobs", [])}
        pinned = [
            {**p, "_pinned": True,
             "jobid": p["job_id"], "name": p["job_name"]}
            for p in pinned_by_cluster.get(name, [])
            if p["job_id"] not in live_ids
        ]
        if pinned:
            data["jobs"] = data.get("jobs", []) + pinned
        # Normalize displayed times to this machine's clock
        data["jobs"] = [normalize_job_times_local(j) for j in data.get("jobs", [])]
        # Attach cached progress percentage for running jobs
        for j in data.get("jobs", []):
            if j.get("state", "").upper() == "RUNNING":
                pct = _cache_get(_progress_cache, (name, j.get("jobid")), PROGRESS_TTL_SEC)
                if pct is not None:
                    j["progress"] = pct

    # Sort: clusters with live running/pending first, empty/errored last
    def cluster_sort_key(item):
        name, data = item
        jobs = data.get("jobs", [])
        has_running = any(j.get("state") in ("RUNNING", "COMPLETING") for j in jobs if not j.get("_pinned"))
        has_pending = any(j.get("state") == "PENDING" for j in jobs if not j.get("_pinned"))
        has_live    = any(not j.get("_pinned") for j in jobs)
        return (not has_running, not has_pending, not has_live, name)

    ordered = dict(sorted(snapshot.items(), key=cluster_sort_key))

    # Async warm-up for jobs users are most likely to inspect soon
    for c, d in ordered.items():
        if d.get("status") != "ok":
            continue
        active_jobs = [
            j for j in d.get("jobs", [])
            if str(j.get("state", "")).upper() in {"RUNNING", "PENDING", "COMPLETING"}
            and not j.get("_pinned")
        ][:3]
        for j in active_jobs:
            _schedule_prefetch(c, j.get("jobid"))

    mounts = _all_mount_status()
    for c, d in ordered.items():
        if c != "local":
            d["mount"] = mounts.get(c, {"mounted": False, "root": ""})
    return jsonify(ordered)


@app.route("/api/mounts")
def api_mounts():
    cluster = request.args.get("cluster", "all")
    if cluster != "all":
        if cluster not in CLUSTERS or cluster == "local":
            return jsonify({"status": "error", "error": "Unknown cluster"}), 404
        return jsonify({"status": "ok", "mounts": {cluster: _cluster_mount_status(cluster)}})
    return jsonify({"status": "ok", "mounts": _all_mount_status()})


@app.route("/api/mount/<action>/<cluster>", methods=["POST"])
def api_mount_action(action, cluster):
    ok, msg = _run_mount_script(action, cluster)
    if not ok:
        return jsonify({"status": "error", "error": msg}), 400
    mounts = _all_mount_status()
    return jsonify({"status": "ok", "message": msg, "mounts": mounts})


@app.route("/api/mount/<action>", methods=["POST"])
def api_mount_action_all(action):
    ok, msg = _run_mount_script(action, "all")
    if not ok:
        return jsonify({"status": "error", "error": msg}), 400
    mounts = _all_mount_status()
    return jsonify({"status": "ok", "message": msg, "mounts": mounts})


@app.route("/api/clear_failed/<cluster>", methods=["POST"])
def api_clear_failed(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    dismiss_by_state_prefix(cluster, list(TERMINAL_STATES))
    return jsonify({"status": "ok"})


@app.route("/api/clear_completed/<cluster>", methods=["POST"])
def api_clear_completed(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    dismiss_by_state_prefix(cluster, ["COMPLETED"])
    return jsonify({"status": "ok"})


@app.route("/api/clear_failed_job/<cluster>/<job_id>", methods=["POST"])
def api_clear_failed_job(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    dismiss_job(cluster, job_id)
    return jsonify({"status": "ok"})


@app.route("/api/jobs/<cluster>")
def api_jobs_cluster(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    data = fetch_cluster_data(cluster)
    with _cache_lock:
        _cache[cluster] = data
    if data.get("status") == "ok":
        live_ids = {j["jobid"] for j in data.get("jobs", [])}
        pinned = [
            {**p, "_pinned": True, "jobid": p["job_id"], "name": p["job_name"]}
            for p in get_board_pinned(cluster)
            if p["job_id"] not in live_ids
        ]
        if pinned:
            data = dict(data)
            data["jobs"] = data.get("jobs", []) + pinned
        data["jobs"] = [normalize_job_times_local(j) for j in data.get("jobs", [])]

        for j in data.get("jobs", []):
            s = str(j.get("state", "")).upper()
            if s in {"RUNNING", "PENDING", "COMPLETING"} and not j.get("_pinned"):
                _schedule_prefetch(cluster, j.get("jobid"))
            if s == "RUNNING":
                pct = _cache_get(_progress_cache, (cluster, j.get("jobid")), PROGRESS_TTL_SEC)
                if pct is not None:
                    j["progress"] = pct
    if cluster != "local":
        data["mount"] = _cluster_mount_status(cluster)
    return jsonify(data)


@app.route("/api/prefetch_visible", methods=["POST"])
def api_prefetch_visible():
    """
    Batch warm-up from one browser call.
    Payload:
      {"jobs":[{"cluster":"ord","job_id":"123"},{"cluster":"ord","job_id":"124"}, ...]}
    """
    payload = request.get_json(silent=True) or {}
    jobs = payload.get("jobs", [])
    by_cluster = {}
    for item in jobs:
        c = item.get("cluster")
        jid = str(item.get("job_id", "")).strip()
        if not c or not jid or c not in CLUSTERS:
            continue
        by_cluster.setdefault(c, []).append(jid)

    # Run in background so request returns quickly
    def _run():
        threads = []
        for c, ids in by_cluster.items():
            t = threading.Thread(target=prefetch_cluster_bulk, args=(c, ids), daemon=True)
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=25)

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "ok", "clusters": list(by_cluster.keys()), "jobs": sum(len(v) for v in by_cluster.values())})


@app.route("/api/cancel/<cluster>/<job_id>", methods=["POST"])
def api_cancel(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    try:
        if cluster == "local":
            os.kill(int(job_id), 15)
            return jsonify({"status": "ok"})
        ssh_run(cluster, f"scancel {job_id}")
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})


@app.route("/api/cancel_all/<cluster>", methods=["POST"])
def api_cancel_all(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    try:
        if cluster == "local":
            return jsonify({"status": "error", "error": "Not supported for local"})
        ssh_run(cluster, "scancel -u $USER")
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})


@app.route("/api/stats/<cluster>/<job_id>")
def api_stats(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    return jsonify(get_job_stats_cached(cluster, job_id))


@app.route("/api/history")
def api_history():
    cluster = request.args.get("cluster", "all")
    limit = int(request.args.get("limit", 200))
    return jsonify(get_history(cluster, limit))


@app.route("/api/log_files/<cluster>/<job_id>")
def api_log_files(cluster, job_id):
    """Return list of log files and explorable root dirs for a job."""
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "files": [], "dirs": [], "error": "Unknown cluster"}), 404
    result = get_job_log_files_cached(cluster, job_id)
    files = []
    for f in result.get("files", []):
        p = f.get("path", "")
        mounted = _resolve_mounted_path(cluster, p, want_dir=False) if (p and cluster != "local") else ""
        if cluster == "local":
            source_hint = "local"
        else:
            source_hint = "mount" if mounted else "ssh"
        files.append({**f, "source_hint": source_hint, "mounted_path": mounted})
    dirs = []
    for d in result.get("dirs", []):
        p = d.get("path", "")
        mounted = _resolve_mounted_path(cluster, p, want_dir=True) if (p and cluster != "local") else ""
        if cluster == "local":
            source_hint = "local"
        else:
            source_hint = "mount" if mounted else "ssh"
        dirs.append({**d, "source_hint": source_hint, "mounted_path": mounted})
    return jsonify({
        "status": "ok",
        "files": files,
        "dirs": dirs,
        "error": result.get("error", "")
    })


@app.route("/api/ls/<cluster>")
def api_ls(cluster):
    """List directory contents on a cluster."""
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    path = request.args.get("path", "")
    force = request.args.get("force", "0") == "1"
    if not path:
        return jsonify({"status": "error", "error": "No path provided"}), 400
    cache_key = (cluster, path)
    if not force:
        cached = _cache_get(_dir_list_cache, cache_key, DIR_LIST_TTL_SEC)
        if cached is not None:
            return jsonify(cached)
    try:
        if cluster == "local":
            entries = _list_local_dir(path)
            payload = {"status": "ok", "path": path, "entries": entries, "source": "local", "resolved_path": path}
            _cache_set(_dir_list_cache, cache_key, payload)
            _prefetch_nested_dir_cache_local(cluster, path, path, entries)
            return jsonify(payload)
        mounted_dir = _resolve_mounted_path(cluster, path, want_dir=True)
        if mounted_dir:
            entries = _list_local_dir(mounted_dir)
            payload = {"status": "ok", "path": path, "entries": entries, "source": "mount", "resolved_path": mounted_dir}
            _cache_set(_dir_list_cache, cache_key, payload)
            _prefetch_nested_dir_cache_local(cluster, path, mounted_dir, entries)
            return jsonify(payload)
        # Single SSH call: list dir with type and size
        cmd = f"""ls -la '{path}' 2>/dev/null | tail -n +2 | awk '{{
  type = ($1 ~ /^d/) ? "d" : "f"
  size = $5
  name = $NF
  if (name != "." && name != "..") print type "|" size "|" name
}}'"""
        out, _ = ssh_run(cluster, cmd)
        entries = []
        for line in out.splitlines():
            parts = line.split("|", 2)
            if len(parts) != 3:
                continue
            ftype, size, name = parts
            full_path = path.rstrip("/") + "/" + name
            entries.append({
                "name": name,
                "path": full_path,
                "is_dir": ftype == "d",
                "size": int(size) if size.isdigit() else None,
            })
        payload = {"status": "ok", "path": path, "entries": entries, "source": "ssh", "resolved_path": path}
        _cache_set(_dir_list_cache, cache_key, payload)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})


@app.route("/api/log/<cluster>/<job_id>")
def api_log(cluster, job_id):
    """Return the content of a specific log file."""
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404

    lines = int(request.args.get("lines", 150))
    log_path = request.args.get("path", "")
    force = request.args.get("force", "0") == "1"

    if not log_path:
        # Auto-pick best file: prefer "main output" over sbatch stdout
        result = get_job_log_files_cached(cluster, job_id)
        files = result["files"]
        if not files:
            return jsonify({"status": "error", "error": "No log files found for this job."})
        preferred = next((f for f in files if "main" in f["label"]), None)
        chosen = preferred or files[0]
        log_path = chosen["path"]

    if not log_path:
        return jsonify({"status": "error", "error": "No log path available."})

    cache_key = (cluster, str(job_id), log_path)
    # For running/pending jobs, allow short-lived cache for snappy modal opens.
    cached = None if force else _cache_get(_log_content_cache, cache_key, LOG_CONTENT_TTL_SEC)
    source = "cache"
    resolved_path = log_path
    if cached is not None:
        content = cached
    else:
        if cluster != "local":
            mounted = _resolve_mounted_path(cluster, log_path, want_dir=False)
            if mounted:
                content = _tail_local_file(mounted, lines)
                source = "mount"
                resolved_path = mounted
            else:
                content = fetch_log_tail(cluster, log_path, lines)
                source = "ssh"
        else:
            content = fetch_log_tail(cluster, log_path, lines)
            source = "local"
        _cache_set(_log_content_cache, cache_key, content)
        pct = extract_progress(content)
        if pct is not None:
            _cache_set(_progress_cache, (cluster, str(job_id)), pct)
    return jsonify({
        "status": "ok",
        "log_path": log_path,
        "content": content,
        "source": source,
        "resolved_path": resolved_path,
    })


if __name__ == "__main__":
    init_db()
    # Mark recent terminal jobs as board_visible=1 so they appear on board after restart
    con = get_db()
    terminal_like = " OR ".join(f"state LIKE '{s}%'" for s in PINNABLE_TERMINAL_STATES)
    con.execute(f"""
        UPDATE job_history SET board_visible=1
        WHERE board_visible IS NULL
          AND ({terminal_like})
          AND cluster != 'local'
          AND (ended_at >= datetime('now', '-3 days') OR ended_at IS NULL)
    """)
    con.commit()
    con.close()

    # Background cleanup for idle pooled SSH connections.
    threading.Thread(target=_ssh_pool_gc_loop, daemon=True).start()

    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=APP_PORT, debug=False)
