# Job Monitor

Lightweight Flask dashboard for monitoring, exploring, and managing Slurm jobs across multiple clusters and local runs.

## Features

- Live multi-cluster board with grouping (`active`, `idle`, `unreachable`, `local`)
- Job actions: cancel one/all, pin terminal runs, clear failed/completed
- Log explorer with:
  - mount-first + SSH fallback reads
  - nested directory browsing
  - copy file path + content
  - syntax-aware rendering for `.json`, `.jsonl`, `.jsonl-async`, `.md`
  - JSONL record viewer (expand/collapse all, per-record copy)
- Stats popup for CPU/GPU details
- SQLite-backed history
- SSH connection pooling + adaptive polling + prefetch caches
- Optional SSHFS mount controls from UI/API

## Quick Start

```bash
cd ~/job-monitor
python -m venv .venv
source .venv/bin/activate
pip install flask paramiko
python app.py
```

Open: [http://localhost:7272](http://localhost:7272)

## Configuration

Environment variables:

- `JOB_MONITOR_SSH_USER` (default: `$USER`)
- `JOB_MONITOR_SSH_KEY` (default: `~/.ssh/id_ed25519`)
- `JOB_MONITOR_MOUNT_MAP` (JSON map of cluster -> mount roots)

Example:

```bash
export JOB_MONITOR_SSH_USER="$USER"
export JOB_MONITOR_SSH_KEY="$HOME/.ssh/id_ed25519"
export JOB_MONITOR_MOUNT_MAP='{"ord":["~/.job-monitor/mounts/ord"],"hsg":["~/.job-monitor/mounts/hsg"]}'
python app.py
```

## SSHFS Mount Helper

The helper script manages per-cluster mounts:

```bash
./scripts/sshfs_logs.sh status
./scripts/sshfs_logs.sh mount ord
./scripts/sshfs_logs.sh unmount ord
./scripts/sshfs_logs.sh mount      # all clusters
./scripts/sshfs_logs.sh unmount    # all clusters
```

Optional env overrides for script:

- `JOB_MONITOR_SSH_USER`
- `JOB_MONITOR_SSH_KEY`

## Systemd (User Service)

Example user unit:

```ini
[Unit]
Description=Cluster Job Monitor
After=network.target

[Service]
Type=simple
WorkingDirectory=%h/job-monitor
ExecStart=%h/miniconda3/bin/python %h/job-monitor/app.py
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
```

Enable:

```bash
systemctl --user daemon-reload
systemctl --user enable --now job-monitor.service
```

## Public Release / Security Checklist

- No hard-coded personal home paths in app logic
- No hard-coded usernames in app logic (uses env/current user)
- No embedded secrets/tokens/API keys in repository files
- SSH key path is configurable via env
- Prefer key-based auth and avoid storing passwords
- Add `.gitignore` for runtime artifacts:
  - `history.db`
  - `__pycache__/`
  - local logs/tmp outputs

## Notes

- Some clusters may block `sshfs`/SFTP. In that case, UI shows `ssh-only` and falls back to SSH reads.
- Local-process logs depend on whether the process writes to files or streams.
