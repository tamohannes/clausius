# Job Monitor

Lightweight Flask dashboard for monitoring, exploring, and managing Slurm jobs across multiple clusters and local runs. Includes an MCP server for AI agent integration.

## Features

### Live Board
- Multi-cluster job board grouped by run name (`active`, `idle`, `unreachable`, `local`)
- Slurm dependency chain detection — parent/child jobs linked with `afterok`, `afterany` badges
- Topological sorting within groups (parent first, children indented with arrows)
- Progress percentage display for running jobs
- Board-pinned terminal jobs (COMPLETED, FAILED, CANCELLED, COMPLETING) persist until manually dismissed
- Job actions: cancel one/all, dismiss pinned runs, clear failed/completed

### Log Explorer
- Mount-first reads with SSH fallback
- Nested directory browsing with lazy-loaded tree
- Concurrent SSH channels for parallel log/dir fetches
- Syntax-aware rendering for `.json`, `.jsonl`, `.jsonl-async`, `.md`
- JSONL record viewer with expand/collapse all and per-record copy
- Copy file path + content to clipboard

### History
- SQLite-backed job history with grouped view (related pipeline jobs together)
- Dependency arrows for child jobs (judge, summarize-results)
- Pagination by run groups (50 groups per page)
- Filterable by cluster and job name/ID
- GPU count and full job name display

### Stats
- GPU/CPU/memory utilization popup for running jobs
- TRES-based GPU metrics fallback when direct probing is unavailable

### Settings (UI)
- Accessible via user button at the bottom of the sidebar
- Modal with left-nav sections:
  - **Refresh** — auto-refresh toggle + interval (default: off, on-demand only)
  - **Mounts** — mount/unmount all or individual clusters via SSHFS
  - **Clusters** — add/edit/remove cluster configs (hot-reloads without restart)
  - **Advanced** — SSH timeout, cache freshness, history page size
  - **Process Filters** — local process include/exclude keywords
- Backend settings persist to `config.json`, frontend settings to `localStorage`

### MCP Server (AI Agent API)
- Stdio-based MCP server (`mcp_server.py`) for Cursor and other MCP-compatible agents
- Tools: `list_jobs`, `get_job_log`, `list_log_files`, `get_job_stats`, `get_history`, `cancel_job`
- Resource: `jobs://summary` — quick cluster overview
- Wraps the Flask API — no SSH, no DB access, no duplicate logic

### Performance
- On-demand fetching: clusters are only polled when a user or agent requests data
- SSH connection pooling with concurrent channel multiplexing
- Per-cluster cache with configurable freshness TTL (default: 30s)
- Prefetch warming for running jobs (log index, first file content, stats)
- No background polling — login nodes are not contacted when nobody is looking

## Quick Start

```bash
cd ~/job-monitor
pip install flask paramiko
cp config.example.json config.json  # edit with your cluster details
python app.py
```

Open: [http://localhost:7272](http://localhost:7272)

### MCP Server Setup

```bash
pip install mcp
```

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "job-monitor": {
      "command": "python3",
      "args": ["/path/to/job-monitor/mcp_server.py"]
    }
  }
}
```

Reload Cursor to activate. Requires the Flask app to be running.

## Configuration

### config.json

Primary configuration file. Editable from the UI Settings panel or directly.

```json
{
  "port": 7272,
  "clusters": {
    "my-cluster": {
      "host": "login-node.example.com",
      "port": 22,
      "gpu_type": "H100",
      "remote_root": "/lustre"
    }
  },
  "log_search_bases": ["/lustre/.../users/$USER"],
  "nemo_run_bases": ["/lustre/.../users/$USER/nemo-run"],
  "mount_lustre_prefixes": ["lustre/fsw/..."],
  "local_process_filters": {
    "include": ["nemo-skills", "python -m nemo_skills"],
    "exclude": ["cursor", "jupyter"]
  },
  "ssh_timeout": 8,
  "cache_fresh_sec": 30
}
```

### Environment Variables

- `JOB_MONITOR_SSH_USER` (default: `$USER`)
- `JOB_MONITOR_SSH_KEY` (default: `~/.ssh/id_ed25519`)
- `JOB_MONITOR_MOUNT_MAP` (JSON map of cluster -> mount roots)

## Job Name Prefix Protocol

Jobs are grouped by project using a name prefix convention:

```
<project>_<run-name>
```

| Component | Rules | Example |
|-----------|-------|---------|
| `<project>` | Lowercase letters, digits, hyphens. Starts with a letter. | `artsiv`, `hle`, `nemo-rl` |
| `_` | Required underscore separator | — |
| `<run-name>` | The experiment/eval name | `eval-math`, `train-v3` |

Set in NeMo-Skills cluster config:

```yaml
job_name_prefix: "artsiv_"
```

The monitor auto-detects projects from the `word_` pattern on first encounter, assigning a color and emoji. Customize in Settings > Projects.

Run name suffixes for dependency chain auto-detection:
- `*-judge-rs<N>` — linked as child of the base eval
- `*-summarize-results` — linked as child of the judge run

## API Endpoints

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/jobs` | All clusters with jobs, mounts, dependency info |
| GET | `/api/jobs/<cluster>` | Force-refresh one cluster |
| GET | `/api/history?cluster=&limit=` | Job history |
| GET | `/api/log_files/<cluster>/<job_id>` | Discover log files |
| GET | `/api/log/<cluster>/<job_id>?path=&lines=` | Read log content |
| GET | `/api/ls/<cluster>?path=` | Directory listing |
| GET | `/api/stats/<cluster>/<job_id>` | Job resource stats |
| GET | `/api/mounts` | Mount status |
| GET | `/api/settings` | Current configuration |
| POST | `/api/settings` | Update configuration (hot-reload) |
| POST | `/api/mount/mount/<cluster>` | Mount one cluster |
| POST | `/api/mount/unmount/<cluster>` | Unmount one cluster |
| POST | `/api/cancel/<cluster>/<job_id>` | Cancel a job |
| POST | `/api/cancel_all/<cluster>` | Cancel all jobs on cluster |
| POST | `/api/clear_failed/<cluster>` | Dismiss failed pins |
| POST | `/api/clear_completed/<cluster>` | Dismiss completed pins |

## SSHFS Mount Helper

```bash
./scripts/sshfs_logs.sh status
./scripts/sshfs_logs.sh mount ord
./scripts/sshfs_logs.sh unmount ord
./scripts/sshfs_logs.sh mount      # all clusters
./scripts/sshfs_logs.sh unmount    # all clusters
```

## Systemd (User Service)

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

```bash
systemctl --user daemon-reload
systemctl --user enable --now job-monitor.service
```

## Testing

The test suite works in two modes:

- **GitHub / CI**: No `config.json` needed. Falls back to `config.example.json` automatically. All tests use a mock cluster (`mock-cluster`) with mocked SSH, so no real infrastructure is required.
- **Local dev**: `config.json` present with real clusters. Unit and integration tests still use the mock cluster. Live tests target the first real cluster from your config (or `TEST_CLUSTER` env var).

### Setup

```bash
pip install pytest pytest-cov
```

For frontend tests (requires Node.js):

```bash
npm install
```

### Running Tests

```bash
# All deterministic tests — safe for CI (no config.json, no SSH, no cluster)
pytest -m "not live"

# Unit tests only
pytest -m unit

# Integration tests (Flask test client with mock cluster)
pytest -m integration

# MCP contract + transport tests
pytest -m mcp

# Live cluster tests (local dev only — requires running app + SSH access)
pytest -m live

# Destructive live tests only
pytest -m "live and destructive"

# Frontend unit tests (requires Node.js)
npx vitest run

# E2E browser tests (requires Node.js + running app)
npx playwright test
```

### Test Architecture

| Layer | Directory | Count | What it covers | Needs config.json? |
|-------|-----------|-------|----------------|---------------------|
| Unit | `tests/unit/` | 133 | Parsers, DB ops, cache, mount resolution, config | No |
| Integration | `tests/integration/` | 69 | All Flask routes via test client + MCP boundary | No |
| MCP | `tests/mcp/` | 38 | Tool contracts, transport errors, edge cases | No |
| Frontend | `tests/frontend/` | — | JS utils, log renderers, history grouping (Vitest) | No |
| E2E | `tests/e2e/` | — | Dashboard, log explorer, history, settings (Playwright) | Yes (running app) |
| Live | `tests/live/` | 19 | Real SSH/Slurm reads + throwaway job cancel | Yes |

### How Mock vs Real Clusters Work

- A `mock-cluster` fixture auto-injects a fake cluster into `CLUSTERS` for every test. Unit and integration tests reference this instead of real cluster names.
- SSH calls are intercepted by the `mock_ssh` fixture which returns canned responses.
- The `first_real_cluster` fixture resolves the first non-local cluster from `config.json` for use in `local_cluster` marked tests. Skips automatically if no real config is present.
- Live tests pick their target cluster from `TEST_CLUSTER` env var, or auto-detect the first cluster in `config.json`.

### Environment Variables

- `TEST_CLUSTER` — override target cluster for live tests (default: first cluster in config.json)
- `TEST_APP_BASE` — app URL for live tests (default: `http://localhost:7272`)

## Security Notes

- No hard-coded paths or usernames in app logic
- No embedded secrets/tokens in repository files
- SSH key path is configurable via env
- `config.json` may contain hostnames — add to `.gitignore` if needed
- `.gitignore` should include: `history.db`, `__pycache__/`, `config.json`
