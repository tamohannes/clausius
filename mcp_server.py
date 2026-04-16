"""MCP server for clausius — HTTP proxy architecture.

All tool calls are forwarded to the gunicorn web service at localhost:7272.
This ensures the MCP process always uses the same code, caches, and SSH pools
as the web UI.  ``systemctl --user restart clausius.service`` immediately
updates both the browser UI and MCP tool behavior.

Only ``health_check`` runs locally (no HTTP dependency).
"""

import os
import sys
import time
from typing import Optional

from mcp.server.fastmcp import FastMCP
import httpx

BASE_URL = os.environ.get("CLAUSIUS_MCP_URL", "http://localhost:7272")
_client = httpx.Client(base_url=BASE_URL, timeout=180)

mcp = FastMCP("clausius")


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _api(method, path, **kwargs):
    """Call the clausius HTTP API with retry on connection failure."""
    last_exc = None
    for attempt in range(3):
        try:
            r = _client.request(method, path, **kwargs)
            if r.status_code == 503:
                if attempt < 2:
                    time.sleep(1)
                    continue
            r.raise_for_status()
            return r.json()
        except httpx.ConnectError as exc:
            last_exc = exc
            if attempt < 2:
                time.sleep(1)
        except httpx.HTTPStatusError as exc:
            try:
                return exc.response.json()
            except Exception:
                return {"status": "error", "error": str(exc)}
        except Exception as exc:
            return {"status": "error", "error": str(exc)}
    return {
        "status": "error",
        "error": f"clausius service unreachable after 3 attempts — run: systemctl --user restart clausius.service ({last_exc})",
    }


def _api_text(method, path, **kwargs):
    """Like _api but returns raw text instead of JSON."""
    last_exc = None
    for attempt in range(3):
        try:
            r = _client.request(method, path, **kwargs)
            r.raise_for_status()
            return r.text
        except httpx.ConnectError as exc:
            last_exc = exc
            if attempt < 2:
                time.sleep(1)
        except Exception as exc:
            return f"Error: {exc}"
    return f"Error: clausius service unreachable — run: systemctl --user restart clausius.service ({last_exc})"


# ── helpers ───────────────────────────────────────────────────────────────────

_JOB_FIELDS = [
    "jobid", "name", "state", "reason", "elapsed", "timelimit",
    "nodes", "gres", "partition", "submitted", "account",
    "started_local", "ended_local",
    "progress", "depends_on", "dependents", "dep_details",
    "project", "project_color", "project_emoji", "campaign",
    "_pinned", "exit_code", "crash_detected", "est_start",
]


def _slim_job(cluster: str, job: dict) -> dict:
    out = {"cluster": cluster}
    for k in _JOB_FIELDS:
        v = job.get(k)
        if v is not None and v != "" and v != []:
            out[k] = v
    return out


# ── tools ─────────────────────────────────────────────────────────────────────

@mcp.tool()
def health_check() -> dict:
    """Quick health check. Returns ok if the MCP server is running."""
    svc = _api("GET", "/api/health")
    if svc.get("status") == "ok":
        return {"status": "ok", "service": "connected", "board_version": svc.get("board_version")}
    return {"status": "ok", "service": "unreachable", "note": "clausius gunicorn may be down"}


@mcp.tool()
def list_jobs(cluster: Optional[str] = None, project: Optional[str] = None) -> list[dict]:
    """List active jobs across all clusters, or filtered by cluster/project.

    Returns compact job records with state, progress, dependencies, and est_start.
    Includes both live squeue jobs and board-pinned terminal jobs.
    """
    if cluster:
        data = _api("GET", f"/api/jobs/{cluster}")
        if data.get("status") == "error":
            return [{"error": data.get("error", "Unknown error")}]
        jobs = [_slim_job(cluster, j) for j in data.get("jobs", [])]
    else:
        snapshot = _api("GET", "/api/jobs")
        jobs = []
        if isinstance(snapshot, dict):
            for cname, cdata in snapshot.items():
                if not isinstance(cdata, dict):
                    continue
                for j in cdata.get("jobs", []):
                    jobs.append(_slim_job(cname, j))

    if project:
        jobs = [j for j in jobs if j.get("project") == project]
    return jobs


@mcp.tool()
def list_log_files(cluster: str, job_id: str) -> dict:
    """Discover available log and result files for a job.

    Returns lists of direct log files and explorable directories.
    """
    return _api("GET", f"/api/log_files/{cluster}/{job_id}", params={"force": "1"})


@mcp.tool()
def get_job_log(
    cluster: str,
    job_id: str,
    path: Optional[str] = None,
    lines: int = 150,
) -> str:
    """Read the tail of a log file for a job.

    If path is omitted, the best file is auto-selected. Returns raw log text.
    """
    params = {"lines": str(lines)}
    if path:
        params["path"] = path
    data = _api("GET", f"/api/log/{cluster}/{job_id}", params=params)
    if isinstance(data, dict):
        if data.get("status") == "error":
            return f"Error: {data.get('error', 'Unknown error')}"
        return data.get("content", "(empty)")
    return str(data)


@mcp.tool()
def get_job_stats(cluster: str, job_id: str) -> dict:
    """Get resource stats for a running job (CPU, memory, GPU utilisation)."""
    return _api("GET", f"/api/stats/{cluster}/{job_id}")


@mcp.tool()
def get_run_info(cluster: str, root_job_id: str) -> dict:
    """Get detailed run info: batch script, scontrol, env vars, conda state, and associated jobs."""
    return _api("GET", f"/api/run_info/{cluster}/{root_job_id}")


@mcp.tool()
def get_history(
    cluster: Optional[str] = None,
    project: Optional[str] = None,
    campaign: Optional[str] = None,
    state: Optional[str] = None,
    partition: Optional[str] = None,
    account: Optional[str] = None,
    search: Optional[str] = None,
    days: Optional[int] = None,
    limit: int = 50,
) -> list[dict]:
    """Get past job history, filterable by cluster, project, campaign, state, partition, account, search, and recent days.

    String filters accept a single value. ``state`` and ``campaign`` also accept comma-separated values.
    """
    params = {"limit": str(limit)}
    if cluster:
        params["cluster"] = cluster
    if project:
        params["project"] = project
    if campaign:
        params["campaign"] = campaign
    if state:
        params["state"] = state
    if partition:
        params["partition"] = partition
    if account:
        params["account"] = account
    if search:
        params["q"] = search
    if days is not None:
        params["days"] = str(days)
    data = _api("GET", "/api/history", params=params)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and data.get("status") == "error":
        return [data]
    return []


@mcp.tool()
def cancel_job(cluster: str, job_id: str) -> dict:
    """Cancel a running or pending job. Destructive — only when user explicitly asks."""
    return _api("POST", f"/api/cancel/{cluster}/{job_id}")


@mcp.tool()
def cancel_jobs(cluster: str, job_ids: list[str]) -> dict:
    """Cancel multiple jobs on a cluster. Destructive — only when user explicitly asks."""
    return _api("POST", f"/api/cancel_jobs/{cluster}", json={"job_ids": job_ids})


@mcp.tool()
def run_script(
    cluster: str,
    script: str,
    interpreter: str = "python3",
    timeout: int = 120,
) -> dict:
    """Run a script on a cluster via SSH and return its output.

    Args:
        cluster: Target cluster name.
        script: Full source code.
        interpreter: "python3" (default), "bash", or "sh".
        timeout: Max seconds (1-300, default 120).
    """
    return _api("POST", f"/api/run_script/{cluster}", json={
        "script": script,
        "interpreter": interpreter,
        "timeout": timeout,
    })


# ── cluster info ──────────────────────────────────────────────────────────────

@mcp.tool()
def get_partitions(cluster: Optional[str] = None) -> dict:
    """Get Slurm partition details: state, time limits, priority, nodes, GPUs, queue depth.

    Returns per-partition data including idle_nodes, pending_jobs, gpus_per_node,
    priority_tier, preempt_mode, and access restrictions.
    """
    if cluster:
        return _api("GET", f"/api/partitions/{cluster}")
    return _api("GET", "/api/partitions")


@mcp.tool()
def where_to_submit(
    nodes: int = 1,
    gpus_per_node: int = 8,
    gpu_type: str = "",
) -> dict:
    """Rank clusters by WDS score (0-100) for job submission.

    Combines PPP allocations, fairshare, team usage, queue pressure, and
    cluster occupancy. Higher WDS = better. >=75 good, 50-74 moderate, <50 unlikely.

    Args:
        nodes: GPU nodes needed (default 1).
        gpus_per_node: GPUs per node (default 8).
        gpu_type: Prefer clusters with this GPU (e.g. "H100", "B200").
    """
    return _api("POST", "/api/where_to_submit", json={
        "nodes": nodes,
        "gpus_per_node": gpus_per_node,
        "gpu_type": gpu_type,
    })


# ── mount & board tools ──────────────────────────────────────────────────────

@mcp.tool()
def get_mounts() -> dict:
    """Get SSHFS mount status for all clusters."""
    return _api("GET", "/api/mounts")


@mcp.tool()
def mount_cluster(cluster: str, action: str = "mount") -> dict:
    """Mount or unmount a cluster's remote filesystem via SSHFS."""
    if action not in ("mount", "unmount"):
        return {"status": "error", "error": "action must be 'mount' or 'unmount'"}
    return _api("POST", f"/api/mount/{action}/{cluster}")


@mcp.tool()
def clear_failed(cluster: str) -> dict:
    """Dismiss all failed/cancelled/timeout job pins from a cluster's board."""
    return _api("POST", f"/api/clear_failed/{cluster}")


@mcp.tool()
def clear_completed(cluster: str) -> dict:
    """Dismiss all completed job pins from a cluster's board."""
    return _api("POST", f"/api/clear_completed/{cluster}")


# ── logbook tools ─────────────────────────────────────────────────────────────

@mcp.tool()
def list_logbook_entries(
    project: str,
    query: Optional[str] = None,
    sort: str = "edited_at",
    limit: int = 50,
    entry_type: Optional[str] = None,
) -> list[dict]:
    """List logbook entries for a project, optionally filtered by BM25 search.

    Returns: id, project, title, body_preview, entry_type, created_at, edited_at.
    Sort: "edited_at" (default), "created_at", "title".
    """
    params = {"sort": sort, "limit": str(limit)}
    if query:
        params["q"] = query
    if entry_type:
        params["type"] = entry_type
    data = _api("GET", f"/api/logbook/{project}/entries", params=params)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and data.get("status") == "error":
        return [data]
    return []


@mcp.tool()
def read_logbook_entry(project: str, entry_id: int) -> dict:
    """Read a single logbook entry with full markdown body."""
    return _api("GET", f"/api/logbook/{project}/entries/{entry_id}")


@mcp.tool()
def bulk_read_logbooks(
    project: Optional[str] = None,
    entry_type: Optional[str] = None,
    sort: str = "created_at",
    limit_per_project: int = 200,
    max_entries: int = 1000,
) -> dict:
    """Bulk-read full logbook entries for one or all projects in a single call.

    Returns full entries with markdown bodies. Use for comprehensive context gathering.
    """
    body = {"sort": sort, "limit_per_project": limit_per_project, "max_entries": max_entries}
    if project:
        body["project"] = project
    if entry_type:
        body["entry_type"] = entry_type
    return _api("POST", "/api/logbook/bulk_read", json=body)


@mcp.tool()
def find_logbook_entries(
    pattern: str,
    project: Optional[str] = None,
    field: str = "title",
    regex: bool = False,
    entry_type: Optional[str] = None,
    full_body: bool = True,
    limit: int = 50,
) -> dict:
    """Find logbook entries by substring or regex match on title or body.

    Args:
        pattern: Search string (substring by default, regex if regex=True).
        field: "title" (default), "body", or "both".
        regex: Treat pattern as Python regex.
        full_body: Return full body (default True) or preview only.
    """
    body = {"pattern": pattern, "field": field, "regex": regex, "full_body": full_body, "limit": limit}
    if project:
        body["project"] = project
    if entry_type:
        body["entry_type"] = entry_type
    return _api("POST", "/api/logbook/find", json=body)


@mcp.tool()
def create_logbook_entry(project: str, title: str, body: str = "", entry_type: str = "note") -> dict:
    """Create a new logbook entry. Supports markdown, #N cross-refs, @run-name refs, images.

    See the project-logbook workspace rule for full formatting guidelines.
    entry_type: "note" (results/findings) or "plan" (plans/designs).
    """
    return _api("POST", f"/api/logbook/{project}/entries", json={
        "title": title,
        "body": body,
        "entry_type": entry_type,
    })


@mcp.tool()
def update_logbook_entry(
    project: str,
    entry_id: int,
    title: Optional[str] = None,
    body: Optional[str] = None,
) -> dict:
    """Update a logbook entry's title and/or body. Bumps edited_at."""
    payload = {}
    if title is not None:
        payload["title"] = title
    if body is not None:
        payload["body"] = body
    return _api("PUT", f"/api/logbook/{project}/entries/{entry_id}", json=payload)


@mcp.tool()
def delete_logbook_entry(project: str, entry_id: int) -> dict:
    """Delete a logbook entry. Destructive."""
    return _api("DELETE", f"/api/logbook/{project}/entries/{entry_id}")


@mcp.tool()
def upload_logbook_image(project: str, image_path: str) -> dict:
    """Upload a local image/HTML file to a project's logbook image store.

    Supported: .png, .jpg, .jpeg, .gif, .webp, .svg, .html, .htm
    See project-logbook workspace rule for embedding and HTML figure requirements.
    """
    if not os.path.isfile(image_path):
        return {"status": "error", "error": f"File not found: {image_path}"}
    filename = os.path.basename(image_path)
    with open(image_path, "rb") as f:
        data = f.read()
    try:
        r = _client.post(
            f"/api/logbook/{project}/images",
            files={"file": (filename, data)},
        )
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


# ── resources ────────────────────────────────────────────────────────────────

@mcp.resource("jobs://summary")
def jobs_summary() -> str:
    """Quick overview of all clusters: running/pending/failed counts."""
    data = _api("GET", "/api/jobs_summary")
    if isinstance(data, dict) and data.get("status") == "ok":
        return data.get("summary", "")
    return f"Error: {data.get('error', 'Unknown error')}" if isinstance(data, dict) else str(data)


# ── main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
