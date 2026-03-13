#!/usr/bin/env bash
set -euo pipefail

# Mount/unmount cluster log roots via sshfs for faster job-monitor log reads.
# Cluster definitions are read from config.json (no hardcoded values here).
#
# Usage:
#   ./scripts/sshfs_logs.sh mount
#   ./scripts/sshfs_logs.sh unmount
#   ./scripts/sshfs_logs.sh status
#   ./scripts/sshfs_logs.sh mount <cluster>
#   ./scripts/sshfs_logs.sh unmount <cluster>
#   ./scripts/sshfs_logs.sh status <cluster>

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../config.json"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Config file not found: ${CONFIG_FILE}"
  echo "Copy config.example.json to config.json and fill in your cluster details."
  exit 1
fi

ACTION="${1:-status}"
BASE="${HOME}/.job-monitor/mounts"
USER_NAME="${JOB_MONITOR_SSH_USER:-${USER:-}}"
if [[ -z "$USER_NAME" ]]; then
  USER_NAME="$(whoami)"
fi
TARGET_CLUSTER="${2:-all}"
FAILED=0
KEY_PATH="${JOB_MONITOR_SSH_KEY:-${HOME}/.ssh/id_ed25519}"

_cluster_field() {
  python3 -c "
import json, sys
with open('${CONFIG_FILE}') as f:
    cfg = json.load(f)
c = cfg.get('clusters', {}).get('$1', {})
print(c.get('$2', '${3:-}'))
"
}

_cluster_names() {
  python3 -c "
import json
with open('${CONFIG_FILE}') as f:
    cfg = json.load(f)
for name in cfg.get('clusters', {}):
    print(name)
"
}

mount_cluster() {
  local c="$1"
  local host; host="$(_cluster_field "$c" host)"
  local port; port="$(_cluster_field "$c" port 22)"
  local remote_root; remote_root="$(_cluster_field "$c" remote_root /)"
  local target="${BASE}/${c}"
  local ssh_cmd
  ssh_cmd="ssh -F ${HOME}/.ssh/config -o BatchMode=yes -o IdentitiesOnly=yes -o PreferredAuthentications=publickey -o StrictHostKeyChecking=accept-new -p ${port}"

  mkdir -p "$target"
  if mountpoint -q "$target"; then
    echo "[${c}] already mounted at ${target}"
    return 0
  fi

  echo "[${c}] mounting ${host}:${remote_root} -> ${target}"
  sshfs "${USER_NAME}@${host}:${remote_root}" "$target" \
    -o ssh_command="${ssh_cmd}" \
    -o IdentityFile="${KEY_PATH}" \
    -o reconnect,ServerAliveInterval=15,ServerAliveCountMax=3 \
    -o cache=yes,kernel_cache,auto_cache \
    -o attr_timeout=60,entry_timeout=60,negative_timeout=15
}

unmount_cluster() {
  local c="$1"
  local target="${BASE}/${c}"
  if mountpoint -q "$target"; then
    echo "[${c}] unmounting ${target}"
    fusermount -u "$target" || umount "$target"
  else
    echo "[${c}] not mounted"
  fi
}

status_cluster() {
  local c="$1"
  local target="${BASE}/${c}"
  if mountpoint -q "$target"; then
    echo "[${c}] mounted at ${target}"
  else
    echo "[${c}] not mounted"
  fi
}

cluster_exists() {
  python3 -c "
import json, sys
with open('${CONFIG_FILE}') as f:
    cfg = json.load(f)
sys.exit(0 if '$1' in cfg.get('clusters', {}) else 1)
"
}

if ! command -v sshfs >/dev/null 2>&1 && [[ "$ACTION" == "mount" ]]; then
  echo "sshfs is not installed. Install it first (e.g. sudo apt install sshfs)."
  exit 1
fi

mkdir -p "$BASE"

case "$ACTION" in
  mount)
    if [[ "$TARGET_CLUSTER" != "all" ]]; then
      cluster_exists "$TARGET_CLUSTER" || { echo "Unknown cluster: ${TARGET_CLUSTER}"; exit 2; }
      if ! mount_cluster "$TARGET_CLUSTER"; then
        echo "[${TARGET_CLUSTER}] mount failed"
        FAILED=1
      fi
    else
      while IFS= read -r c; do
        if ! mount_cluster "$c"; then
          echo "[${c}] mount failed"
          FAILED=1
        fi
      done < <(_cluster_names)
    fi
    ;;
  unmount)
    if [[ "$TARGET_CLUSTER" != "all" ]]; then
      cluster_exists "$TARGET_CLUSTER" || { echo "Unknown cluster: ${TARGET_CLUSTER}"; exit 2; }
      if ! unmount_cluster "$TARGET_CLUSTER"; then
        echo "[${TARGET_CLUSTER}] unmount failed"
        FAILED=1
      fi
    else
      while IFS= read -r c; do
        if ! unmount_cluster "$c"; then
          echo "[${c}] unmount failed"
          FAILED=1
        fi
      done < <(_cluster_names)
    fi
    ;;
  status)
    if [[ "$TARGET_CLUSTER" != "all" ]]; then
      cluster_exists "$TARGET_CLUSTER" || { echo "Unknown cluster: ${TARGET_CLUSTER}"; exit 2; }
      status_cluster "$TARGET_CLUSTER"
    else
      while IFS= read -r c; do status_cluster "$c"; done < <(_cluster_names)
    fi
    ;;
  *)
    echo "Unknown action: ${ACTION}"
    echo "Usage: $0 {mount|unmount|status} [cluster|all]"
    exit 2
    ;;
esac

if [[ "$FAILED" -ne 0 ]]; then
  exit 1
fi
