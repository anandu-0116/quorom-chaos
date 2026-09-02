#!/usr/bin/env bash
# Stops a cluster started by run_cluster.sh.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT_DIR/cluster.pids"

if [[ ! -f "$PID_FILE" ]]; then
  echo "no cluster.pids found -- nothing to stop"
  exit 0
fi

while read -r pid; do
  [[ -z "$pid" ]] && continue
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    echo "stopped pid $pid"
  fi
done <"$PID_FILE"

rm -f "$PID_FILE"
