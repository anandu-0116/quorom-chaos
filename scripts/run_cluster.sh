#!/usr/bin/env bash
# Launches an N-node quorum-chaos cluster locally (as N separate processes)
# for manual testing of leader election.
#
# Usage: scripts/run_cluster.sh [N] [base_port]
#   N          number of nodes (default 5)
#   base_port  first port used; node i listens on 127.0.0.1:<base_port + i>
#              (default 50050)
#
# Generates cluster.conf, starts each node in the background logging to
# logs/node<i>.log with its state persisted under data/node<i>/, and prints
# PIDs. Watch it with:
#   tail -f logs/*.log
# Force a re-election by killing whichever node's log says "-> Leader":
#   kill <pid>
# Tear everything down with:
#   scripts/stop_cluster.sh

set -euo pipefail

N="${1:-5}"
BASE_PORT="${2:-50050}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="$ROOT_DIR/build/raft_node"
CONFIG_FILE="$ROOT_DIR/cluster.conf"
LOG_DIR="$ROOT_DIR/logs"
DATA_DIR="$ROOT_DIR/data"
PID_FILE="$ROOT_DIR/cluster.pids"

if [[ ! -x "$BINARY" ]]; then
  echo "error: $BINARY not found -- build it first:" >&2
  echo "  cmake -B build && cmake --build build" >&2
  exit 1
fi

mkdir -p "$LOG_DIR" "$DATA_DIR"
: > "$CONFIG_FILE"
: > "$PID_FILE"

for ((i = 0; i < N; i++)); do
  echo "$i,127.0.0.1:$((BASE_PORT + i))" >>"$CONFIG_FILE"
done
echo "wrote $CONFIG_FILE for $N nodes"

for ((i = 0; i < N; i++)); do
  node_data_dir="$DATA_DIR/node$i"
  mkdir -p "$node_data_dir"
  "$BINARY" --id="$i" --config="$CONFIG_FILE" --data_dir="$node_data_dir" \
    >"$LOG_DIR/node$i.log" 2>&1 &
  pid=$!
  echo "$pid" >>"$PID_FILE"
  echo "started node$i (pid $pid, port $((BASE_PORT + i))) -> $LOG_DIR/node$i.log"
done

echo
echo "cluster running. tail -f $LOG_DIR/*.log to watch the election."
echo "stop everything with: scripts/stop_cluster.sh"
