#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$ROOT_DIR/logs/prebid-server-local.pid"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" >/dev/null 2>&1; then
    kill "$PID"
    echo "stopped process $PID"
  fi
  rm -f "$PID_FILE"
fi

if lsof -ti tcp:8000 >/dev/null 2>&1; then
  lsof -ti tcp:8000 | xargs kill -9 >/dev/null 2>&1 || true
  echo "stopped listener on port 8000"
fi