#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"
LOG_DIR="$ROOT_DIR/logs"
BIN_PATH="$BIN_DIR/prebid-server-local"
PID_FILE="$LOG_DIR/prebid-server-local.pid"
CONFIG_PATH="${PBS_CONFIG_PATH:-$ROOT_DIR/pbs.yaml}"
ADMIN_USER="${DASH_ADMIN_USER:-admin}"
ADMIN_PASS="${DASH_ADMIN_PASS:-admin}"

mkdir -p "$BIN_DIR" "$LOG_DIR"

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "pbs config not found at $CONFIG_PATH" >&2
  exit 1
fi

if lsof -nP -iTCP:8000 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port 8000 is already in use; stop the existing process first" >&2
  exit 1
fi

pushd "$ROOT_DIR" >/dev/null
go build -o "$BIN_PATH" .
popd >/dev/null

nohup env \
  DASH_ADMIN_USER="$ADMIN_USER" \
  DASH_ADMIN_PASS="$ADMIN_PASS" \
  DASH_DB_DSN="${DASH_DB_DSN:-}" \
  CLICKHOUSE_DSN="${CLICKHOUSE_DSN:-}" \
  CLICKHOUSE_VIDEO_TABLE="${CLICKHOUSE_VIDEO_TABLE:-}" \
  "$BIN_PATH" >"$LOG_DIR/prebid-server-local.out" 2>"$LOG_DIR/prebid-server-local.err" &

echo $! > "$PID_FILE"
sleep 2

if curl -fsS http://localhost:8000/status >/dev/null 2>&1; then
  echo "prebid-server local started on http://localhost:8000"
  echo "pid=$(cat "$PID_FILE")"
  exit 0
fi

echo "server process started but health check failed; inspect logs in $LOG_DIR" >&2
exit 1