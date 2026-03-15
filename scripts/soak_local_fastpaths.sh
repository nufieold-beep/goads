#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
CONCURRENCY="${CONCURRENCY:-200}"
DURATION="${DURATION:-30s}"
TIMEOUT="${TIMEOUT:-5s}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/logs/soak}"
STATUS_URL="${STATUS_URL:-$BASE_URL/status}"
TRACKING_URL="${TRACKING_URL:-$BASE_URL/video/tracking?auction_id=soak-auction&bid_id=soak-bid&bidder=soak&event=start&placement_id=soak-placement&price=1.23}"
ORTB_URL="${ORTB_URL:-}"
VAST_URL="${VAST_URL:-}"

mkdir -p "$LOG_DIR"

run_case() {
  local name="$1"
  local target_url="$2"
  local method="${3:-GET}"
  local body="${4:-}"
  local log_file="$LOG_DIR/${name}.log"

  echo "==> $name"
  echo "url=$target_url" | tee "$log_file"
  if [[ -n "$body" ]]; then
    go run ./cmd/fasthttp-loadtest \
      -url "$target_url" \
      -method "$method" \
      -body "$body" \
      -concurrency "$CONCURRENCY" \
      -duration "$DURATION" \
      -timeout "$TIMEOUT" | tee -a "$log_file"
  else
    go run ./cmd/fasthttp-loadtest \
      -url "$target_url" \
      -method "$method" \
      -concurrency "$CONCURRENCY" \
      -duration "$DURATION" \
      -timeout "$TIMEOUT" | tee -a "$log_file"
  fi
}

pushd "$ROOT_DIR" >/dev/null

curl -fsS "$STATUS_URL" >/dev/null

run_case status "$STATUS_URL"
run_case tracking "$TRACKING_URL"

if [[ -n "$ORTB_URL" ]]; then
  run_case ortb "$ORTB_URL"
fi

if [[ -n "$VAST_URL" ]]; then
  run_case vast "$VAST_URL"
fi

popd >/dev/null

echo "logs written to $LOG_DIR"
