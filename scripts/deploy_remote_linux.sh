#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DEPLOY_HOST="${DEPLOY_HOST:-}"
DEPLOY_USER="${DEPLOY_USER:-root}"
DEPLOY_PORT="${DEPLOY_PORT:-22}"
DEPLOY_INSTALL_DIR="${DEPLOY_INSTALL_DIR:-/opt/prebid-server}"
DEPLOY_SERVICE_NAME="${DEPLOY_SERVICE_NAME:-prebid-server}"
DEPLOY_BINARY_NAME="${DEPLOY_BINARY_NAME:-prebid-server}"
DEPLOY_CONFIG_FILE="${DEPLOY_CONFIG_FILE:-}"
DEPLOY_EXTERNAL_URL="${DEPLOY_EXTERNAL_URL:-}"
DEPLOY_DB_ENV_FILE="${DEPLOY_DB_ENV_FILE:-/etc/prebid-server/database.env}"
DASH_ADMIN_USER="${DASH_ADMIN_USER:-admin}"
DASH_ADMIN_PASS="${DASH_ADMIN_PASS:-}"
DASH_DB_DSN="${DASH_DB_DSN:-}"
CLICKHOUSE_DSN="${CLICKHOUSE_DSN:-}"
CLICKHOUSE_VIDEO_TABLE="${CLICKHOUSE_VIDEO_TABLE:-}"
DEPLOY_PASS="${DEPLOY_PASS:-}"

if [[ -z "$DEPLOY_HOST" ]]; then
  echo "DEPLOY_HOST is required" >&2
  exit 2
fi

if [[ -z "$DASH_ADMIN_PASS" ]]; then
  echo "DASH_ADMIN_PASS is required" >&2
  exit 2
fi

if [[ -n "$DEPLOY_PASS" ]] && ! command -v expect >/dev/null 2>&1; then
  echo "expect is required for password-based deploys" >&2
  exit 2
fi

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

encode_b64() {
  printf '%s' "$1" | base64 | tr -d '\n'
}

bundle_path="$work_dir/${DEPLOY_SERVICE_NAME}-deploy.tar.gz"
binary_path="$work_dir/${DEPLOY_BINARY_NAME}-linux-amd64"
config_path="$work_dir/pbs.yaml"
remote_script_path="$work_dir/remote-deploy.sh"
stage_dir="$work_dir/stage"
ssh_target="${DEPLOY_USER}@${DEPLOY_HOST}"
ssh_base=(ssh -p "$DEPLOY_PORT" -o StrictHostKeyChecking=no -o PubkeyAuthentication=no -o PreferredAuthentications=password)
scp_base=(scp -P "$DEPLOY_PORT" -o StrictHostKeyChecking=no -o PubkeyAuthentication=no -o PreferredAuthentications=password)

run_expect() {
  local password="$1"
  shift
  expect -f - -- "$password" "$@" <<'EOF'
set timeout -1
set password [lindex $argv 0]
set cmd [lrange $argv 1 end]
log_user 1
spawn {*}$cmd
expect {
  -re {.*yes/no.*} { send "yes\r"; exp_continue }
  -re {.*[Pp]assword:.*} { send "$password\r"; exp_continue }
  eof
}
catch wait result
set exit_status [lindex $result 3]
exit $exit_status
EOF
}

remote_scp() {
  if [[ -n "$DEPLOY_PASS" ]]; then
    run_expect "$DEPLOY_PASS" "${scp_base[@]}" "$@"
  else
    scp -P "$DEPLOY_PORT" -o StrictHostKeyChecking=no "$@"
  fi
}

remote_ssh() {
  if [[ -n "$DEPLOY_PASS" ]]; then
    run_expect "$DEPLOY_PASS" "${ssh_base[@]}" "$ssh_target" "$@"
  else
    ssh -p "$DEPLOY_PORT" -o StrictHostKeyChecking=no "$ssh_target" "$@"
  fi
}

echo "Building Linux bundle..."
(
  cd "$ROOT_DIR"
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$binary_path" .
)

if [[ -n "$DEPLOY_CONFIG_FILE" ]]; then
  cp "$DEPLOY_CONFIG_FILE" "$config_path"
else
  if [[ -z "$DEPLOY_EXTERNAL_URL" ]]; then
    DEPLOY_EXTERNAL_URL="http://${DEPLOY_HOST}"
  fi
  cat > "$config_path" <<EOF
port: 80
admin_port: 6060
external_url: "${DEPLOY_EXTERNAL_URL}"
status_response: "ok"
gdpr:
  default_value: "0"
stored_requests:
  filesystem:
    enabled: true
    directorypath: ./stored_requests/data/by_id
stored_responses:
  filesystem:
    enabled: true
    directorypath: ./stored_responses/data/by_id
auction_timeouts_ms:
  default: 800
  max: 1500
tmax_adjustments:
  enabled: true
  bidder_network_latency_buffer_ms: 15
  pbs_response_preparation_duration_ms: 15
  bidder_response_duration_min_ms: 50
http_client:
  max_idle_connections: 5000
  max_idle_connections_per_host: 500
  max_connections_per_host: 0
  idle_connection_timeout_seconds: 30
compression:
  response:
    enable_gzip: true
EOF
fi

deploy_db_env_file_b64="$(encode_b64 "$DEPLOY_DB_ENV_FILE")"
dash_db_dsn_b64="$(encode_b64 "$DASH_DB_DSN")"
clickhouse_dsn_b64="$(encode_b64 "$CLICKHOUSE_DSN")"
clickhouse_video_table_b64="$(encode_b64 "$CLICKHOUSE_VIDEO_TABLE")"

cat > "$remote_script_path" <<EOF
#!/usr/bin/env bash
set -euo pipefail

decode_b64() {
  if [[ -z "\$1" ]]; then
    printf ''
    return 0
  fi
  printf '%s' "\$1" | base64 --decode
}

db_env_file="\$(decode_b64 '$deploy_db_env_file_b64')"
incoming_dash_db_dsn="\$(decode_b64 '$dash_db_dsn_b64')"
incoming_clickhouse_dsn="\$(decode_b64 '$clickhouse_dsn_b64')"
incoming_clickhouse_video_table="\$(decode_b64 '$clickhouse_video_table_b64')"

install_dir="\$1"
service_name="\$2"
binary_name="\$3"
dashboard_user="\$4"
dashboard_pass="\$5"
bundle_name="\$6"

bundle_path="/tmp/\${bundle_name}"
mkdir -p "\$install_dir"
rm -rf "\$install_dir/\$binary_name" "\$install_dir/\${binary_name}-linux-amd64" "\$install_dir/static" "\$install_dir/stored_requests" "\$install_dir/stored_responses" "\$install_dir/pbs.yaml"
tar -xzf "\$bundle_path" -C "\$install_dir"
install -m 0755 "\$install_dir/\${binary_name}-linux-amd64" "\$install_dir/\$binary_name"
find "\$install_dir" \( -name '._*' -o -name '.DS_Store' \) -delete

final_dash_db_dsn=""
final_clickhouse_dsn=""
final_clickhouse_video_table=""
if [[ -f "\$db_env_file" ]]; then
  while IFS= read -r line; do
    case "\$line" in
      DASH_DB_DSN=*)
        final_dash_db_dsn="\${line#DASH_DB_DSN=}"
        ;;
      CLICKHOUSE_DSN=*)
        final_clickhouse_dsn="\${line#CLICKHOUSE_DSN=}"
        ;;
      CLICKHOUSE_VIDEO_TABLE=*)
        final_clickhouse_video_table="\${line#CLICKHOUSE_VIDEO_TABLE=}"
        ;;
    esac
  done < "\$db_env_file"
fi

if [[ -n "\$incoming_dash_db_dsn" ]]; then
  final_dash_db_dsn="\$incoming_dash_db_dsn"
fi
if [[ -n "\$incoming_clickhouse_dsn" ]]; then
  final_clickhouse_dsn="\$incoming_clickhouse_dsn"
fi
if [[ -n "\$incoming_clickhouse_video_table" ]]; then
  final_clickhouse_video_table="\$incoming_clickhouse_video_table"
fi

if [[ -n "\$final_dash_db_dsn" || -n "\$final_clickhouse_dsn" || -n "\$final_clickhouse_video_table" ]]; then
  install -d -m 0755 "\$(dirname "\$db_env_file")"
  : > "\$db_env_file"
  if [[ -n "\$final_dash_db_dsn" ]]; then
    printf 'DASH_DB_DSN=%s\n' "\$final_dash_db_dsn" >> "\$db_env_file"
  fi
  if [[ -n "\$final_clickhouse_dsn" ]]; then
    printf 'CLICKHOUSE_DSN=%s\n' "\$final_clickhouse_dsn" >> "\$db_env_file"
  fi
  if [[ -n "\$final_clickhouse_video_table" ]]; then
    printf 'CLICKHOUSE_VIDEO_TABLE=%s\n' "\$final_clickhouse_video_table" >> "\$db_env_file"
  fi
  chmod 600 "\$db_env_file"
fi

unit_path="/etc/systemd/system/\${service_name}.service"
printf '%s\n' \
  '[Unit]' \
  'Description=GoAds Prebid Server' \
  'After=network.target' \
  '' \
  '[Service]' \
  'Type=simple' \
  "WorkingDirectory=\${install_dir}" \
  "ExecStart=\${install_dir}/\${binary_name}" \
  "Environment=DASH_ADMIN_USER=\${dashboard_user}" \
  "Environment=DASH_ADMIN_PASS=\${dashboard_pass}" \
  "EnvironmentFile=-\${db_env_file}" \
  'Restart=on-failure' \
  'RestartSec=5s' \
  'StandardOutput=journal' \
  'StandardError=journal' \
  '' \
  '[Install]' \
  'WantedBy=multi-user.target' > "\$unit_path"

systemctl daemon-reload
systemctl enable "\$service_name" >/dev/null
systemctl restart "\$service_name"
sleep 3

port="\$(awk '/^port:/ {print \$2; exit}' "\$install_dir/pbs.yaml")"
if [[ -z "\$port" ]]; then
  port=80
fi

systemctl is-active "\$service_name"
curl -fsS "http://127.0.0.1:\${port}/status"
EOF
chmod +x "$remote_script_path"

echo "Creating clean deploy tarball..."
mkdir -p "$stage_dir"
cp "$binary_path" "$stage_dir/$(basename "$binary_path")"
cp "$config_path" "$stage_dir/pbs.yaml"
cp -R "$ROOT_DIR/static" "$stage_dir/static"
cp -R "$ROOT_DIR/stored_requests" "$stage_dir/stored_requests"
cp -R "$ROOT_DIR/stored_responses" "$stage_dir/stored_responses"
find "$stage_dir" \( -name '._*' -o -name '.DS_Store' \) -delete
if command -v xattr >/dev/null 2>&1; then
  xattr -cr "$stage_dir" 2>/dev/null || true
fi

COPYFILE_DISABLE=1 tar \
  --exclude='.DS_Store' \
  --exclude='._*' \
  -czf "$bundle_path" \
  -C "$stage_dir" .

echo "Uploading bundle and remote script to $ssh_target..."
remote_scp "$bundle_path" "$remote_script_path" "$ssh_target:/tmp/"

echo "Deploying on remote host..."
remote_ssh "bash /tmp/$(basename "$remote_script_path") '$DEPLOY_INSTALL_DIR' '$DEPLOY_SERVICE_NAME' '$DEPLOY_BINARY_NAME' '$DASH_ADMIN_USER' '$DASH_ADMIN_PASS' '$(basename "$bundle_path")'"

echo "Verifying public health endpoint..."
curl -fsS "http://${DEPLOY_HOST}/status"
echo
echo "Deploy finished successfully."