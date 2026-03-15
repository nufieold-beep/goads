#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLIST_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$PLIST_DIR/com.goads.prebid-server.local.plist"
RUN_SCRIPT="$ROOT_DIR/scripts/run_local_macos.sh"

mkdir -p "$PLIST_DIR"

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.goads.prebid-server.local</string>

  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$RUN_SCRIPT</string>
  </array>

  <key>WorkingDirectory</key>
  <string>$ROOT_DIR</string>

  <key>RunAtLoad</key>
  <true/>

  <key>KeepAlive</key>
  <false/>

  <key>StandardOutPath</key>
  <string>$ROOT_DIR/logs/launchd.out</string>

  <key>StandardErrorPath</key>
  <string>$ROOT_DIR/logs/launchd.err</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>DASH_ADMIN_USER</key>
    <string>${DASH_ADMIN_USER:-admin}</string>
    <key>DASH_ADMIN_PASS</key>
    <string>${DASH_ADMIN_PASS:-admin}</string>
    <key>DASH_DB_DSN</key>
    <string>${DASH_DB_DSN:-}</string>
    <key>CLICKHOUSE_DSN</key>
    <string>${CLICKHOUSE_DSN:-}</string>
    <key>CLICKHOUSE_VIDEO_TABLE</key>
    <string>${CLICKHOUSE_VIDEO_TABLE:-}</string>
  </dict>
</dict>
</plist>
EOF

launchctl unload "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl load "$PLIST_PATH"
echo "launchd service installed: $PLIST_PATH"