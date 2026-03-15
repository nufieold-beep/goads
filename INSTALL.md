# GoAds — Installation Guide

AdZrvr custom Prebid Server with a built-in VAST video pipeline, demand adapter,
and publisher dashboard.

---

## Prerequisites

| Tool | Minimum version | Notes |
|------|----------------|-------|
| Go   | 1.23           | <https://go.dev/dl/> |
| Git  | any            | Required to clone & build |
| gcc / build-essential | any | Only needed when building with CGO modules |

> **Linux server (recommended):** Ubuntu 22.04 LTS or Debian 12.  
> **Local dev:** macOS, Windows (WSL2), or Linux.

---

## 1 — Clone the repository

```bash
git clone https://github.com/nufieold-beep/goads.git
cd goads
```

---

## 2 — Install Go dependencies

```bash
go mod vendor
go mod tidy
```

Or using the Makefile shortcut:

```bash
make deps
```

---

## 3 — Build the binary

### Quick build (no tests)

```bash
go build -o prebid-server .
```

### Full build with tests

```bash
make build
```

This runs `validate.sh` (unit + integration tests) before compiling.

### Linux cross-compile from Windows / macOS

```bash
GOOS=linux GOARCH=amd64 go build -o prebid-server-linux .
```

---

## 4 — Configuration

Copy the sample config and edit it to match your environment:

```bash
cp pbs.yaml.example pbs.yaml   # if an example file is present, otherwise
# create pbs.yaml manually (see below)
```

**Minimum `pbs.yaml`:**

```yaml
port: 80
admin_port: 6060

# Your server's public hostname or IP — used to build VAST tracking beacon URLs.
external_url: "http://YOUR_SERVER_IP"

status_response: "ok"

gdpr:
  default_value: "0"   # set to "1" if you serve EU users

stored_requests:
  filesystem:
    enabled: true
    directorypath: ./stored_requests/data/by_id

stored_responses:
  filesystem:
    enabled: true
    directorypath: ./stored_responses/data/by_id
```

> **Note:** `pbs.yaml` is never committed to this repository (excluded by
> `.gitignore`) to keep credentials out of source control.

### Environment variables for storage and analytics

Set these before starting the server when you want dashboard CRUD and runtime
ad server config persistence to use PostgreSQL, and video analytics to use
ClickHouse.

```bash
export DASH_ADMIN_USER=admin
export DASH_ADMIN_PASS=change-me

# Primary CRUD / admin persistence
export DASH_DB_DSN='postgres://user:pass@127.0.0.1:5432/goads?sslmode=disable'

# Primary video analytics / metrics fact store
export CLICKHOUSE_DSN='clickhouse://127.0.0.1:9000?database=goads'

# Optional: override the ClickHouse fact table name
export CLICKHOUSE_VIDEO_TABLE='video_event_facts'
```

Persistence behavior:

- Dashboard CRUD entities and `/video/adserver` runtime routing configs use PostgreSQL when `DASH_DB_DSN` is set.
- Video analytics facts use ClickHouse when `CLICKHOUSE_DSN` is set.
- Impression-confirmed revenue and impression counts are derived from the player firing `/video/impression`.
- Requests and opportunities are stored as event facts too, but headline analytics formulas treat the impression beacon as the source of truth for billable delivery.

---

## 5 — Run the server

```bash
./prebid-server
```

The server auto-discovers `pbs.yaml` when it is in the working directory.

To specify a config file explicitly:

```bash
./prebid-server --config /etc/prebid-server/pbs.yaml
```

Default ports:

| Service | Port |
|---------|------|
| Auction / Video / Dashboard | `80` |
| Admin / Metrics (`/admin`) | `6060` |

---

## 6 — Deploy as a systemd service (Linux)

```bash
# copy binary
sudo cp prebid-server-linux /opt/prebid-server/prebid-server
sudo chmod +x /opt/prebid-server/prebid-server

# copy config
sudo cp pbs.yaml /opt/prebid-server/pbs.yaml
```

Create `/etc/systemd/system/prebid-server.service`:

```ini
[Unit]
Description=GoAds Prebid Server
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/prebid-server
ExecStart=/opt/prebid-server/prebid-server
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable prebid-server
sudo systemctl start prebid-server
sudo systemctl status prebid-server
```

---

## 7 — Docker

Build the image:

```bash
docker build -t goads-prebid-server .
```

Run (skip tests during build):

```bash
docker build --build-arg TEST=false -t goads-prebid-server .
docker run -d \
  -p 80:80 \
  -p 6060:6060 \
  -v $(pwd)/pbs.yaml:/opt/prebid-server/pbs.yaml \
  --name goads \
  goads-prebid-server
```

---

## 8 — Verify installation

```bash
# Health check
curl http://localhost/status

# Publisher dashboard (browser)
open http://localhost/dashboard

# Admin metrics
curl http://localhost:6060/metrics
```

Expected `/status` response: `ok`

To verify the persistence backends:

```bash
# PostgreSQL-backed dashboard CRUD
psql "$DASH_DB_DSN" -c "select kind, count(*) from dashboard_entities group by kind order by kind;"

# ClickHouse-backed video analytics
clickhouse-client --query "select event_type, count() from goads.video_event_facts group by event_type order by event_type"
```

Or run the repo-provided smoke test against a running local server:

```bash
BASE_URL=http://localhost:8000 \
DASH_ADMIN_USER=admin \
DASH_ADMIN_PASS=change-me \
DASH_DB_DSN='postgres://user:pass@127.0.0.1:5432/goads?sslmode=disable' \
CLICKHOUSE_DSN='clickhouse://127.0.0.1:9000?database=goads' \
make smoke-storage-analytics
```

That smoke test will:

- create dashboard entities through the live CRUD API,
- confirm records exist in PostgreSQL,
- fire `/video/impression`, and
- confirm the impression fact lands in ClickHouse.

---

## 9 — Key endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET/POST` | `/video/vast` | VAST 3.0 ad response for players |
| `GET/POST` | `/video/ortb` | OpenRTB 2.5 bid request/response |
| `GET` | `/video/impression` | VAST impression beacon |
| `GET` | `/video/tracking` | VAST quartile / event tracking |
| `GET/POST` | `/dashboard` | Publisher admin dashboard |
| `GET` | `/status` | Health check |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `bind: permission denied` on port 80 | Need root or cap_net_bind | Run as root, or use `setcap 'cap_net_bind_service=+ep' prebid-server` |
| `pbs.yaml: no such file` | Config not found | Run from the directory that contains `pbs.yaml` |
| Empty VAST response | No demand configured | Add a `DemandVASTURL` or `DemandOrtbURL` to your placement in the dashboard |
| `Text file busy` on scp/deploy | Service is running | `systemctl stop prebid-server` first, then upload |
