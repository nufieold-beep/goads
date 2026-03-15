# Publisher Revenue Audit

This repository includes an operational audit script at `scripts/publisher_revenue_mismatch_audit.py`.

The script compares:

- `floor_cpm` from the live ad server placement config
- `publisher_revenue_usd * 1000` from recent ClickHouse impression rows

It is intended to catch cases where publisher payout attribution no longer matches the configured source floor.

## Recommended usage

Run the script on the live server, where `clickhouse-client` is already available.

```bash
python3 /tmp/publisher_revenue_mismatch_audit.py \
  --dashboard-url http://127.0.0.1 \
  --username admin \
  --password 'DASHBOARD_PASSWORD' \
  --lookback-minutes 180 \
  --positive-publisher-only
```

## Why `--positive-publisher-only` matters

During a live rollout, older impression rows may predate the `publisher_revenue_usd` column migration and therefore still contain `0`.

Use `--positive-publisher-only` during that transition window to avoid false mismatch alarms from legacy rows.

## Output formats

Human-readable table output:

```bash
python3 /tmp/publisher_revenue_mismatch_audit.py \
  --dashboard-url http://127.0.0.1 \
  --username admin \
  --password 'DASHBOARD_PASSWORD' \
  --lookback-minutes 180 \
  --positive-publisher-only
```

JSON output for alerting or downstream automation:

```bash
python3 /tmp/publisher_revenue_mismatch_audit.py \
  --dashboard-url http://127.0.0.1 \
  --username admin \
  --password 'DASHBOARD_PASSWORD' \
  --lookback-minutes 180 \
  --positive-publisher-only \
  --output-format json
```

CSV output for cron jobs or spreadsheet import:

```bash
python3 /tmp/publisher_revenue_mismatch_audit.py \
  --dashboard-url http://127.0.0.1 \
  --username admin \
  --password 'DASHBOARD_PASSWORD' \
  --lookback-minutes 180 \
  --positive-publisher-only \
  --output-format csv
```

## Exit codes

- `0`: no mismatches found
- `1`: one or more mismatches found
- `2`: execution error, dependency problem, or query/auth failure

This makes the script suitable for cron, CI, or simple alert wrappers.

## Example cron-friendly pattern

```bash
python3 /tmp/publisher_revenue_mismatch_audit.py \
  --dashboard-url http://127.0.0.1 \
  --username admin \
  --password "$DASH_ADMIN_PASS" \
  --lookback-minutes 180 \
  --positive-publisher-only \
  --output-format json \
  > /var/log/prebid-server/publisher-revenue-audit.json
```

If you want alert-only output, combine `--only-mismatches` with `--output-format json`.