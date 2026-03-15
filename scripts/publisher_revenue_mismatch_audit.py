#!/usr/bin/env python3

import argparse
import csv
import json
import io
import shlex
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.cookiejar import CookieJar


@dataclass
class PlacementAudit:
    placement_id: str
    expected_floor_cpm: float
    impressions: int
    avg_publisher_cpm: float
    min_publisher_cpm: float
    max_publisher_cpm: float
    avg_advertiser_cpm: float
    last_seen: str

    @property
    def avg_margin_cpm(self) -> float:
        return self.avg_advertiser_cpm - self.avg_publisher_cpm

    def to_dict(self, tolerance_cpm: float) -> dict[str, object]:
        return {
            "status": "MISMATCH" if is_mismatch(self, tolerance_cpm) else "OK",
            "placement_id": self.placement_id,
            "config_floor_cpm": round(self.expected_floor_cpm, 6),
            "avg_publisher_cpm": round(self.avg_publisher_cpm, 6),
            "min_publisher_cpm": round(self.min_publisher_cpm, 6),
            "max_publisher_cpm": round(self.max_publisher_cpm, 6),
            "avg_advertiser_cpm": round(self.avg_advertiser_cpm, 6),
            "avg_margin_cpm": round(self.avg_margin_cpm, 6),
            "impressions": self.impressions,
            "last_seen_utc": self.last_seen,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit publisher revenue against source floor CPM by comparing live dashboard "
            "placement config with recent ClickHouse impression rows."
        )
    )
    parser.add_argument("--dashboard-url", default="http://127.0.0.1", help="Dashboard base URL")
    parser.add_argument("--username", default="admin", help="Dashboard username")
    parser.add_argument("--password", required=True, help="Dashboard password")
    parser.add_argument("--database", default="goads", help="ClickHouse database name")
    parser.add_argument("--table", default="video_event_facts", help="ClickHouse table name")
    parser.add_argument("--clickhouse-binary", default="clickhouse-client", help="ClickHouse client executable")
    parser.add_argument(
        "--clickhouse-prefix",
        default="",
        help="Optional command prefix used before clickhouse-client, for example an SSH wrapper",
    )
    parser.add_argument(
        "--clickhouse-shell-template",
        default="",
        help=(
            "Optional shell command template containing {binary} and {query}; useful when the query "
            "must be quoted through SSH wrappers"
        ),
    )
    parser.add_argument("--lookback-minutes", type=int, default=120, help="Recent impression window to audit")
    parser.add_argument("--tolerance-cpm", type=float, default=0.0001, help="Allowed CPM delta before flagging mismatch")
    parser.add_argument(
        "--output-format",
        choices=["table", "json", "csv"],
        default="table",
        help="Output format for integrations or manual review",
    )
    parser.add_argument(
        "--positive-publisher-only",
        action="store_true",
        help="Ignore impression rows where publisher_revenue_usd is zero; useful during live migration cutovers",
    )
    parser.add_argument("--only-mismatches", action="store_true", help="Print only mismatched placements")
    return parser.parse_args()


def build_opener() -> urllib.request.OpenerDirector:
    cookie_jar = CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))


def login(opener: urllib.request.OpenerDirector, base_url: str, username: str, password: str) -> None:
    payload = json.dumps({"username": username, "password": password}).encode("utf-8")
    request = urllib.request.Request(
        urllib.parse.urljoin(base_url.rstrip("/") + "/", "dashboard/login"),
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with opener.open(request, timeout=15) as response:
        if response.status >= 400:
            raise RuntimeError(f"dashboard login failed with status {response.status}")


def fetch_configs(opener: urllib.request.OpenerDirector, base_url: str) -> dict[str, float]:
    request = urllib.request.Request(
        urllib.parse.urljoin(base_url.rstrip("/") + "/", "video/adserver"),
        method="GET",
    )
    with opener.open(request, timeout=15) as response:
        raw = response.read().decode("utf-8")
    payload = json.loads(raw)
    floors: dict[str, float] = {}
    for entry in payload:
        placement_id = str(entry.get("placement_id") or "").strip()
        if not placement_id:
            continue
        floors[placement_id] = float(entry.get("floor_cpm") or 0.0)
    return floors


def query_clickhouse(
    database: str,
    table: str,
    lookback_minutes: int,
    positive_publisher_only: bool,
    clickhouse_binary: str,
    clickhouse_prefix: str,
    clickhouse_shell_template: str,
) -> list[dict[str, object]]:
    zero_filter = "AND publisher_revenue_usd > 0" if positive_publisher_only else ""
    query = f"""
        SELECT
            placement_id,
            count() AS impressions,
            round(avg(publisher_revenue_usd) * 1000, 6) AS avg_publisher_cpm,
            round(min(publisher_revenue_usd) * 1000, 6) AS min_publisher_cpm,
            round(max(publisher_revenue_usd) * 1000, 6) AS max_publisher_cpm,
            round(avg(price_cpm), 6) AS avg_advertiser_cpm,
            formatDateTime(max(event_time), '%F %T', 'UTC') AS last_seen
        FROM {database}.{table}
        WHERE event_type = 'impression'
          AND event_time >= now() - INTERVAL {int(lookback_minutes)} MINUTE
                    {zero_filter}
        GROUP BY placement_id
        ORDER BY last_seen DESC
        FORMAT JSONEachRow
    """
    query = " ".join(query.split())
    if clickhouse_shell_template:
        command = clickhouse_shell_template.format(
            binary=shlex.quote(clickhouse_binary),
            query=shlex.quote(query),
        )
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            shell=True,
        )
    else:
        command = shlex.split(clickhouse_prefix) if clickhouse_prefix else []
        command.extend([clickhouse_binary, "--query", query])
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
    rows: list[dict[str, object]] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def build_audits(config_floors: dict[str, float], rows: list[dict[str, object]]) -> list[PlacementAudit]:
    audits: list[PlacementAudit] = []
    for row in rows:
        placement_id = str(row.get("placement_id") or "").strip()
        if not placement_id or placement_id not in config_floors:
            continue
        audits.append(
            PlacementAudit(
                placement_id=placement_id,
                expected_floor_cpm=config_floors[placement_id],
                impressions=int(row.get("impressions") or 0),
                avg_publisher_cpm=float(row.get("avg_publisher_cpm") or 0.0),
                min_publisher_cpm=float(row.get("min_publisher_cpm") or 0.0),
                max_publisher_cpm=float(row.get("max_publisher_cpm") or 0.0),
                avg_advertiser_cpm=float(row.get("avg_advertiser_cpm") or 0.0),
                last_seen=str(row.get("last_seen") or ""),
            )
        )
    return audits


def is_mismatch(audit: PlacementAudit, tolerance_cpm: float) -> bool:
    if abs(audit.avg_publisher_cpm - audit.expected_floor_cpm) > tolerance_cpm:
        return True
    if abs(audit.min_publisher_cpm - audit.expected_floor_cpm) > tolerance_cpm:
        return True
    if abs(audit.max_publisher_cpm - audit.expected_floor_cpm) > tolerance_cpm:
        return True
    return False


def filtered_audits(audits: list[PlacementAudit], tolerance_cpm: float, only_mismatches: bool) -> list[PlacementAudit]:
    if not only_mismatches:
        return audits
    return [audit for audit in audits if is_mismatch(audit, tolerance_cpm)]


def emit_table(audits: list[PlacementAudit], tolerance_cpm: float) -> None:
    if not audits:
        print("No placements with recent impression data were found in the requested window.")
        return

    header = (
        "status\tplacement_id\tconfig_floor_cpm\tavg_publisher_cpm\tmin_publisher_cpm\t"
        "max_publisher_cpm\tavg_margin_cpm\timpressions\tlast_seen_utc"
    )
    print(header)
    for audit in audits:
        status = "MISMATCH" if is_mismatch(audit, tolerance_cpm) else "OK"
        print(
            "\t".join(
                [
                    status,
                    audit.placement_id,
                    f"{audit.expected_floor_cpm:.6f}",
                    f"{audit.avg_publisher_cpm:.6f}",
                    f"{audit.min_publisher_cpm:.6f}",
                    f"{audit.max_publisher_cpm:.6f}",
                    f"{audit.avg_margin_cpm:.6f}",
                    str(audit.impressions),
                    audit.last_seen,
                ]
            )
        )


def emit_json(audits: list[PlacementAudit], tolerance_cpm: float) -> None:
    payload = {
        "count": len(audits),
        "mismatch_count": sum(1 for audit in audits if is_mismatch(audit, tolerance_cpm)),
        "placements": [audit.to_dict(tolerance_cpm) for audit in audits],
    }
    print(json.dumps(payload, indent=2))


def emit_csv(audits: list[PlacementAudit], tolerance_cpm: float) -> None:
    output = io.StringIO()
    fieldnames = [
        "status",
        "placement_id",
        "config_floor_cpm",
        "avg_publisher_cpm",
        "min_publisher_cpm",
        "max_publisher_cpm",
        "avg_advertiser_cpm",
        "avg_margin_cpm",
        "impressions",
        "last_seen_utc",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for audit in audits:
        writer.writerow(audit.to_dict(tolerance_cpm))
    print(output.getvalue(), end="")


def print_report(audits: list[PlacementAudit], tolerance_cpm: float, only_mismatches: bool, output_format: str) -> int:
    selected_audits = filtered_audits(audits, tolerance_cpm, only_mismatches)
    if output_format == "json":
        emit_json(selected_audits, tolerance_cpm)
    elif output_format == "csv":
        emit_csv(selected_audits, tolerance_cpm)
    else:
        emit_table(selected_audits, tolerance_cpm)
    return 1 if any(is_mismatch(audit, tolerance_cpm) for audit in selected_audits) else 0


def main() -> int:
    args = parse_args()
    opener = build_opener()
    try:
        login(opener, args.dashboard_url, args.username, args.password)
        config_floors = fetch_configs(opener, args.dashboard_url)
        rows = query_clickhouse(
            args.database,
            args.table,
            args.lookback_minutes,
            args.positive_publisher_only,
            args.clickhouse_binary,
            args.clickhouse_prefix,
            args.clickhouse_shell_template,
        )
    except urllib.error.HTTPError as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        return 2
    except urllib.error.URLError as exc:
        print(f"URL error: {exc}", file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as exc:
        print(exc.stderr.strip() or str(exc), file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"Command not found: {exc}", file=sys.stderr)
        return 2
    audits = build_audits(config_floors, rows)
    return print_report(audits, args.tolerance_cpm, args.only_mismatches, args.output_format)


if __name__ == "__main__":
    raise SystemExit(main())