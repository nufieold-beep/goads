#!/usr/bin/env python3
import argparse
import concurrent.futures
import statistics
import time
import urllib.error
import urllib.request


def hit(url: str, timeout: float) -> tuple[int, float]:
    started = time.perf_counter()
    status = 0
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            status = resp.getcode()
            resp.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.read()
    except Exception:
        status = 0
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return status, elapsed_ms


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * pct))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser(description="Basic concurrent load test without external dependencies")
    parser.add_argument("--url", default="http://localhost:8000/status")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--ok-status", default="200,204")
    args = parser.parse_args()

    ok_statuses = {int(item) for item in args.ok_status.split(",") if item.strip()}
    latencies: list[float] = []
    success = 0
    failures = 0
    started = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(hit, args.url, args.timeout) for _ in range(args.requests)]
        for future in concurrent.futures.as_completed(futures):
            status, latency_ms = future.result()
            latencies.append(latency_ms)
            if status in ok_statuses:
                success += 1
            else:
                failures += 1

    total_seconds = max(time.perf_counter() - started, 0.001)
    print(f"requests={args.requests}")
    print(f"concurrency={args.concurrency}")
    print(f"success={success}")
    print(f"failures={failures}")
    print(f"throughput_rps={args.requests / total_seconds:.2f}")
    print(f"latency_avg_ms={statistics.mean(latencies):.2f}")
    print(f"latency_p50_ms={percentile(latencies, 0.50):.2f}")
    print(f"latency_p95_ms={percentile(latencies, 0.95):.2f}")
    print(f"latency_max_ms={max(latencies) if latencies else 0.0:.2f}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())