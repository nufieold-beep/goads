#!/usr/bin/env python3
import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar


def request(opener, method, url, body=None, headers=None):
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with opener.open(req, timeout=15) as resp:
            return resp.getcode(), resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)


def expect(name, status, allowed):
    ok = status in allowed
    print(f"[{ 'PASS' if ok else 'FAIL' }] {name}: status={status}, expected={sorted(allowed)}")
    return ok


def main():
    parser = argparse.ArgumentParser(description="Run a staging smoke test against PBS routes")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default=None)
    args = parser.parse_args()

    password = args.password or ""
    base_url = args.base_url.rstrip("/")

    cookie_jar = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    passed = True

    status_code, _, _ = request(opener, "GET", f"{base_url}/status")
    passed &= expect("GET /status", status_code, {200, 204})

    login_code, _, _ = request(
        opener,
        "POST",
        f"{base_url}/dashboard/login",
        body={"username": args.user, "password": password},
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    passed &= expect("POST /dashboard/login", login_code, {200})

    dash_code, _, _ = request(opener, "GET", f"{base_url}/dashboard")
    passed &= expect("GET /dashboard", dash_code, {200})

    auction_payload = {
        "id": "smoke-auction",
        "imp": [{"id": "1", "banner": {"format": [{"w": 300, "h": 250}]}}],
        "site": {"page": "https://example.com"},
        "device": {"ua": "pbs-smoke-test", "ip": "127.0.0.1"},
        "user": {"id": "smoke-user"},
        "tmax": 500,
        "cur": ["USD"],
    }
    auction_code, _, _ = request(
        opener,
        "POST",
        f"{base_url}/openrtb2/auction",
        body=auction_payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    passed &= expect("POST /openrtb2/auction", auction_code, {200, 204, 400})

    vast_query = urllib.parse.urlencode({"placement_id": "smoke-placement", "app_bundle": "smoke.bundle"})
    vast_code, _, _ = request(opener, "GET", f"{base_url}/video/vast?{vast_query}")
    passed &= expect("GET /video/vast", vast_code, {200})

    ortb_code, _, _ = request(opener, "GET", f"{base_url}/video/ortb?{vast_query}")
    passed &= expect("GET /video/ortb", ortb_code, {200, 204})

    beacon_query = urllib.parse.urlencode(
        {
            "auction_id": "smoke-auction",
            "bid_id": "smoke-bid",
            "bidder": "smoke",
            "placement_id": "smoke-placement",
            "event": "start",
        }
    )
    imp_code, _, _ = request(opener, "GET", f"{base_url}/video/impression?auction_id=smoke-auction&bid_id=smoke-bid&placement_id=smoke-placement")
    passed &= expect("GET /video/impression", imp_code, {200})

    tracking_code, _, _ = request(opener, "GET", f"{base_url}/video/tracking?{beacon_query}")
    passed &= expect("GET /video/tracking", tracking_code, {200})

    if not passed:
        return 1
    print("Smoke test completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())