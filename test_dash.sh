#!/bin/bash
# Test dashboard login and page load
echo "=== Testing login ==="
RESP=$(curl -s -c /tmp/cookies.txt -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}' \
  http://localhost/dashboard/login)
echo "Login response: $RESP"

echo ""
echo "=== Testing dashboard page ==="
curl -s -o /dev/null -w "dashboard: status=%{http_code} time=%{time_total}s size=%{size_download}\n" \
  -b /tmp/cookies.txt http://localhost/dashboard

echo ""
echo "=== Testing stats/video ==="
curl -s -o /dev/null -w "stats:    status=%{http_code} time=%{time_total}s size=%{size_download}\n" \
  -b /tmp/cookies.txt http://localhost/dashboard/stats/video

echo ""
echo "=== Testing publishers ==="
curl -s -o /dev/null -w "publishers: status=%{http_code} time=%{time_total}s size=%{size_download}\n" \
  -b /tmp/cookies.txt http://localhost/dashboard/publishers

echo ""
echo "=== Testing config ==="
curl -s -o /dev/null -w "config:   status=%{http_code} time=%{time_total}s size=%{size_download}\n" \
  -b /tmp/cookies.txt http://localhost/dashboard/config

rm -f /tmp/cookies.txt
echo ""
echo "DONE"
