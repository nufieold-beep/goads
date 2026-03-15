# Production Readiness

This repository is suitable for staging and controlled rollout, but it is not yet ready for an unrestricted production rollout.

## Green Now

- Native fasthttp hot paths are wired for the main high-traffic routes: status, dashboard auth pages, video VAST, video ORTB, impression, and tracking.
- Route and method parity was reviewed between the fast dispatcher and the legacy router for dashboard login/logout, dashboard stats/config, video adserver, video VAST, video ORTB, impression, and tracking.
- The broad non-scripts test sweep is green with `DASH_ADMIN_PASS=test go test $(go list ./... | grep -v '/scripts$')`.
- Fast ORTB response behavior has regression coverage to ensure JSON output does not end with a trailing newline.
- A local load generator already exists at `cmd/fasthttp-loadtest` and can exercise the deployed server without external benchmarking tools.

## Must Fix Before Full Production

- Fast video handlers create their request timeout context from `context.Background()` instead of inheriting caller cancellation. The legacy net/http path uses `r.Context()`, so client disconnect and upstream cancellation semantics are currently weaker on the fast path.
- Production evidence is still missing for long-running soak behavior under realistic placement mix, real adapters, and infrastructure timeouts. Current proof is primarily microbenchmarks, local smoke runs, and the broad test sweep.
- External dependency behavior still needs operational review. Historical logs show vendor-list fetch timeouts; those do not block local development but they are production concerns if they repeat at startup or during config refresh.

## Recommended Before Scaling Up

- Run sustained soak tests with representative traffic and alert on latency, 5xx rate, timeout rate, and goroutine growth.
- Validate semantic parity with end-to-end fixtures that compare fast and legacy responses for the same placements, especially around no-fill, win notice, impression, and billing notice flows.
- Verify dashboard and admin routes behind the intended reverse proxy so forwarded scheme, secure cookies, and CORS behavior match deployment expectations.
- Capture a rollback plan that can disable or bypass fast hot paths if production metrics diverge from the adapted router.

## Soak Plan

Use `scripts/soak_local_fastpaths.sh` against a running local or staging server.

Default coverage:

- `/status`
- `/video/tracking`

Optional coverage via environment variables:

- `ORTB_URL`
- `VAST_URL`

Suggested first gate:

1. Start the server locally or in staging.
2. Run `make soak-fastpaths` for the default status and tracking soak.
3. Re-run with real `ORTB_URL` and `VAST_URL` that point to provisioned placements.
4. Inspect the generated logs under `logs/soak/` and confirm stable latency, zero transport errors, and expected HTTP status distribution.
