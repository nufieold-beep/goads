# Makefile

all: deps test build-modules build

.PHONY: deps test build-modules build build-analytics image format smoke-storage-analytics smoke-demand-forwarding smoke-nurl-vast-modes bench-fasthttp loadtest-status loadtest-video-tracking

# deps will clean out the vendor directory and use go mod for a fresh install
deps:
	GOPROXY="https://proxy.golang.org" go mod vendor -v && go mod tidy -v
	
# test will ensure that all of our dependencies are available and run validate.sh
test: deps
# If there is no indentation, Make will treat it as a directive for itself; otherwise, it's regarded as a shell script.
# https://stackoverflow.com/a/4483467
ifeq "$(adapter)" ""
	./validate.sh
else
	go test github.com/prebid/prebid-server/v4/adapters/$(adapter) -bench=.
endif

# build-modules generates modules/builder.go file which provides a list of all available modules
build-modules:
	go generate modules/modules.go

# build-analytics validates analytics packages after analytics module changes
build-analytics:
	go generate ./analytics/build
	go test ./analytics/...

# build will ensure all of our tests pass and then build the go binary
build: test
	go build -mod=vendor ./...

# image will build a docker image
image:
	docker build -t prebid-server .

# smoke-storage-analytics verifies PostgreSQL CRUD persistence and ClickHouse
# impression analytics against a running local server.
smoke-storage-analytics:
	go run ./scripts/storage_analytics_smoke.go

# smoke-demand-forwarding verifies ad-unit request forwarding and demand
# responses across VAST/ORTB routing paths against a running local server.
smoke-demand-forwarding:
	go run ./scripts/demand_forwarding_smoke.go

# smoke-nurl-vast-modes verifies NURL-only handling plus VAST inline/wrapper
# behavior against a running local server.
smoke-nurl-vast-modes:
	go run ./scripts/nurl_vast_modes_smoke.go

# bench-fasthttp compares the native fasthttp dispatch path against the
# legacy adapted net/http path for representative routes.
bench-fasthttp:
	go test -run '^$$' -bench 'BenchmarkFastHTTP' -benchmem ./router ./endpoints

# loadtest-status measures end-to-end throughput against a running local server.
loadtest-status:
	go run ./cmd/fasthttp-loadtest -url http://127.0.0.1:8000/status -concurrency 200 -duration 15s

# loadtest-video-tracking exercises the hot video tracking beacon path against
# a running local server without relying on external demand.
loadtest-video-tracking:
	go run ./cmd/fasthttp-loadtest -url 'http://127.0.0.1:8000/video/tracking?auction_id=bench-auction&bid_id=bench-bid&bidder=bench&event=start&placement_id=bench-placement&price=1.23' -concurrency 200 -duration 15s

# format runs format
format:
	./scripts/format.sh -f true

# formatcheck runs format for diagnostics, without modifying the code
formatcheck:
	./scripts/format.sh -f false
