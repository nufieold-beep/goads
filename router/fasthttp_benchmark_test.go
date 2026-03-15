package router

import (
	"net/http"
	"net"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/prebid/prebid-server/v4/endpoints"
	metricsConf "github.com/prebid/prebid-server/v4/metrics/config"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
	"github.com/valyala/fasthttp/fasthttputil"
)

func BenchmarkFastHTTPRouteDispatch(b *testing.B) {
	router := newFastHTTPBenchmarkRouter()

	benchRoutePair(b, "status", router.FastHTTPHandler(), fasthttpadaptor.NewFastHTTPHandler(http.HandlerFunc(router.ServeHTTP)), http.MethodGet, "/status", nil, fasthttp.StatusOK)
	benchRoutePair(b, "version", router.FastHTTPHandler(), fasthttpadaptor.NewFastHTTPHandler(http.HandlerFunc(router.ServeHTTP)), http.MethodGet, "/version", nil, fasthttp.StatusOK)
	benchRoutePair(b, "dashboard_stats_unauthorized", router.FastHTTPHandler(), fasthttpadaptor.NewFastHTTPHandler(http.HandlerFunc(router.ServeHTTP)), http.MethodGet, "/dashboard/stats", nil, fasthttp.StatusFound)
	benchRoutePair(b, "dashboard_login_json_invalid", router.FastHTTPHandler(), fasthttpadaptor.NewFastHTTPHandler(http.HandlerFunc(router.ServeHTTP)), http.MethodPost, "/dashboard/login", []byte(`{"username":"admin","password":"invalid"}`), fasthttp.StatusUnauthorized)
}

func newFastHTTPBenchmarkRouter() *Router {
	r := &Router{
		Router:          httprouter.New(),
		MetricsEngine:   &metricsConf.DetailedMetricsEngine{},
		fastStatus:      endpoints.NewFastStatusHandler("ok", nil),
		versionResponse: []byte(`{"revision":"bench","version":"bench"}`),
	}

	auth := endpoints.DashboardAuthMiddleware
	r.GET("/status", endpoints.NewStatusEndpoint("ok", nil))
	r.Handler(http.MethodGet, "/version", endpoints.NewVersionEndpoint("bench", "bench"))
	r.GET("/", serveIndex)
	r.POST("/dashboard/login", endpoints.NewDashboardLoginPostHandler())
	r.GET("/dashboard/stats", auth(endpoints.NewDashboardStatsHandler(nil)))

	return r
}

func benchRoutePair(b *testing.B, name string, native fasthttp.RequestHandler, adapted fasthttp.RequestHandler, method, path string, body []byte, expectedStatus int) {
	b.Run(name+"/native", func(b *testing.B) {
		benchmarkFastHTTPHandler(b, native, method, path, body, expectedStatus)
	})
	b.Run(name+"/adapted", func(b *testing.B) {
		benchmarkFastHTTPHandler(b, adapted, method, path, body, expectedStatus)
	})
}

func benchmarkFastHTTPHandler(b *testing.B, handler fasthttp.RequestHandler, method, path string, body []byte, expectedStatus int) {
	listener := fasthttputil.NewInmemoryListener()
	server := &fasthttp.Server{Handler: handler}
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		_ = server.Serve(listener)
	}()
	defer func() {
		_ = server.Shutdown()
		_ = listener.Close()
		<-serverDone
	}()

	client := &fasthttp.Client{Dial: func(string) (net.Conn, error) { return listener.Dial() }}
	request := fasthttp.AcquireRequest()
	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(request)
	defer fasthttp.ReleaseResponse(response)

	request.Header.SetMethod(method)
	request.SetRequestURI("http://bench.local" + path)
	if len(body) > 0 {
		request.SetBodyRaw(body)
		request.Header.SetContentType("application/json")
		request.Header.Set("Accept", "application/json")
	}

	if err := client.Do(request, response); err != nil {
		b.Fatalf("warmup request failed: %v", err)
	}
	if response.StatusCode() != expectedStatus {
		b.Fatalf("warmup status=%d want=%d", response.StatusCode(), expectedStatus)
	}

	b.ReportAllocs()
	b.ResetTimer()
	started := time.Now()
	for i := 0; i < b.N; i++ {
		response.Reset()
		if err := client.Do(request, response); err != nil {
			b.Fatalf("request failed: %v", err)
		}
		if response.StatusCode() != expectedStatus {
			b.Fatalf("status=%d want=%d", response.StatusCode(), expectedStatus)
		}
	}
	elapsed := time.Since(started)
	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "req/s")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "ns/req")
	if code := response.StatusCode(); code != expectedStatus {
		b.Fatalf("final status=%d want=%d", code, expectedStatus)
	}
}