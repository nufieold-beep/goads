package endpoints

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prebid/openrtb/v20/openrtb2"
	metricsConf "github.com/prebid/prebid-server/v4/metrics/config"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
	"github.com/valyala/fasthttp/fasthttputil"
)

type benchmarkDemandAdapter struct {
	resp *DemandResponse
	err  error
}

func (a benchmarkDemandAdapter) Execute(context.Context, *PlayerRequest, *AdServerConfig) (*DemandResponse, error) {
	return a.resp, a.err
}

func BenchmarkFastHTTPVideoTracking(b *testing.B) {
	const placementID = "bench-placement"
	handler := &VideoPipelineHandler{
		metricsEng:  &metricsConf.NilMetricsEngine{},
		configStore: &adServerConfigStore{configs: map[string]*AdServerConfig{
			placementID: {
				PlacementID: placementID,
				PublisherID: "bench-publisher",
				Active:      true,
				MinDuration: 15,
				MaxDuration: 30,
			},
		}},
		tracking:    &trackingStore{},
		videoStats:  newVideoStatsStore(""),
		firedImpressions: newImpressionDeduper(),
		done:        make(chan struct{}),
	}

	native := handler.HandleFastTracking
	adapted := fasthttpadaptor.NewFastHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.TrackingEndpoint()(w, r, nil)
	}))
	path := "/video/tracking?auction_id=bench-auction&bid_id=bench-bid&bidder=bench&event=start&placement_id=" + placementID + "&price=1.23"

	b.Run("native", func(b *testing.B) {
		benchmarkFastEndpointHandler(b, native, http.MethodGet, path, fasthttp.StatusOK)
	})
	b.Run("adapted", func(b *testing.B) {
		benchmarkFastEndpointHandler(b, adapted, http.MethodGet, path, fasthttp.StatusOK)
	})
}

func BenchmarkFastHTTPVideoImpression(b *testing.B) {
	const (
		placementID = "bench-placement"
		auctionID   = "bench-auction"
	)
	newHandler := func() *VideoPipelineHandler {
		handler := &VideoPipelineHandler{
			metricsEng:  &metricsConf.NilMetricsEngine{},
			configStore: &adServerConfigStore{configs: map[string]*AdServerConfig{
				placementID: {
					PlacementID:  placementID,
					PublisherID:  "bench-publisher",
					AdvertiserID: "bench-advertiser",
					Active:       true,
				},
			}},
			tracking:   &trackingStore{},
			videoStats: newVideoStatsStore(""),
			firedImpressions: newImpressionDeduper(),
			done:       make(chan struct{}),
		}
		handler.videoStats.auctionDims[auctionID] = &auctionDimKey{
			Bidder:       "bench",
			Placement:    placementID,
			PriceCPM:     1.23,
			PublisherID:  "bench-publisher",
			AdvertiserID: "bench-advertiser",
		}
		return handler
	}
	basePath := "/video/impression?auction_id=" + auctionID + "&placement_id=" + placementID + "&bidder=bench&crid=creative-1&price=1.23"
	prevWriter := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(prevWriter)

	b.Run("native", func(b *testing.B) {
		handler := newHandler()
		benchmarkFastUniqueQueryHandler(b, handler.HandleFastImpression, http.MethodGet, basePath, "bid_id=bench-bid-", fasthttp.StatusOK)
	})
	b.Run("adapted", func(b *testing.B) {
		handler := newHandler()
		adapted := fasthttpadaptor.NewFastHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler.ImpressionEndpoint()(w, r, nil)
		}))
		benchmarkFastUniqueQueryHandler(b, adapted, http.MethodGet, basePath, "bid_id=bench-bid-", fasthttp.StatusOK)
	})
}

func BenchmarkFastHTTPVideoORTB(b *testing.B) {
	const placementID = "bench-placement"
	baseBidResp := &openrtb2.BidResponse{
		ID: "bench-auction",
		SeatBid: []openrtb2.SeatBid{{
			Seat: "bench-seat",
			Bid: []openrtb2.Bid{{
				ID:     "bench-bid",
				ImpID:  placementID,
				Price:  1.23,
				AdM:    "<VAST version=\"3.0\"></VAST>",
				CrID:   "bench-creative",
				ADomain: []string{"advertiser.example"},
				NURL:   "https://nurl.example/win",
				BURL:   "https://burl.example/bill",
			}},
		}},
	}
	newHandler := func() *VideoPipelineHandler {
		resp := &DemandResponse{
			BidResp:    cloneBidResponse(baseBidResp),
			WinPrice:   1.23,
			NoFill:     false,
			Bidder:     "bench-seat",
			CrID:       "bench-creative",
			ADomain:    []string{"advertiser.example"},
			AuctionID:  "bench-auction",
			BURL:       "https://burl.example/bill",
		}
		handler := &VideoPipelineHandler{
			metricsEng: &metricsConf.NilMetricsEngine{},
			configStore: &adServerConfigStore{configs: map[string]*AdServerConfig{
				placementID: {
					PlacementID:       placementID,
					PublisherID:       "bench-publisher",
					AdvertiserID:      "bench-advertiser",
					DemandOrtbURL:     "https://demand.example/openrtb",
					VideoPlacementType:"instream",
					Active:            true,
					MinDuration:       15,
					MaxDuration:       30,
					TimeoutMS:         500,
				},
			}},
			tracking:          &trackingStore{},
			videoStats:        newVideoStatsStore(""),
			firedImpressions:  newImpressionDeduper(),
			adapterRouterFn:   func(RouterKey) DemandAdapter { return benchmarkDemandAdapter{resp: resp} },
			bufPool: sync.Pool{
				New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) },
			},
			done:              make(chan struct{}),
		}
		return handler
	}
	path := "/video/ortb?placement_id=" + placementID + "&app_bundle=tv.example.app&domain=tv.example&ip=203.0.113.10&ua=BenchmarkUA/1.0&device_type=3&w=1920&h=1080&min_dur=15&max_dur=30&country_code=ID&tmax=500"
	prevWriter := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(prevWriter)

	b.Run("native", func(b *testing.B) {
		handler := newHandler()
		benchmarkFastEndpointHandler(b, handler.HandleFastORTB, http.MethodGet, path, fasthttp.StatusOK)
	})
	b.Run("adapted", func(b *testing.B) {
		handler := newHandler()
		adapted := fasthttpadaptor.NewFastHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler.ORTBEndpoint()(w, r, nil)
		}))
		benchmarkFastEndpointHandler(b, adapted, http.MethodGet, path, fasthttp.StatusOK)
	})
}

func cloneBidResponse(src *openrtb2.BidResponse) *openrtb2.BidResponse {
	if src == nil {
		return nil
	}
	clone := *src
	if len(src.SeatBid) > 0 {
		clone.SeatBid = make([]openrtb2.SeatBid, len(src.SeatBid))
		for i := range src.SeatBid {
			clone.SeatBid[i] = src.SeatBid[i]
			if len(src.SeatBid[i].Bid) > 0 {
				clone.SeatBid[i].Bid = make([]openrtb2.Bid, len(src.SeatBid[i].Bid))
				copy(clone.SeatBid[i].Bid, src.SeatBid[i].Bid)
			}
		}
	}
	return &clone
}

func benchmarkFastEndpointHandler(b *testing.B, handler fasthttp.RequestHandler, method, path string, expectedStatus int) {
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
}

func benchmarkFastUniqueQueryHandler(b *testing.B, handler fasthttp.RequestHandler, method, basePath, bidPrefix string, expectedStatus int) {
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
	request.SetRequestURI("http://bench.local/health")

	makeURI := func(i int) string {
		return "http://bench.local" + basePath + "&" + bidPrefix + strconv.Itoa(i)
	}

	request.SetRequestURI(makeURI(0))
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
		request.SetRequestURI(makeURI(i + 1))
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
}