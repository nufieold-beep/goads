package endpoints

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"testing"

	"github.com/prebid/openrtb/v20/openrtb2"
	metricsConf "github.com/prebid/prebid-server/v4/metrics/config"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func TestHandleFastORTBJSONHasNoTrailingNewline(t *testing.T) {
	const placementID = "bench-placement"
	baseBidResp := &openrtb2.BidResponse{
		ID: "bench-auction",
		SeatBid: []openrtb2.SeatBid{{
			Seat: "bench-seat",
			Bid: []openrtb2.Bid{{
				ID:      "bench-bid",
				ImpID:   placementID,
				Price:   1.23,
				AdM:     "<VAST version=\"3.0\"></VAST>",
				CrID:    "bench-creative",
				ADomain: []string{"advertiser.example"},
			}},
		}},
	}
	handler := &VideoPipelineHandler{
		metricsEng: &metricsConf.NilMetricsEngine{},
		configStore: &adServerConfigStore{configs: map[string]*AdServerConfig{
			placementID: {
				PlacementID:        placementID,
				PublisherID:        "bench-publisher",
				AdvertiserID:       "bench-advertiser",
				DemandOrtbURL:      "https://demand.example/openrtb",
				VideoPlacementType: "instream",
				Active:             true,
				MinDuration:        15,
				MaxDuration:        30,
				TimeoutMS:          500,
			},
		}},
		tracking:         &trackingStore{},
		videoStats:       newVideoStatsStore(""),
		firedImpressions: newImpressionDeduper(),
		adapterRouterFn: func(RouterKey) DemandAdapter {
			return benchmarkDemandAdapter{resp: &DemandResponse{
				BidResp:   cloneBidResponse(baseBidResp),
				WinPrice:  1.23,
				NoFill:    false,
				Bidder:    "bench-seat",
				CrID:      "bench-creative",
				ADomain:   []string{"advertiser.example"},
				AuctionID: "bench-auction",
			}}
		},
		bufPool: sync.Pool{
			New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) },
		},
		done: make(chan struct{}),
	}

	prevWriter := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(prevWriter)

	listener := fasthttputil.NewInmemoryListener()
	server := &fasthttp.Server{Handler: handler.HandleFastORTB}
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

	request.Header.SetMethod(http.MethodGet)
	request.SetRequestURI("http://bench.local/video/ortb?placement_id=" + placementID + "&app_bundle=tv.example.app&domain=tv.example&ip=203.0.113.10&ua=BenchmarkUA/1.0&device_type=3&w=1920&h=1080&min_dur=15&max_dur=30&country_code=ID&tmax=500")

	if err := client.Do(request, response); err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status=%d want=%d", response.StatusCode(), fasthttp.StatusOK)
	}
	body := response.Body()
	if len(body) == 0 {
		t.Fatal("empty response body")
	}
	if body[len(body)-1] == '\n' {
		t.Fatalf("response body has trailing newline: %q", body)
	}
}