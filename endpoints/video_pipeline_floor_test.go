package endpoints

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	metricsConf "github.com/prebid/prebid-server/v4/metrics/config"
)

type telemetryDemandAdapter struct{}

func (telemetryDemandAdapter) Execute(_ context.Context, _ *PlayerRequest, cfg *AdServerConfig) (*DemandResponse, error) {
	if cfg.CampaignID == "extra-campaign" {
		return &DemandResponse{
			VASTXml:   `<VAST version="3.0"></VAST>`,
			WinPrice:  3.0,
			NoFill:    false,
			Bidder:    "extra-seat",
			AuctionID: "auction-extra",
			ADomain:   []string{"extra.example"},
			CrID:      "creative-extra",
		}, nil
	}
	return &DemandResponse{
		VASTXml:   `<VAST version="3.0"></VAST>`,
		WinPrice:  0.5,
		NoFill:    false,
		Bidder:    "primary-seat",
		AuctionID: "auction-primary",
		ADomain:   []string{"primary.example"},
		CrID:      "creative-primary",
	}, nil
}

func TestBuildOpenRTBRequestUsesDemandFloorForDirectDemand(t *testing.T) {
	handler := &VideoPipelineHandler{}
	request := &PlayerRequest{
		PlacementID: "placement-1",
		AppBundle:   "tv.example.app",
		DeviceType:  3,
		Width:       1920,
		Height:      1080,
	}
	config := &AdServerConfig{
		PlacementID:        "placement-1",
		PublisherID:        "pub-1",
		DomainOrApp:        "tv.example.app",
		FloorCPM:           2.50,
		DemandFloorCPM:     4.25,
		DemandOrtbURL:      "https://demand.example/openrtb",
		VideoPlacementType: "instream",
		MinDuration:        15,
		MaxDuration:        30,
	}

	bidReq := handler.buildOpenRTBRequest(request, config)
	if len(bidReq.Imp) != 1 {
		t.Fatalf("expected 1 imp, got %d", len(bidReq.Imp))
	}
	if bidReq.Imp[0].BidFloor != 4.25 {
		t.Fatalf("expected demand floor bidfloor 4.25, got %v", bidReq.Imp[0].BidFloor)
	}
}

func TestBuildOpenRTBRequestUsesSourceFloorWithoutDirectDemand(t *testing.T) {
	handler := &VideoPipelineHandler{}
	request := &PlayerRequest{
		PlacementID: "placement-2",
		Domain:      "tv.example.com",
		DeviceType:  3,
		Width:       1920,
		Height:      1080,
	}
	config := &AdServerConfig{
		PlacementID:        "placement-2",
		PublisherID:        "pub-2",
		DomainOrApp:        "tv.example.com",
		FloorCPM:           2.50,
		DemandFloorCPM:     4.25,
		VideoPlacementType: "instream",
		MinDuration:        15,
		MaxDuration:        30,
	}

	bidReq := handler.buildOpenRTBRequest(request, config)
	if len(bidReq.Imp) != 1 {
		t.Fatalf("expected 1 imp, got %d", len(bidReq.Imp))
	}
	if bidReq.Imp[0].BidFloor != 2.50 {
		t.Fatalf("expected source floor bidfloor 2.50, got %v", bidReq.Imp[0].BidFloor)
	}
}

func TestFetchTrackedVASTDemandUsesDemandFloorInPBSBeacons(t *testing.T) {
	handler := &VideoPipelineHandler{
		externalURL: "https://pbs.example",
		bufPool: sync.Pool{
			New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) },
		},
		demandClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`<VAST version="3.0"><Ad><InLine><AdSystem>test</AdSystem><AdTitle>direct</AdTitle></InLine></Ad></VAST>`)),
				Header:     make(http.Header),
			}, nil
		})},
	}
	request := &PlayerRequest{PlacementID: "placement-vast"}
	config := &AdServerConfig{
		PlacementID:    "placement-vast",
		PublisherID:    "pub-1",
		FloorCPM:       2.50,
		DemandFloorCPM: 4.25,
		DemandVASTURL:  "https://demand.example/vast",
	}

	vastXML, _, noFill, err := handler.fetchTrackedVASTDemand(context.Background(), request, config)
	if err != nil {
		t.Fatalf("fetchTrackedVASTDemand returned error: %v", err)
	}
	if noFill {
		t.Fatal("expected direct VAST demand to fill")
	}
	if !strings.Contains(vastXML, "price=4.25") {
		t.Fatalf("expected beacons to carry demand floor price 4.25, got %q", vastXML)
	}
	if strings.Contains(vastXML, "price=2.5") {
		t.Fatalf("expected source floor to be excluded from demand beacons, got %q", vastXML)
	}
}

func TestDirectVASTAdaptersUseDemandFloorAsWinPrice(t *testing.T) {
	handler := &VideoPipelineHandler{
		bufPool: sync.Pool{
			New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) },
		},
		demandClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`<VAST version="3.0"><Ad><InLine><AdSystem>test</AdSystem><AdTitle>direct</AdTitle></InLine></Ad></VAST>`)),
				Header:     make(http.Header),
			}, nil
		})},
	}
	request := &PlayerRequest{PlacementID: "placement-vast"}
	config := &AdServerConfig{
		PlacementID:    "placement-vast",
		PublisherID:    "pub-1",
		FloorCPM:       2.50,
		DemandFloorCPM: 4.25,
		DemandVASTURL:  "https://demand.example/vast",
	}

	vastResp, err := (&vastToVASTAdapter{h: handler}).Execute(context.Background(), request, config)
	if err != nil {
		t.Fatalf("vastToVASTAdapter.Execute returned error: %v", err)
	}
	if vastResp.WinPrice != 4.25 {
		t.Fatalf("expected direct VAST win price 4.25, got %v", vastResp.WinPrice)
	}

	ortbResp, err := (&ortbToVASTAdapter{h: handler}).Execute(context.Background(), request, config)
	if err != nil {
		t.Fatalf("ortbToVASTAdapter.Execute returned error: %v", err)
	}
	if ortbResp.WinPrice != 4.25 {
		t.Fatalf("expected ORTB->VAST win price 4.25, got %v", ortbResp.WinPrice)
	}
	if ortbResp.BidResp == nil || len(ortbResp.BidResp.SeatBid) == 0 || len(ortbResp.BidResp.SeatBid[0].Bid) == 0 {
		t.Fatal("expected ORTB->VAST adapter to return a bid response")
	}
	if ortbResp.BidResp.SeatBid[0].Bid[0].Price != 4.25 {
		t.Fatalf("expected ORTB->VAST bid price 4.25, got %v", ortbResp.BidResp.SeatBid[0].Bid[0].Price)
	}
}

func TestVASTEndpointExtraDemandWinnerUsesExtraTelemetryConfig(t *testing.T) {
	const placementID = "placement-telemetry"
	handler := &VideoPipelineHandler{
		metricsEng: &metricsConf.NilMetricsEngine{},
		configStore: &adServerConfigStore{configs: map[string]*AdServerConfig{
			placementID: {
				PlacementID:        placementID,
				PublisherID:        "pub-1",
				CampaignID:         "primary-campaign",
				AdvertiserID:       "adv-primary",
				DemandOrtbURL:      "https://primary.example/openrtb",
				FloorCPM:           0.5,
				DemandFloorCPM:     0.5,
				VideoPlacementType: "instream",
				MinDuration:        15,
				MaxDuration:        30,
				Active:             true,
				ExtraDemand: []ExtraDemandCfg{{
					OrtbURL:      "https://extra.example/openrtb",
					FloorCPM:     3.0,
					CampaignID:   "extra-campaign",
					AdvertiserID: "adv-extra",
				}},
			},
		}},
		tracking:         &trackingStore{},
		videoStats:       newVideoStatsStore(""),
		bidReport:        &BidReportHandler{store: newBidReportStore("")},
		firedImpressions: newImpressionDeduper(),
		adapterRouterFn:  func(RouterKey) DemandAdapter { return telemetryDemandAdapter{} },
		bufPool: sync.Pool{
			New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) },
		},
		done: make(chan struct{}),
	}

	req := httptest.NewRequest(http.MethodGet, "/video/vast?placement_id="+placementID+"&app_bundle=tv.example.app&ip=203.0.113.10&ua=TestUA/1.0&device_type=3&w=1920&h=1080&min_dur=15&max_dur=30&country_code=ID&tmax=500", nil)
	resp := httptest.NewRecorder()

	handler.VASTEndpoint()(resp, req, nil)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.Code)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for len(handler.bidReport.store.list()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	entries := handler.bidReport.store.list()
	if len(entries) != 1 {
		t.Fatalf("expected 1 bid report entry, got %d", len(entries))
	}
	if entries[0].CampaignID != "extra-campaign" {
		t.Fatalf("expected bid report campaign extra-campaign, got %q", entries[0].CampaignID)
	}
	if entries[0].Price != 3.0 {
		t.Fatalf("expected bid report price 3.0, got %v", entries[0].Price)
	}

	handler.videoStats.mu.Lock()
	dk := handler.videoStats.auctionDims["auction-extra"]
	handler.videoStats.mu.Unlock()
	if dk == nil {
		t.Fatal("expected auction dims for extra winner")
	}
	if dk.AdvertiserID != "adv-extra" {
		t.Fatalf("expected advertiser adv-extra, got %q", dk.AdvertiserID)
	}
	if dk.PriceCPM != 3.0 {
		t.Fatalf("expected cached winner price 3.0, got %v", dk.PriceCPM)
	}
	if dk.PublisherPriceCPM != 0.5 {
		t.Fatalf("expected publisher floor 0.5, got %v", dk.PublisherPriceCPM)
	}
}
