package endpoints

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

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
				Body: io.NopCloser(strings.NewReader(`<VAST version="3.0"><Ad><InLine><AdSystem>test</AdSystem><AdTitle>direct</AdTitle></InLine></Ad></VAST>`)),
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
				Body: io.NopCloser(strings.NewReader(`<VAST version="3.0"><Ad><InLine><AdSystem>test</AdSystem><AdTitle>direct</AdTitle></InLine></Ad></VAST>`)),
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
