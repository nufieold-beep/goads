package endpoints

import "testing"

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
