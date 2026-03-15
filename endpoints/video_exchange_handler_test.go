package endpoints

import "testing"

func TestVideoExchangeSyncPipelineCfgPropagatesSourceAndCampaignFields(t *testing.T) {
	handler := NewVideoExchangeHandler("")
	campaigns := newCampaignStore("")
	handler.SetCampaignStore(campaigns)

	var registered *AdServerConfig
	handler.SetPipelineRegister(func(cfg *AdServerConfig) {
		registered = cfg
	})

	campaign := campaigns.create(&Campaign{
		Name:            "Demand Campaign",
		AdvertiserID:    "adv-1",
		PublisherID:     "pub-1",
		VASTTagURL:      "https://demand.example/vast",
		OrtbEndpointURL: "https://demand.example/openrtb",
		FloorCPM:        4.25,
		BAdv:            []string{"blocked.example"},
		BCat:            []string{"IAB25"},
		MimeTypes:       []string{"video/mp4", "application/dash+xml"},
		Protocols:       []int{7, 8},
		APIs:            []int{2, 7},
	})

	handler.syncPipelineCfg(&VideoExchangeEntry{
		ID:           "placement-1",
		PublisherID:  "pub-1",
		Name:         "Supply Source",
		Environment:  VideoEnvCTV,
		Placement:    PlacementInStream,
		DomainOrApp:  "tv.example.com",
		BundleID:     "com.example.fallback",
		ContentURL:   "https://tv.example.com/watch/live",
		TargetingExt: map[string]interface{}{"channel": "sports", "premium": true},
		MinDuration:  15,
		MaxDuration:  30,
		Bidders:      []string{"appnexus", "ix"},
		FloorCPM:     2.5,
		CampaignID:   campaign.ID,
		SellerDomain: "exchange.example.com",
		Active:       true,
		TimeoutMS:    650,
	})

	if registered == nil {
		t.Fatal("expected config to be registered")
	}
	if registered.DomainOrApp != "tv.example.com" {
		t.Fatalf("expected domain_or_app from source, got %q", registered.DomainOrApp)
	}
	if registered.ContentURL != "https://tv.example.com/watch/live" {
		t.Fatalf("expected content_url to sync, got %q", registered.ContentURL)
	}
	if registered.TargetingExt["channel"] != "sports" {
		t.Fatalf("expected targeting_ext.channel to sync, got %#v", registered.TargetingExt)
	}
	if registered.FloorCPM != 2.5 {
		t.Fatalf("expected source floor to remain on runtime config, got %v", registered.FloorCPM)
	}
	if registered.DemandFloorCPM != 4.25 {
		t.Fatalf("expected campaign floor to become demand floor, got %v", registered.DemandFloorCPM)
	}
	if registered.DemandVASTURL != "https://demand.example/vast" {
		t.Fatalf("expected campaign vast url, got %q", registered.DemandVASTURL)
	}
	if registered.DemandOrtbURL != "https://demand.example/openrtb" {
		t.Fatalf("expected campaign ortb url, got %q", registered.DemandOrtbURL)
	}
	if registered.SellerDomain != "exchange.example.com" {
		t.Fatalf("expected seller_domain to sync, got %q", registered.SellerDomain)
	}
	if len(registered.BAdv) != 1 || registered.BAdv[0] != "blocked.example" {
		t.Fatalf("expected badv from campaign, got %#v", registered.BAdv)
	}
	if len(registered.Protocols) != 2 || registered.Protocols[0] != 7 {
		t.Fatalf("expected campaign protocols to drive runtime config, got %#v", registered.Protocols)
	}
	if len(registered.APIs) != 2 || registered.APIs[0] != 2 {
		t.Fatalf("expected campaign apis to sync, got %#v", registered.APIs)
	}
}

func TestVideoExchangeSyncPipelineCfgFallsBackToBundleID(t *testing.T) {
	handler := NewVideoExchangeHandler("")

	var registered *AdServerConfig
	handler.SetPipelineRegister(func(cfg *AdServerConfig) {
		registered = cfg
	})

	handler.syncPipelineCfg(&VideoExchangeEntry{
		ID:          "placement-2",
		Name:        "In-App Source",
		Environment: VideoEnvInApp,
		Placement:   PlacementRewarded,
		BundleID:    "com.example.app",
		MinDuration: 5,
		MaxDuration: 30,
		Active:      true,
	})

	if registered == nil {
		t.Fatal("expected config to be registered")
	}
	if registered.DomainOrApp != "com.example.app" {
		t.Fatalf("expected bundle_id fallback, got %q", registered.DomainOrApp)
	}
}

func TestVideoExchangeSyncPipelineCfgUsesReverseSupplyLinkWhenCampaignIDMissing(t *testing.T) {
	handler := NewVideoExchangeHandler("")
	campaigns := newCampaignStore("")
	handler.SetCampaignStore(campaigns)

	var registered *AdServerConfig
	handler.SetPipelineRegister(func(cfg *AdServerConfig) {
		registered = cfg
	})

	campaign := campaigns.create(&Campaign{
		Name:            "Reverse Linked Campaign",
		AdvertiserID:    "adv-2",
		PublisherID:     "pub-2",
		OrtbEndpointURL: "https://demand.example/reverse-openrtb",
		IntegrationType: "open_rtb",
		Status:          "active",
		SupplyLinks:     []string{"placement-reverse"},
	})

	handler.syncPipelineCfg(&VideoExchangeEntry{
		ID:          "placement-reverse",
		PublisherID: "pub-2",
		Environment: VideoEnvCTV,
		Placement:   PlacementInStream,
		MinDuration: 15,
		MaxDuration: 30,
		Active:      true,
	})

	if registered == nil {
		t.Fatal("expected config to be registered")
	}
	if registered.CampaignID != campaign.ID {
		t.Fatalf("expected reverse-linked campaign id %q, got %q", campaign.ID, registered.CampaignID)
	}
	if registered.DemandOrtbURL != "https://demand.example/reverse-openrtb" {
		t.Fatalf("expected reverse-linked ortb url, got %q", registered.DemandOrtbURL)
	}
}
