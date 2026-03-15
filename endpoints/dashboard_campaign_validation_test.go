package endpoints

import "testing"

func TestCampaignValidateNormalizesSupportedIntegrationType(t *testing.T) {
	campaign := &Campaign{
		Name:            "Demand Route",
		AdvertiserID:    "adv-1",
		IntegrationType: " OpenRTB ",
		OrtbEndpointURL: " https://demand.example/openrtb ",
	}

	if msg := campaign.validate(); msg != "" {
		t.Fatalf("expected campaign to validate, got %q", msg)
	}
	if campaign.IntegrationType != "open_rtb" {
		t.Fatalf("expected normalized integration_type, got %q", campaign.IntegrationType)
	}
	if campaign.OrtbEndpointURL != "https://demand.example/openrtb" {
		t.Fatalf("expected trimmed ortb url, got %q", campaign.OrtbEndpointURL)
	}
}

func TestCampaignValidateRejectsUnsupportedIntegrationType(t *testing.T) {
	campaign := &Campaign{
		Name:            "Demand Route",
		AdvertiserID:    "adv-1",
		IntegrationType: "direct_demand",
		OrtbEndpointURL: "https://demand.example/openrtb",
	}

	if msg := campaign.validate(); msg != "integration_type must be one of: tag_based, open_rtb" {
		t.Fatalf("unexpected validation message: %q", msg)
	}
}

func TestVideoExchangeEntryValidateNormalizesIntegrationType(t *testing.T) {
	entry := &VideoExchangeEntry{
		Name:            "Supply Source",
		Environment:     VideoEnvCTV,
		Placement:       PlacementInStream,
		IntegrationType: " vast ",
		MinDuration:     15,
		MaxDuration:     30,
	}

	if msg := entry.validate(); msg != "" {
		t.Fatalf("expected video entry to validate, got %q", msg)
	}
	if entry.IntegrationType != "tag_based" {
		t.Fatalf("expected normalized source integration_type, got %q", entry.IntegrationType)
	}
}

func TestCampaignValidateForcesOpenAuctionType(t *testing.T) {
	campaign := &Campaign{
		Name:             "Demand Route",
		AdvertiserID:     "adv-1",
		IntegrationType:  "open_rtb",
		OrtbEndpointURL:  "https://demand.example/openrtb",
		AuctionType:      "pmp",
		AuctionPriceType: "second_price",
	}

	if msg := campaign.validate(); msg != "" {
		t.Fatalf("expected campaign to validate, got %q", msg)
	}
	if campaign.AuctionType != "open" {
		t.Fatalf("expected auction_type to be forced to open, got %q", campaign.AuctionType)
	}
	if campaign.AuctionPriceType != "second" {
		t.Fatalf("expected normalized auction_price_type, got %q", campaign.AuctionPriceType)
	}
}

func TestYieldRuleValidateNormalizesLegacyPriority(t *testing.T) {
	rule := &YieldRule{
		Name:        "Legacy Guaranteed Rule",
		Priority:    "guaranteed",
		AuctionType: "first",
		FloorCPM:    1.25,
	}

	if msg := rule.validate(); msg != "" {
		t.Fatalf("expected yield rule to validate, got %q", msg)
	}
	if rule.Priority != YieldPriorityProgrammatic {
		t.Fatalf("expected legacy priority to normalize to programmatic, got %q", rule.Priority)
	}
	if rule.AuctionType != "first_price" {
		t.Fatalf("expected auction type normalization, got %q", rule.AuctionType)
	}
}

func TestDynamicFloorRuleValidateNormalizesLegacyDemandPath(t *testing.T) {
	rule := &DynamicFloorRule{
		Name:       "Legacy PMP Floor",
		BaseFloor:  2.1,
		DemandPath: "pmp",
	}

	if msg := rule.validate(); msg != "" {
		t.Fatalf("expected dynamic floor rule to validate, got %q", msg)
	}
	if rule.DemandPath != "" {
		t.Fatalf("expected legacy demand path to normalize to any, got %q", rule.DemandPath)
	}
}
