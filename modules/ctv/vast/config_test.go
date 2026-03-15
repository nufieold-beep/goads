package vast

import "testing"

func TestMergeCTVVastConfigNilInputs(t *testing.T) {
	result := MergeCTVVastConfig(nil, nil, nil)
	if result != (CTVVastConfig{}) {
		t.Fatalf("expected zero config, got %+v", result)
	}
}

func TestMergeCTVVastConfigPrecedenceAndDefaults(t *testing.T) {
	hostEnabled := true
	accountEnabled := false
	profileDebug := true
	floor := 1.5

	host := &CTVVastConfig{
		Enabled:         &hostEnabled,
		DefaultCurrency: "EUR",
		MaxAdsInPod:     3,
		Placement: &PlacementRulesConfig{
			Pricing: &PricingRulesConfig{FloorCPM: &floor},
		},
	}
	account := &CTVVastConfig{
		Enabled:            &accountEnabled,
		VastVersionDefault: "4.1",
		Placement: &PlacementRulesConfig{
			AdvertiserPlacement: PlacementExtension,
		},
	}
	profile := &CTVVastConfig{
		Debug: &profileDebug,
	}

	merged := MergeCTVVastConfig(host, account, profile)
	rc := merged.ReceiverConfig()

	if rc.DefaultCurrency != "EUR" {
		t.Fatalf("expected default currency EUR, got %s", rc.DefaultCurrency)
	}
	if rc.VastVersionDefault != "4.1" {
		t.Fatalf("expected vast version 4.1, got %s", rc.VastVersionDefault)
	}
	if rc.MaxAdsInPod != 3 {
		t.Fatalf("expected max ads 3, got %d", rc.MaxAdsInPod)
	}
	if !rc.Debug {
		t.Fatal("expected debug to be true")
	}
	if rc.Placement.Pricing.Currency != "EUR" {
		t.Fatalf("expected placement pricing currency EUR, got %s", rc.Placement.Pricing.Currency)
	}
	if rc.Placement.AdvertiserPlacement != PlacementExtension {
		t.Fatalf("expected advertiser placement %s, got %s", PlacementExtension, rc.Placement.AdvertiserPlacement)
	}
}

func TestMergeCTVVastConfigBoolPointers(t *testing.T) {
	trueVal := true
	falseVal := false

	host := &CTVVastConfig{Enabled: &trueVal, Debug: &falseVal}
	account := &CTVVastConfig{Debug: &trueVal}
	profile := &CTVVastConfig{Enabled: &falseVal}

	result := MergeCTVVastConfig(host, account, profile)
	if result.Enabled == nil || *result.Enabled {
		t.Fatalf("expected enabled=false from profile, got %+v", result.Enabled)
	}
	if result.Debug == nil || !*result.Debug {
		t.Fatalf("expected debug=true from account, got %+v", result.Debug)
	}
}

func TestMergeCTVVastConfigPlacementMerge(t *testing.T) {
	floorHost := 1.0
	ceilingProfile := 100.0
	placementDebug := true

	host := &CTVVastConfig{
		DefaultCurrency: "EUR",
		Placement: &PlacementRulesConfig{
			Pricing:    &PricingRulesConfig{FloorCPM: &floorHost},
			Advertiser: &AdvertiserRulesConfig{BlockedDomains: []string{"host-blocked.com"}},
		},
	}
	account := &CTVVastConfig{
		Placement: &PlacementRulesConfig{
			Categories: &CategoryRulesConfig{BlockedCategories: []string{"IAB25"}},
		},
	}
	profile := &CTVVastConfig{
		Placement: &PlacementRulesConfig{
			Pricing: &PricingRulesConfig{CeilingCPM: &ceilingProfile},
			Debug:   &placementDebug,
		},
	}

	result := MergeCTVVastConfig(host, account, profile)
	rc := result.ReceiverConfig()

	if rc.Placement.Pricing.FloorCPM != 1.0 {
		t.Fatalf("expected floor 1.0, got %v", rc.Placement.Pricing.FloorCPM)
	}
	if rc.Placement.Pricing.CeilingCPM != 100.0 {
		t.Fatalf("expected ceiling 100.0, got %v", rc.Placement.Pricing.CeilingCPM)
	}
	if rc.Placement.Pricing.Currency != "EUR" {
		t.Fatalf("expected inherited currency EUR, got %s", rc.Placement.Pricing.Currency)
	}
	if len(rc.Placement.Advertiser.BlockedDomains) != 1 || rc.Placement.Advertiser.BlockedDomains[0] != "host-blocked.com" {
		t.Fatalf("expected blocked domain from host, got %+v", rc.Placement.Advertiser.BlockedDomains)
	}
	if len(rc.Placement.Categories.BlockedCategories) != 1 || rc.Placement.Categories.BlockedCategories[0] != "IAB25" {
		t.Fatalf("expected blocked category from account, got %+v", rc.Placement.Categories.BlockedCategories)
	}
	if !rc.Placement.Debug {
		t.Fatal("expected placement debug=true from profile")
	}
}

func TestReceiverConfigDefaults(t *testing.T) {
	rc := (CTVVastConfig{}).ReceiverConfig()
	if rc.Receiver != ReceiverType(DefaultReceiver) {
		t.Fatalf("expected receiver %s, got %s", DefaultReceiver, rc.Receiver)
	}
	if rc.DefaultCurrency != DefaultCurrency {
		t.Fatalf("expected currency %s, got %s", DefaultCurrency, rc.DefaultCurrency)
	}
	if rc.VastVersionDefault != DefaultVastVersion {
		t.Fatalf("expected vast version %s, got %s", DefaultVastVersion, rc.VastVersionDefault)
	}
	if rc.MaxAdsInPod != DefaultMaxAdsInPod {
		t.Fatalf("expected max ads %d, got %d", DefaultMaxAdsInPod, rc.MaxAdsInPod)
	}
	if rc.SelectionStrategy != SelectionStrategy(DefaultSelectionStrategy) {
		t.Fatalf("expected strategy %s, got %s", DefaultSelectionStrategy, rc.SelectionStrategy)
	}
	if rc.CollisionPolicy != CollisionPolicy(DefaultCollisionPolicy) {
		t.Fatalf("expected collision policy %s, got %s", DefaultCollisionPolicy, rc.CollisionPolicy)
	}
}

func TestReceiverConfigPlacementPricingDefaultCurrency(t *testing.T) {
	floor := 1.0
	cfg := CTVVastConfig{
		DefaultCurrency: "JPY",
		Placement: &PlacementRulesConfig{
			Pricing: &PricingRulesConfig{FloorCPM: &floor},
		},
	}

	rc := cfg.ReceiverConfig()
	if rc.Placement.Pricing.Currency != "JPY" {
		t.Fatalf("expected placement pricing currency JPY, got %s", rc.Placement.Pricing.Currency)
	}
}

func TestMergeCTVVastConfigEmptyStringsDoNotOverride(t *testing.T) {
	host := &CTVVastConfig{Receiver: "GAM_SSU", DefaultCurrency: "EUR"}
	account := &CTVVastConfig{Receiver: "", DefaultCurrency: "USD"}

	result := MergeCTVVastConfig(host, account, nil)
	if result.Receiver != "GAM_SSU" {
		t.Fatalf("expected receiver to remain GAM_SSU, got %s", result.Receiver)
	}
	if result.DefaultCurrency != "USD" {
		t.Fatalf("expected currency to override to USD, got %s", result.DefaultCurrency)
	}
}

func TestMergeCTVVastConfigZeroIntDoesNotOverride(t *testing.T) {
	host := &CTVVastConfig{MaxAdsInPod: 5}
	account := &CTVVastConfig{MaxAdsInPod: 0}

	result := MergeCTVVastConfig(host, account, nil)
	if result.MaxAdsInPod != 5 {
		t.Fatalf("expected max ads to remain 5, got %d", result.MaxAdsInPod)
	}
}

func TestCTVVastConfigIsEnabled(t *testing.T) {
	trueVal := true
	falseVal := false

	if (CTVVastConfig{}).IsEnabled() {
		t.Fatal("expected nil enabled to be false")
	}
	if !(CTVVastConfig{Enabled: &trueVal}).IsEnabled() {
		t.Fatal("expected enabled=true to be true")
	}
	if (CTVVastConfig{Enabled: &falseVal}).IsEnabled() {
		t.Fatal("expected enabled=false to be false")
	}
}
