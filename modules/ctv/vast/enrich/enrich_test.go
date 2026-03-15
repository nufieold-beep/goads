package enrich

import (
	"testing"

	"github.com/prebid/prebid-server/v4/modules/ctv/vast"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast/model"
)

func TestEnrichAddsPricingAdvertiserAndDebugExtension(t *testing.T) {
	enricher := NewEnricher()
	ad := &model.Ad{InLine: &model.InLine{Creatives: &model.Creatives{Creative: []model.Creative{{Linear: &model.Linear{}}}}}}
	meta := vast.CanonicalMeta{BidID: "bid-1", ImpID: "imp-1", Seat: "seat-a", Price: 1.25, Currency: "EUR", Adomain: "example.com", Cats: []string{"IAB1"}, DurSec: 30}
	cfg := vast.DefaultConfig()
	cfg.Debug = true

	warnings, err := enricher.Enrich(ad, meta, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
	if ad.InLine.Pricing == nil || ad.InLine.Pricing.Currency != "EUR" {
		t.Fatal("expected pricing enrichment with EUR currency")
	}
	if ad.InLine.Advertiser != "example.com" {
		t.Fatalf("expected advertiser example.com, got %s", ad.InLine.Advertiser)
	}
	if ad.InLine.Creatives.Creative[0].Linear.Duration != "00:00:30" {
		t.Fatalf("expected enriched duration, got %s", ad.InLine.Creatives.Creative[0].Linear.Duration)
	}
	if len(ad.InLine.Extensions.Extension) != 2 {
		t.Fatalf("expected category and debug extensions, got %d", len(ad.InLine.Extensions.Extension))
	}
}
