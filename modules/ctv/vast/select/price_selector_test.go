package bidselect

import (
	"testing"

	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast"
)

func TestPriceSelectorFiltersAndSorts(t *testing.T) {
	selector := NewPriceSelector(0)
	cfg := vast.DefaultConfig()
	cfg.MaxAdsInPod = 2
	resp := &openrtb2.BidResponse{
		Cur: "EUR",
		SeatBid: []openrtb2.SeatBid{{
			Seat: "seat-a",
			Bid: []openrtb2.Bid{
				{ID: "ignored-empty-adm", Price: 10},
				{ID: "bid-2", Price: 5, AdM: "<VAST></VAST>", DealID: "deal-1"},
				{ID: "bid-1", Price: 5, AdM: "<VAST></VAST>"},
				{ID: "bid-0", Price: 7, AdM: "<VAST></VAST>", ADomain: []string{"example.com"}, Cat: []string{"IAB1"}, Dur: 15},
			},
		}},
	}

	selected, warnings, err := selector.Select(nil, resp, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected one filtering warning, got %v", warnings)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 selected bids, got %d", len(selected))
	}
	if selected[0].Bid.ID != "bid-0" {
		t.Fatalf("expected highest bid first, got %s", selected[0].Bid.ID)
	}
	if selected[0].Meta.Currency != "EUR" {
		t.Fatalf("expected EUR currency, got %s", selected[0].Meta.Currency)
	}
	if selected[1].Bid.ID != "bid-2" {
		t.Fatalf("expected deal bid before same-price non-deal bid, got %s", selected[1].Bid.ID)
	}
}
