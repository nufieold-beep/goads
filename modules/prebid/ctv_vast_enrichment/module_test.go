package ctv_vast_enrichment

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/adapters"
	"github.com/prebid/prebid-server/v4/hooks/hookstage"
	"github.com/prebid/prebid-server/v4/modules/moduledeps"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
)

func TestBuilderCreatesModule(t *testing.T) {
	raw := json.RawMessage(`{"enabled":true,"default_currency":"EUR"}`)
	built, err := Builder(raw, moduledeps.ModuleDeps{})
	if err != nil {
		t.Fatalf("unexpected builder error: %v", err)
	}
	if _, ok := built.(Module); !ok {
		t.Fatalf("expected Module, got %T", built)
	}
}

func TestHandleRawBidderResponseHookEnrichesVideoVast(t *testing.T) {
	built, err := Builder(json.RawMessage(`{"enabled":true,"debug":true}`), moduledeps.ModuleDeps{})
	if err != nil {
		t.Fatalf("unexpected builder error: %v", err)
	}
	module := built.(Module)

	payload := hookstage.RawBidderResponsePayload{
		Bidder: "test-bidder",
		BidderResponse: &adapters.BidderResponse{
			Currency: "USD",
			Bids: []*adapters.TypedBid{{
				Bid: &openrtb2.Bid{
					ID:      "bid-1",
					ImpID:   "imp-1",
					Price:   3.5,
					ADomain: []string{"example.com"},
					Cat:     []string{"IAB1"},
					AdM:     `<VAST version="4.0"><Ad id="1"><InLine><AdTitle>Example</AdTitle><Creatives><Creative><Linear></Linear></Creative></Creatives></InLine></Ad></VAST>`,
				},
				BidType: openrtb_ext.BidTypeVideo,
				BidVideo: &openrtb_ext.ExtBidPrebidVideo{
					Duration: 15,
				},
			}},
		},
	}

	result, err := module.HandleRawBidderResponseHook(context.Background(), hookstage.ModuleInvocationContext{}, payload)
	if err != nil {
		t.Fatalf("unexpected hook error: %v", err)
	}
	if len(result.ChangeSet.Mutations()) != 1 {
		t.Fatalf("expected one mutation, got %d", len(result.ChangeSet.Mutations()))
	}

	updatedPayload := payload
	for _, mutation := range result.ChangeSet.Mutations() {
		updatedPayload, err = mutation.Apply(updatedPayload)
		if err != nil {
			t.Fatalf("unexpected mutation error: %v", err)
		}
	}

	adm := updatedPayload.BidderResponse.Bids[0].Bid.AdM
	if !strings.Contains(adm, `<Pricing model="CPM" currency="USD">3.5</Pricing>`) {
		t.Fatalf("expected enriched pricing in AdM, got %s", adm)
	}
	if !strings.Contains(adm, `<Advertiser>example.com</Advertiser>`) {
		t.Fatalf("expected enriched advertiser in AdM, got %s", adm)
	}
	if !strings.Contains(adm, `<Duration>00:00:15</Duration>`) {
		t.Fatalf("expected enriched duration in AdM, got %s", adm)
	}
	if strings.Count(adm, `<AdTitle>Example</AdTitle>`) != 1 {
		t.Fatalf("expected AdTitle only once after enrichment, got %s", adm)
	}
	if !strings.Contains(adm, `<Extension type="iab_category"><Category>IAB1</Category></Extension>`) {
		t.Fatalf("expected category extension in AdM, got %s", adm)
	}
	if !strings.Contains(adm, `<Extension type="openrtb">`) {
		t.Fatalf("expected debug extension in AdM, got %s", adm)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", result.Warnings)
	}
}

func TestHandleRawBidderResponseHookHonorsAccountDisable(t *testing.T) {
	built, err := Builder(json.RawMessage(`{"enabled":true}`), moduledeps.ModuleDeps{})
	if err != nil {
		t.Fatalf("unexpected builder error: %v", err)
	}
	module := built.(Module)

	payload := hookstage.RawBidderResponsePayload{
		BidderResponse: &adapters.BidderResponse{Bids: []*adapters.TypedBid{{
			Bid:     &openrtb2.Bid{ID: "bid-1", AdM: `<VAST version="4.0"></VAST>`},
			BidType: openrtb_ext.BidTypeVideo,
		}}},
	}
	miCtx := hookstage.ModuleInvocationContext{AccountConfig: json.RawMessage(`{"enabled":false}`)}

	result, err := module.HandleRawBidderResponseHook(context.Background(), miCtx, payload)
	if err != nil {
		t.Fatalf("unexpected hook error: %v", err)
	}
	if len(result.ChangeSet.Mutations()) != 0 {
		t.Fatalf("expected no mutations when account disables module, got %d", len(result.ChangeSet.Mutations()))
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("expected no warnings when skipped, got %v", result.Warnings)
	}
}