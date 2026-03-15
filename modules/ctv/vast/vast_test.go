package vast

import (
	"context"
	"errors"
	"testing"

	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast/model"
)

type testSelector struct {
	selected []SelectedBid
	err      error
}

func (s testSelector) Select(_ *openrtb2.BidRequest, _ *openrtb2.BidResponse, _ ReceiverConfig) ([]SelectedBid, []string, error) {
	return s.selected, nil, s.err
}

type testEnricher struct {
	err error
}

func (e testEnricher) Enrich(_ *model.Ad, _ CanonicalMeta, _ ReceiverConfig) ([]string, error) {
	return nil, e.err
}

type testFormatter struct {
	xml []byte
	err error
}

func (f testFormatter) Format(_ []EnrichedAd, _ ReceiverConfig) ([]byte, []string, error) {
	return f.xml, nil, f.err
}

func TestBuildVastFromBidResponseValidatesDependencies(t *testing.T) {
	_, err := BuildVastFromBidResponse(context.Background(), nil, nil, DefaultConfig(), nil, testEnricher{}, testFormatter{})
	if !errors.Is(err, ErrNilSelector) {
		t.Fatalf("expected ErrNilSelector, got %v", err)
	}
}

func TestBuildVastFromBidResponseRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := BuildVastFromBidResponse(ctx, nil, nil, DefaultConfig(), testSelector{}, testEnricher{}, testFormatter{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if !result.NoAd {
		t.Fatal("expected no-ad result when context is canceled")
	}
}

func TestBuildVastFromBidResponseBuildsNoAdWhenSelectionEmpty(t *testing.T) {
	result, err := BuildVastFromBidResponse(context.Background(), nil, &openrtb2.BidResponse{}, DefaultConfig(), testSelector{}, testEnricher{}, testFormatter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.NoAd {
		t.Fatal("expected no-ad result")
	}
	if len(result.VastXML) == 0 {
		t.Fatal("expected VAST XML in no-ad result")
	}
}

func TestBuildVastFromBidResponseSuccessPath(t *testing.T) {
	selected := []SelectedBid{{
		Bid:  openrtb2.Bid{ID: "bid-1", AdM: `<VAST version="3.0"><Ad><InLine><Creatives><Creative><Linear><Duration>00:00:30</Duration></Linear></Creative></Creatives></InLine></Ad></VAST>`},
		Meta: CanonicalMeta{BidID: "bid-1"},
	}}
	expectedXML := []byte("<xml/>")

	result, err := BuildVastFromBidResponse(context.Background(), nil, &openrtb2.BidResponse{}, DefaultConfig(), testSelector{selected: selected}, testEnricher{}, testFormatter{xml: expectedXML})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NoAd {
		t.Fatal("expected ad result")
	}
	if string(result.VastXML) != string(expectedXML) {
		t.Fatalf("expected XML %q, got %q", expectedXML, result.VastXML)
	}
}
