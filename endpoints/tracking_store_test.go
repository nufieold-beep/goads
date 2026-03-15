package endpoints

import (
	"context"
	"testing"
	"time"
)

func TestTrackingStoreRoundTripPreservesOptionalFields(t *testing.T) {
	store := &trackingStore{}
	now := time.Unix(1710489600, 123456789)
	store.record(TrackingEvent{
		AuctionID:   "auction-1",
		BidID:       "bid-1",
		Bidder:      "bench-bidder",
		PlacementID: "placement-1",
		Event:       EventStart,
		CrID:        "creative-1",
		Price:       1.23,
		ADomain:     "advertiser.example",
		ReceivedAt:  now,
	})
	store.record(TrackingEvent{
		AuctionID:   "auction-2",
		BidID:       "bid-2",
		Bidder:      "bench-bidder",
		PlacementID: "placement-2",
		Event:       EventType("custom-event"),
		ReceivedAt:  now.Add(time.Second),
	})

	events := store.all()
	if len(events) != 2 {
		t.Fatalf("len(events)=%d, want 2", len(events))
	}

	if events[0].CrID != "creative-1" {
		t.Fatalf("events[0].CrID=%q, want creative-1", events[0].CrID)
	}
	if events[0].ADomain != "advertiser.example" {
		t.Fatalf("events[0].ADomain=%q, want advertiser.example", events[0].ADomain)
	}
	if !events[0].ReceivedAt.Equal(now) {
		t.Fatalf("events[0].ReceivedAt=%s, want %s", events[0].ReceivedAt, now)
	}
	if events[1].Event != EventType("custom-event") {
		t.Fatalf("events[1].Event=%q, want custom-event", events[1].Event)
	}
}

func TestImpressionDeduperSeenOrAddAndCleanup(t *testing.T) {
	deduper := newImpressionDeduper()
	key := impressionKey{AuctionID: "auction-1", BidID: "bid-1"}

	if deduper.SeenOrAdd(key, 100) {
		t.Fatal("first SeenOrAdd returned duplicate=true, want false")
	}
	if !deduper.SeenOrAdd(key, 101) {
		t.Fatal("second SeenOrAdd returned duplicate=false, want true")
	}

	deduper.CleanupBefore(101)
	if deduper.SeenOrAdd(key, 102) {
		t.Fatal("SeenOrAdd after cleanup returned duplicate=true, want false")
	}
}

func TestWithResolvedTimeoutReusesEarlierParentDeadline(t *testing.T) {
	parent, parentCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer parentCancel()

	ctx, cancel := withResolvedTimeout(parent, 2*time.Second)
	defer cancel()

	if ctx != parent {
		t.Fatal("withResolvedTimeout should reuse parent context when parent deadline is earlier")
	}
}

func TestWithResolvedTimeoutPropagatesParentCancellation(t *testing.T) {
	parent, parentCancel := context.WithCancel(context.Background())
	ctx, cancel := withResolvedTimeout(parent, time.Second)
	defer cancel()

	parentCancel()

	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("withResolvedTimeout should propagate parent cancellation")
	}

	if err := ctx.Err(); err != context.Canceled {
		t.Fatalf("ctx.Err() = %v, want %v", err, context.Canceled)
	}
}