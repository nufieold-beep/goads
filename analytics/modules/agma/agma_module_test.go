package agma

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/prebid/prebid-server/v4/config"
)

func TestLookupAccountCodeUsesScopedLookup(t *testing.T) {
	module, err := newAgmaLogger(config.AgmaAnalytics{
		Buffers: config.AgmaAnalyticsBuffer{BufferSize: "1KB", EventCount: 1, Timeout: "1s"},
		Accounts: []config.AgmaAnalyticsAccount{
			{Code: "publisher", PublisherId: "pub-1"},
			{Code: "scoped", PublisherId: "pub-1", SiteAppId: "bundle-1"},
		},
	}, func(payload []byte) error { return nil }, clock.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	code, ok := module.lookupAccountCode("pub-1", "bundle-1")
	if !ok {
		t.Fatal("expected scoped account to match")
	}
	if code != "scoped" {
		t.Fatalf("expected scoped code, got %s", code)
	}
}

func TestEnqueueDoesNotBlockWhenBufferIsFull(t *testing.T) {
	module, err := newAgmaLogger(config.AgmaAnalytics{
		Buffers: config.AgmaAnalyticsBuffer{BufferSize: "1KB", EventCount: 1, Timeout: "1s"},
		Accounts: []config.AgmaAnalyticsAccount{{Code: "publisher", PublisherId: "pub-1"}},
	}, func(payload []byte) error { return nil }, clock.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	module.bufferCh <- []byte("first")
	done := make(chan struct{})
	go func() {
		module.enqueue([]byte("second"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected enqueue to drop instead of block")
	}
}