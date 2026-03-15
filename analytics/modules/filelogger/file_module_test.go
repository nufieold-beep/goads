package filelogger

import (
	"sync"
	"testing"
	"time"

	"github.com/prebid/prebid-server/v4/analytics"
)

type testLogger struct {
	mu      sync.Mutex
	entries []string
	flushes int
	release chan struct{}
}

func (l *testLogger) Debug(v ...interface{}) {
	if l.release != nil {
		<-l.release
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(v) > 0 {
		if s, ok := v[0].(string); ok {
			l.entries = append(l.entries, s)
		}
	}
}

func (l *testLogger) Flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.flushes++
}

func TestFileLoggerDoesNotBlockWhenWriterIsSlow(t *testing.T) {
	stub := &testLogger{release: make(chan struct{})}
	logger := &FileLogger{Logger: stub, entries: make(chan string, 8), done: make(chan struct{})}
	go logger.run()

	logger.LogAuctionObject(&analytics.AuctionObject{})

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			logger.LogAuctionObject(&analytics.AuctionObject{})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected logging loop to stay non-blocking")
	}

	close(stub.release)
	logger.Shutdown()
}

func TestFileLoggerShutdownFlushesQueuedEntries(t *testing.T) {
	stub := &testLogger{}
	logger := &FileLogger{Logger: stub, entries: make(chan string, 8), done: make(chan struct{})}
	go logger.run()

	logger.LogAuctionObject(&analytics.AuctionObject{})
	logger.Shutdown()

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.entries) == 0 {
		t.Fatal("expected queued entries to be written before shutdown")
	}
	if stub.flushes == 0 {
		t.Fatal("expected shutdown to flush logger")
	}
}