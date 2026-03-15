package eventchannel

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
)

func TestPushDoesNotBlockWhenSenderIsSlow(t *testing.T) {
	releaseSend := make(chan struct{})
	senderStarted := make(chan struct{}, 1)

	channel := NewEventChannel(func(payload []byte) error {
		select {
		case senderStarted <- struct{}{}:
		default:
		}
		<-releaseSend
		return nil
	}, clock.New(), 1, 1, time.Hour)
	defer close(releaseSend)
	defer channel.Close()

	if !channel.Push([]byte("a")) {
		t.Fatal("expected first push to succeed")
	}

	select {
	case <-senderStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected sender to start")
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			channel.Push([]byte("b"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("push loop blocked while sender was slow")
	}
}

func TestCloseFlushesBufferedPayload(t *testing.T) {
	received := make(chan []byte, 1)

	channel := NewEventChannel(func(payload []byte) error {
		received <- payload
		return nil
	}, clock.New(), 1024, 10, time.Hour)

	if !channel.Push([]byte("event")) {
		t.Fatal("expected push to succeed")
	}

	channel.Close()

	select {
	case payload := <-received:
		if len(payload) == 0 {
			t.Fatal("expected flushed payload")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected buffered payload to flush on close")
	}
}