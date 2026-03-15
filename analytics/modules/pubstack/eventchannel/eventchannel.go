package eventchannel

import (
	"bytes"
	"compress/gzip"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/prebid/prebid-server/v4/logger"
)

type Metrics struct {
	bufferSize int64
	eventCount int64
}

type Limit struct {
	maxByteSize   int64
	maxEventCount int64
	maxTime       time.Duration
}

type EventChannel struct {
	gz   *gzip.Writer
	buff *bytes.Buffer
	ch          chan []byte
	sendCh      chan []byte
	endCh       chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	metrics     Metrics
	muxGzBuffer sync.RWMutex
	send        Sender
	limit       Limit
	clock       clock.Clock
}

func NewEventChannel(sender Sender, clock clock.Clock, maxByteSize, maxEventCount int64, maxTime time.Duration) *EventChannel {
	b := &bytes.Buffer{}
	gzw := gzip.NewWriter(b)
	c := EventChannel{
		gz:      gzw,
		buff:    b,
		ch:      make(chan []byte, queueSize(maxEventCount)),
		sendCh:  make(chan []byte, 4),
		endCh:   make(chan struct{}),
		done:    make(chan struct{}),
		metrics: Metrics{},
		send:    sender,
		limit:   Limit{maxByteSize, maxEventCount, maxTime},
		clock:   clock,
	}
	go c.sendLoop()
	go c.start()
	return &c
}

func (c *EventChannel) Push(event []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}

	select {
	case c.ch <- event:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

func (c *EventChannel) Close() {
	c.closeOnce.Do(func() {
		close(c.endCh)
		<-c.done
	})
}

func (c *EventChannel) buffer(event []byte) {
	c.muxGzBuffer.Lock()
	defer c.muxGzBuffer.Unlock()
	_, err := c.gz.Write(event)
	if err != nil {
		logger.Warnf("[pubstack] fail to compress, skip the event")
		return
	}
	c.metrics.eventCount++
	c.metrics.bufferSize += int64(len(event))
}

func (c *EventChannel) isBufferFull() bool {
	c.muxGzBuffer.RLock()
	defer c.muxGzBuffer.RUnlock()
	return c.metrics.eventCount >= c.limit.maxEventCount || c.metrics.bufferSize >= c.limit.maxByteSize
}

func (c *EventChannel) reset() {
	c.gz.Reset(c.buff)
	c.buff.Reset()
	c.metrics.eventCount = 0
	c.metrics.bufferSize = 0
}

func (c *EventChannel) flush() {
	c.muxGzBuffer.Lock()
	defer c.muxGzBuffer.Unlock()
	if c.metrics.eventCount == 0 || c.metrics.bufferSize == 0 {
		return
	}
	defer c.reset()
	if err := c.gz.Close(); err != nil {
		logger.Warnf("[pubstack] fail to close gzipped buffer")
		return
	}
	payload := make([]byte, c.buff.Len())
	if _, err := c.buff.Read(payload); err != nil {
		logger.Warnf("[pubstack] fail to copy the buffer")
		return
	}

	select {
	case c.sendCh <- payload:
	default:
		logger.Warnf("[pubstack] send queue is full, dropping analytics payload")
	}
}

func (c *EventChannel) start() {
	ticker := c.clock.Ticker(c.limit.maxTime)
	defer ticker.Stop()
	for {
		select {
		case <-c.endCh:
			c.drainPendingEvents()
			c.flush()
			close(c.sendCh)
			close(c.done)
			return
		case event := <-c.ch:
			c.buffer(event)
			if c.isBufferFull() {
				c.flush()
			}
		case <-ticker.C:
			c.flush()
		}
	}
}

func (c *EventChannel) drainPendingEvents() {
	for {
		select {
		case event := <-c.ch:
			c.buffer(event)
		default:
			return
		}
	}
}

func (c *EventChannel) sendLoop() {
	for payload := range c.sendCh {
		if err := c.send(payload); err != nil {
			logger.Warnf("[pubstack] fail to send analytics payload: %v", err)
		}
	}
}

func queueSize(maxEventCount int64) int {
	if maxEventCount <= 0 {
		return 1
	}
	if maxEventCount > 256 {
		return 256
	}
	return int(maxEventCount)
}