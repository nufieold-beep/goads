package agma

import (
	"bytes"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/docker/go-units"
	"github.com/prebid/go-gdpr/vendorconsent"
	"github.com/prebid/prebid-server/v4/analytics"
	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/logger"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
)

type httpSender = func(payload []byte) error

const (
	agmaGVLID = 1122
	p9        = 9
	agmaScopeSeparator = "\x00"
)

type AgmaLogger struct {
	sender            httpSender
	clock             clock.Clock
	publisherCodes    map[string]string
	scopeCodes        map[string]string
	eventCount        int64
	maxEventCount     int64
	maxBufferByteSize int64
	maxDuration       time.Duration
	mux               sync.RWMutex
	sigTermCh         chan os.Signal
	buffer            bytes.Buffer
	bufferCh          chan []byte
}

func newAgmaLogger(cfg config.AgmaAnalytics, sender httpSender, clock clock.Clock) (*AgmaLogger, error) {
	pSize, err := units.FromHumanSize(cfg.Buffers.BufferSize)
	if err != nil {
		return nil, err
	}
	pDuration, err := time.ParseDuration(cfg.Buffers.Timeout)
	if err != nil {
		return nil, err
	}
	if len(cfg.Accounts) == 0 {
		return nil, errors.New("Please configure at least one account for Agma Analytics")
	}

	buffer := bytes.Buffer{}
	buffer.Write([]byte("["))
	publisherCodes, scopeCodes := buildAccountIndexes(cfg.Accounts)

	return &AgmaLogger{
		sender:            sender,
		clock:             clock,
		publisherCodes:    publisherCodes,
		scopeCodes:        scopeCodes,
		maxBufferByteSize: pSize,
		eventCount:        0,
		maxEventCount:     int64(cfg.Buffers.EventCount),
		maxDuration:       pDuration,
		buffer:            buffer,
		bufferCh:          make(chan []byte, agmaQueueSize(cfg.Buffers.EventCount)),
		sigTermCh:         make(chan os.Signal, 1),
	}, nil
}

func NewModule(httpClient *http.Client, cfg config.AgmaAnalytics, clock clock.Clock) (analytics.Module, error) {
	sender, err := createHttpSender(httpClient, cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	m, err := newAgmaLogger(cfg, sender, clock)
	if err != nil {
		return nil, err
	}

	signal.Notify(m.sigTermCh, os.Interrupt, syscall.SIGTERM)

	go m.start()

	return m, nil
}

func (l *AgmaLogger) start() {
	ticker := l.clock.Ticker(l.maxDuration)
	defer ticker.Stop()
	for {
		select {
		case <-l.sigTermCh:
			logger.Infof("[AgmaAnalytics] Received Close, trying to flush buffer")
			l.flush()
			return
		case event := <-l.bufferCh:
			l.bufferEvent(event)
			if l.isFull() {
				l.flush()
			}
		case <-ticker.C:
			l.flush()
		}
	}
}

func (l *AgmaLogger) bufferEvent(data []byte) {
	l.mux.Lock()
	defer l.mux.Unlock()

	l.buffer.Write(data)
	l.buffer.WriteByte(',')
	l.eventCount++
}

func (l *AgmaLogger) isFull() bool {
	l.mux.RLock()
	defer l.mux.RUnlock()
	return l.eventCount >= l.maxEventCount || int64(l.buffer.Len()) >= l.maxBufferByteSize
}

func (l *AgmaLogger) flush() {
	l.mux.Lock()

	if l.eventCount == 0 || l.buffer.Len() == 0 {
		l.mux.Unlock()
		return
	}

	l.buffer.Truncate(l.buffer.Len() - 1)
	l.buffer.Write([]byte("]"))

	payload := make([]byte, l.buffer.Len())
	_, err := l.buffer.Read(payload)
	if err != nil {
		l.reset()
		l.mux.Unlock()
		logger.Warnf("[AgmaAnalytics] fail to copy the buffer")
		return
	}

	go l.sender(payload)

	l.reset()
	l.mux.Unlock()
}

func (l *AgmaLogger) reset() {
	l.buffer.Reset()
	l.buffer.Write([]byte("["))
	l.eventCount = 0
}

func (l *AgmaLogger) extractPublisherAndSite(requestWrapper *openrtb_ext.RequestWrapper) (string, string) {
	publisherId := ""
	appSiteId := ""
	if requestWrapper.Site != nil {
		if requestWrapper.Site.Publisher != nil {
			publisherId = requestWrapper.Site.Publisher.ID
		}
		appSiteId = requestWrapper.Site.ID
	}
	if requestWrapper.App != nil {
		if requestWrapper.App.Publisher != nil {
			publisherId = requestWrapper.App.Publisher.ID
		}
		appSiteId = requestWrapper.App.ID
		if appSiteId == "" {
			appSiteId = requestWrapper.App.Bundle
		}
	}
	return publisherId, appSiteId
}

func (l *AgmaLogger) shouldTrackEvent(requestWrapper *openrtb_ext.RequestWrapper) (bool, string) {
	if requestWrapper.User == nil {
		return false, ""
	}
	consentStr := requestWrapper.User.Consent

	parsedConsent, err := vendorconsent.ParseString(consentStr)
	if err != nil {
		return false, ""
	}

	p9Allowed := parsedConsent.PurposeAllowed(p9)
	agmaAllowed := parsedConsent.VendorConsent(agmaGVLID)
	if !p9Allowed || !agmaAllowed {
		return false, ""
	}

	publisherId, appSiteId := l.extractPublisherAndSite(requestWrapper)
	if publisherId == "" && appSiteId == "" {
		return false, ""
	}

	code, ok := l.lookupAccountCode(publisherId, appSiteId)
	if ok {
		return true, code
	}

	return false, ""
}

func (l *AgmaLogger) lookupAccountCode(publisherID, siteAppID string) (string, bool) {
	if siteAppID != "" {
		if code, ok := l.scopeCodes[buildAgmaScopeKey(publisherID, siteAppID)]; ok {
			return code, true
		}
	}

	if code, ok := l.publisherCodes[publisherID]; ok {
		return code, true
	}

	return "", false
}

func (l *AgmaLogger) LogAuctionObject(event *analytics.AuctionObject) {
	if event == nil || event.Status != http.StatusOK || event.RequestWrapper == nil {
		return
	}
	shouldTrack, code := l.shouldTrackEvent(event.RequestWrapper)
	if !shouldTrack {
		return
	}
	data, err := serializeAnayltics(event.RequestWrapper, EventTypeAuction, code, event.StartTime)
	if err != nil {
		logger.Errorf("[AgmaAnalytics] Error serializing auction object: %v", err)
		return
	}
	l.enqueue(data)
}

func (l *AgmaLogger) LogAmpObject(event *analytics.AmpObject) {
	if event == nil || event.Status != http.StatusOK || event.RequestWrapper == nil {
		return
	}
	shouldTrack, code := l.shouldTrackEvent(event.RequestWrapper)
	if !shouldTrack {
		return
	}
	data, err := serializeAnayltics(event.RequestWrapper, EventTypeAmp, code, event.StartTime)
	if err != nil {
		logger.Errorf("[AgmaAnalytics] Error serializing amp object: %v", err)
		return
	}
	l.enqueue(data)
}

func (l *AgmaLogger) LogVideoObject(event *analytics.VideoObject) {
	if event == nil || event.Status != http.StatusOK || event.RequestWrapper == nil {
		return
	}
	shouldTrack, code := l.shouldTrackEvent(event.RequestWrapper)
	if !shouldTrack {
		return
	}
	data, err := serializeAnayltics(event.RequestWrapper, EventTypeVideo, code, event.StartTime)
	if err != nil {
		logger.Errorf("[AgmaAnalytics] Error serializing video object: %v", err)
		return
	}
	l.enqueue(data)
}

func (l *AgmaLogger) Shutdown() {
	logger.Infof("[AgmaAnalytics] Shutdown, trying to flush buffer")
	l.flush()
}

func (l *AgmaLogger) enqueue(data []byte) {
	select {
	case l.bufferCh <- data:
	default:
		logger.Warnf("[AgmaAnalytics] buffer is full, dropping analytics event")
	}
}

func buildAccountIndexes(accounts []config.AgmaAnalyticsAccount) (map[string]string, map[string]string) {
	publisherCodes := make(map[string]string, len(accounts))
	scopeCodes := make(map[string]string, len(accounts))
	for _, account := range accounts {
		if account.PublisherId == "" || account.Code == "" {
			continue
		}
		if account.SiteAppId == "" {
			publisherCodes[account.PublisherId] = account.Code
			continue
		}
		scopeCodes[buildAgmaScopeKey(account.PublisherId, account.SiteAppId)] = account.Code
	}
	return publisherCodes, scopeCodes
}

func buildAgmaScopeKey(publisherID, siteAppID string) string {
	return publisherID + agmaScopeSeparator + siteAppID
}

func agmaQueueSize(eventCount int) int {
	if eventCount <= 0 {
		return 1
	}
	if eventCount > 256 {
		return 256
	}
	return eventCount
}

func (l *AgmaLogger) LogCookieSyncObject(event *analytics.CookieSyncObject)         {}
func (l *AgmaLogger) LogNotificationEventObject(event *analytics.NotificationEvent) {}
func (l *AgmaLogger) LogSetUIDObject(event *analytics.SetUIDObject)                 {}