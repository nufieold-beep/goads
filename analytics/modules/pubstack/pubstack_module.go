package pubstack

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/prebid/prebid-server/v4/analytics"
	"github.com/prebid/prebid-server/v4/analytics/modules/pubstack/eventchannel"
	"github.com/prebid/prebid-server/v4/analytics/modules/pubstack/helpers"
	"github.com/prebid/prebid-server/v4/logger"
)

type Configuration struct {
	ScopeID  string          `json:"scopeId"`
	Endpoint string          `json:"endpoint"`
	Features map[string]bool `json:"features"`
}

const (
	auction    = "auction"
	cookieSync = "cookiesync"
	amp        = "amp"
	setUID     = "setuid"
	video      = "video"
)

type bufferConfig struct {
	timeout time.Duration
	count   int64
	size    int64
}

type PubstackModule struct {
	eventChannels map[string]*eventchannel.EventChannel
	httpClient    *http.Client
	sigTermCh     chan os.Signal
	stopCh        chan struct{}
	scope         string
	cfg           *Configuration
	buffsCfg      *bufferConfig
	muxConfig     sync.RWMutex
	clock         clock.Clock
}

func NewModule(client *http.Client, scope, endpoint, configRefreshDelay string, maxEventCount int, maxByteSize, maxTime string, clock clock.Clock) (analytics.Module, error) {
	configUpdateTask, err := NewConfigUpdateHttpTask(client, scope, endpoint, configRefreshDelay)
	if err != nil {
		return nil, err
	}

	return NewModuleWithConfigTask(client, scope, endpoint, maxEventCount, maxByteSize, maxTime, configUpdateTask, clock)
}

func NewModuleWithConfigTask(client *http.Client, scope, endpoint string, maxEventCount int, maxByteSize, maxTime string, configTask ConfigUpdateTask, clock clock.Clock) (analytics.Module, error) {
	logger.Infof("[pubstack] Initializing module scope=%s endpoint=%s\n", scope, endpoint)

	bufferCfg, err := newBufferConfig(maxEventCount, maxByteSize, maxTime)
	if err != nil {
		return nil, fmt.Errorf("fail to parse the module args, arg=analytics.pubstack.buffers, :%v", err)
	}

	defaultFeatures := map[string]bool{auction: false, video: false, amp: false, cookieSync: false, setUID: false}
	defaultConfig := &Configuration{ScopeID: scope, Endpoint: endpoint, Features: defaultFeatures}

	pb := PubstackModule{
		scope:         scope,
		httpClient:    client,
		cfg:           defaultConfig,
		buffsCfg:      bufferCfg,
		sigTermCh:     make(chan os.Signal),
		stopCh:        make(chan struct{}),
		eventChannels: make(map[string]*eventchannel.EventChannel),
		muxConfig:     sync.RWMutex{},
		clock:         clock,
	}

	signal.Notify(pb.sigTermCh, os.Interrupt, syscall.SIGTERM)

	configChannel := configTask.Start(pb.stopCh)
	go pb.start(configChannel)

	logger.Infof("[pubstack] Pubstack analytics configured and ready")
	return &pb, nil
}

func (p *PubstackModule) LogAuctionObject(ao *analytics.AuctionObject) {
	channel, scope, ok := p.getEventChannel(auction)
	if !ok {
		return
	}
	payload, err := helpers.JsonifyAuctionObject(ao, scope)
	if err != nil {
		logger.Warnf("[pubstack] Cannot serialize auction")
		return
	}
	channel.Push(payload)
}

func (p *PubstackModule) LogNotificationEventObject(ne *analytics.NotificationEvent) {}

func (p *PubstackModule) LogVideoObject(vo *analytics.VideoObject) {
	channel, scope, ok := p.getEventChannel(video)
	if !ok {
		return
	}
	payload, err := helpers.JsonifyVideoObject(vo, scope)
	if err != nil {
		logger.Warnf("[pubstack] Cannot serialize video")
		return
	}
	channel.Push(payload)
}

func (p *PubstackModule) LogSetUIDObject(so *analytics.SetUIDObject) {
	channel, scope, ok := p.getEventChannel(setUID)
	if !ok {
		return
	}
	payload, err := helpers.JsonifySetUIDObject(so, scope)
	if err != nil {
		logger.Warnf("[pubstack] Cannot serialize video")
		return
	}
	channel.Push(payload)
}

func (p *PubstackModule) LogCookieSyncObject(cso *analytics.CookieSyncObject) {
	channel, scope, ok := p.getEventChannel(cookieSync)
	if !ok {
		return
	}
	payload, err := helpers.JsonifyCookieSync(cso, scope)
	if err != nil {
		logger.Warnf("[pubstack] Cannot serialize video")
		return
	}
	channel.Push(payload)
}

func (p *PubstackModule) LogAmpObject(ao *analytics.AmpObject) {
	channel, scope, ok := p.getEventChannel(amp)
	if !ok {
		return
	}
	payload, err := helpers.JsonifyAmpObject(ao, scope)
	if err != nil {
		logger.Warnf("[pubstack] Cannot serialize video")
		return
	}
	channel.Push(payload)
}

func (p *PubstackModule) Shutdown() {
	logger.Infof("[PubstackModule] Shutdown")
}

func (p *PubstackModule) start(c <-chan *Configuration) {
	for {
		select {
		case <-p.sigTermCh:
			close(p.stopCh)
			cfg := p.cfg.clone().disableAllFeatures()
			p.updateConfig(cfg)
			return
		case config := <-c:
			p.updateConfig(config)
			logger.Infof("[pubstack] Updating config: %v", p.cfg)
		}
	}
}

func (p *PubstackModule) updateConfig(config *Configuration) {
	p.muxConfig.Lock()
	defer p.muxConfig.Unlock()
	if p.cfg.isSameAs(config) {
		return
	}
	p.cfg = config
	p.closeAllEventChannels()
	p.registerChannel(amp)
	p.registerChannel(auction)
	p.registerChannel(cookieSync)
	p.registerChannel(video)
	p.registerChannel(setUID)
}

func (p *PubstackModule) isFeatureEnable(feature string) bool {
	val, ok := p.cfg.Features[feature]
	return ok && val
}

func (p *PubstackModule) getEventChannel(feature string) (*eventchannel.EventChannel, string, bool) {
	p.muxConfig.RLock()
	defer p.muxConfig.RUnlock()

	if !p.isFeatureEnable(feature) {
		return nil, "", false
	}

	channel, ok := p.eventChannels[feature]
	if !ok || channel == nil {
		return nil, "", false
	}

	return channel, p.scope, true
}

func (p *PubstackModule) registerChannel(feature string) {
	if p.isFeatureEnable(feature) {
		sender := eventchannel.BuildEndpointSender(p.httpClient, p.cfg.Endpoint, feature)
		p.eventChannels[feature] = eventchannel.NewEventChannel(sender, p.clock, p.buffsCfg.size, p.buffsCfg.count, p.buffsCfg.timeout)
	}
}

func (p *PubstackModule) closeAllEventChannels() {
	for key, ch := range p.eventChannels {
		ch.Close()
		delete(p.eventChannels, key)
	}
}