// Package endpoints — video_pipeline.go
//
// Implements the full programmatic video ad pipeline:
//
//  Stage 1  Player/Publisher Request   — Player hits the VAST tag URL
//  Stage 2  Ad Server Config           — Resolve ad-server config for the placement
//  Stage 3  OpenRTB Bid Request        — Build and forward OpenRTB bid request to demand (SSP/DSP)
//  Stage 4  Bid Response               — Receive bid response with VAST/adm
//  Stage 5  Build/Wrap VAST Response   — Ad server wraps VAST with impression trackers
//  Stage 6  VAST Returned to Player    — Serialise final VAST XML and write HTTP response
//  Stage 7  Playback + Tracking        — Record impression/quartile/complete events
//
// HTTP endpoints exposed:
//
//	GET  /video/vast              — runs stages 1-6, returns VAST XML to calling player
//	POST /video/vast              — same as GET but accepts extended request body
//	POST /video/tracking          — stage-7 event beacon (impression / quartile / complete)

package endpoints

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	adcom1 "github.com/prebid/openrtb/v20/adcom1"
	"github.com/julienschmidt/httprouter"
	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/exchange"
	"github.com/prebid/prebid-server/v4/hooks/hookexecution"
	"github.com/prebid/prebid-server/v4/metrics"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
	"github.com/prebid/prebid-server/v4/util/jsonutil"
)

// ─────────────────────────────────────────────────────────────────────────────
// Stage 2 — Ad Server Config types
// ─────────────────────────────────────────────────────────────────────────────

// AdServerConfig holds placement-level configuration resolved before every auction.
// ExtraDemandCfg holds a secondary/fallback demand endpoint used in waterfall
// when the primary demand source (DemandVASTURL / DemandOrtbURL) returns no fill.
type ExtraDemandCfg struct {
	VASTTagURL string  `json:"vast_tag_url,omitempty"`
	OrtbURL    string  `json:"ortb_url,omitempty"`
	FloorCPM   float64 `json:"floor_cpm,omitempty"`
	BCat       []string `json:"bcat,omitempty"`
	BAdv       []string `json:"badv,omitempty"`
}

type AdServerConfig struct {
	PlacementID   string   `json:"placement_id"`
	PublisherID   string   `json:"publisher_id"`
	ContentURL    string   `json:"content_url,omitempty"`
	DomainOrApp   string   `json:"domain_or_app,omitempty"`
	MinDuration   int      `json:"min_duration"`
	MaxDuration   int      `json:"max_duration"`
	Protocols     []int    `json:"protocols,omitempty"`
	APIs          []int    `json:"apis,omitempty"`
	AllowedBidders []string `json:"allowed_bidders,omitempty"`
	FloorCPM      float64  `json:"floor_cpm,omitempty"`
	// Targeting keys forwarded into OpenRTB ext.
	TargetingExt map[string]interface{} `json:"targeting_ext,omitempty"`
	// Direct demand routing — populated when the Ad Unit is linked to a Campaign.
	// When DemandVASTURL is set the pipeline returns a VAST wrapper around it.
	// When DemandOrtbURL is set the pipeline POSTs an OpenRTB request to that URL.
	DemandVASTURL string `json:"demand_vast_url,omitempty"`
	DemandOrtbURL string `json:"demand_ortb_url,omitempty"`
	// Blocked advertisers and categories forwarded to demand (from campaign).
	BAdv []string `json:"badv,omitempty"`
	BCat []string `json:"bcat,omitempty"`
	// ExtraDemand holds additional demand sources tried in waterfall order
	// when the primary source returns no fill.
	ExtraDemand []ExtraDemandCfg `json:"extra_demand,omitempty"`
	// SeatWeights maps seat/bidder name to a weight used in tie-breaking.
	// Higher weight wins when bids have equal price.
	SeatWeights map[string]float64 `json:"seat_weights,omitempty"`
	// RequestBaseURL is set per-request to the scheme+host seen by the player
	// (e.g. "http://adzrvr.com"). Used to build self-referencing tracking URLs
	// instead of the static external_url config value.
	RequestBaseURL string `json:"-"`

	// VideoPlacementType holds the ad unit's placement type string
	// ("instream"|"outstream"|"interstitial"|"rewarded") used to set
	// imp.video.placement in outbound ORTB requests.
	VideoPlacementType string `json:"-"`
	// CampaignID is the dashboard campaign linked to this ad unit (informational,
	// used for bid reporting).
	CampaignID string `json:"-"`

	// CampaignProtocols holds the DEMAND-SIDE protocol list from the linked
	// Campaign config (what the campaign can deliver). This is stored
	// separately from Protocols (the supply-side / player capability list)
	// and is NOT injected into imp.video.protocols. It is available for
	// informational / filtering use only.
	CampaignProtocols []int `json:"-"`
}

// adServerConfigStore is a thread-safe registry of AdServerConfig keyed by PlacementID.
// Configs are persisted to disk so placement settings survive server restarts.
type adServerConfigStore struct {
	mu       sync.RWMutex
	configs  map[string]*AdServerConfig
	filePath string
}

func newAdServerConfigStore(filePath string) *adServerConfigStore {
	s := &adServerConfigStore{configs: make(map[string]*AdServerConfig), filePath: filePath}
	s.load()
	return s
}

// load reads previously-saved configs from disk (best-effort; skips if file absent).
func (s *adServerConfigStore) load() {
	if s.filePath == "" {
		return
	}
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("config store: load: %v", err)
		}
		return
	}
	var m map[string]AdServerConfig
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("config store: parse: %v", err)
		return
	}
	for k, v := range m {
		cp := v
		s.configs[k] = &cp
	}
}

// save writes current configs to disk atomically.
func (s *adServerConfigStore) save() {
	if s.filePath == "" {
		return
	}
	s.mu.RLock()
	out := make(map[string]AdServerConfig, len(s.configs))
	for k, v := range s.configs {
		out[k] = *v
	}
	s.mu.RUnlock()
	if err := os.MkdirAll(filepath.Dir(s.filePath), 0755); err != nil {
		log.Printf("config store: mkdir: %v", err)
		return
	}
	data, err := json.Marshal(out)
	if err != nil {
		log.Printf("config store: marshal: %v", err)
		return
	}
	tmp := s.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("config store: write: %v", err)
		return
	}
	if err := os.Rename(tmp, s.filePath); err != nil {
		log.Printf("config store: rename: %v", err)
	}
}

// set registers or replaces a config entry and persists in the background.
func (s *adServerConfigStore) set(c *AdServerConfig) {
	s.mu.Lock()
	s.configs[c.PlacementID] = c
	s.mu.Unlock()
	go s.save()
}

// get returns the config for placementID, or nil if not found.
func (s *adServerConfigStore) get(placementID string) *AdServerConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configs[placementID]
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 7 — Tracking event store
// ─────────────────────────────────────────────────────────────────────────────

// EventType enumerates VAST tracking events.
type EventType string

const (
	EventImpression   EventType = "impression"
	EventStart        EventType = "start"
	EventFirstQuartile EventType = "firstQuartile"
	EventMidpoint     EventType = "midpoint"
	EventThirdQuartile EventType = "thirdQuartile"
	EventComplete     EventType = "complete"
	EventClick        EventType = "click"
)

// TrackingEvent records a single player-fired tracking beacon.
type TrackingEvent struct {
	AuctionID   string    `json:"auction_id"`
	BidID       string    `json:"bid_id"`
	Bidder      string    `json:"bidder"`
	PlacementID string    `json:"placement_id"`
	Event       EventType `json:"event"`
	CrID        string    `json:"crid,omitempty"`
	Price       float64   `json:"price,omitempty"`
	ADomain     string    `json:"adom,omitempty"`
	ReceivedAt  time.Time `json:"received_at"`
}

// trackingStore persists tracking events in memory.
type trackingStore struct {
	mu     sync.RWMutex
	events []TrackingEvent
}

func (t *trackingStore) record(ev TrackingEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, ev)
}

func (t *trackingStore) all() []TrackingEvent {
	// Hold the read-lock only long enough to snapshot the slice header.
	// The actual copy happens outside the lock so readers don't block writers
	// for the duration of a potentially large memcopy.
	t.mu.RLock()
	snap := t.events[:len(t.events):len(t.events)]
	t.mu.RUnlock()
	if len(snap) == 0 {
		return []TrackingEvent{}
	}
	cp := make([]TrackingEvent, len(snap))
	copy(cp, snap)
	return cp
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-publisher real-time ad statistics
// ─────────────────────────────────────────────────────────────────────────────

// VideoStats holds live counters for a single publisher.
type VideoStats struct {
	AdRequests    int64   `json:"ad_requests"`
	Opportunities int64   `json:"opportunities"` // VAST served (adapter returned a creative)
	Impressions   int64   `json:"impressions"`   // player-confirmed: /video/tracking?event=impression
	Completes     int64   `json:"completes"`     // player-confirmed: /video/tracking?event=complete
	VCR           float64 `json:"vcr_pct"`       // video completion rate: completes ÷ impressions × 100
	Revenue       float64 `json:"revenue"`       // actual revenue in USD (bid CPM ÷ 1000 per opportunity)
}

// VideoStatsPayload is the JSON payload returned by /dashboard/stats/video.
type VideoStatsPayload struct {
	ByPublisher map[string]*VideoStats `json:"by_publisher"`
	Total       VideoStats             `json:"total"`
}

// videoStatsStore tracks per-publisher stats protected by a single mutex.
type videoStatsStore struct {
	mu       sync.Mutex
	byPub    map[string]*VideoStats
	filePath string
}

func newVideoStatsStore(filePath string) *videoStatsStore {
	s := &videoStatsStore{byPub: make(map[string]*VideoStats), filePath: filePath}
	s.load()
	return s
}

// load reads previously-saved stats from disk (best effort; skips if file absent).
func (s *videoStatsStore) load() {
	if s.filePath == "" {
		return
	}
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("video stats: load: %v", err)
		}
		return
	}
	var m map[string]VideoStats
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("video stats: parse: %v", err)
		return
	}
	for k, v := range m {
		cp := v
		s.byPub[k] = &cp
	}
}

// save writes current stats to disk atomically.
func (s *videoStatsStore) save() {
	if s.filePath == "" {
		return
	}
	s.mu.Lock()
	out := make(map[string]VideoStats, len(s.byPub))
	for k, v := range s.byPub {
		out[k] = *v
	}
	s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.filePath), 0755); err != nil {
		log.Printf("video stats: mkdir: %v", err)
		return
	}
	data, err := json.Marshal(out)
	if err != nil {
		log.Printf("video stats: marshal: %v", err)
		return
	}
	tmp := s.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("video stats: write: %v", err)
		return
	}
	if err := os.Rename(tmp, s.filePath); err != nil {
		log.Printf("video stats: rename: %v", err)
	}
}

// getOrCreate returns the VideoStats entry for pubID under the lock.
// Callers that need to increment must hold the lock themselves to avoid
// a double lock-unlock round-trip (get then re-lock).
func (s *videoStatsStore) getOrCreate(pubID string) *VideoStats {
	v := s.byPub[pubID]
	if v == nil {
		v = &VideoStats{}
		s.byPub[pubID] = v
	}
	return v
}

func (s *videoStatsStore) incRequest(pubID string) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	s.getOrCreate(pubID).AdRequests++
	s.mu.Unlock()
}

func (s *videoStatsStore) incFill(pubID string, price float64) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	v := s.getOrCreate(pubID)
	v.Opportunities++
	v.Revenue += price / 1000 // price is CPM; store actual dollars per opportunity
	s.mu.Unlock()
}

// incImpression is called when the player fires the client-side
// event=impression beacon, confirming the ad was rendered.
func (s *videoStatsStore) incImpression(pubID string) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	s.getOrCreate(pubID).Impressions++
	s.mu.Unlock()
}

func (s *videoStatsStore) incComplete(pubID string) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	s.getOrCreate(pubID).Completes++
	s.mu.Unlock()
}

func (s *videoStatsStore) snapshot() VideoStatsPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := VideoStatsPayload{ByPublisher: make(map[string]*VideoStats, len(s.byPub))}
	for k, v := range s.byPub {
		cp := *v
		if cp.Impressions > 0 {
			cp.VCR = float64(cp.Completes) / float64(cp.Impressions) * 100
		}
		out.ByPublisher[k] = &cp
		out.Total.AdRequests += cp.AdRequests
		out.Total.Opportunities += cp.Opportunities
		out.Total.Impressions += cp.Impressions
		out.Total.Completes += cp.Completes
		out.Total.Revenue += cp.Revenue
	}
	if out.Total.Impressions > 0 {
		out.Total.VCR = float64(out.Total.Completes) / float64(out.Total.Impressions) * 100
	}
	return out
}

// reset clears all per-publisher stats from memory and removes the on-disk
// persistence file so the next save() starts clean.
func (s *videoStatsStore) reset() {
	s.mu.Lock()
	s.byPub = make(map[string]*VideoStats)
	s.mu.Unlock()
	if s.filePath != "" {
		_ = os.Remove(s.filePath)
		_ = os.Remove(s.filePath + ".tmp")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// VAST XML types — minimal subset sufficient for wrapping
// ─────────────────────────────────────────────────────────────────────────────

// vastRoot is the top-level VAST document.
type vastRoot struct {
	XMLName xml.Name  `xml:"VAST"`
	Version string    `xml:"version,attr"`
	Ad      []vastAd  `xml:"Ad"`
}

// vastAd represents one <Ad> element.
type vastAd struct {
	ID      string       `xml:"id,attr,omitempty"`
	Inline  *vastInline  `xml:"InLine,omitempty"`
	Wrapper *vastWrapper `xml:"Wrapper,omitempty"`
}

// vastInline carries the actual creative.
type vastInline struct {
	AdSystem    string           `xml:"AdSystem"`
	AdTitle     string           `xml:"AdTitle"`
	Impression  []vastImpression `xml:"Impression"`
	Creatives   vastCreatives    `xml:"Creatives"`
}

// vastWrapper wraps an upstream VAST URI.
type vastWrapper struct {
	AdSystem       string              `xml:"AdSystem"`
	VASTAdTagURI   vastCDATA           `xml:"VASTAdTagURI"`
	Impression     []vastImpression    `xml:"Impression"`
	// TrackingEvents allows BURL and other server-side trackers to be fired
	// from a Wrapper without waiting for the chained InLine to load.
	TrackingEvents *vastTrackingEvents `xml:"TrackingEvents,omitempty"`
}

type vastImpression struct {
	ID    string    `xml:"id,attr,omitempty"`
	Inner vastCDATA `xml:",innerxml"`
}

type vastCreatives struct {
	Creative []vastCreative `xml:"Creative"`
}

type vastCreative struct {
	ID     string        `xml:"id,attr,omitempty"`
	Linear *vastLinear   `xml:"Linear,omitempty"`
}

type vastLinear struct {
	Duration       string              `xml:"Duration"`
	TrackingEvents *vastTrackingEvents `xml:"TrackingEvents,omitempty"`
	MediaFiles     vastMediaFiles      `xml:"MediaFiles"`
	VideoClicks    *vastVideoClicks    `xml:"VideoClicks,omitempty"`
}

type vastTracking struct {
	Event string    `xml:"event,attr"`
	Inner vastCDATA `xml:",innerxml"`
}

type vastMediaFiles struct {
	MediaFile []vastMediaFile `xml:"MediaFile"`
}

type vastMediaFile struct {
	Delivery string    `xml:"delivery,attr"`
	Type     string    `xml:"type,attr"`
	Width    int       `xml:"width,attr,omitempty"`
	Height   int       `xml:"height,attr,omitempty"`
	Inner    vastCDATA `xml:",innerxml"`
}

type vastVideoClicks struct {
	ClickThrough *vastCDATA `xml:"ClickThrough,omitempty"`
}

// vastTrackingEvents groups all <Tracking> elements under a single <TrackingEvents> parent.
// Using the nested path tag xml:"TrackingEvents>Tracking" on a slice field would produce
// a separate <TrackingEvents> wrapper per element, which breaks the VAST spec.
type vastTrackingEvents struct {
	Tracking []vastTracking `xml:"Tracking"`
}

// vastCDATA is serialised as a CDATA section.
type vastCDATA struct {
	Text string `xml:",cdata"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline handler
// ─────────────────────────────────────────────────────────────────────────────

// VideoPipelineHandler orchestrates all 7 pipeline stages.
type VideoPipelineHandler struct {
	exchange     exchange.Exchange
	cfg          *config.Configuration
	metricsEng   metrics.MetricsEngine
	configStore  *adServerConfigStore
	tracking     *trackingStore
	videoStats   *videoStatsStore
	bidReport    *BidReportHandler
	externalURL  string
	// demandClient is a shared, pool-backed HTTP client for all outbound calls
	// to demand partners. Tuned for high QPS: 256 keepalive conns per host,
	// 1 s dial timeout, HTTP/2 enabled.
	demandClient *http.Client
	// bufPool reuses byte buffers across requests to eliminate per-request
	// heap allocations for VAST XML and JSON response serialization.
	bufPool sync.Pool
}

// NewVideoPipelineHandler constructs the handler and returns HTTP handles for
// the router. dataDir is the directory used for persistent stats storage;
// pass "" to disable persistence.
func NewVideoPipelineHandler(
	ex exchange.Exchange,
	cfg *config.Configuration,
	me metrics.MetricsEngine,
	dataDir string,
) *VideoPipelineHandler {
	var statsFile string
	if dataDir != "" {
		statsFile = filepath.Join(dataDir, "video_stats.json")
	}
	var configFile string
	if dataDir != "" {
		configFile = filepath.Join(dataDir, "ad_server_configs.json")
	}
	vs := newVideoStatsStore(statsFile)
	h := &VideoPipelineHandler{
		exchange:    ex,
		cfg:         cfg,
		metricsEng:  me,
		configStore: newAdServerConfigStore(configFile),
		tracking:    &trackingStore{},
		videoStats:  vs,
		bidReport:   NewBidReportHandler(dataDir),
		externalURL: cfg.ExternalURL,
		bufPool: sync.Pool{
			New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) },
		},
		demandClient: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   1 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:          1024,
				MaxIdleConnsPerHost:   256,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   3 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				ForceAttemptHTTP2:     true,
				TLSClientConfig:       &tls.Config{InsecureSkipVerify: os.Getenv("PBS_INSECURE_TLS") == "1"}, //nolint:gosec
			},
		},
	}
	// Persist stats to disk every 30 seconds.
	if statsFile != "" {
		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			for range t.C {
				vs.save()
			}
		}()
	}
	return h
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 1 — Player/Publisher Request handler
// ─────────────────────────────────────────────────────────────────────────────

// VASTEndpoint is the HTTP entry-point (GET or POST).
// The player calls this URL with at minimum ?placement_id=<id>&app_bundle=<bundle>
// (or a JSON body for POST requests).
//
// Route: GET  /video/vast
//        POST /video/vast
func (h *VideoPipelineHandler) VASTEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		start := time.Now()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		// ── Stage 1: parse player/publisher request ──────────────────────
		req, err := h.parsePlayerRequest(r)
		if err != nil {
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}

		// ── Stage 2: resolve ad server config ────────────────────────────
		adsCfg, err := h.resolveAdServerConfig(req.PlacementID)
		if err != nil {
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
			http.Error(w, "unknown placement: "+err.Error(), http.StatusNotFound)
			return
		}
		// Stamp the request's base URL so tracking pixels refer back to the
		// same host that served this VAST (e.g. a custom domain or IP).
		cfgCopy := *adsCfg
		cfgCopy.RequestBaseURL = requestBaseURL(r)
		adsCfg = &cfgCopy

		// ── Stages 3-6: demand routing via adapter ───────────────────────
		// The adapter is selected by (InboundVAST, demand type) and handles
		// all four schemas: VAST→VAST, VAST→ORTB and Prebid fallback.
		adapter := h.adapterRouter(RouterKey{InboundVAST, resolveDemandType(adsCfg)})
		h.videoStats.incRequest(adsCfg.PublisherID)
		resp, err := adapter.Execute(ctx, req, adsCfg)
		if err != nil && resolveDemandType(adsCfg) != DemandTypeVAST {
			// Waterfall: try extra demand sources in order before returning no-fill.
			// Skipped when primary campaign uses a VAST tag (adapter handles fallback internally).
			for _, extra := range adsCfg.ExtraDemand {
				extraCfg := *adsCfg
				extraCfg.DemandVASTURL = extra.VASTTagURL
				extraCfg.DemandOrtbURL = extra.OrtbURL
				if extra.FloorCPM > 0 {
					extraCfg.FloorCPM = extra.FloorCPM
				}
				if len(extra.BCat) > 0 {
					extraCfg.BCat = extra.BCat
				}
				if len(extra.BAdv) > 0 {
					extraCfg.BAdv = extra.BAdv
				}
				extraCfg.ExtraDemand = nil // avoid recursion
				extraAdapter := h.adapterRouter(RouterKey{InboundVAST, resolveDemandType(&extraCfg)})
				resp, err = extraAdapter.Execute(ctx, req, &extraCfg)
				if err == nil {
					break
				}
			}
		}
		if err != nil {
			// No fill — return an empty VAST 3.0 document so players (Roku,
			// Fire TV, Samsung, etc.) can gracefully skip the ad slot.
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
			h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))
			h.writeVASTResponse(w, emptyVAST)
			return
		}

		// Only count a monetised opportunity when the adapter returned a real
		// creative (not a passthrough wrapper fallback from vastToVASTAdapter).
		if !resp.NoFill {
			h.videoStats.incFill(adsCfg.PublisherID, resp.WinPrice)
			h.recordBidReport(adsCfg, req, resp, "win")
		}
		h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
		// NOTE: RecordImps is intentionally NOT called here.
		// Impressions are only counted when the player fires the /video/tracking
		// beacon with event=impression (client-confirmed render, not server-serve).
		h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))

		// ── Stage 6: return VAST to player ───────────────────────────────
		h.writeVASTResponse(w, resp.VASTXml)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// OpenRTB inbound endpoint
// ─────────────────────────────────────────────────────────────────────────────

// ORTBEndpoint is the HTTP entry-point for players / publishers that natively
// speak OpenRTB 2.5.  It mirrors VASTEndpoint but returns a JSON BidResponse
// instead of VAST XML.
//
// The demand type configured on the linked Campaign determines which adapter
// schema runs: ORTB→ORTB, ORTB→VAST, or the Prebid fallback.
//
// Route: GET  /video/ortb
//        POST /video/ortb
func (h *VideoPipelineHandler) ORTBEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		start := time.Now()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		// ── Stage 1: parse player/publisher request ──────────────────────
		req, err := h.parsePlayerRequest(r)
		if err != nil {
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}

		// ── Stage 2: resolve ad server config ────────────────────────────
		adsCfg, err := h.resolveAdServerConfig(req.PlacementID)
		if err != nil {
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
			http.Error(w, "unknown placement: "+err.Error(), http.StatusNotFound)
			return
		}
		cfgCopy := *adsCfg
		cfgCopy.RequestBaseURL = requestBaseURL(r)
		adsCfg = &cfgCopy

		// ── Stages 3-5: demand routing via adapter ───────────────────────
		// The adapter is selected by (InboundORTB, demand type) and handles
		// ORTB→ORTB, ORTB→VAST and Prebid fallback schemas.
		adapter := h.adapterRouter(RouterKey{InboundORTB, resolveDemandType(adsCfg)})
		h.videoStats.incRequest(adsCfg.PublisherID)
		resp, err := adapter.Execute(ctx, req, adsCfg)
		if err != nil && resolveDemandType(adsCfg) != DemandTypeVAST {
			// Waterfall: try extra demand sources in order before returning no-fill.
			// Skipped when primary campaign uses a VAST tag (adapter handles fallback internally).
			for _, extra := range adsCfg.ExtraDemand {
				extraCfg := *adsCfg
				extraCfg.DemandVASTURL = extra.VASTTagURL
				extraCfg.DemandOrtbURL = extra.OrtbURL
				if extra.FloorCPM > 0 {
					extraCfg.FloorCPM = extra.FloorCPM
				}
				if len(extra.BCat) > 0 {
					extraCfg.BCat = extra.BCat
				}
				if len(extra.BAdv) > 0 {
					extraCfg.BAdv = extra.BAdv
				}
				extraCfg.ExtraDemand = nil
				extraAdapter := h.adapterRouter(RouterKey{InboundORTB, resolveDemandType(&extraCfg)})
				resp, err = extraAdapter.Execute(ctx, req, &extraCfg)
				if err == nil {
					break
				}
			}
		}
		if err != nil {
			// No fill — return empty 204
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
			h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if !resp.NoFill {
			h.videoStats.incFill(adsCfg.PublisherID, resp.WinPrice)
			h.recordBidReport(adsCfg, req, resp, "win")
		}
		h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
		// NOTE: RecordImps is intentionally NOT called here.
		// Impressions are only counted when the player fires the /video/tracking
		// beacon with event=impression (client-confirmed render, not server-serve).
		h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))

		// ── Stage 6: return BidResponse to player ────────────────────────
		buf := h.bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		if data, merr := json.Marshal(resp.BidResp); merr == nil {
			buf.Write(data)
		}
		hdr := w.Header()
		hdr.Set("Content-Type", "application/json")
		hdr.Set("Content-Length", strconv.Itoa(buf.Len()))
		w.WriteHeader(http.StatusOK)
		w.Write(buf.Bytes()) //nolint:errcheck
		h.bufPool.Put(buf)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 1 — parsePlayerRequest
// ─────────────────────────────────────────────────────────────────────────────

// PlayerRequest captures parameters arriving from the player / publisher.
type PlayerRequest struct {
	PlacementID string `json:"placement_id,omitempty"`
	PublisherID string `json:"publisher_id,omitempty"`
	AppBundle   string `json:"app_bundle,omitempty"`
	Domain      string `json:"domain,omitempty"`
	PageURL     string `json:"page_url,omitempty"`
	GDPR        string `json:"gdpr,omitempty"`
	Consent     string `json:"gdpr_consent,omitempty"`
	CCPA        string `json:"us_privacy,omitempty"`
	COPPA       int8   `json:"coppa,omitempty"`
	GPP         string `json:"gpp,omitempty"`
	GPPSID      string `json:"gpp_sid,omitempty"`
	Width       int64  `json:"w,omitempty"`
	Height      int64  `json:"h,omitempty"`
	// Optional overrides for duration
	MinDuration int `json:"min_duration,omitempty"`
	MaxDuration int `json:"max_duration,omitempty"`

	// Device signals — forwarded into the OpenRTB Device object and used for
	// macro substitution in demand VAST tag URLs.
	IP          string `json:"ip,omitempty"`           // client IP (overrides auto-detect)
	UA          string `json:"ua,omitempty"`           // user-agent (overrides header)
	AppName     string `json:"app_name,omitempty"`
	AppStoreURL string `json:"app_store_url,omitempty"`
	DeviceMake  string `json:"device_make,omitempty"`
	DeviceModel string `json:"device_model,omitempty"`
	// DeviceType is the IAB OpenRTB device-type integer:
	// 1=mobile/tablet, 2=PC, 3=CTV, 4=phone, 5=tablet, 6=connected device, 7=set-top box.
	DeviceType  int    `json:"device_type,omitempty"`
	DeviceOS    string `json:"os,omitempty"`
	OSVersion   string `json:"osv,omitempty"`          // device OS version (e.g. "11")
	IFA         string `json:"ifa,omitempty"`          // IDFA (iOS) or GAID (Android)
	IFAType     string `json:"ifa_type,omitempty"`     // "aaid", "idfa", "ppid", etc.
	LMT         int8   `json:"lmt,omitempty"`          // Limit Ad Tracking: 0=unrestricted, 1=limited
	Language    string `json:"language,omitempty"`     // device language ISO-639-1
	CountryCode string `json:"country_code,omitempty"`
	DNT         int8   `json:"dnt,omitempty"`
	ContentGenre  string `json:"ct_genre,omitempty"`
	ContentLang   string `json:"ct_lang,omitempty"`
	ContentRating string `json:"ct_rating,omitempty"`   // content rating (e.g. "TV-PG")
	LiveStream    int8   `json:"ct_livestream,omitempty"` // 1=live, 0=on-demand
	ContentLen    int64  `json:"ct_len,omitempty"`       // content duration in seconds
	ContentTitle  string `json:"ct_title,omitempty"`     // episode/video title
	ContentSeries string `json:"ct_series,omitempty"`    // show/series name (CTV)
	ContentSeason string `json:"ct_season,omitempty"`    // season identifier (e.g. "2")
	ContentURL    string `json:"ct_url,omitempty"`       // canonical content URL
	ContentCat    string `json:"ct_cat,omitempty"`       // comma-sep IAB content categories
	ContentProdQ  int    `json:"ct_prodq,omitempty"`     // IAB production quality: 0=unknown,1=professionally produced,2=prosumer,3=UGC
	SiteName      string `json:"site_name,omitempty"`    // human-readable site/app name
	SiteCat       string `json:"site_cat,omitempty"`     // comma-sep IAB content categories for site/app
	SiteKeywords  string `json:"site_keywords,omitempty"` // comma-sep keywords for site/app
	PageRef       string `json:"page_ref,omitempty"`     // referring URL (retargeting signal)
	AppVer        string `json:"app_ver,omitempty"`      // app version string
	// App context
	AppID string `json:"app_id,omitempty"` // app.id for app-traffic requests
	// Video impression controls
	Skip       int8  `json:"skip,omitempty"`        // 0=non-skippable (default), 1=skippable
	StartDelay int   `json:"start_delay,omitempty"` // 0=pre-roll (default), -1=mid-roll, -2=post-roll
	Secure     int8  `json:"secure,omitempty"`      // 0=HTTP (default), 1=HTTPS
	// Request-level controls
	TMax int64  `json:"tmax,omitempty"` // auction timeout ms; default 500
	BCat string `json:"bcat,omitempty"` // comma-separated blocked IAB categories
	BAdv string `json:"badv,omitempty"` // comma-separated blocked advertiser domains
	// User identity
	UserID string `json:"user_id,omitempty"` // exchange user ID
}

// parsePlayerRequest decodes a PlayerRequest from URL query-params (GET) or
// a JSON body (POST).  Query params always take precedence.
func (h *VideoPipelineHandler) parsePlayerRequest(r *http.Request) (*PlayerRequest, error) {
	pr := &PlayerRequest{}

	// JSON body (POST)
	if r.Method == http.MethodPost && r.Body != nil {
		defer r.Body.Close()
		limited := &io.LimitedReader{R: r.Body, N: 65536}
		dec := json.NewDecoder(limited)
		if err := dec.Decode(pr); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("decode body: %w", err)
		}
	}

	// Query-param overrides
	q := r.URL.Query()

	if v := q.Get("placement_id"); v != "" {
		pr.PlacementID = v
	} else if v := q.Get("sid"); v != "" {
		// sid is accepted as a common alias for placement_id in VAST tag URLs
		pr.PlacementID = v
	}
	if v := q.Get("publisher_id"); v != "" {
		pr.PublisherID = v
	}
	if v := q.Get("app_bundle"); v != "" {
		pr.AppBundle = v
	}
	if v := q.Get("domain"); v != "" {
		pr.Domain = v
	}
	if v := q.Get("page_url"); v != "" {
		if dec, err := url.PathUnescape(v); err == nil {
			v = dec
		}
		pr.PageURL = v
	}
	if v := q.Get("gdpr"); v != "" {
		pr.GDPR = v
	}
	if v := q.Get("gdpr_consent"); v != "" {
		pr.Consent = v
	}
	if v := q.Get("us_privacy"); v != "" {
		pr.CCPA = v
	}
	if v := q.Get("gpp"); v != "" {
		pr.GPP = v
	}
	if v := q.Get("gpp_sid"); v != "" {
		pr.GPPSID = v
	}

	// Viewport dimensions
	if v := q.Get("w"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil {
			pr.Width = n
		}
	}
	if v := q.Get("h"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil {
			pr.Height = n
		}
	}

	// Duration overrides
	if v := q.Get("min_dur"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			pr.MinDuration = n
		}
	}
	if v := q.Get("max_dur"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			pr.MaxDuration = n
		}
	}

	// Device signals
	if v := q.Get("ip"); v != "" {
		pr.IP = v
	}
	if v := q.Get("ua"); v != "" {
		pr.UA = v
	}
	if v := q.Get("app_name"); v != "" {
		pr.AppName = v
	}
	if v := q.Get("app_store_url"); v != "" {
		if dec, err := url.PathUnescape(v); err == nil {
			v = dec
		}
		pr.AppStoreURL = v
	}
	if v := q.Get("device_make"); v != "" {
		pr.DeviceMake = v
	}
	if v := q.Get("device_model"); v != "" {
		pr.DeviceModel = v
	}
	if v := q.Get("device_type"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			pr.DeviceType = n
		}
	}
	if v := q.Get("os"); v != "" {
		pr.DeviceOS = v
	}
	if v := q.Get("ifa"); v != "" {
		pr.IFA = v
	}
	if v := q.Get("country_code"); v != "" {
		pr.CountryCode = v
	}
	if v := q.Get("dnt"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 8); e == nil {
			pr.DNT = int8(n)
		}
	}
	if v := q.Get("ct_genre"); v != "" {
		pr.ContentGenre = v
	}
	if v := q.Get("ct_lang"); v != "" {
		pr.ContentLang = v
	}
	if v := q.Get("ct_rating"); v != "" {
		pr.ContentRating = v
	}
	if v := q.Get("ct_livestream"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 8); e == nil {
			pr.LiveStream = int8(n)
		}
	}
	if v := q.Get("ct_len"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil {
			pr.ContentLen = n
		}
	}
	if v := q.Get("app_id"); v != "" {
		pr.AppID = v
	}
	if v := q.Get("osv"); v != "" {
		pr.OSVersion = v
	}
	if v := q.Get("lmt"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 8); e == nil {
			pr.LMT = int8(n)
		}
	}
	if v := q.Get("language"); v != "" {
		pr.Language = v
	}
	if v := q.Get("ifa_type"); v != "" {
		pr.IFAType = v
	}
	if v := q.Get("user_id"); v != "" {
		pr.UserID = v
	}
	if v := q.Get("ct_title"); v != "" {
		pr.ContentTitle = v
	}
	if v := q.Get("ct_series"); v != "" {
		pr.ContentSeries = v
	}
	if v := q.Get("ct_season"); v != "" {
		pr.ContentSeason = v
	}
	if v := q.Get("ct_url"); v != "" {
		pr.ContentURL = v
	}
	if v := q.Get("ct_cat"); v != "" {
		pr.ContentCat = v
	}
	if v := q.Get("ct_prodq"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			pr.ContentProdQ = n
		}
	}
	if v := q.Get("site_name"); v != "" {
		pr.SiteName = v
	}
	if v := q.Get("site_cat"); v != "" {
		pr.SiteCat = v
	}
	if v := q.Get("site_keywords"); v != "" {
		pr.SiteKeywords = v
	}
	if v := q.Get("page_ref"); v != "" {
		pr.PageRef = v
	}
	if v := q.Get("app_ver"); v != "" {
		pr.AppVer = v
	}
	if v := q.Get("skip"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 8); e == nil {
			pr.Skip = int8(n)
		}
	}
	if v := q.Get("start_delay"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			pr.StartDelay = n
		}
	}
	if v := q.Get("secure"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 8); e == nil {
			pr.Secure = int8(n)
		}
	} else if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		pr.Secure = 1
	}
	if v := q.Get("tmax"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil {
			pr.TMax = n
		}
	}
	if v := q.Get("bcat"); v != "" {
		pr.BCat = v
	}
	if v := q.Get("badv"); v != "" {
		pr.BAdv = v
	}
	if pr.IP == "" {
		pr.IP = extractClientIP(r)
	}
	if pr.UA == "" {
		pr.UA = r.Header.Get("User-Agent")
	}

	if pr.PlacementID == "" {
		return nil, fmt.Errorf("placement_id is required")
	}

	return pr, nil
}

// extractClientIP returns the best-effort client IP from the request.
// It checks X-Forwarded-For, then X-Real-IP, then falls back to RemoteAddr.
func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if idx := strings.Index(xff, ","); idx > 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 2 — resolveAdServerConfig
// ─────────────────────────────────────────────────────────────────────────────

// resolveAdServerConfig looks up the ad server configuration for a placement.
// Falls back to a sensible default so the pipeline works even without an
// explicit registration.
func (h *VideoPipelineHandler) resolveAdServerConfig(placementID string) (*AdServerConfig, error) {
	if cfg := h.configStore.get(placementID); cfg != nil {
		return cfg, nil
	}
	// Default permissive config — allows all bidders to compete.
	return &AdServerConfig{
		PlacementID: placementID,
		MinDuration: 5,
		MaxDuration: 30,
		Protocols:   []int{2, 3, 5, 6, 7, 8}, // VAST 2.0, 3.0, 2.0W, 3.0W, 4.0, 4.0W
		APIs:        []int{1, 2, 7},           // VPAID 1.0, VPAID 2.0, OMID 1.0
	}, nil
}

// RegisterAdServerConfig allows external callers (e.g. the dashboard CRUD) to
// install or update placement-level ad server configuration at runtime.
func (h *VideoPipelineHandler) RegisterAdServerConfig(cfg *AdServerConfig) {
	h.configStore.set(cfg)
}

// detectIFAType infers the IAB ifa_type string from OS, make, model, and UA.
// Priority: iOS/tvOS → Roku → Samsung (Tizen) → Amazon FireTV → LG →
//           Vizio → Xiaomi (Mi Box/TV) → Sony Bravia → Android → dpid.
func detectIFAType(osStr, make, model, ua string) string {
	osL := strings.ToLower(osStr)
	makeL := strings.ToLower(make)
	modelL := strings.ToLower(model)
	uaL := strings.ToLower(ua)

	// ── iOS / tvOS (Apple IDFA) ───────────────────────────────────────────
	if osL == "ios" || osL == "tvos" {
		return "idfa"
	}

	// ── Roku (RIDA — Roku ID for Advertising) ────────────────────────────
	if strings.Contains(makeL, "roku") ||
		strings.Contains(modelL, "roku") ||
		strings.Contains(uaL, "roku") {
		return "rida"
	}

	// ── Samsung Smart TV — Tizen (TIFA) ──────────────────────────────────
	if strings.Contains(makeL, "samsung") ||
		strings.Contains(osL, "tizen") ||
		strings.Contains(uaL, "tizen") {
		return "tifa"
	}

	// ── Amazon Fire TV (AFAI) ─────────────────────────────────────────────
	if strings.Contains(makeL, "amazon") ||
		strings.Contains(modelL, "firetv") ||
		strings.Contains(modelL, "fire tv") ||
		strings.Contains(modelL, "aftm") || strings.Contains(modelL, "aftb") ||
		strings.Contains(modelL, "aftt") || strings.Contains(modelL, "afts") ||
		strings.Contains(uaL, "silk/") || strings.Contains(uaL, "fire tv") {
		return "afai"
	}

	// ── LG Smart TV — webOS (LGUDID) ──────────────────────────────────────
	if strings.Contains(makeL, "lg") ||
		strings.Contains(osL, "webos") ||
		strings.Contains(uaL, "webos") || strings.Contains(uaL, "netcast") {
		return "lgudid"
	}

	// ── Vizio SmartCast (VIDA) ─────────────────────────────────────────────
	if strings.Contains(makeL, "vizio") ||
		strings.Contains(uaL, "vizio") {
		return "vida"
	}

	// ── Xiaomi Mi Box / Mi TV (AAID — runs AOSP Android) ─────────────────
	if strings.Contains(makeL, "xiaomi") ||
		strings.Contains(modelL, "mibox") || strings.Contains(modelL, "mi box") ||
		strings.Contains(modelL, "mitv") || strings.Contains(modelL, "mi tv") {
		return "aaid"
	}

	// ── Sony Bravia (AAID — runs Android TV) ─────────────────────────────
	if strings.Contains(makeL, "sony") ||
		strings.Contains(modelL, "bravia") ||
		strings.Contains(uaL, "bravia") {
		return "aaid"
	}

	// ── Generic Android (AAID) ────────────────────────────────────────────
	if strings.Contains(osL, "android") {
		return "aaid"
	}

	// ── Unknown / fallback ────────────────────────────────────────────────
	return "dpid"
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 3 — forwardToExchange
// ─────────────────────────────────────────────────────────────────────────────

// forwardToExchange builds an OpenRTB bid request from the player request and
// ad server config, then submits it to the exchange for competitive bidding.
func (h *VideoPipelineHandler) forwardToExchange(
	ctx context.Context,
	pr *PlayerRequest,
	adsCfg *AdServerConfig,
) (*openrtb2.BidResponse, error) {
	// ── Build OpenRTB Bid Request ─────────────────────────────────────────
	bidReq := h.buildOpenRTBRequest(pr, adsCfg)

	// ── Wrap in RequestWrapper ────────────────────────────────────────────
	reqWrapper := &openrtb_ext.RequestWrapper{BidRequest: bidReq}

	// ── Resolve account (best-effort) ────────────────────────────────────
	account := config.Account{ID: adsCfg.PublisherID}

	// ── Build AuctionRequest ──────────────────────────────────────────────
	impExtInfoMap := make(map[string]exchange.ImpExtInfo, len(bidReq.Imp))
	for _, imp := range bidReq.Imp {
		impExtInfoMap[imp.ID] = exchange.ImpExtInfo{}
	}
	auctionReq := &exchange.AuctionRequest{
		BidRequestWrapper: reqWrapper,
		Account:           account,
		StartTime:         time.Now(),
		RequestType:       metrics.ReqTypeVideo,
		ImpExtInfoMap:     impExtInfoMap,
		HookExecutor:      &hookexecution.EmptyHookExecutor{},
	}

	// ── Run auction ───────────────────────────────────────────────────────
	auctionResp, err := h.exchange.HoldAuction(ctx, auctionReq, &exchange.DebugLog{})
	if err != nil {
		return nil, fmt.Errorf("HoldAuction: %w", err)
	}

	return auctionResp.BidResponse, nil
}

// buildOpenRTBRequest constructs a fully-populated OpenRTB 2.5 bid request.
// All fields are sourced from the incoming PlayerRequest; sensible defaults
// are applied when the publisher does not supply a value.
func (h *VideoPipelineHandler) buildOpenRTBRequest(
	pr *PlayerRequest,
	adsCfg *AdServerConfig,
) *openrtb2.BidRequest {
	auctionID := fastGenerateID()
	impID := fastGenerateID()

	// ── Duration (publisher override > ad-server config > hard default) ───
	minDur := adsCfg.MinDuration
	if minDur == 0 {
		minDur = 5
	}
	if pr.MinDuration > 0 {
		minDur = pr.MinDuration
	}
	maxDur := adsCfg.MaxDuration
	if maxDur == 0 {
		maxDur = 30
	}
	if pr.MaxDuration > 0 {
		maxDur = pr.MaxDuration
	}

	// ── Protocols — SUPPLY SIDE (what the publisher's player can render) ──
	// These come from the ad unit config and are independent of the linked
	// campaign's Protocols (demand side / what the buyer can deliver).
	// AdServerConfig.CampaignProtocols holds the demand-side list for
	// informational use; it is intentionally NOT used here.
	var protocols []adcom1.MediaCreativeSubtype
	if len(adsCfg.Protocols) > 0 {
		protocols = make([]adcom1.MediaCreativeSubtype, len(adsCfg.Protocols))
		for i, p := range adsCfg.Protocols {
			protocols[i] = adcom1.MediaCreativeSubtype(p)
		}
	} else {
		// Fallback: VAST 2.0, 3.0, 2.0 Wrapper, 3.0 Wrapper, 4.0, 4.0 Wrapper.
		protocols = []adcom1.MediaCreativeSubtype{2, 3, 5, 6, 7, 8}
	}

	// ── API frameworks ───────────────────────────────────────────────────
	var apis []adcom1.APIFramework
	for _, a := range adsCfg.APIs {
		apis = append(apis, adcom1.APIFramework(a))
	}
	// Always include OMID 1.0 (7) — required for measurement on CTV/mobile.
	homidPresent := false
	for _, a := range apis {
		if a == adcom1.APIOMID10 {
			homidPresent = true
			break
		}
	}
	if !homidPresent {
		apis = append(apis, adcom1.APIOMID10)
	}

	// ── Video player dimensions (publisher > default 1920×1080) ──────────
	vidW := int64(1920)
	if pr.Width > 0 {
		vidW = pr.Width
	}
	vidH := int64(1080)
	if pr.Height > 0 {
		vidH = pr.Height
	}

	// ── Video control flags ───────────────────────────────────────────────
	skipVal := pr.Skip                          // 0=non-skippable (default)
	startDelay := adcom1.StartDelay(pr.StartDelay) // 0=pre-roll (default)
	boxingVal := int8(1)
	seqVal := int8(1)

	// ── Imp.Secure: from publisher; auto-detected in parsePlayerRequest ───
	secureVal := pr.Secure

imp := openrtb2.Imp{
		ID:          impID,
		TagID:       adsCfg.PlacementID,
		BidFloor:    adsCfg.FloorCPM,
		BidFloorCur: "USD",
		Secure:      &secureVal,
		Exp:         300, // advisory: max seconds between auction and impression serve
		Video: &openrtb2.Video{
			MIMEs:        []string{"video/mp4", "application/x-mpegURL", "video/mp4;codecs=avc1"},
			Linearity:    adcom1.LinearityLinear,
			MinDuration:  int64(minDur),
			MaxDuration:  int64(maxDur),
			Protocols:    protocols,
			API:          apis,
			W:            &vidW,
			H:            &vidH,
			StartDelay:   startDelay.Ptr(),
			Skip:          &skipVal,
			Sequence:      seqVal,
			BoxingAllowed: &boxingVal,
			Placement:     videoPlacementSubtype(adsCfg.VideoPlacementType),
			Pos:           videoAdPosition(adsCfg.VideoPlacementType),
			// MaxExtended: -1 allows DSPs to run creatives beyond MaxDuration
			// (e.g. 5 extra seconds for outstream completion).
			// 0 = extension not allowed (default). -1 = unlimited extension.
			MaxExtended:   -1,
		},
	}

	// ── Imp.Ext — bidder params ───────────────────────────────────────────
	impExtMap := map[string]json.RawMessage{}
	adzrvrRaw, _ := json.Marshal(openrtb_ext.ImpExtAdzrvr{
		PlacementID: adsCfg.PlacementID,
		PublisherID: adsCfg.PublisherID,
	})
	for _, b := range adsCfg.AllowedBidders {
		if b == "adzrvr" {
			impExtMap["adzrvr"] = adzrvrRaw
		} else {
			impExtMap[b] = json.RawMessage(`{}`)
		}
	}
	if len(impExtMap) > 0 {
		if extRaw, mErr := jsonutil.Marshal(impExtMap); mErr == nil {
			imp.Ext = extRaw
		}
	}

	// ── Request-level auction parameters ─────────────────────────────────
	tmax := int64(500) // fallback
	if pr.TMax > 0 {
		tmax = pr.TMax
	}

	var bcat []string
	for _, c := range strings.Split(pr.BCat, ",") {
		if c = strings.TrimSpace(c); c != "" {
			bcat = append(bcat, c)
		}
	}
	// Merge campaign-level blocked categories.
	for _, c := range adsCfg.BCat {
		if c != "" {
			bcat = append(bcat, c)
		}
	}
	var badv []string
	for _, d := range strings.Split(pr.BAdv, ",") {
		if d = strings.TrimSpace(d); d != "" {
			badv = append(badv, d)
		}
	}
	// Merge campaign-level blocked advertisers.
	for _, d := range adsCfg.BAdv {
		if d != "" {
			badv = append(badv, d)
		}
	}

	bidReq := &openrtb2.BidRequest{
		ID:   auctionID,
		Imp:  []openrtb2.Imp{imp},
		AT:   1, // first-price auction
		TMax: tmax,
		Cur:  []string{"USD"},
		BCat: bcat,
		BAdv: badv,
	}

	// ── App or Site context ───────────────────────────────────────────────
	buildContent := func() *openrtb2.Content {
		if pr.ContentGenre == "" && pr.ContentLang == "" &&
			pr.ContentRating == "" && pr.ContentLen == 0 &&
			pr.ContentTitle == "" && pr.ContentSeries == "" &&
			pr.ContentURL == "" && pr.ContentCat == "" {
			return nil
		}
		lsVal := pr.LiveStream
		c := &openrtb2.Content{
			Genre:         pr.ContentGenre,
			Language:      pr.ContentLang,
			ContentRating: pr.ContentRating,
			LiveStream:    &lsVal,
			Title:         pr.ContentTitle,
			Series:        pr.ContentSeries,
			Season:        pr.ContentSeason,
			URL:           pr.ContentURL,
			// prodq=1 (Professionally Produced) is the strongest positive signal:
			// premium content commands higher CPMs from brand-safe buyers.
			// Default to 1 when the caller does not specify (safe assumption for
			// broadcast/streaming inventory; override via ct_prodq=0 for UGC).
			ProdQ: prodQPtr(pr.ContentProdQ),
		}
		if pr.ContentLen > 0 {
			c.Len = pr.ContentLen
		}
		if pr.ContentCat != "" {
			for _, cat := range strings.Split(pr.ContentCat, ",") {
				if cat = strings.TrimSpace(cat); cat != "" {
					c.Cat = append(c.Cat, cat)
				}
			}
		}
		return c
	}

	// parseSiteCat converts a comma-separated IAB category string to a []string.
	parseCatList := func(csv string) []string {
		var cats []string
		for _, s := range strings.Split(csv, ",") {
			if s = strings.TrimSpace(s); s != "" {
				cats = append(cats, s)
			}
		}
		return cats
	}

	// Publisher identity — shared across site and app contexts.
	pub := &openrtb2.Publisher{
		ID:     adsCfg.PublisherID,
		Domain: adsCfg.DomainOrApp,
	}
	// Populate publisher IAB categories from site_cat when present.
	if pr.SiteCat != "" {
		pub.Cat = parseCatList(pr.SiteCat)
	}

	if pr.AppBundle != "" {
		appID := pr.AppID
		if appID == "" {
			appID = adsCfg.PublisherID
		}
		app := &openrtb2.App{
			ID:        appID,
			Bundle:    pr.AppBundle,
			Name:      pr.AppName,
			StoreURL:  pr.AppStoreURL,
			Ver:       pr.AppVer,
			Publisher: pub,
			Content:   buildContent(),
		}
		if pr.SiteName != "" {
			app.Name = pr.SiteName
		}
		if pr.SiteCat != "" {
			app.Cat = parseCatList(pr.SiteCat)
		}
		if pr.SiteKeywords != "" {
			app.Keywords = pr.SiteKeywords
		}
		bidReq.App = app
	} else if adsCfg.DomainOrApp != "" && !strings.Contains(adsCfg.DomainOrApp, " ") && !strings.HasPrefix(adsCfg.DomainOrApp, "http") {
		// adsCfg.DomainOrApp is a bundle ID (e.g. com.samsung.tv, 12345 for Roku).
		// Use app context so demand partners can identify CTV inventory.
		appID := pr.AppID
		if appID == "" {
			appID = adsCfg.PublisherID
		}
		app := &openrtb2.App{
			ID:        appID,
			Bundle:    adsCfg.DomainOrApp,
			Name:      pr.SiteName,
			Ver:       pr.AppVer,
			Publisher: pub,
			Content:   buildContent(),
		}
		if pr.SiteCat != "" {
			app.Cat = parseCatList(pr.SiteCat)
		}
		if pr.SiteKeywords != "" {
			app.Keywords = pr.SiteKeywords
		}
		bidReq.App = app
	} else {
		domain := pr.Domain
		if domain == "" {
			domain = adsCfg.DomainOrApp
		}
		site := &openrtb2.Site{
			Page:      pr.PageURL,
			Ref:       pr.PageRef,
			Domain:    domain,
			Name:      pr.SiteName,
			Publisher: pub,
			Content:   buildContent(),
		}
		if pr.SiteCat != "" {
			site.Cat = parseCatList(pr.SiteCat)
		}
		if pr.SiteKeywords != "" {
			site.Keywords = pr.SiteKeywords
		}
		bidReq.Site = site
	}

	// ── Device (always included for video/CTV) ────────────────────────────
	{
		dntVal := int8(pr.DNT)
		lmtVal := pr.LMT
		dev := &openrtb2.Device{
			UA:         pr.UA,
			IP:         pr.IP,
			Make:       pr.DeviceMake,
			Model:      pr.DeviceModel,
			OS:         pr.DeviceOS,
			OSV:        pr.OSVersion,
			IFA:        pr.IFA,
			DeviceType: adcom1.DeviceType(pr.DeviceType),
			Language:   pr.Language,
			DNT:        &dntVal,
			Lmt:        &lmtVal,
		}
		if pr.CountryCode != "" {
			dev.Geo = &openrtb2.Geo{Country: iso2ToISO3(pr.CountryCode)}
		}
		ifaType := pr.IFAType
		if ifaType == "" && pr.IFA != "" {
			ifaType = detectIFAType(pr.DeviceOS, pr.DeviceMake, pr.DeviceModel, pr.UA)
		}
		if ifaType != "" {
			if extRaw, err := json.Marshal(map[string]string{"ifa_type": ifaType}); err == nil {
				dev.Ext = extRaw
			}
		}
		bidReq.Device = dev
	}

	// ── Regs (always included, default no restrictions) ───────────────────
	regs := &openrtb2.Regs{COPPA: pr.COPPA}
	if pr.CCPA != "" {
		regs.USPrivacy = pr.CCPA
	}
	if pr.GDPR != "" {
		if gdprVal, err := strconv.ParseInt(pr.GDPR, 10, 8); err == nil {
			v := int8(gdprVal)
			regs.GDPR = &v
		}
	}
	if pr.GPP != "" {
		regs.GPP = pr.GPP
	}
	if pr.GPPSID != "" {
		var gppSIDs []int8
		for _, s := range strings.Split(pr.GPPSID, ",") {
			if s = strings.TrimSpace(s); s == "" {
				continue
			}
			if v, err := strconv.ParseInt(s, 10, 8); err == nil {
				gppSIDs = append(gppSIDs, int8(v))
			}
		}
		regs.GPPSID = gppSIDs
	}
	// When no GPP SID is supplied leave regs.GPPSID nil (omitempty).
	// Do NOT default to [0] — section ID 0 is not a valid GPP section.
	bidReq.Regs = regs

	// ── User ─────────────────────────────────────────────────────────────
	if pr.UserID != "" || pr.Consent != "" {
		user := &openrtb2.User{ID: pr.UserID}
		if pr.Consent != "" {
			extUser := openrtb_ext.ExtUser{Consent: pr.Consent}
			if raw, err := jsonutil.Marshal(extUser); err == nil {
				user.Ext = raw
			}
		}
		bidReq.User = user
	}

	return bidReq
}

// ─────────────────────────────────────────────────────────────────────────────
// VAST wrapper helper  (used by demand adapters and buildVASTResponse)
// ─────────────────────────────────────────────────────────────────────────────

// substituteMacros replaces VAST tag macro placeholders in demandURL with actual
// values from the incoming player request and ad server config.
//
// Supported macros:
//
//	{cb}             — random cache-buster integer
//	{uip}            — client IP address
//	{ua}             — URL-encoded User-Agent string
//	{app_bundle}     — app bundle identifier
//	{app_name}       — URL-encoded application name
//	{app_store_url}  — URL-encoded app store URL
//	{device_make}    — device manufacturer
//	{device_model}   — device model
//	{device_type}    — IAB OpenRTB device-type integer
//	{idfa} / {ifa}   — IDFA (iOS) or GAID (Android)
//	{device_os}/{os} — device operating system
//	{country_code}   — ISO-3166-1 alpha-3 country code (as sent to SSPs in geo.country)
//	{dnt}            — do-not-track flag (0 or 1)
//	{us_privacy}     — CCPA us_privacy string
//	{gdpr}           — GDPR flag (0 or 1)
//	{gdpr_consent}   — GDPR TCF consent string
//	{w}              — creative width in pixels
//	{h}              — creative height in pixels
//	{min_dur}        — minimum video duration (seconds)
//	{max_dur}        — maximum video duration (seconds)
func substituteMacros(demandURL string, pr *PlayerRequest, adsCfg *AdServerConfig) string {
	// Random cache-buster — math/rand is ~10x faster than crypto/rand for
	// non-security values; Go 1.20+ global source is goroutine-safe.
	cbStr := strconv.FormatUint(uint64(mrand.Uint32()), 10)

	minDur := adsCfg.MinDuration
	if pr.MinDuration > 0 {
		minDur = pr.MinDuration
	}
	maxDur := adsCfg.MaxDuration
	if pr.MaxDuration > 0 {
		maxDur = pr.MaxDuration
	}

	wStr   := strconv.FormatInt(pr.Width, 10)
	hStr   := strconv.FormatInt(pr.Height, 10)
	minStr := strconv.Itoa(minDur)
	maxStr := strconv.Itoa(maxDur)

	replacer := strings.NewReplacer(
		"{cb}",            cbStr,
		"{uip}",           pr.IP,
		"{ua}",            url.QueryEscape(pr.UA),
		"{app_bundle}",    pr.AppBundle,
		"{app_name}",      url.QueryEscape(pr.AppName),
		"{app_store_url}", url.QueryEscape(pr.AppStoreURL),
		"{device_make}",   url.QueryEscape(pr.DeviceMake),
		"{device_model}",  url.QueryEscape(pr.DeviceModel),
		"{device_type}",   strconv.Itoa(pr.DeviceType),
		"{idfa}",          pr.IFA,
		"{ifa}",           pr.IFA,
		"{ifa_type}",      pr.IFAType,
		"{device_os}",     url.QueryEscape(pr.DeviceOS),
		"{os}",            url.QueryEscape(pr.DeviceOS),
		"{device_osv}",    url.QueryEscape(pr.OSVersion),
		"{osv}",           url.QueryEscape(pr.OSVersion),
		"{version_os}",    url.QueryEscape(pr.OSVersion),
		"{lang}",          pr.Language,
		"{language}",      pr.Language,
		"{country_code}",  pr.CountryCode,
		"{dnt}",           strconv.Itoa(int(pr.DNT)),
		"{lmt}",           strconv.Itoa(int(pr.LMT)),
		"{skip}",          strconv.Itoa(int(pr.Skip)),
		"{coppa}",         strconv.Itoa(int(pr.COPPA)),
		"{us_privacy}",    pr.CCPA,
		"{gdpr}",          pr.GDPR,
		"{gdpr_consent}",  pr.Consent,
		"{content_rating}", url.QueryEscape(pr.ContentRating),
		"{content_length}", strconv.FormatInt(pr.ContentLen, 10),
		// Dimension aliases (width/height are more common than w/h in tag URLs)
		"{w}",             wStr,
		"{h}",             hStr,
		"{width}",         wStr,
		"{height}",        hStr,
		// Duration aliases
		"{min_dur}",       minStr,
		"{max_dur}",       maxStr,
		"{min_duration}",  minStr,
		"{max_duration}",  maxStr,
	)
	return replacer.Replace(demandURL)
}

// ─────────────────────────────────────────────────────────────────────────────
// postToDemandORTB — shared ORTB HTTP transport (used by VAST→ORTB and ORTB→ORTB)
// ─────────────────────────────────────────────────────────────────────────────

// postToDemandORTB builds an enriched OpenRTB 2.5 BidRequest from the player
// request, POSTs it to the campaign's demand ORTB endpoint, and returns the
// applyOutboundHeaders sets outbound OpenRTB request headers directly on httpReq.
// Writing directly avoids allocating the intermediate http.Header map.
func applyOutboundHeaders(httpReq *http.Request, bidReq *openrtb2.BidRequest) {
	httpReq.Header.Set("Content-Type", "application/json;charset=utf-8")
	httpReq.Header.Set("Accept", "application/json")
	if bidReq.Device != nil {
		if bidReq.Device.UA != "" {
			httpReq.Header.Set("User-Agent", bidReq.Device.UA)
		}
		if bidReq.Device.IP != "" {
			httpReq.Header.Set("X-Forwarded-For", bidReq.Device.IP)
		} else if bidReq.Device.IPv6 != "" {
			httpReq.Header.Set("X-Forwarded-For", bidReq.Device.IPv6)
		}
	}
	if bidReq.Site != nil && bidReq.Site.Page != "" {
		httpReq.Header.Set("Referer", bidReq.Site.Page)
	}
}

// postToDemandORTB builds an enriched OpenRTB 2.5 BidRequest from the player
// request, POSTs it to the campaign's demand ORTB endpoint, and returns the
// raw BidResponse.  The caller decides whether to convert the response to VAST
// (vastToORTBAdapter) or proxy it directly (ortbToORTBAdapter).
func (h *VideoPipelineHandler) postToDemandORTB(
	ctx context.Context,
	pr *PlayerRequest,
	adsCfg *AdServerConfig,
) (*openrtb2.BidResponse, error) {
	bidReq := h.buildOpenRTBRequest(pr, adsCfg)
	body, err := json.Marshal(bidReq)
	if err != nil {
		return nil, fmt.Errorf("marshal OpenRTB request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, adsCfg.DemandOrtbURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build HTTP request: %w", err)
	}
	applyOutboundHeaders(httpReq, bidReq)

	resp, err := h.demandClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("POST to demand ORTB endpoint: %w", err)
	}

	if resp.StatusCode == http.StatusNoContent {
		resp.Body.Close()
		return nil, fmt.Errorf("no fill from demand (204)")
	}

	// Read body into a pooled buffer, then unmarshal.
	// This is faster than json.NewDecoder which allocates a decoder struct and
	// does incremental reads.
	buf := h.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	_, copyErr := io.Copy(buf, io.LimitReader(resp.Body, 1<<20)) // 1 MB cap
	resp.Body.Close()
	if copyErr != nil {
		h.bufPool.Put(buf)
		return nil, fmt.Errorf("read bid response: %w", copyErr)
	}
	var bidResp openrtb2.BidResponse
	if err := json.Unmarshal(sanitizeBidResponse(buf.Bytes()), &bidResp); err != nil {
		h.bufPool.Put(buf)
		return nil, fmt.Errorf("decode bid response: %w", err)
	}
	h.bufPool.Put(buf)
	return &bidResp, nil
}

// bidRespIntFieldRe matches OpenRTB integer fields that some demand partners
// erroneously send as JSON strings (e.g. "mtype":"1" instead of "mtype":1).
// The sanitizer converts them to bare numbers before unmarshaling so that the
// strict openrtb2 types (MarkupType, CreativeAttribute, etc.) don't reject them.
//
// Fields covered: mtype, attr items, dur, w, h, wratio, hratio, expdir
var bidRespIntFieldRe = regexp.MustCompile(`("(?:mtype|dur|w|h|wratio|hratio|expdir)"\s*:\s*)"(\d+)"`)

// mtypeNameRe matches named mtype constants used by some demand partners
// instead of the numeric values defined by OpenRTB 2.5 §5.1.
// Mapping: BANNER→1, VIDEO→2, AUDIO→3, NATIVE→4.
var mtypeNameRe = regexp.MustCompile(`"mtype"\s*:\s*"(?i:CREATIVE_MARKUP_)?(BANNER|VIDEO|AUDIO|NATIVE)"`)

var mtypeNameToInt = map[string]string{
	"BANNER": "1",
	"VIDEO":  "2",
	"AUDIO":  "3",
	"NATIVE": "4",
}

// sanitizeBidResponse normalises a raw BidResponse body from demand before
// JSON unmarshaling.  It is a no-op (returns the original slice) when no
// stringified integer fields are found, keeping the hot path allocation-free.
func sanitizeBidResponse(data []byte) []byte {
	if !bytes.Contains(data, []byte(`":"`)) {
		return data
	}
	// Fix named mtype constants (CREATIVE_MARKUP_VIDEO, VIDEO, BANNER, etc.)
	data = mtypeNameRe.ReplaceAllFunc(data, func(match []byte) []byte {
		upper := strings.ToUpper(string(match))
		for name, num := range mtypeNameToInt {
			if strings.Contains(upper, name) {
				return []byte(`"mtype":` + num)
			}
		}
		return match
	})
	// Fix numeric-as-string fields ("mtype":"2" → "mtype":2)
	return bidRespIntFieldRe.ReplaceAll(data, []byte(`${1}${2}`))
}

// fastGenerateID returns a pseudo-random 16-hex-character ID using the global
// math/rand source. It is ~10x faster than crypto/rand and sufficient for
// non-security auction/impression IDs. Go 1.20+ global PRNG is goroutine-safe.
func fastGenerateID() string {
	return strconv.FormatUint(mrand.Uint64(), 16)
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 4 — extractWinningBid
// ─────────────────────────────────────────────────────────────────────────────

// WinningBid holds the raw bid together with its creative and bidder identity.
type WinningBid struct {
	BidID   string
	AdM     string  // VAST XML, VAST Wrapper tag URI, or inline media URL
	NURL    string  // Win-notification URL — fire when bid wins (before serving)
	BURL    string  // Billing URL — fire when impression is rendered (on start)
	Price   float64
	Width   int64
	Height  int64
	CrID    string
	DealID  string
	ADomain []string  // Advertiser domains
}

// bidCandidate is used internally by extractWinningBid for tie-break sorting.
type bidCandidate struct {
	win    WinningBid
	weight float64
	pos    int    // response order index — proxy for seat latency
	key    string // stable sort key: seat + ":" + bid.ID
	seat   string
}

// extractWinningBid picks the best bid from the BidResponse using a
// 4-level tie-break rule:
//
//  1. Top price    — highest bid.Price wins.
//  2. Seat weight  — when prices are equal, higher weight (adsCfg.SeatWeights) wins.
//  3. Latency      — when weight is equal, the bid that appeared earliest in the
//                    response wins (earlier index = faster seat).
//  4. Stable key   — when all else is equal, lexicographic seat+":"+bid.ID
//                    ensures repeatability across requests.
func (h *VideoPipelineHandler) extractWinningBid(
	resp *openrtb2.BidResponse,
	adsCfg *AdServerConfig,
) (*WinningBid, string, error) {
	if resp == nil {
		return nil, "", fmt.Errorf("nil bid response")
	}

	var best *bidCandidate
	pos := 0

	for _, seatBid := range resp.SeatBid {
		seatWeight := 1.0
		if adsCfg != nil && adsCfg.SeatWeights != nil {
			if w, ok := adsCfg.SeatWeights[seatBid.Seat]; ok {
				seatWeight = w
			}
		}
		for i := range seatBid.Bid {
			bid := &seatBid.Bid[i]
			pos++
			if bid.AdM == "" && bid.NURL == "" {
				log.Printf("extractWinningBid: skipping bid %s from seat %s — empty AdM and NURL", bid.ID, seatBid.Seat)
				continue
			}
			// Enforce price floor: reject bids below the configured floor CPM.
			if adsCfg != nil && adsCfg.FloorCPM > 0 && bid.Price < adsCfg.FloorCPM {
				log.Printf("extractWinningBid: skipping bid %s from seat %s — price %.4f below floor %.4f", bid.ID, seatBid.Seat, bid.Price, adsCfg.FloorCPM)
				continue
			}
					c := &bidCandidate{
					win: WinningBid{
						BidID:   bid.ID,
						AdM:     bid.AdM,
						NURL:    bid.NURL,
						BURL:    bid.BURL,
						Price:   bid.Price,
						Width:   bid.W,
						Height:  bid.H,
						CrID:    bid.CrID,
						DealID:  bid.DealID,
						ADomain: bid.ADomain,
					},
				weight: seatWeight,
				pos:    pos,
				key:    seatBid.Seat + ":" + bid.ID,
				seat:   seatBid.Seat,
			}
			if best == nil || bidCandidateBetter(c, best) {
				best = c
			}
		}
	}

	if best == nil {
		return nil, "", fmt.Errorf("no fill")
	}
	return &best.win, best.seat, nil
}

// bidCandidateBetter returns true when a beats b under the 4-level tie-break rule.
func bidCandidateBetter(a, b *bidCandidate) bool {
	if a.win.Price != b.win.Price {
		return a.win.Price > b.win.Price
	}
	if a.weight != b.weight {
		return a.weight > b.weight
	}
	if a.pos != b.pos {
		return a.pos < b.pos // lower index = earlier = faster
	}
	return a.key < b.key // lexicographic stable key
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 5 — buildVASTResponse
// ─────────────────────────────────────────────────────────────────────────────

// vastCreativeType classifies what is inside WinningBid.AdM.
type vastCreativeType uint8

const (
	adMIsVAST  vastCreativeType = iota // AdM contains a VAST XML document (Inline or Wrapper)
	adMIsURI                            // AdM is a bare URL → use as Wrapper VASTAdTagURI
	adMIsMedia                          // AdM is a direct media URL → build Inline around it
	adMEmpty                            // AdM is empty; fall back to NURL/BURL
)

// mediaTypeForURL returns the MIME type and VAST delivery attribute for a media
// URL, inferred from the URL path extension (query/fragment stripped).
// Defaults to video/mp4 + progressive for unknown extensions.
func mediaTypeForURL(rawURL string) (mimeType, delivery string) {
	// strip query / fragment so filepath.Ext sees only the path
	if u, err := url.Parse(rawURL); err == nil {
		rawURL = u.Path
	}
	switch strings.ToLower(filepath.Ext(rawURL)) {
	case ".m3u8":
		return "application/x-mpegURL", "streaming"
	case ".mpd":
		return "application/dash+xml", "streaming"
	case ".webm":
		return "video/webm", "progressive"
	case ".mov":
		return "video/quicktime", "progressive"
	case ".ts":
		return "video/mp2t", "progressive"
	default:
		return "video/mp4", "progressive"
	}
}

// classifyAdM determines how the bid AdM field should be interpreted.
func classifyAdM(adm string) vastCreativeType {
	if strings.TrimSpace(adm) == "" {
		// Treat whitespace-only AdM (e.g. "adm":" ") the same as an absent one.
		return adMEmpty
	}
	probe := adm
	if len(probe) > 512 {
		probe = probe[:512]
	}
	upper := strings.ToUpper(probe)
	// A valid VAST document always contains a <VAST element. The <?xml prolog alone
	// is not sufficient — any XML document would match it, including error responses.
	if strings.Contains(upper, "<VAST") {
		return adMIsVAST
	}
	if strings.HasPrefix(strings.TrimSpace(adm), "http") {
		return adMIsURI
	}
	return adMIsMedia
}

// validateVASTAdM performs a lightweight structural check on an AdM value that
// has already been classified as adMIsVAST.  Returns a non-nil error when the
// document is malformed so the bid is rejected rather than forwarded to the player.
//
// Checks performed (in order):
//  1. The XML must be parseable — tokens up to (and including) the first <Ad>
//     child are decoded so we confirm the document is not truncated garbage.
//  2. The root element name must be "VAST" (case-insensitive).
//  3. The first <Ad> child must itself contain either <InLine> or <Wrapper>.
//     — Wrapper: accepted immediately after the XML structure check.  The
//       document deliberately contains no self-contained creative; the player
//       chain-fetches the next VAST URL, so we must not reject it.
//     — InLine:  accepted; it carries the actual creative.
//     — Neither: rejected (e.g. <VAST><Error>…</Error></VAST> error docs).
func validateVASTAdM(adm string) error {
	dec := xml.NewDecoder(strings.NewReader(adm))
	dec.Strict = false

	// ── Step 1: confirm root is <VAST> ───────────────────────────────────────
	foundVAST := false
	for {
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("VAST AdM is malformed XML: %w", err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue // skip <?xml …?>, comments, whitespace
		}
		if !strings.EqualFold(se.Name.Local, "VAST") {
			return fmt.Errorf("VAST AdM root element is <%s>, expected <VAST>", se.Name.Local)
		}
		foundVAST = true
		break
	}
	if !foundVAST {
		return fmt.Errorf("VAST AdM contains no root element")
	}

	// ── Step 2: walk to the first <Ad> child ─────────────────────────────────
	// A <VAST> document that contains only <Error> (no <Ad>) is an error-only
	// response and must not be forwarded as if it were a valid creative.
	for {
		tok, err := dec.Token()
		if err != nil {
			// EOF without finding any <Ad> — treat as error-only / empty.
			return fmt.Errorf("VAST AdM contains no <Ad> element")
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		if !strings.EqualFold(se.Name.Local, "Ad") {
			// <Error>, <Extensions>, etc. at VAST level — skip whole subtree.
			if err := dec.Skip(); err != nil {
				return fmt.Errorf("VAST AdM is malformed XML in <%s>: %w", se.Name.Local, err)
			}
			continue
		}
		// Found the first <Ad> element. Now look for <Wrapper> or <InLine>.
		for {
			inner, err := dec.Token()
			if err != nil {
				return fmt.Errorf("VAST AdM <Ad> element is incomplete: %w", err)
			}
			ise, ok := inner.(xml.StartElement)
			if !ok {
				if _, isEnd := inner.(xml.EndElement); isEnd {
					// </Ad> with no recognised child — treat as empty.
					return fmt.Errorf("VAST AdM <Ad> element contains neither <InLine> nor <Wrapper>")
				}
				continue
			}
			switch strings.ToLower(ise.Name.Local) {
			case "wrapper":
				// Wrapper is valid by definition — the player resolves the chain.
				// Do NOT apply any further content checks; return success.
				return nil
			case "inline":
				// InLine carries the actual creative — accepted.
				return nil
			default:
				// Unexpected child element inside <Ad>; skip it and keep looking.
				if err := dec.Skip(); err != nil {
					return fmt.Errorf("VAST AdM <Ad> child <%s> is malformed: %w", ise.Name.Local, err)
				}
			}
		}
	}
}

// buildVASTResponse constructs a VAST 3.0 response from the winning bid.
//
// NURL / BURL semantics (OpenRTB 2.5 §7.2):
//
//	NURL (win notice)     — fired server-side immediately when the bid wins.
//	                        MUST NOT appear in the VAST document; embedding it
//	                        would expose the ${AUCTION_PRICE} macro to the player
//	                        and cause incorrect fire timing.
//	BURL (billing notice) — player-fired on ad render.  Embedded as
//	                        <Impression id="burl"> so all VAST 2.0+ players fire
//	                        it at the same time as other impression pixels.
//	                        Auction macros are resolved before embedding.
//
// Creative-type routing:
//  1. AdM is VAST XML  — inject PBS <Impression> + resolved BURL <Impression> + tracking events
//  2. AdM is a tag URI — build VAST Wrapper with PBS tracker + BURL + tracking events
//  3. AdM is media URL — build VAST InLine with PBS tracking + BURL
//  4. AdM is empty     — NURL NOT pre-fired; wrap NURL as VASTAdTagURI so player fires it once
func (h *VideoPipelineHandler) buildVASTResponse(
	pr *PlayerRequest,
	adsCfg *AdServerConfig,
	win *WinningBid,
	bidder string,
	auctionID string,
) (string, error) {
	// ── NURL — server-side win notice ────────────────────────────────────────
	// Fire NURL server-side ONLY when the bid has a real AdM so the URL is not
	// also wrapped as VASTAdTagURI (which would cause the player to fire it a
	// second time, double-counting the win notice with SSPs that track it).
	nurlForWrap := ""
	if win.NURL != "" {
		resolved := resolveAuctionMacros(win.NURL, win, auctionID, bidder)
		if win.AdM != "" {
			// AdM present — fire immediately, no need to wrap NURL.
			h.fireWinNotice(resolved)
		} else {
			// No AdM — NURL doubles as the creative URI; do NOT fire it here
			// so the player fires it exactly once when it fetches the creative.
			nurlForWrap = resolved
		}
	}

	// ── BURL — billing notice ─────────────────────────────────────────────────
	// Resolve macros once; pass the resolved URL to all VAST builders.
	resolvedBURL := resolveAuctionMacros(win.BURL, win, auctionID, bidder)

	reqBaseURL := adsCfg.RequestBaseURL
	pbsImpURL := h.buildImpressionURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, win.CrID, win.Price, win.ADomain)

	// Build PBS tracking beacons (start/quartile/complete) once; reused across all paths.
	trackingEvents := h.buildTrackingEventList(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, win.CrID, win.Price, win.ADomain)

	switch classifyAdM(win.AdM) {

	case adMIsVAST:
		// Validate structure before touching the document. A malformed or
		// whitespace-only AdM is rejected here so the pipeline falls through to
		// no-fill rather than forwarding broken VAST to the player.
		if err := validateVASTAdM(win.AdM); err != nil {
			log.Printf("buildVASTResponse: rejecting bid %s — %v", win.BidID, err)
			return "", fmt.Errorf("invalid VAST AdM: %w", err)
		}
		// Bid returned a full VAST document — inject impression trackers then
		// inject PBS quartile/complete beacons so VCR is always counted.
		vast := injectVASTImpression(win.AdM, pbsImpURL)
		if resolvedBURL != "" {
			// Inject BURL as a named <Impression id="burl"> so it is
			// distinguishable from PBS and DSP impression pixels.
			vast = injectVASTImpressionWithID(vast, resolvedBURL, "burl")
		}
		vast = injectVASTTracking(vast, trackingEvents)
		return vast, nil

	case adMIsURI:
		// AdM is a bare VAST tag URL → build a Wrapper around it.
		return h.buildVASTWrapperDoc(win, pbsImpURL, win.AdM, resolvedBURL, trackingEvents)

	case adMIsMedia:
		// AdM holds a direct media URL → build an InLine document.
		return h.buildVASTInlineDoc(pr, win, bidder, reqBaseURL, pbsImpURL, win.AdM, resolvedBURL, auctionID, adsCfg)

	default: // adMEmpty
		if nurlForWrap != "" {
			// No AdM. NURL not yet fired — wrap it so the player chain-fetches
			// the downstream creative and fires the win notice in the process
			// (OpenRTB pre-2.0 backwards compatibility).
			return h.buildVASTWrapperDoc(win, pbsImpURL, nurlForWrap, resolvedBURL, trackingEvents)
		}
		return "", fmt.Errorf("bid has no AdM and no NURL — cannot build VAST")
	}
}

// buildVASTWrapperDoc creates a VAST 3.0 Wrapper document.
//
// Impressions emitted:
//
//	<Impression> — PBS server-side tracker (always present)
//	<Impression> — Player-fired billing notification (when resolvedBURL != "")
//
// NURL is intentionally absent — it has already been fired server-side by
// buildVASTResponse before this function is called.
func (h *VideoPipelineHandler) buildVASTWrapperDoc(
	win *WinningBid,
	pbsImpURL string,
	tagURI string,
	resolvedBURL string,
	trackingEvents []vastTracking,
) (string, error) {
	impressions := []vastImpression{
		{Inner: vastCDATA{Text: pbsImpURL}},
	}
	if resolvedBURL != "" {
		// id="burl" marks this pixel as the OpenRTB billing notice URL
		// so it can be distinguished from PBS and DSP impression pixels.
		impressions = append(impressions, vastImpression{
			ID:    "burl",
			Inner: vastCDATA{Text: resolvedBURL},
		})
	}

	var te *vastTrackingEvents
	if len(trackingEvents) > 0 {
		te = &vastTrackingEvents{Tracking: trackingEvents}
	}

	doc := vastRoot{
		Version: "3.0",
		Ad: []vastAd{{
			ID: win.BidID,
			Wrapper: &vastWrapper{
				AdSystem:       "AdZrvr",
				VASTAdTagURI:   vastCDATA{Text: tagURI},
				Impression:     impressions,
				TrackingEvents: te,
			},
		}},
	}
	xmlBytes, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", fmt.Errorf("xml marshal: %w", err)
	}
	return xml.Header + string(xmlBytes), nil
}

// buildVASTInlineDoc creates a VAST 3.0 InLine document with a MediaFile.
//
// Impressions emitted:
//
//	<Impression> — PBS server-side tracker (always present)
//	<Impression> — Player-fired billing notification (when resolvedBURL != "")
//
// NURL is intentionally absent — it has already been fired server-side by
// buildVASTResponse before this function is called.
func (h *VideoPipelineHandler) buildVASTInlineDoc(
	pr *PlayerRequest,
	win *WinningBid,
	bidder string,
	reqBaseURL string,
	pbsImpURL string,
	mediaURL string,
	resolvedBURL string,
	auctionID string,
	adsCfg *AdServerConfig,
) (string, error) {
	impressions := []vastImpression{
		{Inner: vastCDATA{Text: pbsImpURL}},
	}
	if resolvedBURL != "" {
		// id="burl" marks this pixel as the OpenRTB billing notice URL.
		impressions = append(impressions, vastImpression{
			ID:    "burl",
			Inner: vastCDATA{Text: resolvedBURL},
		})
	}

	events := []vastTracking{
		{Event: "start",         Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, "start", win.CrID, win.Price, win.ADomain)}},
		{Event: "firstQuartile", Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, "firstQuartile", win.CrID, win.Price, win.ADomain)}},
		{Event: "midpoint",      Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, "midpoint", win.CrID, win.Price, win.ADomain)}},
		{Event: "thirdQuartile", Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, "thirdQuartile", win.CrID, win.Price, win.ADomain)}},
		{Event: "complete",      Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, "complete", win.CrID, win.Price, win.ADomain)}},
	}

	mimeType, deliveryMode := mediaTypeForURL(mediaURL)
	doc := vastRoot{
		Version: "3.0",
		Ad: []vastAd{{
			ID: win.BidID,
			Inline: &vastInline{
				AdSystem:   "AdZrvr",
				AdTitle:    "Video Ad",
				Impression: impressions,
				Creatives: vastCreatives{
					Creative: []vastCreative{{
						ID: win.CrID,
						Linear: &vastLinear{
							Duration:       formatDuration(adsCfg.MaxDuration),
							TrackingEvents: &vastTrackingEvents{Tracking: events},
							MediaFiles: vastMediaFiles{
								MediaFile: []vastMediaFile{{
									Delivery: deliveryMode,
									Type:     mimeType,
									Width:    int(win.Width),
									Height:   int(win.Height),
									Inner:    vastCDATA{Text: mediaURL},
								}},
							},
						},
					}},
				},
			},
		}},
	}
	xmlBytes, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", fmt.Errorf("xml marshal: %w", err)
	}
	return xml.Header + string(xmlBytes), nil
}

// injectVASTImpression inserts a <Impression> CDATA element into an
// existing VAST XML string at the most appropriate position.
//
// We probe a small set of known capitalizations for each close-tag rather than
// uppercasing the entire document (which would allocate a copy of potentially
// 100 KB+ of VAST XML just to scan it).
//
// Insertion order:
//  1. After the last </Impression> — keeps all impression pixels together.
//  2. Before </InLine> — when no Impression element exists yet.
//  3. Before </Wrapper> — last resort for Wrapper-only documents.
func injectVASTImpression(vast, trackURL string) string {
	return injectVASTImpressionWithID(vast, trackURL, "")
}

// injectVASTImpressionWithID is like injectVASTImpression but also sets
// an optional id attribute on the <Impression> element (e.g. id="burl"),
// which is useful for distinguishing PBS, DSP, and billing pixels server-side.
func injectVASTImpressionWithID(vast, trackURL, id string) string {
	var tag string
	if id != "" {
		tag = `<Impression id="` + id + `"><![CDATA[` + trackURL + `]]></Impression>`
	} else {
		tag = `<Impression><![CDATA[` + trackURL + `]]></Impression>`
	}

	for _, closeImp := range [3]string{"</Impression>", "</IMPRESSION>", "</impression>"} {
		if idx := strings.LastIndex(vast, closeImp); idx != -1 {
			insertAt := idx + len(closeImp)
			return vast[:insertAt] + tag + vast[insertAt:]
		}
	}
	for _, closeInline := range [3]string{"</InLine>", "</Inline>", "</INLINE>"} {
		if idx := strings.Index(vast, closeInline); idx != -1 {
			return vast[:idx] + tag + vast[idx:]
		}
	}
	for _, closeWrapper := range [3]string{"</Wrapper>", "</WRAPPER>", "</wrapper>"} {
		if idx := strings.Index(vast, closeWrapper); idx != -1 {
			return vast[:idx] + tag + vast[idx:]
		}
	}
	return vast
}

// injectVASTTracking injects PBS <Tracking> pixel elements into an existing VAST
// XML document so that our quartile/complete beacons are always recorded even
// when demand returns its own fully-formed VAST XML.
//
// Strategy (in preference order):
//  1. Append after the last </Tracking> — extends an existing <TrackingEvents>.
//  2. Insert a fresh <TrackingEvents> block before </Linear>.
//  3. Insert a fresh <TrackingEvents> block before </Wrapper>.
func injectVASTTracking(vast string, events []vastTracking) string {
	if len(events) == 0 {
		return vast
	}
	var sb strings.Builder
	for _, ev := range events {
		fmt.Fprintf(&sb, `<Tracking event="%s"><![CDATA[%s]]></Tracking>`, ev.Event, ev.Inner.Text)
	}
	block := sb.String()

	// Strategy 1: append after the last existing </Tracking>
	for _, closeTag := range [3]string{"</Tracking>", "</TRACKING>", "</tracking>"} {
		if idx := strings.LastIndex(vast, closeTag); idx != -1 {
			insertAt := idx + len(closeTag)
			return vast[:insertAt] + block + vast[insertAt:]
		}
	}
	wrapped := "<TrackingEvents>" + block + "</TrackingEvents>"
	// Strategy 2: inject before </Linear>
	for _, closeTag := range [3]string{"</Linear>", "</LINEAR>", "</linear>"} {
		if idx := strings.Index(vast, closeTag); idx != -1 {
			return vast[:idx] + wrapped + vast[idx:]
		}
	}
	// Strategy 3: inject before </Wrapper>
	for _, closeTag := range [3]string{"</Wrapper>", "</WRAPPER>", "</wrapper>"} {
		if idx := strings.Index(vast, closeTag); idx != -1 {
			return vast[:idx] + wrapped + vast[idx:]
		}
	}
	return vast
}

// buildTrackingEventList returns PBS start/quartile/complete tracking events as
// a []vastTracking slice ready to embed in any VAST document type.
func (h *VideoPipelineHandler) buildTrackingEventList(reqBaseURL, auctionID, bidID, bidder, placementID, crid string, price float64, adom []string) []vastTracking {
	evNames := []string{"start", "firstQuartile", "midpoint", "thirdQuartile", "complete"}
	out := make([]vastTracking, len(evNames))
	for i, ev := range evNames {
		out[i] = vastTracking{
			Event: ev,
			Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, bidID, bidder, placementID, ev, crid, price, adom)},
		}
	}
	return out
}

// requestBaseURL derives the scheme+host from an inbound request.
// It prefers X-Forwarded-Proto for reverse-proxy deployments.
// Falls back to "http" when TLS is not in use.
func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	} else if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = p
	}
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}
	return scheme + "://" + host
}

// buildImpressionURL constructs the PBS /video/impression beacon URL that is
// injected into the VAST <Impression> tag. When the player fires this URL the
// dedicated ImpressionEndpoint records the impression directly.
func (h *VideoPipelineHandler) buildImpressionURL(reqBaseURL, auctionID, bidID, bidder, placementID, crid string, price float64, adom []string) string {
	base := reqBaseURL
	if base == "" {
		base = h.externalURL
	}
	q := url.Values{}
	q.Set("auction_id", auctionID)
	q.Set("bid_id", bidID)
	q.Set("bidder", bidder)
	q.Set("placement_id", placementID)
	if crid != "" {
		q.Set("crid", crid)
	}
	if price > 0 {
		q.Set("price", strconv.FormatFloat(price, 'f', -1, 64))
	}
	if len(adom) > 0 {
		q.Set("adom", strings.Join(adom, ","))
	}
	return base + "/video/impression?" + q.Encode()
}

// buildTrackingURL constructs the PBS /video/tracking beacon URL.
// reqBaseURL, when non-empty, overrides the configured external_url so that
// the tracking pixel always refers back to the same host that served the VAST
// (e.g. the player's custom domain). All parameter values are URL-encoded.
func (h *VideoPipelineHandler) buildTrackingURL(reqBaseURL, auctionID, bidID, bidder, placementID, event, crid string, price float64, adom []string) string {
	base := reqBaseURL
	if base == "" {
		base = h.externalURL
	}
	q := url.Values{}
	q.Set("auction_id", auctionID)
	q.Set("bid_id", bidID)
	q.Set("bidder", bidder)
	q.Set("placement_id", placementID)
	q.Set("event", event)
	if crid != "" {
		q.Set("crid", crid)
	}
	if price > 0 {
		q.Set("price", strconv.FormatFloat(price, 'f', -1, 64))
	}
	if len(adom) > 0 {
		q.Set("adom", strings.Join(adom, ","))
	}
	return base + "/video/tracking?" + q.Encode()
}

// iso2ToISO3 converts an ISO 3166-1 alpha-2 country code (e.g. "US") to the
// corresponding alpha-3 code (e.g. "USA") required by OpenRTB 2.5 geo.country.
// If the code is already 3 characters or is unrecognised the input is returned
// unchanged so we never silently drop a value.
func iso2ToISO3(alpha2 string) string {
	// Complete UN M.49 / ISO 3166-1 mapping (249 entries).
	const mapping = `` +
		"AD:AND,AE:ARE,AF:AFG,AG:ATG,AI:AIA,AL:ALB,AM:ARM,AO:AGO,AQ:ATA,AR:ARG," +
		"AS:ASM,AT:AUT,AU:AUS,AW:ABW,AX:ALA,AZ:AZE," +
		"BA:BIH,BB:BRB,BD:BGD,BE:BEL,BF:BFA,BG:BGR,BH:BHR,BI:BDI,BJ:BEN,BL:BLM," +
		"BM:BMU,BN:BRN,BO:BOL,BQ:BES,BR:BRA,BS:BHS,BT:BTN,BV:BVT,BW:BWA,BY:BLR,BZ:BLZ," +
		"CA:CAN,CC:CCK,CD:COD,CF:CAF,CG:COG,CH:CHE,CI:CIV,CK:COK,CL:CHL,CM:CMR," +
		"CN:CHN,CO:COL,CR:CRI,CU:CUB,CV:CPV,CW:CUW,CX:CXR,CY:CYP,CZ:CZE," +
		"DE:DEU,DJ:DJI,DK:DNK,DM:DMA,DO:DOM,DZ:DZA," +
		"EC:ECU,EE:EST,EG:EGY,EH:ESH,ER:ERI,ES:ESP,ET:ETH," +
		"FI:FIN,FJ:FJI,FK:FLK,FM:FSM,FO:FRO,FR:FRA," +
		"GA:GAB,GB:GBR,GD:GRD,GE:GEO,GF:GUF,GG:GGY,GH:GHA,GI:GIB,GL:GRL,GM:GMB," +
		"GN:GIN,GP:GLP,GQ:GNQ,GR:GRC,GS:SGS,GT:GTM,GU:GUM,GW:GNB,GY:GUY," +
		"HK:HKG,HM:HMD,HN:HND,HR:HRV,HT:HTI,HU:HUN," +
		"ID:IDN,IE:IRL,IL:ISR,IM:IMN,IN:IND,IO:IOT,IQ:IRQ,IR:IRN,IS:ISL,IT:ITA," +
		"JE:JEY,JM:JAM,JO:JOR,JP:JPN," +
		"KE:KEN,KG:KGZ,KH:KHM,KI:KIR,KM:COM,KN:KNA,KP:PRK,KR:KOR,KW:KWT,KY:CYM,KZ:KAZ," +
		"LA:LAO,LB:LBN,LC:LCA,LI:LIE,LK:LKA,LR:LBR,LS:LSO,LT:LTU,LU:LUX,LV:LVA,LY:LBY," +
		"MA:MAR,MC:MCO,MD:MDA,ME:MNE,MF:MAF,MG:MDG,MH:MHL,MK:MKD,ML:MLI,MM:MMR," +
		"MN:MNG,MO:MAC,MP:MNP,MQ:MTQ,MR:MRT,MS:MSR,MT:MLT,MU:MUS,MV:MDV,MW:MWI," +
		"MX:MEX,MY:MYS,MZ:MOZ," +
		"NA:NAM,NC:NCL,NE:NER,NF:NFK,NG:NGA,NI:NIC,NL:NLD,NO:NOR,NP:NPL,NR:NRU,NU:NIU,NZ:NZL," +
		"OM:OMN," +
		"PA:PAN,PE:PER,PF:PYF,PG:PNG,PH:PHL,PK:PAK,PL:POL,PM:SPM,PN:PCN,PR:PRI," +
		"PS:PSE,PT:PRT,PW:PLW,PY:PRY," +
		"QA:QAT," +
		"RE:REU,RO:ROU,RS:SRB,RU:RUS,RW:RWA," +
		"SA:SAU,SB:SLB,SC:SYC,SD:SDN,SE:SWE,SG:SGP,SH:SHN,SI:SVN,SJ:SJM,SK:SVK," +
		"SL:SLE,SM:SMR,SN:SEN,SO:SOM,SR:SUR,SS:SSD,ST:STP,SV:SLV,SX:SXM,SY:SYR,SZ:SWZ," +
		"TC:TCA,TD:TCD,TF:ATF,TG:TGO,TH:THA,TJ:TJK,TK:TKL,TL:TLS,TM:TKM,TN:TUN,TO:TON," +
		"TR:TUR,TT:TTO,TV:TUV,TW:TWN,TZ:TZA," +
		"UA:UKR,UG:UGA,UM:UMI,US:USA,UY:URY,UZ:UZB," +
		"VA:VAT,VC:VCT,VE:VEN,VG:VGB,VI:VIR,VN:VNM,VU:VUT," +
		"WF:WLF,WS:WSM," +
		"YE:YEM,YT:MYT," +
		"ZA:ZAF,ZM:ZMB,ZW:ZWE"
	if len(alpha2) == 3 {
		return alpha2 // already alpha-3
	}
	key := strings.ToUpper(alpha2) + ":"
	if idx := strings.Index(mapping, key); idx != -1 {
		return mapping[idx+3 : idx+6]
	}
	return alpha2 // unknown — pass through unchanged
}

// prodQPtr returns the adcom1.ProductionQuality pointer for use in
// openrtb2.Content.ProdQ.  When the caller passes 0 (unknown/unset) we default
// to ProductionProfessional (1) which signals premium/broadcast content and
// attracts higher CPMs from brand-safe buyers.
func prodQPtr(v int) *adcom1.ProductionQuality {
	var pq adcom1.ProductionQuality
	if v > 0 {
		pq = adcom1.ProductionQuality(v)
	} else {
		pq = adcom1.ProductionProfessional
	}
	return &pq
}

// videoAdPosition returns the IAB slot position for the given placement type.
// Used to populate imp.video.pos in the OpenRTB bid request.
//
//	"outstream" → 1 (Above The Fold — in-feed/in-content)
//	 all others → 7 (Full Screen — instream CTV pre/mid/post-roll)
func videoAdPosition(placementType string) *adcom1.PlacementPosition {
	switch placementType {
	case "outstream":
		return adcom1.PositionAboveFold.Ptr()
	default: // instream, interstitial, rewarded, CTV
		return adcom1.PositionFullScreen.Ptr()
	}
}

// videoPlacementSubtype maps the ad unit placement string to the OpenRTB 2.5
// adcom1.VideoPlacementSubtype sent in imp.video.placement.
//
//	"instream"     → 1 (In-Stream   — pre/mid/post-roll alongside streaming content)
//	"outstream"    → 4 (In-Feed     — standalone unit in content/social feeds)
//	"interstitial" → 5 (Interstitial/Slider/Floating — full-screen overlay)
//	"rewarded"     → 5 (treated as full-screen interstitial on the buy side)
//	""  / unknown  → 1 (default: In-Stream)
func videoPlacementSubtype(placementType string) adcom1.VideoPlacementSubtype {
	switch placementType {
	case "outstream":
		return adcom1.VideoPlacementInFeed
	case "interstitial", "rewarded":
		return adcom1.VideoPlacementAlwaysVisible
	default: // "instream" or empty
		return adcom1.VideoPlacementInStream
	}
}

// formatDuration converts seconds to VAST HH:MM:SS format.
func formatDuration(seconds int) string {
	h := seconds / 3600
	m := (seconds % 3600) / 60
	s := seconds % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

// ─────────────────────────────────────────────────────────────────────────────
// NURL / BURL helpers — auction macro resolution and win-notice firing
// ─────────────────────────────────────────────────────────────────────────────

// resolveAuctionMacros substitutes standard OpenRTB 2.5 §5.1 auction macros
// in rawURL (typically NURL or BURL) with their runtime values.
//
// Supported macros:
//
//	${AUCTION_ID}       — top-level bid-request ID
//	${AUCTION_BID_ID}   — bid.id of the winning bid
//	${AUCTION_IMP_ID}   — imp.id (first impression; "1" by convention)
//	${AUCTION_SEAT_ID}  — bidder / seat identifier
//	${AUCTION_AD_ID}    — creative ID (bid.adid)
//	${AUCTION_PRICE}    — clearing price, URL-encoded
//	${AUCTION_CURRENCY} — bid currency (USD)
//	${AUCTION_LOSS}     — loss reason code (empty for winning bids)
//
// Returns rawURL unchanged when it contains no "${" prefix (fast path).
func resolveAuctionMacros(rawURL string, win *WinningBid, auctionID, bidder string) string {
	if rawURL == "" || !strings.Contains(rawURL, "${") {
		return rawURL
	}
	priceStr := strconv.FormatFloat(win.Price, 'f', -1, 64)
	r := strings.NewReplacer(
		"${AUCTION_ID}", url.QueryEscape(auctionID),
		"${AUCTION_BID_ID}", url.QueryEscape(win.BidID),
		"${AUCTION_IMP_ID}", "1",
		"${AUCTION_SEAT_ID}", url.QueryEscape(bidder),
		"${AUCTION_AD_ID}", url.QueryEscape(win.CrID),
		"${AUCTION_PRICE}", url.QueryEscape(priceStr),
		"${AUCTION_CURRENCY}", "USD",
		"${AUCTION_LOSS}", "",
	)
	return r.Replace(rawURL)
}

// fireWinNotice fires the NURL win notification asynchronously via HTTP GET.
//
// Per OpenRTB 2.5 §7.2, NURL must be called by the exchange (us) when the bid
// wins — not by the player.  Errors are silently discarded; best-effort only.
func (h *VideoPipelineHandler) fireWinNotice(nurl string) {
	if nurl == "" {
		return
	}
	client := h.demandClient
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, nurl, nil)
		if err != nil {
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			return
		}
		resp.Body.Close()
	}()
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 6 — writeVASTResponse
// ─────────────────────────────────────────────────────────────────────────────

// recordBidReport saves a BidReportEntry to the persistent bid report store
// whenever a real demand fill occurs.  It is fire-and-forget (best effort).
func (h *VideoPipelineHandler) recordBidReport(cfg *AdServerConfig, pr *PlayerRequest, resp *DemandResponse, eventType string) {
	if h.bidReport == nil {
		return
	}
	entry := &BidReportEntry{
		RequestID:   resp.AuctionID,
		ImpID:       pr.PlacementID,
		PublisherID: cfg.PublisherID,
		AdUnitID:    pr.PlacementID,
		Bidder:      resp.Bidder,
		ADomain:     resp.ADomain,
		CrID:        resp.CrID,
		CampaignID:  cfg.CampaignID,
		DealID:      resp.DealID,
		BURL:        resp.BURL,
		Price:       resp.WinPrice,
		Currency:    "USD",
		EventType:   eventType,
		EventTime:   time.Now().UTC(),
		AppBundle:   pr.AppBundle,
		Domain:      pr.Domain,
		CountryCode: pr.CountryCode,
	}
	go func() {
		_ = h.bidReport.store.create(entry)
	}()
}

// writeVASTResponse serialises the VAST document to the HTTP response writer.
// It uses a pooled buffer so that the []byte conversion of vastXML is reused
// across requests instead of allocating a fresh slice each time.
func (h *VideoPipelineHandler) writeVASTResponse(w http.ResponseWriter, vastXML string) {
	buf := h.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteString(vastXML)
	hdr := w.Header()
	hdr.Set("Content-Type", "application/xml; charset=utf-8")
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(http.StatusOK)
	w.Write(buf.Bytes()) //nolint:errcheck
	h.bufPool.Put(buf)
}

// emptyVAST is a minimal well-formed VAST 3.0 document with no Ad elements.
// Video players that cannot handle HTTP 204 (e.g. Roku, Fire TV, Samsung) use
// this to gracefully skip the ad slot without throwing an error.
const emptyVAST = `<?xml version="1.0" encoding="UTF-8"?><VAST version="3.0"/>`

// ─────────────────────────────────────────────────────────────────────────────
// Stage 7 — TrackingEndpoint
// ─────────────────────────────────────────────────────────────────────────────

// TrackingEndpoint handles playback + tracking beacons fired by the player.
// Supported events: impression, start, firstQuartile, midpoint, thirdQuartile,
// complete, click.
//
// Route: GET /video/tracking
func (h *VideoPipelineHandler) TrackingEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		q := r.URL.Query()

		priceVal := 0.0
		if ps := q.Get("price"); ps != "" {
			priceVal, _ = strconv.ParseFloat(ps, 64)
		}
		ev := TrackingEvent{
			AuctionID:   q.Get("auction_id"),
			BidID:       q.Get("bid_id"),
			Bidder:      q.Get("bidder"),
			PlacementID: q.Get("placement_id"),
			Event:       EventType(q.Get("event")),
			CrID:        q.Get("crid"),
			Price:       priceVal,
			ADomain:     q.Get("adom"),
			ReceivedAt:  time.Now(),
		}

		if ev.AuctionID == "" || ev.BidID == "" || ev.Event == "" {
			http.Error(w, "auction_id, bid_id, and event are required", http.StatusBadRequest)
			return
		}

		h.tracking.record(ev)

		// Count player-confirmed completes (100% viewed).
		if ev.Event == EventComplete {
			if cfg, err2 := h.resolveAdServerConfig(ev.PlacementID); err2 == nil {
				h.videoStats.incComplete(cfg.PublisherID)
			}
		}

		// Respond with a 1×1 transparent GIF so players treating this as
		// an image pixel don't fail.
		w.Header().Set("Content-Type", "image/gif")
		w.WriteHeader(http.StatusOK)
		// Minimal 1×1 transparent GIF bytes
		w.Write([]byte{
			0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00,
			0x01, 0x00, 0x80, 0x00, 0x00, 0xff, 0xff, 0xff,
			0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00,
			0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44,
			0x01, 0x00, 0x3b,
		}) //nolint:errcheck
	}
}

// ImpressionEndpoint is the dedicated handler for the VAST <Impression> beacon.
// The VAST <Impression> tag points here (not to /video/tracking) so that
// impression counting is cleanly separated from generic playback events.
//
// Route: GET /video/impression
func (h *VideoPipelineHandler) ImpressionEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		q := r.URL.Query()
		placementID := q.Get("placement_id")
		auctionID := q.Get("auction_id")
		bidID := q.Get("bid_id")
		if auctionID == "" || bidID == "" {
			http.Error(w, "auction_id and bid_id are required", http.StatusBadRequest)
			return
		}

		// Record a TrackingEvent so the events log stays complete.
		priceVal := 0.0
		if ps := q.Get("price"); ps != "" {
			priceVal, _ = strconv.ParseFloat(ps, 64)
		}
		h.tracking.record(TrackingEvent{
			AuctionID:   auctionID,
			BidID:       bidID,
			Bidder:      q.Get("bidder"),
			PlacementID: placementID,
			Event:       EventImpression,
			CrID:        q.Get("crid"),
			Price:       priceVal,
			ADomain:     q.Get("adom"),
			ReceivedAt:  time.Now(),
		})

		h.metricsEng.RecordImps(metrics.ImpLabels{VideoImps: true})
		if placementID != "" {
			if cfg, err := h.resolveAdServerConfig(placementID); err == nil {
				h.videoStats.incImpression(cfg.PublisherID)
			}
		}

		// Respond with a 1×1 transparent GIF.
		w.Header().Set("Content-Type", "image/gif")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte{
			0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00,
			0x01, 0x00, 0x80, 0x00, 0x00, 0xff, 0xff, 0xff,
			0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00,
			0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44,
			0x01, 0x00, 0x3b,
		}) //nolint:errcheck
	}
}

// TrackingEventsEndpoint exposes recorded tracking events as JSON (useful for
// the dashboard / debugging).
//
// Route: GET /video/tracking/events
func (h *VideoPipelineHandler) TrackingEventsEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		events := h.tracking.all()
		data, _ := json.Marshal(events)
		hdr := w.Header()
		hdr.Set("Content-Type", "application/json")
		hdr.Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data) //nolint:errcheck
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Video Statistics endpoint
// ─────────────────────────────────────────────────────────────────────────────

// VideoStatsEndpoint exposes per-publisher ad request / opportunity / impression
// / revenue counters accumulated since the server last started.
//
// Route: GET /dashboard/stats/video
func (h *VideoPipelineHandler) VideoStatsEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		payload := h.videoStats.snapshot()
		data, _ := json.Marshal(payload)
		hdr := w.Header()
		hdr.Set("Content-Type", "application/json")
		hdr.Set("Cache-Control", "no-cache, no-store, must-revalidate")
		hdr.Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data) //nolint:errcheck
	}
}

// Route: POST /dashboard/stats/reset
func (h *VideoPipelineHandler) ResetStatsEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h.videoStats.reset()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`)) //nolint:errcheck
	}
}

// dashboardConfigResponse is the JSON shape returned by GET /dashboard/config.
// It exposes the live server settings that the Exchange Optimisation checklist
// reads to colour-code its status badges.
type dashboardConfigResponse struct {
	AuctionTimeouts struct {
		Default int `json:"default"`
		Max     int `json:"max"`
	} `json:"auction_timeouts_ms"`
	HTTPClient struct {
		MaxIdleConns        int `json:"max_idle_connections"`
		MaxIdleConnsPerHost int `json:"max_idle_connections_per_host"`
		IdleConnTimeout     int `json:"idle_connection_timeout_seconds"`
	} `json:"http_client"`
	Compression struct {
		Response struct {
			EnableGzip bool `json:"enable_gzip"`
		} `json:"response"`
		Request struct {
			EnableGzip bool `json:"enable_gzip"`
		} `json:"request"`
	} `json:"compression"`
}

// DashboardConfigEndpoint returns the live server configuration that the
// dashboard Exchange Optimisation checklist needs to colour its badges.
//
// Route: GET /dashboard/config
func (h *VideoPipelineHandler) DashboardConfigEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		var resp dashboardConfigResponse
		if h.cfg != nil {
			resp.AuctionTimeouts.Default = int(h.cfg.AuctionTimeouts.Default)
			resp.AuctionTimeouts.Max = int(h.cfg.AuctionTimeouts.Max)
			resp.HTTPClient.MaxIdleConns = h.cfg.Client.MaxIdleConns
			resp.HTTPClient.MaxIdleConnsPerHost = h.cfg.Client.MaxIdleConnsPerHost
			resp.HTTPClient.IdleConnTimeout = h.cfg.Client.IdleConnTimeout
			resp.Compression.Response.EnableGzip = h.cfg.Compression.Response.GZIP
			resp.Compression.Request.EnableGzip = h.cfg.Compression.Request.GZIP
		}
		data, _ := json.Marshal(resp)
		hdr := w.Header()
		hdr.Set("Content-Type", "application/json")
		hdr.Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		w.Write(data) //nolint:errcheck
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Ad Server Config CRUD endpoints (used by the dashboard)
// ─────────────────────────────────────────────────────────────────────────────

// AdServerConfigEndpoint exposes GET (list) and POST (upsert) for ad server configs.
//
// Route: GET  /video/adserver
//        POST /video/adserver
func (h *VideoPipelineHandler) AdServerConfigEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		switch r.Method {

		case http.MethodGet:
			h.configStore.mu.RLock()
			configs := make([]AdServerConfig, 0, len(h.configStore.configs))
			for _, c := range h.configStore.configs {
				configs = append(configs, *c) // deep copy — prevent data race on pointer
			}
			h.configStore.mu.RUnlock()

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(configs) //nolint:errcheck

		case http.MethodPost:
			var cfg AdServerConfig
			limited := &io.LimitedReader{R: r.Body, N: 65536}
			if err := json.NewDecoder(limited).Decode(&cfg); err != nil {
				http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
				return
			}
			if cfg.PlacementID == "" {
				http.Error(w, "placement_id is required", http.StatusBadRequest)
				return
			}
			h.configStore.set(&cfg)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(cfg) //nolint:errcheck

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────


