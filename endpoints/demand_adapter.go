// Package endpoints — demand_adapter.go
//
// DemandAdapter abstracts the four demand routing schemas supported by the
// ad server.  Each schema is identified by the (InboundProtocol, DemandType)
// pair and encapsulates all HTTP I/O and response transformation required to
// deliver a bid to the calling player.
//
//   Schema         Inbound     Demand      Description
//   ─────────────────────────────────────────────────────────────────────────
//   VAST → VAST    /video/vast VAST URL    Fetch upstream VAST tag, return XML
//   VAST → ORTB    /video/vast ORTB URL    POST OpenRTB, convert bid → VAST
//   ORTB → ORTB    /video/ortb ORTB URL    POST OpenRTB, proxy BidResponse
//   ORTB → VAST    /video/ortb VAST URL    Fetch upstream VAST, wrap as BidResponse
//
// Two Prebid fallback adapters are also provided for placements that have no
// Campaign demand endpoint configured.

package endpoints

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/prebid/openrtb/v20/openrtb2"
)

// ─────────────────────────────────────────────────────────────────────────────
// Core types
// ─────────────────────────────────────────────────────────────────────────────

// InboundProtocol identifies how the player / publisher contacted the ad server.
type InboundProtocol uint8

const (
	InboundVAST InboundProtocol = iota // player called GET/POST /video/vast
	InboundORTB                         // player called GET/POST /video/ortb
)

// DemandType identifies the protocol of the Campaign's demand endpoint.
type DemandType uint8

const (
	DemandTypePrebid DemandType = iota // no campaign — run Prebid header-bidding auction
	DemandTypeVAST                      // campaign has a third-party VAST tag URL
	DemandTypeORTB                      // campaign has a third-party OpenRTB endpoint
)

// RouterKey pairs an inbound protocol with a demand type to select an adapter.
type RouterKey struct {
	Inbound InboundProtocol
	Demand  DemandType
}

// DemandResponse is the unified result of executing a DemandAdapter.
// Exactly one field is populated depending on the inbound protocol:
//   - VASTXml is set for InboundVAST responses.
//   - BidResp is set for InboundORTB responses.
//   - WinPrice holds the winning bid CPM (0 when unavailable, e.g. VAST-tag demand).
//   - NoFill is true when the adapter returned a passthrough wrapper
//     (upstream fetch failed); the response should be served to the player but
//     must NOT be counted as a monetised opportunity.
type DemandResponse struct {
	VASTXml    string
	BidResp    *openrtb2.BidResponse
	WinPrice   float64
	NoFill     bool     // true = wrapper fallback, not a real demand fill
	// Win details — populated when a real bid wins (NoFill=false).
	Bidder     string
	CrID       string
	ADomain    []string // advertiser domains
	AuctionID  string
	BURL       string
	DealID     string
}

// DemandAdapter is the interface every routing adapter must satisfy.
type DemandAdapter interface {
	// Execute selects and contacts the demand endpoint, then transforms the
	// response into the format expected by the inbound player protocol.
	Execute(ctx context.Context, pr *PlayerRequest, cfg *AdServerConfig) (*DemandResponse, error)
}

// ─────────────────────────────────────────────────────────────────────────────
// Router & factory
// ─────────────────────────────────────────────────────────────────────────────

// resolveDemandType inspects cfg to determine which demand protocol to use.
// Priority: DemandVASTURL > DemandOrtbURL > Prebid.
func resolveDemandType(cfg *AdServerConfig) DemandType {
	if cfg.DemandVASTURL != "" {
		return DemandTypeVAST
	}
	if cfg.DemandOrtbURL != "" {
		return DemandTypeORTB
	}
	return DemandTypePrebid
}

// adapterRouter returns the correct DemandAdapter for the given RouterKey.
// Unknown combinations fall back to the Prebid VAST adapter.
func (h *VideoPipelineHandler) adapterRouter(key RouterKey) DemandAdapter {
	switch key {
	case RouterKey{InboundVAST, DemandTypeVAST}:
		return &vastToVASTAdapter{h: h}
	case RouterKey{InboundVAST, DemandTypeORTB}:
		return &vastToORTBAdapter{h: h}
	case RouterKey{InboundVAST, DemandTypePrebid}:
		return &vastPrebidAdapter{h: h}
	case RouterKey{InboundORTB, DemandTypeORTB}:
		return &ortbToORTBAdapter{h: h}
	case RouterKey{InboundORTB, DemandTypeVAST}:
		return &ortbToVASTAdapter{h: h}
	case RouterKey{InboundORTB, DemandTypePrebid}:
		return &ortbPrebidAdapter{h: h}
	default:
		return &vastPrebidAdapter{h: h}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema 1 — VAST → VAST
// ─────────────────────────────────────────────────────────────────────────────

// vastToVASTAdapter routes a VAST inbound request to a VAST demand tag.
//
// Flow:
//  1. Resolve all macros in the demand VAST URL from the PlayerRequest.
//  2. Eagerly fetch the demand VAST tag via HTTP GET.
//  3. Validate the response is well-formed XML and return it directly —
//     the player renders the ad without a second fetch hop.
//
// Fallback: if the demand tag is unreachable or returns invalid XML, the
// adapter returns a VAST Wrapper document so the player can chain-fetch.
type vastToVASTAdapter struct{ h *VideoPipelineHandler }

func (a *vastToVASTAdapter) Execute(
	ctx context.Context,
	pr *PlayerRequest,
	cfg *AdServerConfig,
) (*DemandResponse, error) {
	resolvedURL := substituteMacros(cfg.DemandVASTURL, pr, cfg)

	vastXML, err := fetchVAST(ctx, a.h.demandClient, resolvedURL, pr.UA)
	if err != nil {
		// Fallback: return a VAST Wrapper — player chain-fetches the demand tag.
		// NoFill=true so the endpoint does NOT count this as a monetised opportunity.
		return &DemandResponse{VASTXml: wrapInVAST(resolvedURL), NoFill: true}, nil
	}
	return &DemandResponse{VASTXml: vastXML}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema 2 — VAST → ORTB
// ─────────────────────────────────────────────────────────────────────────────

// vastToORTBAdapter routes a VAST inbound request to an OpenRTB demand endpoint.
//
// Flow:
//  1. Build an enriched OpenRTB 2.5 BidRequest from the PlayerRequest.
//  2. POST the BidRequest to the campaign's ortb_endpoint_url.
//  3. Extract the highest-priced winning bid.
//  4. Build a VAST InLine (or Wrapper) document from the bid AdM / NURL.
//  5. Return the VAST XML to the player.
type vastToORTBAdapter struct{ h *VideoPipelineHandler }

func (a *vastToORTBAdapter) Execute(
	ctx context.Context,
	pr *PlayerRequest,
	cfg *AdServerConfig,
) (*DemandResponse, error) {
	bidResp, err := a.h.postToDemandORTB(ctx, pr, cfg)
	if err != nil {
		return nil, err
	}
	win, bidder, err := a.h.extractWinningBid(bidResp, cfg)
	if err != nil {
		return nil, err
	}
	vastXML, err := a.h.buildVASTResponse(pr, cfg, win, bidder, bidResp.ID)
	if err != nil {
		return nil, err
	}
	return &DemandResponse{VASTXml: vastXML, WinPrice: win.Price, Bidder: bidder, CrID: win.CrID, ADomain: win.ADomain, AuctionID: bidResp.ID, BURL: win.BURL, DealID: win.DealID}, nil
}
// ─────────────────────────────────────────────────────────────────────────────

// ortbToORTBAdapter routes an OpenRTB inbound request to an OpenRTB demand
// endpoint and proxies the raw BidResponse back to the player.
//
// Flow:
//  1. Build an enriched OpenRTB 2.5 BidRequest from the PlayerRequest.
//  2. POST the BidRequest to the campaign's ortb_endpoint_url.
//  3. Return the BidResponse JSON to the player.
type ortbToORTBAdapter struct{ h *VideoPipelineHandler }

func (a *ortbToORTBAdapter) Execute(
	ctx context.Context,
	pr *PlayerRequest,
	cfg *AdServerConfig,
) (*DemandResponse, error) {
	bidResp, err := a.h.postToDemandORTB(ctx, pr, cfg)
	if err != nil {
		return nil, err
	}
	win, bidder, _ := a.h.extractWinningBid(bidResp, cfg)
	var winPrice float64
	var adom []string
	var crid, burl, dealID string
	if win != nil {
		winPrice = win.Price
		adom = win.ADomain
		crid = win.CrID
		burl = win.BURL
		dealID = win.DealID
	}
	return &DemandResponse{BidResp: bidResp, WinPrice: winPrice, Bidder: bidder, CrID: crid, ADomain: adom, AuctionID: bidResp.ID, BURL: burl, DealID: dealID}, nil
}
// ─────────────────────────────────────────────────────────────────────────────

// ortbToVASTAdapter routes an OpenRTB inbound request to a VAST demand tag.
// It fetches the macro-resolved demand VAST URL and wraps the response as an
// OpenRTB 2.5 BidResponse so that OpenRTB-native players can process it.
//
// Flow:
//  1. Resolve all macros in the demand VAST URL from the PlayerRequest.
//  2. Eagerly fetch the demand VAST tag via HTTP GET.
//  3. Embed the VAST XML as bid[0].adm inside a synthetic BidResponse.
//  4. Return the BidResponse to the player.
type ortbToVASTAdapter struct{ h *VideoPipelineHandler }

func (a *ortbToVASTAdapter) Execute(
	ctx context.Context,
	pr *PlayerRequest,
	cfg *AdServerConfig,
) (*DemandResponse, error) {
	resolvedURL := substituteMacros(cfg.DemandVASTURL, pr, cfg)
	vastXML, err := fetchVAST(ctx, a.h.demandClient, resolvedURL, pr.UA)
	if err != nil {
		return nil, fmt.Errorf("fetch demand VAST: %w", err)
	}
	bidResp := vastXMLToBidResponse(vastXML, cfg)
	return &DemandResponse{BidResp: bidResp, WinPrice: cfg.FloorCPM}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Prebid fallback — VAST inbound (no Campaign)
// ─────────────────────────────────────────────────────────────────────────────

// vastPrebidAdapter runs the Prebid header-bidding auction when no Campaign
// demand endpoint is configured, and returns a VAST InLine response.
type vastPrebidAdapter struct{ h *VideoPipelineHandler }

func (a *vastPrebidAdapter) Execute(
	ctx context.Context,
	pr *PlayerRequest,
	cfg *AdServerConfig,
) (*DemandResponse, error) {
	bidResp, err := a.h.forwardToExchange(ctx, pr, cfg)
	if err != nil {
		return nil, err
	}
	win, bidder, err := a.h.extractWinningBid(bidResp, cfg)
	if err != nil {
		return nil, err
	}
	vastXML, err := a.h.buildVASTResponse(pr, cfg, win, bidder, bidResp.ID)
	if err != nil {
		return nil, err
	}
	return &DemandResponse{VASTXml: vastXML, WinPrice: win.Price, Bidder: bidder, CrID: win.CrID, ADomain: win.ADomain, AuctionID: bidResp.ID, BURL: win.BURL, DealID: win.DealID}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Prebid fallback — ORTB inbound (no Campaign)
// ─────────────────────────────────────────────────────────────────────────────

// ortbPrebidAdapter runs the Prebid header-bidding auction when no Campaign
// demand endpoint is configured, and returns an OpenRTB 2.5 BidResponse.
type ortbPrebidAdapter struct{ h *VideoPipelineHandler }

func (a *ortbPrebidAdapter) Execute(
	ctx context.Context,
	pr *PlayerRequest,
	cfg *AdServerConfig,
) (*DemandResponse, error) {
	bidResp, err := a.h.forwardToExchange(ctx, pr, cfg)
	if err != nil {
		return nil, err
	}
	win, bidder, _ := a.h.extractWinningBid(bidResp, cfg)
	var winPrice float64
	var adom []string
	var crid, burl, dealID string
	if win != nil {
		winPrice = win.Price
		adom = win.ADomain
		crid = win.CrID
		burl = win.BURL
		dealID = win.DealID
	}
	return &DemandResponse{BidResp: bidResp, WinPrice: winPrice, Bidder: bidder, CrID: crid, ADomain: adom, AuctionID: bidResp.ID, BURL: burl, DealID: dealID}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

// fetchVAST performs an HTTP GET on vastURL, reads up to 1 MB of the response
// body, and validates that it is well-formed XML before returning the string.
// It is used by vastToVASTAdapter and ortbToVASTAdapter.
// ua is forwarded as the User-Agent header (pass empty string to omit).
func fetchVAST(ctx context.Context, client *http.Client, vastURL string, ua string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, vastURL, nil)
	if err != nil {
		return "", fmt.Errorf("build VAST request: %w", err)
	}
	if ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("GET demand VAST: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("demand VAST returned HTTP %d", resp.StatusCode)
	}

	body := io.Reader(resp.Body)
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gr, gerr := gzip.NewReader(resp.Body)
		if gerr != nil {
			return "", fmt.Errorf("gzip reader: %w", gerr)
		}
		defer gr.Close()
		body = gr
	}

	limited := &io.LimitedReader{R: body, N: 1 << 20} // 1 MB cap
	data, err := io.ReadAll(limited)
	if err != nil {
		return "", fmt.Errorf("read demand VAST body: %w", err)
	}

	// XML validation: attempt to read the root token.
	if _, err := xml.NewDecoder(bytes.NewReader(data)).Token(); err != nil {
		return "", fmt.Errorf("demand response is not valid XML: %w", err)
	}
	// VAST-specific check: root element must be <VAST (not a generic XML error doc).
	probe := data
	if len(probe) > 512 {
		probe = probe[:512]
	}
	if !strings.Contains(strings.ToUpper(string(probe)), "<VAST") {
		return "", fmt.Errorf("demand response is valid XML but not a VAST document")
	}
	return string(data), nil
}

// wrapInVAST returns a minimal VAST 3.0 Wrapper whose VASTAdTagURI points at
// tagURI.  Used as fallback when eager VAST fetch fails in vastToVASTAdapter.
func wrapInVAST(tagURI string) string {
	return xml.Header + `<VAST version="3.0">` + "\n" +
		`  <Ad id="direct-demand"><Wrapper><AdSystem>AdZrvr</AdSystem>` +
		`<VASTAdTagURI><![CDATA[` + tagURI + `]]></VASTAdTagURI>` +
		`</Wrapper></Ad>` + "\n" +
		`</VAST>`
}

// vastXMLToBidResponse wraps a VAST XML string into a minimal OpenRTB 2.5
// BidResponse so that OpenRTB-native players can process it natively.
// Used by ortbToVASTAdapter.
func vastXMLToBidResponse(vastXML string, cfg *AdServerConfig) *openrtb2.BidResponse {
	auctionID := fastGenerateID()
	bidID := fastGenerateID()
	return &openrtb2.BidResponse{
		ID: auctionID,
		SeatBid: []openrtb2.SeatBid{
			{
				Seat: "direct-vast",
				Bid: []openrtb2.Bid{
					{
						ID:    bidID,
						ImpID: "1",
						Price: cfg.FloorCPM,
						AdM:   vastXML,
					},
				},
			},
		},
	}
}

// topBidPrice returns the highest bid price across all seatbids in resp.
// Used by ORTB-proxy adapters that need to record revenue without altering
// the BidResponse forwarded to the player.
func topBidPrice(resp *openrtb2.BidResponse) float64 {
	var top float64
	if resp == nil {
		return 0
	}
	for _, sb := range resp.SeatBid {
		for _, b := range sb.Bid {
			if b.Price > top {
				top = b.Price
			}
		}
	}
	return top
}
