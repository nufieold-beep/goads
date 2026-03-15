package exchange

import (
	"time"

	"github.com/prebid/prebid-server/v4/exchange/entities"
	jsonpatch "gopkg.in/evanphx/json-patch.v5"

	"github.com/prebid/prebid-server/v4/analytics"
	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/endpoints/events"
	"github.com/prebid/prebid-server/v4/injector"
	"github.com/prebid/prebid-server/v4/macros"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
	"github.com/prebid/prebid-server/v4/util/jsonutil"
	"github.com/prebid/prebid-server/v4/version"
)

// eventTracking has configuration fields needed for adding event tracking to an auction response
type eventTracking struct {
	accountID          string
	enabledForAccount  bool
	enabledForRequest  bool
	auctionTimestampMs int64
	integrationType    string
	bidderInfos        config.BidderInfos
	externalURL        string
	events             injector.VASTEvents
	macroProvider      *macros.MacroProvider
}

// getEventTracking creates an eventTracking object from the different configuration sources
func getEventTracking(requestExtPrebid *openrtb_ext.ExtRequestPrebid, ts time.Time, account *config.Account, bidderInfos config.BidderInfos, externalURL string, macroProvider *macros.MacroProvider) *eventTracking {
	return &eventTracking{
		accountID:          account.ID,
		enabledForAccount:  account.Events.Enabled,
		enabledForRequest:  requestExtPrebid != nil && requestExtPrebid.Events != nil,
		auctionTimestampMs: ts.UnixNano() / 1e+6,
		integrationType:    getIntegrationType(requestExtPrebid),
		bidderInfos:        bidderInfos,
		externalURL:        externalURL,
		events:             convertToVastEvent(account.Events),
		macroProvider:      macroProvider,
	}
}

func getIntegrationType(requestExtPrebid *openrtb_ext.ExtRequestPrebid) string {
	if requestExtPrebid != nil {
		return requestExtPrebid.Integration
	}
	return ""
}

// modifyBidsForEvents adds bidEvents and modifies VAST AdM if necessary.
func (ev *eventTracking) modifyBidsForEvents(seatBids map[openrtb_ext.BidderName]*entities.PbsOrtbSeatBid) map[openrtb_ext.BidderName]*entities.PbsOrtbSeatBid {
	for bidderName, seatBid := range seatBids {
		modifyingVastXMLAllowed := ev.isModifyingVASTXMLAllowed(bidderName.String())
		for _, pbsBid := range seatBid.Bids {
			if modifyingVastXMLAllowed {
				ev.modifyBidVAST(pbsBid, bidderName)
			}
			pbsBid.BidEvents = ev.makeBidExtEvents(pbsBid, bidderName)
		}
	}
	return seatBids
}

// isModifyingVASTXMLAllowed returns true if this bidder config allows modifying VAST XML for event tracking
func (ev *eventTracking) isModifyingVASTXMLAllowed(bidderName string) bool {
	return ev.bidderInfos[bidderName].ModifyingVastXmlAllowed && ev.isEventAllowed()
}

// modifyBidVAST injects event Impression url if needed, otherwise returns original VAST string
func (ev *eventTracking) modifyBidVAST(pbsBid *entities.PbsOrtbBid, bidderName openrtb_ext.BidderName) {
	bid := pbsBid.Bid
	if pbsBid.BidType != openrtb_ext.BidTypeVideo || len(bid.AdM) == 0 && len(bid.NURL) == 0 {
		return
	}
	ev.injectTrackers(pbsBid, bidderName)
	bidID := bid.ID
	if len(pbsBid.GeneratedBidID) > 0 {
		bidID = pbsBid.GeneratedBidID
	}
	if newVastXML, ok := events.ModifyVastXmlString(ev.externalURL, bid.AdM, bidID, bidderName.String(), ev.accountID, ev.auctionTimestampMs, ev.integrationType); ok {
		bid.AdM = newVastXML
	}
}

// modifyBidJSON injects "wurl" (win) event url if needed, otherwise returns original json
func (ev *eventTracking) modifyBidJSON(pbsBid *entities.PbsOrtbBid, bidderName openrtb_ext.BidderName, jsonBytes []byte) ([]byte, error) {
	if !ev.isEventAllowed() || pbsBid.BidType == openrtb_ext.BidTypeVideo {
		return jsonBytes, nil
	}
	var winEventURL string
	if pbsBid.BidEvents != nil { // All bids should have already been updated with win/imp event URLs
		winEventURL = pbsBid.BidEvents.Win
	} else {
		winEventURL = ev.makeEventURL(analytics.Win, pbsBid, bidderName)
	}
	// wurl attribute is not in the schema, so we have to patch
	patch, err := jsonutil.Marshal(map[string]string{"wurl": winEventURL})
	if err != nil {
		return jsonBytes, err
	}
	modifiedJSON, err := jsonpatch.MergePatch(jsonBytes, patch)
	if err != nil {
		return jsonBytes, err
	}
	return modifiedJSON, nil
}

// makeBidExtEvents make the data for bid.ext.prebid.events if needed, otherwise returns nil
func (ev *eventTracking) makeBidExtEvents(pbsBid *entities.PbsOrtbBid, bidderName openrtb_ext.BidderName) *openrtb_ext.ExtBidPrebidEvents {
	if !ev.isEventAllowed() || pbsBid.BidType == openrtb_ext.BidTypeVideo {
		return nil
	}
	return &openrtb_ext.ExtBidPrebidEvents{
		Win: ev.makeEventURL(analytics.Win, pbsBid, bidderName),
		Imp: ev.makeEventURL(analytics.Imp, pbsBid, bidderName),
	}
}

// makeEventURL returns an analytics event url for the requested type (win or imp)
func (ev *eventTracking) makeEventURL(evType analytics.EventType, pbsBid *entities.PbsOrtbBid, bidderName openrtb_ext.BidderName) string {
	bidId := pbsBid.Bid.ID
	if len(pbsBid.GeneratedBidID) > 0 {
		bidId = pbsBid.GeneratedBidID
	}
	pbsVer := version.Ver
	if pbsVer == "" {
		pbsVer = version.VerUnknown
	}
	er := &analytics.EventRequest{
		Type:        evType,
		BidID:       bidId,
		Bidder:      string(bidderName),
		AccountID:   ev.accountID,
		Timestamp:   ev.auctionTimestampMs,
		Integration: ev.integrationType,
		Price:       pbsBid.Bid.Price,
		ImpID:       pbsBid.Bid.ImpID,
		Width:       pbsBid.Bid.W,
		Height:      pbsBid.Bid.H,
		PbsVersion:  pbsVer,
	}
	if len(pbsBid.Bid.ADomain) > 0 {
		er.Adomain = pbsBid.Bid.ADomain[0]
	}
	return events.EventRequestToUrl(ev.externalURL, er)
}

// isEventAllowed checks if events are enabled by default or on account/request level
func (ev *eventTracking) isEventAllowed() bool {
	return ev.enabledForAccount || ev.enabledForRequest
}

func (ev *eventTracking) injectTrackers(pbsBid *entities.PbsOrtbBid, bidderName openrtb_ext.BidderName) {
	if ev.macroProvider == nil {
		return
	}

	ev.macroProvider.PopulateBidMacros(pbsBid, bidderName.String())
	tracker := injector.NewTrackerInjector(
		macros.NewStringIndexBasedReplacer(),
		ev.macroProvider,
		ev.events,
	)

	if adm, err := tracker.InjectTracker(pbsBid.Bid.AdM, pbsBid.Bid.NURL); err == nil {
		pbsBid.Bid.AdM = adm
	}
}

func convertToVastEvent(eventsCfg config.Events) injector.VASTEvents {
	vastEvents := injector.VASTEvents{
		TrackingEvents: make(map[string][]string),
	}

	for _, vastEvent := range eventsCfg.VASTEvents {
		switch vastEvent.CreateElement {
		case config.ErrorVASTElement:
			vastEvents.Errors = appendURLs(vastEvents.Errors, vastEvent, eventsCfg.DefaultURL)
		case config.TrackingVASTElement:
			vastEvents.TrackingEvents[string(vastEvent.Type)] = appendURLs(vastEvents.TrackingEvents[string(vastEvent.Type)], vastEvent, eventsCfg.DefaultURL)
		case config.ClickTrackingVASTElement:
			vastEvents.VideoClicks = appendURLs(vastEvents.VideoClicks, vastEvent, eventsCfg.DefaultURL)
		case config.NonLinearClickTrackingVASTElement:
			vastEvents.NonLinearClickTracking = appendURLs(vastEvents.NonLinearClickTracking, vastEvent, eventsCfg.DefaultURL)
		case config.CompanionClickThroughVASTElement:
			vastEvents.CompanionClickThrough = appendURLs(vastEvents.CompanionClickThrough, vastEvent, eventsCfg.DefaultURL)
		}
	}

	return vastEvents
}

func appendURLs(urls []string, vastEvent config.VASTEvent, defaultURL string) []string {
	urls = append(urls, vastEvent.URLs...)
	if !vastEvent.ExcludeDefaultURL {
		urls = append(urls, defaultURL)
	}
	return urls
}
