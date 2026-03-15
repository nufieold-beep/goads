package exchange

import (
	"testing"

	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/exchange/entities"
	"github.com/prebid/prebid-server/v4/macros"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
	"github.com/stretchr/testify/assert"
)

func TestConvertToVastEvent(t *testing.T) {
	converted := convertToVastEvent(config.Events{
		DefaultURL: "http://default.url",
		VASTEvents: []config.VASTEvent{
			{
				CreateElement: config.ErrorVASTElement,
				URLs:          []string{"http://error.url"},
			},
			{
				CreateElement: config.TrackingVASTElement,
				Type:          config.Start,
				URLs:          []string{"http://tracking.url"},
			},
			{
				CreateElement: config.ClickTrackingVASTElement,
				ExcludeDefaultURL: true,
				URLs:              []string{"http://click.url"},
			},
			{
				CreateElement: config.CompanionClickThroughVASTElement,
				URLs:          []string{"http://companion.url"},
			},
		},
	})

	assert.Equal(t, []string{"http://error.url", "http://default.url"}, converted.Errors)
	assert.Equal(t, map[string][]string{"start": {"http://tracking.url", "http://default.url"}}, converted.TrackingEvents)
	assert.Equal(t, []string{"http://click.url"}, converted.VideoClicks)
	assert.Equal(t, []string{"http://companion.url", "http://default.url"}, converted.CompanionClickThrough)
}

func TestAppendURLs(t *testing.T) {
	assert.Equal(t, []string{"http://default.url"}, appendURLs(nil, config.VASTEvent{}, "http://default.url"))
	assert.Equal(t, []string{"http://existing.url", "http://event.url"}, appendURLs(
		[]string{"http://existing.url"},
		config.VASTEvent{URLs: []string{"http://event.url"}, ExcludeDefaultURL: true},
		"http://default.url",
	))
}

func TestModifyBidVASTUsesGeneratedBidID(t *testing.T) {
	ev := &eventTracking{
		enabledForRequest:  true,
		accountID:          "acc-1",
		auctionTimestampMs: 1234,
		externalURL:        "http://external-url",
		macroProvider:      macros.NewProvider(&openrtb_ext.RequestWrapper{BidRequest: &openrtb2.BidRequest{}}),
	}
	pbsBid := &entities.PbsOrtbBid{
		BidType:        openrtb_ext.BidTypeVideo,
		GeneratedBidID: "generated-bid-id",
		Bid: &openrtb2.Bid{
			ID:  "original-bid-id",
			AdM: `<VAST version="3.0"><Ad><Wrapper><Impression></Impression><Creatives></Creatives></Wrapper></Ad></VAST>`,
		},
	}

	ev.modifyBidVAST(pbsBid, openrtb_ext.BidderOpenx)

	assert.Contains(t, pbsBid.Bid.AdM, "b=generated-bid-id")
	assert.Contains(t, pbsBid.Bid.AdM, "bidder=openx")
	assert.Contains(t, pbsBid.Bid.AdM, "t=imp")
}

func TestModifyBidVASTSkipsNonVideoBids(t *testing.T) {
	ev := &eventTracking{
		enabledForRequest:  true,
		accountID:          "acc-1",
		auctionTimestampMs: 1234,
		externalURL:        "http://external-url",
		macroProvider:      macros.NewProvider(&openrtb_ext.RequestWrapper{BidRequest: &openrtb2.BidRequest{}}),
	}
	original := `<div>banner</div>`
	pbsBid := &entities.PbsOrtbBid{
		BidType: openrtb_ext.BidTypeBanner,
		Bid: &openrtb2.Bid{
			ID:  "original-bid-id",
			AdM: original,
		},
	}

	ev.modifyBidVAST(pbsBid, openrtb_ext.BidderOpenx)

	assert.Equal(t, original, pbsBid.Bid.AdM)
}

func TestModifyBidVASTWrapsNURLBeforeImpressionRewrite(t *testing.T) {
	ev := &eventTracking{
		enabledForRequest:  true,
		accountID:          "acc-1",
		auctionTimestampMs: 1234,
		externalURL:        "http://external-url",
		macroProvider:      macros.NewProvider(&openrtb_ext.RequestWrapper{BidRequest: &openrtb2.BidRequest{}}),
	}
	pbsBid := &entities.PbsOrtbBid{
		BidType: openrtb_ext.BidTypeVideo,
		Bid: &openrtb2.Bid{
			ID:   "bid-1",
			NURL: "http://nurl.test",
		},
	}

	ev.modifyBidVAST(pbsBid, openrtb_ext.BidderOpenx)

	assert.Contains(t, pbsBid.Bid.AdM, `<VASTAdTagURI><![CDATA[http://nurl.test]]></VASTAdTagURI>`)
	assert.Contains(t, pbsBid.Bid.AdM, `t=imp`)
	assert.Contains(t, pbsBid.Bid.AdM, `bidder=openx`)
}