package injector

import (
	"testing"

	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/exchange/entities"
	"github.com/prebid/prebid-server/v4/macros"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInjectTrackerAddsInlineVideoEvents(t *testing.T) {
	reqWrapper := &openrtb_ext.RequestWrapper{BidRequest: &openrtb2.BidRequest{}}
	provider := macros.NewProvider(reqWrapper)
	provider.PopulateBidMacros(&entities.PbsOrtbBid{Bid: &openrtb2.Bid{ID: "bid123"}}, "openx")

	tracker := NewTrackerInjector(
		macros.NewStringIndexBasedReplacer(),
		provider,
		VASTEvents{
			Errors:      []string{"http://error.example/##PBS-BIDID##"},
			Impressions: []string{"http://impression.example/##PBS-BIDID##"},
			VideoClicks: []string{"http://click.example/##PBS-BIDID##"},
			TrackingEvents: map[string][]string{
				"firstQuartile": {"http://quartile.example/##PBS-BIDID##"},
			},
		},
	)

	vast := `<VAST version="4.0"><Ad><InLine><Error><![CDATA[http://origin.example/error]]></Error><Impression></Impression><Creatives><Creative adId="creative-1"><Linear></Linear></Creative></Creatives></InLine></Ad></VAST>`

	modified, err := tracker.InjectTracker(vast, "")
	require.NoError(t, err)

	assert.Contains(t, modified, `http://origin.example/error`)
	assert.Contains(t, modified, `http://error.example/bid123`)
	assert.Contains(t, modified, `http://impression.example/bid123`)
	assert.Contains(t, modified, `<VideoClicks><ClickTracking><![CDATA[http://click.example/bid123]]></ClickTracking></VideoClicks>`)
	assert.Contains(t, modified, `<TrackingEvents><Tracking event="firstQuartile"><![CDATA[http://quartile.example/bid123]]></Tracking></TrackingEvents>`)
}

func TestInjectTrackerAddsWrapperCompanionTracking(t *testing.T) {
	reqWrapper := &openrtb_ext.RequestWrapper{BidRequest: &openrtb2.BidRequest{}}
	provider := macros.NewProvider(reqWrapper)
	provider.PopulateBidMacros(&entities.PbsOrtbBid{Bid: &openrtb2.Bid{ID: "bid456"}}, "openx")

	tracker := NewTrackerInjector(
		macros.NewStringIndexBasedReplacer(),
		provider,
		VASTEvents{
			Errors:                 []string{"http://error.example/##PBS-BIDID##"},
			Impressions:            []string{"http://impression.example/##PBS-BIDID##"},
			CompanionClickThrough:  []string{"http://companion.example/##PBS-BIDID##"},
			NonLinearClickTracking: []string{"http://nonlinear.example/##PBS-BIDID##"},
		},
	)

	vast := `<VAST version="4.0"><Ad><Wrapper><Error><![CDATA[http://origin.example/error]]></Error><Impression></Impression><Creatives><Creative adId="creative-2"><CompanionAds></CompanionAds></Creative><Creative adId="creative-3"><NonLinearAds></NonLinearAds></Creative></Creatives><VASTAdTagURI><![CDATA[http://example.com/vast]]></VASTAdTagURI></Wrapper></Ad></VAST>`

	modified, err := tracker.InjectTracker(vast, "")
	require.NoError(t, err)

	assert.Contains(t, modified, `http://companion.example/bid456`)
	assert.Contains(t, modified, `http://nonlinear.example/bid456`)
	assert.Contains(t, modified, `http://impression.example/bid456`)
	assert.Contains(t, modified, `http://error.example/bid456`)
}

func TestInjectTrackerBuildsWrapperWhenOnlyNURLProvided(t *testing.T) {
	reqWrapper := &openrtb_ext.RequestWrapper{BidRequest: &openrtb2.BidRequest{}}
	provider := macros.NewProvider(reqWrapper)
	tracker := NewTrackerInjector(
		macros.NewStringIndexBasedReplacer(),
		provider,
		VASTEvents{},
	)

	modified, err := tracker.InjectTracker("", "http://example.com/nurl")
	require.NoError(t, err)
	assert.Equal(t, `<VAST version="3.0"><Ad><Wrapper><AdSystem>prebid.org wrapper</AdSystem><VASTAdTagURI><![CDATA[http://example.com/nurl]]></VASTAdTagURI><Impression></Impression><Creatives></Creatives></Wrapper></Ad></VAST>`, modified)
}