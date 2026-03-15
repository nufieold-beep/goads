package events

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
	"github.com/prebid/prebid-server/v4/prebid_cache_client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBiddersAllowingVastUpdatePreservesCaseSensitiveBidderNames(t *testing.T) {
	req := &BidCacheRequest{
		Puts: []prebid_cache_client.Cacheable{
			{Bidder: "APPNEXUS"},
			{Bidder: "ApPnExUs"},
		},
	}

	bidderInfos := config.BidderInfos{
		"appnexus": {ModifyingVastXmlAllowed: true},
	}
	normalize := func(name string) (openrtb_ext.BidderName, bool) {
		if strings.EqualFold(name, "appnexus") {
			return openrtb_ext.BidderName("appnexus"), true
		}
		return "", false
	}

	allowed := getBiddersAllowingVastUpdate(req, &bidderInfos, false, normalize)

	assert.Len(t, allowed, 2)
	_, ok := allowed["APPNEXUS"]
	assert.True(t, ok)
	_, ok = allowed["ApPnExUs"]
	assert.True(t, ok)
}

func TestGetBiddersAllowingVastUpdateAllowsUnknownBiddersWhenConfigured(t *testing.T) {
	req := &BidCacheRequest{
		Puts: []prebid_cache_client.Cacheable{{Bidder: "unknown-bidder"}},
	}

	allowed := getBiddersAllowingVastUpdate(req, nil, true, func(name string) (openrtb_ext.BidderName, bool) {
		return "", false
	})

	assert.Len(t, allowed, 1)
	_, ok := allowed["unknown-bidder"]
	assert.True(t, ok)
}

func TestModifyVastXmlStringAppendsTrackingToExistingImpression(t *testing.T) {
	vast := `<VAST version="3.0"><Ad><Wrapper><Impression>content</Impression><Creatives></Creatives></Wrapper></Ad></VAST>`

	modified, ok := ModifyVastXmlString("http://external-url", vast, "bid-1", "openx", "acc-1", 1234, "app")

	assert.True(t, ok)
	assert.Contains(t, modified, `<Impression>content</Impression><Impression><![CDATA[http://external-url/event?t=imp&b=bid-1&a=acc-1&bidder=openx&f=b&int=app&ts=1234]]></Impression>`)
}

func TestModifyVastXmlJSONLeavesVastWithoutImpressionUnchanged(t *testing.T) {
	original := `<VAST version="3.0"><Ad><Wrapper><Creatives></Creatives></Wrapper></Ad></VAST>`
	encoded, err := json.Marshal(original)
	require.NoError(t, err)

	modified := ModifyVastXmlJSON("http://external-url", encoded, "bid-1", "openx", "acc-1", 1234, "app")

	assert.JSONEq(t, string(encoded), string(modified))
}