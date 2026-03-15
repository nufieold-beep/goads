package exchange

import (
	"testing"

	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/stretchr/testify/assert"
)

func TestMakeVASTUsesAdMWhenPresent(t *testing.T) {
	expected := `<VAST version="3.0"></VAST>`

	actual := makeVAST(&openrtb2.Bid{AdM: expected})

	assert.Equal(t, expected, actual)
}

func TestMakeVASTWrapsNURLWhenAdMEmpty(t *testing.T) {
	url := "http://domain.test/win-notify/1"
	expected := `<VAST version="3.0"><Ad><Wrapper>` +
		`<AdSystem>prebid.org wrapper</AdSystem>` +
		`<VASTAdTagURI><![CDATA[` + url + `]]></VASTAdTagURI>` +
		`<Impression></Impression><Creatives></Creatives>` +
		`</Wrapper></Ad></VAST>`

	actual := makeVAST(&openrtb2.Bid{NURL: url})

	assert.Equal(t, expected, actual)
}