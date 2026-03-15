package format

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prebid/prebid-server/v4/modules/ctv/vast"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast/model"
)

func TestFormatNoAdMatchesGolden(t *testing.T) {
	formatter := NewFormatter()
	xml, warnings, err := formatter.Format(nil, vast.DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
	assertGoldenXML(t, "no_ad.xml", xml)
}

func TestFormatSingleAdMatchesGolden(t *testing.T) {
	formatter := NewFormatter()
	ads := []vast.EnrichedAd{{
		Ad: &model.Ad{
			InLine: &model.InLine{
				AdSystem:   &model.AdSystem{Value: "TestAdServer"},
				AdTitle:    "Test Ad",
				Advertiser: "advertiser.example",
				Creatives: &model.Creatives{Creative: []model.Creative{{
					Linear: &model.Linear{
						Duration: "00:00:15",
						MediaFiles: &model.MediaFiles{MediaFile: []model.MediaFile{{
							Delivery: "progressive",
							Type:     "video/mp4",
							Width:    1920,
							Height:   1080,
							Value:    "https://example.com/video.mp4",
						}}},
					},
				}}},
				Extensions: &model.Extensions{Extension: []model.ExtensionXML{{
					Type:     "iab_category",
					InnerXML: "<Category>IAB1</Category>",
				}}},
			},
		},
		Meta: vast.CanonicalMeta{BidID: "bid-123"},
	}}

	xml, warnings, err := formatter.Format(ads, vast.DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
	assertGoldenXML(t, "single_ad.xml", xml)
}

func TestFormatPodTwoAdsMatchesGolden(t *testing.T) {
	formatter := NewFormatter()
	ads := []vast.EnrichedAd{
		buildEnrichedAd("bid-001", "TestAdServer", "First Ad", "00:00:15", "https://example.com/first.mp4", 1),
		buildEnrichedAd("bid-002", "TestAdServer", "Second Ad", "00:00:30", "https://example.com/second.mp4", 2),
	}

	xml, warnings, err := formatter.Format(ads, vast.DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
	assertGoldenXML(t, "pod_two_ads.xml", xml)
}

func TestFormatPodThreeAdsMatchesGolden(t *testing.T) {
	formatter := NewFormatter()
	ads := []vast.EnrichedAd{
		buildEnrichedAd("bid-alpha", "AdServer1", "Alpha Ad", "00:00:15", "https://example.com/alpha.mp4", 1),
		buildEnrichedAd("bid-beta", "AdServer2", "Beta Ad", "00:00:30", "https://example.com/beta.mp4", 2),
		buildEnrichedAd("bid-gamma", "AdServer3", "Gamma Ad", "00:00:45", "https://example.com/gamma.mp4", 3),
	}

	xml, warnings, err := formatter.Format(ads, vast.DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
	assertGoldenXML(t, "pod_three_ads.xml", xml)
}

func buildEnrichedAd(bidID, adSystem, adTitle, duration, mediaURL string, sequence int) vast.EnrichedAd {
	return vast.EnrichedAd{
		Ad: &model.Ad{
			InLine: &model.InLine{
				AdSystem: &model.AdSystem{Value: adSystem},
				AdTitle:  adTitle,
				Creatives: &model.Creatives{Creative: []model.Creative{{
					Linear: &model.Linear{
						Duration: duration,
						MediaFiles: &model.MediaFiles{MediaFile: []model.MediaFile{{
							Delivery: "progressive",
							Type:     "video/mp4",
							Width:    1920,
							Height:   1080,
							Value:    mediaURL,
						}}},
					},
				}}},
			},
		},
		Meta:     vast.CanonicalMeta{BidID: bidID},
		Sequence: sequence,
	}
}

func assertGoldenXML(t *testing.T, name string, actual []byte) {
	t.Helper()
	want, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("failed to read golden file %s: %v", name, err)
	}
	if string(actual) != string(want) {
		t.Fatalf("golden mismatch for %s\nwant:\n%s\n\ngot:\n%s", name, string(want), string(actual))
	}
}
