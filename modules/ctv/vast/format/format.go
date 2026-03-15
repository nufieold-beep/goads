package format

import (
	"encoding/xml"

	"github.com/prebid/prebid-server/v4/modules/ctv/vast"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast/model"
)

type VastFormatter struct{}

func NewFormatter() *VastFormatter {
	return &VastFormatter{}
}

func (f *VastFormatter) Format(ads []vast.EnrichedAd, cfg vast.ReceiverConfig) ([]byte, []string, error) {
	var warnings []string
	version := cfg.VastVersionDefault
	if version == "" {
		version = "4.0"
	}
	if len(ads) == 0 {
		return model.BuildNoAdVast(version), warnings, nil
	}

	vastDoc := model.Vast{
		Version: version,
		Ads:     make([]model.Ad, 0, len(ads)),
	}
	isPod := len(ads) > 1

	for _, enriched := range ads {
		if enriched.Ad == nil {
			warnings = append(warnings, "skipping nil ad in format")
			continue
		}
		ad := copyAd(enriched.Ad)
		ad.ID = deriveAdID(enriched.Meta)
		if isPod && enriched.Sequence > 0 {
			ad.Sequence = enriched.Sequence
		} else if !isPod {
			ad.Sequence = 0
		}
		vastDoc.Ads = append(vastDoc.Ads, *ad)
	}

	if len(vastDoc.Ads) == 0 {
		warnings = append(warnings, "all ads were nil, returning no-ad VAST")
		return model.BuildNoAdVast(version), warnings, nil
	}

	xmlBytes, err := xml.MarshalIndent(vastDoc, "", "  ")
	if err != nil {
		return nil, warnings, err
	}

	return append([]byte(xml.Header), xmlBytes...), warnings, nil
}

func deriveAdID(meta vast.CanonicalMeta) string {
	if meta.BidID != "" {
		return meta.BidID
	}
	if meta.ImpID != "" {
		return "imp-" + meta.ImpID
	}
	return ""
}

func copyAd(src *model.Ad) *model.Ad {
	if src == nil {
		return nil
	}
	ad := *src
	return &ad
}

var _ vast.Formatter = (*VastFormatter)(nil)
