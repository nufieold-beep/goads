package enrich

import (
	"fmt"
	"strings"

	"github.com/prebid/prebid-server/v4/modules/ctv/vast"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast/model"
)

type VastEnricher struct{}

func NewEnricher() *VastEnricher {
	return &VastEnricher{}
}

func (e *VastEnricher) Enrich(ad *model.Ad, meta vast.CanonicalMeta, cfg vast.ReceiverConfig) ([]string, error) {
	var warnings []string
	if ad == nil {
		return warnings, nil
	}
	if ad.InLine == nil {
		warnings = append(warnings, "skipping enrichment: ad is not InLine")
		return warnings, nil
	}

	inline := ad.InLine
	if inline.Extensions == nil {
		inline.Extensions = &model.Extensions{}
	}

	warnings = append(warnings, e.enrichPricing(inline, meta, cfg)...)
	warnings = append(warnings, e.enrichAdvertiser(inline, meta, cfg)...)
	warnings = append(warnings, e.enrichDuration(inline, meta)...)
	warnings = append(warnings, e.enrichCategories(inline, meta)...)
	if cfg.Debug || cfg.Placement.Debug {
		e.addDebugExtension(inline, meta)
	}

	return warnings, nil
}

func (e *VastEnricher) enrichPricing(inline *model.InLine, meta vast.CanonicalMeta, cfg vast.ReceiverConfig) []string {
	var warnings []string
	if meta.Price <= 0 {
		return warnings
	}
	if inline.Pricing != nil && inline.Pricing.Value != "" {
		warnings = append(warnings, "pricing: VAST_WINS - keeping existing pricing")
		return warnings
	}

	priceStr := formatPrice(meta.Price)
	currency := meta.Currency
	if currency == "" {
		currency = cfg.DefaultCurrency
	}
	if currency == "" {
		currency = "USD"
	}

	placement := cfg.Placement.PricingPlacement
	if placement == "" {
		placement = vast.PlacementVastPricing
	}

	switch placement {
	case vast.PlacementExtension:
		inline.Extensions.Extension = append(inline.Extensions.Extension, model.ExtensionXML{
			Type:     "pricing",
			InnerXML: fmt.Sprintf("<Price model=\"CPM\" currency=\"%s\">%s</Price>", currency, priceStr),
		})
	default:
		inline.Pricing = &model.Pricing{
			Model:    "CPM",
			Currency: currency,
			Value:    priceStr,
		}
	}

	return warnings
}

func (e *VastEnricher) enrichAdvertiser(inline *model.InLine, meta vast.CanonicalMeta, cfg vast.ReceiverConfig) []string {
	var warnings []string
	if meta.Adomain == "" {
		return warnings
	}
	if strings.TrimSpace(inline.Advertiser) != "" {
		warnings = append(warnings, "advertiser: VAST_WINS - keeping existing advertiser")
		return warnings
	}

	placement := cfg.Placement.AdvertiserPlacement
	if placement == "" {
		placement = vast.PlacementAdvertiserTag
	}

	switch placement {
	case vast.PlacementExtension:
		inline.Extensions.Extension = append(inline.Extensions.Extension, model.ExtensionXML{
			Type:     "advertiser",
			InnerXML: fmt.Sprintf("<Advertiser>%s</Advertiser>", escapeXML(meta.Adomain)),
		})
	default:
		inline.Advertiser = meta.Adomain
	}

	return warnings
}

func (e *VastEnricher) enrichDuration(inline *model.InLine, meta vast.CanonicalMeta) []string {
	var warnings []string
	if meta.DurSec <= 0 {
		return warnings
	}
	if inline.Creatives == nil || len(inline.Creatives.Creative) == 0 {
		return warnings
	}

	for i := range inline.Creatives.Creative {
		creative := &inline.Creatives.Creative[i]
		if creative.Linear == nil {
			continue
		}
		if strings.TrimSpace(creative.Linear.Duration) != "" {
			warnings = append(warnings, "duration: VAST_WINS - keeping existing duration")
			continue
		}
		creative.Linear.Duration = model.SecToHHMMSS(meta.DurSec)
	}

	return warnings
}

func (e *VastEnricher) enrichCategories(inline *model.InLine, meta vast.CanonicalMeta) []string {
	var warnings []string
	if len(meta.Cats) == 0 {
		return warnings
	}

	var categoryXML strings.Builder
	for _, cat := range meta.Cats {
		categoryXML.WriteString(fmt.Sprintf("<Category>%s</Category>", escapeXML(cat)))
	}
	inline.Extensions.Extension = append(inline.Extensions.Extension, model.ExtensionXML{
		Type:     "iab_category",
		InnerXML: categoryXML.String(),
	})

	return warnings
}

func (e *VastEnricher) addDebugExtension(inline *model.InLine, meta vast.CanonicalMeta) {
	var debugXML strings.Builder
	debugXML.WriteString(fmt.Sprintf("<BidID>%s</BidID>", escapeXML(meta.BidID)))
	debugXML.WriteString(fmt.Sprintf("<ImpID>%s</ImpID>", escapeXML(meta.ImpID)))
	if meta.DealID != "" {
		debugXML.WriteString(fmt.Sprintf("<DealID>%s</DealID>", escapeXML(meta.DealID)))
	}
	debugXML.WriteString(fmt.Sprintf("<Seat>%s</Seat>", escapeXML(meta.Seat)))
	debugXML.WriteString(fmt.Sprintf("<Price>%s</Price>", formatPrice(meta.Price)))
	debugXML.WriteString(fmt.Sprintf("<Currency>%s</Currency>", escapeXML(meta.Currency)))

	inline.Extensions.Extension = append(inline.Extensions.Extension, model.ExtensionXML{
		Type:     "openrtb",
		InnerXML: debugXML.String(),
	})
}

func formatPrice(price float64) string {
	s := fmt.Sprintf("%.4f", price)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" {
		return "0"
	}
	return s
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&quot;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	return s
}

var _ vast.Enricher = (*VastEnricher)(nil)
