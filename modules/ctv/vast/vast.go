// Package vast provides the main CTV VAST processing pipeline.
package vast

import (
	"context"
	"errors"

	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast/model"
)

var ErrNilSelector = errors.New("vast selector is required")
var ErrNilEnricher = errors.New("vast enricher is required")
var ErrNilFormatter = errors.New("vast formatter is required")

// BuildVastFromBidResponse orchestrates selection, parsing, enrichment, and formatting.
func BuildVastFromBidResponse(
	ctx context.Context,
	req *openrtb2.BidRequest,
	resp *openrtb2.BidResponse,
	cfg ReceiverConfig,
	selector BidSelector,
	enricher Enricher,
	formatter Formatter,
) (VastResult, error) {
	result := VastResult{
		Warnings: make([]string, 0),
		Errors:   make([]error, 0),
	}

	if err := validatePipeline(selector, enricher, formatter); err != nil {
		return failWithNoAd(result, cfg, err)
	}
	if err := ctx.Err(); err != nil {
		return failWithNoAd(result, cfg, err)
	}

	selected, selectWarnings, err := selector.Select(req, resp, cfg)
	if err != nil {
		return failWithNoAd(result, cfg, err)
	}
	result.Warnings = append(result.Warnings, selectWarnings...)
	result.Selected = selected

	if len(selected) == 0 {
		return noAdResult(result, cfg, nil), nil
	}

	enrichedAds := make([]EnrichedAd, 0, len(selected))
	parserCfg := model.ParserConfig{
		AllowSkeletonVast:  cfg.AllowSkeletonVast,
		VastVersionDefault: cfg.VastVersionDefault,
	}

	for _, sb := range selected {
		if err := ctx.Err(); err != nil {
			return failWithNoAd(result, cfg, err)
		}

		parsedVast, parseWarnings, parseErr := model.ParseVastOrSkeleton(sb.Bid.AdM, parserCfg)
		result.Warnings = append(result.Warnings, parseWarnings...)
		if parseErr != nil {
			result.Warnings = append(result.Warnings, "failed to parse VAST for bid "+sb.Bid.ID+": "+parseErr.Error())
			continue
		}

		ad := model.ExtractFirstAd(parsedVast)
		if ad == nil {
			result.Warnings = append(result.Warnings, "no ad found in VAST for bid "+sb.Bid.ID)
			continue
		}

		enrichWarnings, enrichErr := enricher.Enrich(ad, sb.Meta, cfg)
		result.Warnings = append(result.Warnings, enrichWarnings...)
		if enrichErr != nil {
			result.Warnings = append(result.Warnings, "enrichment failed for bid "+sb.Bid.ID+": "+enrichErr.Error())
		}

		enrichedAds = append(enrichedAds, EnrichedAd{
			Ad:       ad,
			Meta:     sb.Meta,
			Sequence: sb.Sequence,
		})
	}

	if len(enrichedAds) == 0 {
		result.Warnings = append(result.Warnings, "all selected bids failed VAST parsing")
		return noAdResult(result, cfg, nil), nil
	}

	xmlBytes, formatWarnings, formatErr := formatter.Format(enrichedAds, cfg)
	result.Warnings = append(result.Warnings, formatWarnings...)
	if formatErr != nil {
		return failWithNoAd(result, cfg, formatErr)
	}

	result.VastXML = xmlBytes
	result.NoAd = false

	return result, nil
}

// Processor bundles the pipeline dependencies behind a reusable object.
type Processor struct {
	selector  BidSelector
	enricher  Enricher
	formatter Formatter
	config    ReceiverConfig
}

// NewProcessor constructs a reusable VAST processor.
func NewProcessor(cfg ReceiverConfig, selector BidSelector, enricher Enricher, formatter Formatter) *Processor {
	return &Processor{
		selector:  selector,
		enricher:  enricher,
		formatter: formatter,
		config:    cfg,
	}
}

// Process executes the full VAST pipeline and returns its result.
func (p *Processor) Process(ctx context.Context, req *openrtb2.BidRequest, resp *openrtb2.BidResponse) VastResult {
	result, _ := BuildVastFromBidResponse(ctx, req, resp, p.config, p.selector, p.enricher, p.formatter)
	return result
}

func validatePipeline(selector BidSelector, enricher Enricher, formatter Formatter) error {
	switch {
	case selector == nil:
		return ErrNilSelector
	case enricher == nil:
		return ErrNilEnricher
	case formatter == nil:
		return ErrNilFormatter
	default:
		return nil
	}
}

func failWithNoAd(result VastResult, cfg ReceiverConfig, err error) (VastResult, error) {
	if err != nil {
		result.Errors = append(result.Errors, err)
	}
	return noAdResult(result, cfg, err), err
}

func noAdResult(result VastResult, cfg ReceiverConfig, err error) VastResult {
	result.NoAd = true
	result.VastXML = model.BuildNoAdVast(cfg.VastVersionDefault)
	return result
}

// DefaultConfig returns a baseline receiver configuration for local use.
func DefaultConfig() ReceiverConfig {
	return ReceiverConfig{
		Receiver:           ReceiverGAMSSU,
		DefaultCurrency:    "USD",
		VastVersionDefault: "4.0",
		MaxAdsInPod:        5,
		SelectionStrategy:  SelectionMaxRevenue,
		CollisionPolicy:    CollisionReject,
		Placement: PlacementRules{
			Pricing: PricingRules{
				FloorCPM:   0,
				CeilingCPM: 0,
				Currency:   "USD",
			},
			Advertiser: AdvertiserRules{
				BlockedDomains: []string{},
				AllowedDomains: []string{},
			},
			Categories: CategoryRules{
				BlockedCategories: []string{},
				AllowedCategories: []string{},
			},
			PricingPlacement:    PlacementVastPricing,
			AdvertiserPlacement: PlacementAdvertiserTag,
			Debug:               false,
		},
		Debug: false,
	}
}
