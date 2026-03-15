package ctv_vast_enrichment

import (
	"context"
	"encoding/json"

	"github.com/prebid/prebid-server/v4/adapters"
	"github.com/prebid/prebid-server/v4/hooks/hookstage"
	ctvvast "github.com/prebid/prebid-server/v4/modules/ctv/vast"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast/enrich"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast/model"
	"github.com/prebid/prebid-server/v4/modules/moduledeps"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
)

// Builder initializes the PBS module wrapper around the local CTV VAST pipeline.
func Builder(cfg json.RawMessage, _ moduledeps.ModuleDeps) (interface{}, error) {
	var hostCfg ctvvast.CTVVastConfig
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &hostCfg); err != nil {
			return nil, err
		}
	}

	return Module{
		hostConfig: hostCfg,
		enricher:   enrich.NewEnricher(),
	}, nil
}

// Module applies VAST enrichment to bidder video responses during the raw bidder response stage.
type Module struct {
	hostConfig ctvvast.CTVVastConfig
	enricher   ctvvast.Enricher
}

// HandleRawBidderResponseHook enriches bidder VAST XML in-place when the module is enabled.
func (m Module) HandleRawBidderResponseHook(
	ctx context.Context,
	miCtx hookstage.ModuleInvocationContext,
	payload hookstage.RawBidderResponsePayload,
) (hookstage.HookResult[hookstage.RawBidderResponsePayload], error) {
	result := hookstage.HookResult[hookstage.RawBidderResponsePayload]{}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if payload.BidderResponse == nil || len(payload.BidderResponse.Bids) == 0 {
		return result, nil
	}

	accountCfg, err := parseAccountConfig(miCtx.AccountConfig)
	if err != nil {
		return result, err
	}

	mergedCfg := ctvvast.MergeCTVVastConfig(&m.hostConfig, accountCfg, nil)
	if !mergedCfg.IsEnabled() {
		return result, nil
	}

	receiverCfg := mergedCfg.ReceiverConfig()
	modifiedBids := make([]*adapters.TypedBid, 0, len(payload.BidderResponse.Bids))
	changesMade := false

	for _, typedBid := range payload.BidderResponse.Bids {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		updatedBid, warnings, changed := m.enrichTypedBid(typedBid, receiverCfg, payload.BidderResponse.Currency, payload.Bidder)
		result.Warnings = append(result.Warnings, warnings...)
		modifiedBids = append(modifiedBids, updatedBid)
		changesMade = changesMade || changed
	}

	if changesMade {
		changeSet := hookstage.ChangeSet[hookstage.RawBidderResponsePayload]{}
		changeSet.RawBidderResponse().Bids().UpdateBids(modifiedBids)
		result.ChangeSet = changeSet
	}

	return result, nil
}

func parseAccountConfig(raw json.RawMessage) (*ctvvast.CTVVastConfig, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	var cfg ctvvast.CTVVastConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (m Module) enrichTypedBid(typedBid *adapters.TypedBid, cfg ctvvast.ReceiverConfig, bidderCurrency string, bidder string) (*adapters.TypedBid, []string, bool) {
	if typedBid == nil || typedBid.Bid == nil {
		return typedBid, nil, false
	}
	if typedBid.BidType != openrtb_ext.BidTypeVideo {
		return typedBid, nil, false
	}

	parserCfg := model.ParserConfig{
		AllowSkeletonVast:  cfg.AllowSkeletonVast,
		VastVersionDefault: cfg.VastVersionDefault,
	}
	parsedVast, warnings, err := model.ParseVastOrSkeleton(typedBid.Bid.AdM, parserCfg)
	if err != nil {
		return typedBid, warnings, false
	}
	if parsedVast == nil || len(parsedVast.Ads) == 0 {
		return typedBid, warnings, false
	}

	meta := buildCanonicalMeta(typedBid, bidderCurrency, bidder, cfg)
	for i := range parsedVast.Ads {
		enrichWarnings, enrichErr := m.enricher.Enrich(&parsedVast.Ads[i], meta, cfg)
		warnings = append(warnings, enrichWarnings...)
		if enrichErr != nil {
			warnings = append(warnings, "enrichment failed for bid "+typedBid.Bid.ID+": "+enrichErr.Error())
		}
	}

	xmlBytes, err := parsedVast.Marshal()
	if err != nil {
		warnings = append(warnings, "failed to marshal VAST for bid "+typedBid.Bid.ID+": "+err.Error())
		return typedBid, warnings, false
	}

	updatedTypedBid := *typedBid
	updatedBid := *typedBid.Bid
	updatedBid.AdM = string(xmlBytes)
	updatedTypedBid.Bid = &updatedBid

	return &updatedTypedBid, warnings, true
}

func buildCanonicalMeta(typedBid *adapters.TypedBid, bidderCurrency string, bidder string, cfg ctvvast.ReceiverConfig) ctvvast.CanonicalMeta {
	meta := ctvvast.CanonicalMeta{
		BidID:    typedBid.Bid.ID,
		ImpID:    typedBid.Bid.ImpID,
		DealID:   typedBid.Bid.DealID,
		Seat:     bidder,
		Price:    typedBid.Bid.Price,
		Currency: cfg.DefaultCurrency,
		DurSec:   bidDurationSeconds(typedBid),
	}
	if bidderCurrency != "" {
		meta.Currency = bidderCurrency
	}
	if len(typedBid.Bid.ADomain) > 0 {
		meta.Adomain = typedBid.Bid.ADomain[0]
	}
	if len(typedBid.Bid.Cat) > 0 {
		meta.Cats = typedBid.Bid.Cat
	}
	return meta
}

func bidDurationSeconds(typedBid *adapters.TypedBid) int {
	if typedBid == nil {
		return 0
	}
	if typedBid.Bid != nil && typedBid.Bid.Dur > 0 {
		return int(typedBid.Bid.Dur)
	}
	if typedBid.BidVideo != nil && typedBid.BidVideo.Duration > 0 {
		return typedBid.BidVideo.Duration
	}
	return 0
}

var _ hookstage.RawBidderResponse = Module{}