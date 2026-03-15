package bidselect

import (
	"sort"
	"strings"

	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast"
)

// PriceSelector selects bids using price-first ranking.
//
// Selection order:
// 1. Filter out bids with non-positive price.
// 2. Filter out empty AdM bids unless skeleton VAST is allowed.
// 3. Sort by price descending, then deal presence, then bid ID.
// 4. Return up to maxBids, or cfg.MaxAdsInPod when maxBids is zero.
type PriceSelector struct {
	maxBids int
}

func NewPriceSelector(maxBids int) *PriceSelector {
	return &PriceSelector{maxBids: maxBids}
}

type bidWithSeat struct {
	bid  openrtb2.Bid
	seat string
}

// Select returns the ranked subset of bids for the VAST pipeline.
func (s *PriceSelector) Select(req *openrtb2.BidRequest, resp *openrtb2.BidResponse, cfg vast.ReceiverConfig) ([]vast.SelectedBid, []string, error) {
	_ = req
	var warnings []string
	if resp == nil || len(resp.SeatBid) == 0 {
		return nil, warnings, nil
	}

	currency := responseCurrency(resp, cfg)
	filteredBids, warnings := s.filterBids(resp, cfg)
	if len(filteredBids) == 0 {
		return nil, warnings, nil
	}

	sortBids(filteredBids)
	maxToReturn := s.maxBidsToReturn(len(filteredBids), cfg)
	selectedBids := make([]vast.SelectedBid, maxToReturn)
	for i := 0; i < maxToReturn; i++ {
		selectedBids[i] = buildSelectedBid(filteredBids[i], currency, i+1)
	}

	return selectedBids, warnings, nil
}

func responseCurrency(resp *openrtb2.BidResponse, cfg vast.ReceiverConfig) string {
	if resp != nil && resp.Cur != "" {
		return resp.Cur
	}
	return cfg.DefaultCurrency
}

func (s *PriceSelector) filterBids(resp *openrtb2.BidResponse, cfg vast.ReceiverConfig) ([]bidWithSeat, []string) {
	allBids := collectBids(resp)
	warnings := make([]string, 0)
	filteredBids := make([]bidWithSeat, 0, len(allBids))
	for _, bws := range allBids {
		if bws.bid.Price <= 0 {
			warnings = append(warnings, "bid "+bws.bid.ID+" filtered: price <= 0")
			continue
		}
		if !cfg.AllowSkeletonVast && strings.TrimSpace(bws.bid.AdM) == "" {
			warnings = append(warnings, "bid "+bws.bid.ID+" filtered: empty AdM (skeleton VAST not allowed)")
			continue
		}
		filteredBids = append(filteredBids, bws)
	}
	return filteredBids, warnings
}

func (s *PriceSelector) maxBidsToReturn(filteredBidCount int, cfg vast.ReceiverConfig) int {
	maxToReturn := s.maxBids
	if maxToReturn == 0 {
		maxToReturn = cfg.MaxAdsInPod
	}
	if maxToReturn <= 0 {
		maxToReturn = 1
	}
	if maxToReturn > filteredBidCount {
		maxToReturn = filteredBidCount
	}
	return maxToReturn
}

func collectBids(resp *openrtb2.BidResponse) []bidWithSeat {
	if resp == nil {
		return nil
	}
	allBids := make([]bidWithSeat, 0)
	for _, seatBid := range resp.SeatBid {
		for _, bid := range seatBid.Bid {
			allBids = append(allBids, bidWithSeat{bid: bid, seat: seatBid.Seat})
		}
	}
	return allBids
}

func sortBids(filteredBids []bidWithSeat) {
	sort.Slice(filteredBids, func(i, j int) bool {
		bi, bj := filteredBids[i].bid, filteredBids[j].bid
		if bi.Price != bj.Price {
			return bi.Price > bj.Price
		}
		iHasDeal := bi.DealID != ""
		jHasDeal := bj.DealID != ""
		if iHasDeal != jHasDeal {
			return iHasDeal
		}
		return bi.ID < bj.ID
	})
}

func buildSelectedBid(bws bidWithSeat, currency string, sequence int) vast.SelectedBid {
	bid := bws.bid
	adomain := ""
	if len(bid.ADomain) > 0 {
		adomain = bid.ADomain[0]
	}
	durSec := 0
	if bid.Dur > 0 {
		durSec = int(bid.Dur)
	}

	return vast.SelectedBid{
		Bid:      bid,
		Seat:     bws.seat,
		Sequence: sequence,
		Meta: vast.CanonicalMeta{
			BidID:     bid.ID,
			ImpID:     bid.ImpID,
			DealID:    bid.DealID,
			Seat:      bws.seat,
			Price:     bid.Price,
			Currency:  currency,
			Adomain:   adomain,
			Cats:      bid.Cat,
			DurSec:    durSec,
			SlotInPod: sequence,
		},
	}
}

var _ vast.BidSelector = (*PriceSelector)(nil)
