// Package vast provides CTV-oriented VAST processing primitives.
package vast

import (
	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/modules/ctv/vast/model"
)

type ReceiverType string

const (
	// ReceiverGAMSSU targets Google Ad Manager server-side unified flows.
	ReceiverGAMSSU ReceiverType = "GAM_SSU"
	// ReceiverGeneric is a fallback receiver with generic formatting assumptions.
	ReceiverGeneric ReceiverType = "GENERIC"
)

type SelectionStrategy string

const (
	// SelectionSingle returns only the top-ranked bid.
	SelectionSingle SelectionStrategy = "SINGLE"
	// SelectionTopN returns up to the configured pod size.
	SelectionTopN SelectionStrategy = "TOP_N"
	// SelectionMaxRevenue favors the highest total price outcome.
	SelectionMaxRevenue SelectionStrategy = "max_revenue"
	// SelectionMinDuration favors the shortest eligible duration.
	SelectionMinDuration SelectionStrategy = "min_duration"
	// SelectionBalanced leaves room for future balancing strategies.
	SelectionBalanced SelectionStrategy = "balanced"
)

type CollisionPolicy string

const (
	// CollisionReject rejects conflicting enrichment values.
	CollisionReject CollisionPolicy = "reject"
	// CollisionWarn keeps the conflict but records a warning.
	CollisionWarn CollisionPolicy = "warn"
	// CollisionIgnore silently ignores the conflict.
	CollisionIgnore CollisionPolicy = "ignore"
)

// VastResult is the final output of a VAST processing run.
type VastResult struct {
	VastXML  []byte
	NoAd     bool
	Warnings []string
	Errors   []error
	Selected []SelectedBid
}

// SelectedBid couples a chosen OpenRTB bid with normalized metadata.
type SelectedBid struct {
	Bid      openrtb2.Bid
	Seat     string
	Sequence int
	Meta     CanonicalMeta
}

// CanonicalMeta stores receiver-agnostic metadata extracted from a bid.
type CanonicalMeta struct {
	BidID     string
	ImpID     string
	DealID    string
	Seat      string
	Price     float64
	Currency  string
	Adomain   string
	Cats      []string
	DurSec    int
	SlotInPod int
}

// ReceiverConfig controls selection, enrichment, and formatting behavior.
type ReceiverConfig struct {
	Receiver           ReceiverType
	DefaultCurrency    string
	VastVersionDefault string
	MaxAdsInPod        int
	SelectionStrategy  SelectionStrategy
	CollisionPolicy    CollisionPolicy
	Placement          PlacementRules
	AllowSkeletonVast  bool
	Debug              bool
}

// PlacementRules groups optional pricing, advertiser, and category rules.
type PlacementRules struct {
	Pricing             PricingRules
	Advertiser          AdvertiserRules
	Categories          CategoryRules
	PricingPlacement    string
	AdvertiserPlacement string
	Debug               bool
}

// PricingRules defines price bounds and currency expectations.
type PricingRules struct {
	FloorCPM   float64
	CeilingCPM float64
	Currency   string
}

// AdvertiserRules constrains domains allowed into the final pod.
type AdvertiserRules struct {
	BlockedDomains []string
	AllowedDomains []string
}

// CategoryRules constrains category values allowed into the final pod.
type CategoryRules struct {
	BlockedCategories []string
	AllowedCategories []string
}

// BidSelector chooses the bids that should proceed into VAST processing.
type BidSelector interface {
	Select(req *openrtb2.BidRequest, resp *openrtb2.BidResponse, cfg ReceiverConfig) ([]SelectedBid, []string, error)
}

// Enricher mutates a VAST ad using canonical bid metadata.
type Enricher interface {
	Enrich(ad *model.Ad, meta CanonicalMeta, cfg ReceiverConfig) ([]string, error)
}

// EnrichedAd is a VAST ad paired with the metadata used to enrich it.
type EnrichedAd struct {
	Ad       *model.Ad
	Meta     CanonicalMeta
	Sequence int
}

// Formatter serializes enriched ads into the final VAST XML document.
type Formatter interface {
	Format(ads []EnrichedAd, cfg ReceiverConfig) ([]byte, []string, error)
}
