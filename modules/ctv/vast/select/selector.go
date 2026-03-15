// Package bidselect provides selector factories for the CTV VAST pipeline.
package bidselect

import "github.com/prebid/prebid-server/v4/modules/ctv/vast"

// Selector is the public selector interface used by the VAST pipeline.
type Selector interface {
	vast.BidSelector
}

// NewSelector creates a selector implementation for the requested strategy.
func NewSelector(strategy vast.SelectionStrategy) Selector {
	switch strategy {
	case vast.SelectionSingle:
		return NewPriceSelector(1)
	case vast.SelectionTopN:
		return NewPriceSelector(0)
	default:
		return NewPriceSelector(0)
	}
}

// NewSingleSelector returns a selector that yields a single bid.
func NewSingleSelector() Selector {
	return NewPriceSelector(1)
}

// NewTopNSelector returns a selector that uses pod-size limits from config.
func NewTopNSelector() Selector {
	return NewPriceSelector(0)
}

var _ Selector = (*PriceSelector)(nil)
