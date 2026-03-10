package openrtb_ext

// ImpExtAdzrvr defines the contract for bidrequest.imp.ext.prebid.bidder
// when using the adzrvr adapter.
type ImpExtAdzrvr struct {
	PlacementID string `json:"placement_id"`
	PublisherID string `json:"publisher_id,omitempty"`
}
