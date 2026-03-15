package vast

import (
	"context"
	"net/http"

	"github.com/prebid/openrtb/v20/openrtb2"
)

// Handler exposes the VAST pipeline behind an HTTP endpoint.
type Handler struct {
	Config      ReceiverConfig
	Selector    BidSelector
	Enricher    Enricher
	Formatter   Formatter
	AuctionFunc func(ctx context.Context, req *openrtb2.BidRequest) (*openrtb2.BidResponse, error)
}

// NewHandler creates a handler with default configuration.
func NewHandler() *Handler {
	return &Handler{Config: DefaultConfig()}
}

// ServeHTTP builds a request, optionally runs an auction, then returns VAST XML.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.Selector == nil || h.Enricher == nil || h.Formatter == nil {
		http.Error(w, "Handler not properly configured", http.StatusInternalServerError)
		return
	}

	bidRequest := h.buildBidRequest(r)
	var bidResponse *openrtb2.BidResponse
	var err error
	if h.AuctionFunc != nil {
		bidResponse, err = h.AuctionFunc(ctx, bidRequest)
		if err != nil {
			http.Error(w, "Auction failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		bidResponse = &openrtb2.BidResponse{}
	}

	result, _ := BuildVastFromBidResponse(ctx, bidRequest, bidResponse, h.Config, h.Selector, h.Enricher, h.Formatter)

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	if result.NoAd {
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.Write(result.VastXML)
}

// buildBidRequest creates a minimal OpenRTB request from HTTP query parameters.
func (h *Handler) buildBidRequest(r *http.Request) *openrtb2.BidRequest {
	query := r.URL.Query()
	podID := query.Get("pod_id")
	if podID == "" {
		podID = "ctv-pod-1"
	}

	return &openrtb2.BidRequest{
		ID: podID,
		Imp: []openrtb2.Imp{{
			ID: "imp-1",
			Video: &openrtb2.Video{
				MIMEs:       []string{"video/mp4"},
				MinDuration: 5,
				MaxDuration: 30,
			},
		}},
		Site: &openrtb2.Site{Page: r.Header.Get("Referer")},
	}
}

// WithConfig overrides the default receiver configuration.
func (h *Handler) WithConfig(cfg ReceiverConfig) *Handler {
	h.Config = cfg
	return h
}

// WithSelector injects the bid selector dependency.
func (h *Handler) WithSelector(s BidSelector) *Handler {
	h.Selector = s
	return h
}

// WithEnricher injects the ad enricher dependency.
func (h *Handler) WithEnricher(e Enricher) *Handler {
	h.Enricher = e
	return h
}

// WithFormatter injects the formatter dependency.
func (h *Handler) WithFormatter(f Formatter) *Handler {
	h.Formatter = f
	return h
}

// WithAuctionFunc injects the auction execution dependency.
func (h *Handler) WithAuctionFunc(fn func(ctx context.Context, req *openrtb2.BidRequest) (*openrtb2.BidResponse, error)) *Handler {
	h.AuctionFunc = fn
	return h
}
