package adzrvr

import (
	"fmt"
	"net/http"

	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/adapters"
	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/errortypes"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
	"github.com/prebid/prebid-server/v4/util/jsonutil"
)

type adapter struct {
	endpoint string
}

// Builder builds a new instance of the Adzrvr adapter for the given bidder with the given config.
func Builder(bidderName openrtb_ext.BidderName, config config.Adapter, server config.Server) (adapters.Bidder, error) {
	bidder := &adapter{
		endpoint: config.Endpoint,
	}
	return bidder, nil
}

func (a *adapter) MakeRequests(request *openrtb2.BidRequest, reqInfo *adapters.ExtraRequestInfo) ([]*adapters.RequestData, []error) {
	var extRequests []*adapters.RequestData
	var errs []error

	for _, imp := range request.Imp {
		extRequest, err := a.makeRequest(*request, imp)
		if err != nil {
			errs = append(errs, err)
		} else {
			extRequests = append(extRequests, extRequest)
		}
	}
	return extRequests, errs
}

func (a *adapter) makeRequest(ortbRequest openrtb2.BidRequest, ortbImp openrtb2.Imp) (*adapters.RequestData, error) {
	if ortbImp.Banner == nil && ortbImp.Video == nil && ortbImp.Native == nil {
		return nil, &errortypes.BadInput{
			Message: fmt.Sprintf("For Imp ID %s Banner, Video, or Native is undefined", ortbImp.ID),
		}
	}

	var bidderExt adapters.ExtImpBidder
	if err := jsonutil.Unmarshal(ortbImp.Ext, &bidderExt); err != nil {
		return nil, &errortypes.BadInput{
			Message: fmt.Sprintf("Error unmarshalling ExtImpBidder: %s", err.Error()),
		}
	}

	var adzrvrExt openrtb_ext.ImpExtAdzrvr
	if err := jsonutil.Unmarshal(bidderExt.Bidder, &adzrvrExt); err != nil {
		return nil, &errortypes.BadInput{
			Message: fmt.Sprintf("Error unmarshalling ImpExtAdzrvr: %s", err.Error()),
		}
	}

	if adzrvrExt.PlacementID == "" {
		return nil, &errortypes.BadInput{
			Message: fmt.Sprintf("placement_id is required for Imp ID %s", ortbImp.ID),
		}
	}

	ortbImp.TagID = adzrvrExt.PlacementID
	ortbImp.Ext = nil
	ortbRequest.Imp = []openrtb2.Imp{ortbImp}

	// Propagate publisher ID onto site/app if provided.
	if adzrvrExt.PublisherID != "" {
		if ortbRequest.Site != nil {
			siteCopy := *ortbRequest.Site
			if siteCopy.Publisher == nil {
				siteCopy.Publisher = &openrtb2.Publisher{}
			}
			pubCopy := *siteCopy.Publisher
			pubCopy.ID = adzrvrExt.PublisherID
			siteCopy.Publisher = &pubCopy
			ortbRequest.Site = &siteCopy
		} else if ortbRequest.App != nil {
			appCopy := *ortbRequest.App
			if appCopy.Publisher == nil {
				appCopy.Publisher = &openrtb2.Publisher{}
			}
			pubCopy := *appCopy.Publisher
			pubCopy.ID = adzrvrExt.PublisherID
			appCopy.Publisher = &pubCopy
			ortbRequest.App = &appCopy
		}
	}

	requestJSON, err := jsonutil.Marshal(ortbRequest)
	if err != nil {
		return nil, err
	}

	requestData := &adapters.RequestData{
		Method:  http.MethodPost,
		Uri:     a.endpoint,
		Body:    requestJSON,
		Headers: setHeaders(&ortbRequest),
		ImpIDs:  openrtb_ext.GetImpIDs(ortbRequest.Imp),
	}
	return requestData, nil
}

func (a *adapter) MakeBids(internalRequest *openrtb2.BidRequest, externalRequest *adapters.RequestData, response *adapters.ResponseData) (*adapters.BidderResponse, []error) {
	if response.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if response.StatusCode == http.StatusBadRequest {
		return nil, []error{&errortypes.BadInput{
			Message: fmt.Sprintf("Unexpected status code: %d.", response.StatusCode),
		}}
	}

	if response.StatusCode != http.StatusOK {
		return nil, []error{&errortypes.BadServerResponse{
			Message: fmt.Sprintf("Unexpected status code: %d.", response.StatusCode),
		}}
	}

	var bidResp openrtb2.BidResponse
	if err := jsonutil.Unmarshal(response.Body, &bidResp); err != nil {
		return nil, []error{err}
	}

	// Build a lookup map once so media-type resolution is O(1) per bid
	// rather than O(imps) per bid.
	impTypeMap := make(map[string]openrtb_ext.BidType, len(internalRequest.Imp))
	for _, imp := range internalRequest.Imp {
		switch {
		case imp.Video != nil:
			impTypeMap[imp.ID] = openrtb_ext.BidTypeVideo
		case imp.Banner != nil:
			impTypeMap[imp.ID] = openrtb_ext.BidTypeBanner
		case imp.Native != nil:
			impTypeMap[imp.ID] = openrtb_ext.BidTypeNative
		default:
			impTypeMap[imp.ID] = openrtb_ext.BidTypeVideo
		}
	}

	// Pre-count total bids so the slice is allocated at the right capacity.
	totalBids := 0
	for _, sb := range bidResp.SeatBid {
		totalBids += len(sb.Bid)
	}
	bidResponse := adapters.NewBidderResponseWithBidsCapacity(totalBids)

	var errs []error
	for _, sb := range bidResp.SeatBid {
		for i := range sb.Bid {
			if sb.Bid[i].Price <= 0 {
				continue
			}
			bidType, ok := impTypeMap[sb.Bid[i].ImpID]
			if !ok {
				errs = append(errs, &errortypes.BadServerResponse{
					Message: fmt.Sprintf("bid references unknown imp ID %q", sb.Bid[i].ImpID),
				})
				continue
			}
			bidResponse.Bids = append(bidResponse.Bids, &adapters.TypedBid{
				Bid:     &sb.Bid[i],
				BidType: bidType,
			})
		}
	}
	return bidResponse, errs
}

func setHeaders(request *openrtb2.BidRequest) http.Header {
	headers := http.Header{}
	headers.Add("Content-Type", "application/json;charset=utf-8")
	headers.Add("Accept", "application/json")
	headers.Add("X-OpenRTB-Version", "2.5")

	if request.Device != nil {
		if request.Device.UA != "" {
			headers.Add("User-Agent", request.Device.UA)
		}
		if request.Device.IP != "" {
			headers.Add("X-Forwarded-For", request.Device.IP)
		} else if request.Device.IPv6 != "" {
			headers.Add("X-Forwarded-For", request.Device.IPv6)
		}
	}

	if request.Site != nil && request.Site.Page != "" {
		headers.Add("Referer", request.Site.Page)
	}
	return headers
}
