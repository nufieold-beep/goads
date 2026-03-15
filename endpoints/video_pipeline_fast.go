package endpoints

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/prebid/prebid-server/v4/metrics"
	"github.com/valyala/fasthttp"
)

var (
	fastImpressionDuplicateLogCount int64
	fastImpressionBeaconLogCount    int64
	fastHTTPMethodPost              = []byte(fasthttp.MethodPost)
	fastHTTPMethodGet               = []byte(fasthttp.MethodGet)
	fastHTTPMethodDelete            = []byte(fasthttp.MethodDelete)
	fastHTTPSBytes                  = []byte("https")
)

func shouldLogSampledCounter(counter *int64, n int64) (int64, bool) {
	cur := atomic.AddInt64(counter, 1)
	return cur, cur == 1 || cur%n == 0
}

func (h *VideoPipelineHandler) HandleFastVAST(ctx *fasthttp.RequestCtx) {
	start := time.Now()
	req, err := h.parsePlayerRequestFast(ctx)
	if err != nil {
		h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
		log.Printf("HandleFastVAST parsePlayerRequest: %v", err)
		writeFastTextError(ctx, fasthttp.StatusBadRequest, "bad request")
		return
	}

	adsCfg, err := h.resolveAdServerConfig(req.PlacementID)
	if err != nil {
		h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
		writeFastTextError(ctx, fasthttp.StatusNotFound, "placement not found")
		return
	}
	if !adsCfg.Active {
		h.writeFastVASTResponse(ctx, emptyVAST())
		return
	}
	cfgCopy := *adsCfg
	cfgCopy.RequestBaseURL = requestBaseURLFast(ctx)
	adsCfg = &cfgCopy

	requestCtx, cancel := withResolvedTimeout(ctx, resolveEndpointTimeout(adsCfg, req))
	defer cancel()

	h.videoStats.incRequestBatch(adsCfg.PublisherID, adsCfg.AdvertiserID)
	h.recordRequestMetric(req, adsCfg)
	resp, err := h.runDemandWaterfall(requestCtx, req, adsCfg, InboundVAST)
	if err != nil {
		log.Printf("HandleFastVAST: no fill for placement %s after all demand sources", req.PlacementID)
		h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
		h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))
		h.writeFastVASTResponse(ctx, emptyVAST())
		return
	}

	if !resp.NoFill {
		h.videoStats.incFillBatch(adsCfg.PublisherID, adsCfg.AdvertiserID)
		h.videoStats.incDimFill(adsCfg, req, resp, resolveDemandType(adsCfg))
		h.recordOpportunityMetric(req, adsCfg, resp)
		h.recordBidReport(adsCfg, req, resp, "win")
	}
	h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
	h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))

	h.writeFastVASTResponse(ctx, resp.VASTXml)
}

func (h *VideoPipelineHandler) HandleFastORTB(ctx *fasthttp.RequestCtx) {
	start := time.Now()
	req, err := h.parsePlayerRequestFast(ctx)
	if err != nil {
		h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
		log.Printf("HandleFastORTB parsePlayerRequest: %v", err)
		writeFastTextError(ctx, fasthttp.StatusBadRequest, "bad request")
		return
	}

	adsCfg, err := h.resolveAdServerConfig(req.PlacementID)
	if err != nil {
		h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
		writeFastTextError(ctx, fasthttp.StatusNotFound, "placement not found")
		return
	}
	if !adsCfg.Active {
		ctx.SetStatusCode(fasthttp.StatusNoContent)
		return
	}
	cfgCopy := *adsCfg
	cfgCopy.RequestBaseURL = requestBaseURLFast(ctx)
	adsCfg = &cfgCopy

	requestCtx, cancel := withResolvedTimeout(ctx, resolveEndpointTimeout(adsCfg, req))
	defer cancel()

	h.videoStats.incRequestBatch(adsCfg.PublisherID, adsCfg.AdvertiserID)
	h.recordRequestMetric(req, adsCfg)
	resp, err := h.runDemandWaterfall(requestCtx, req, adsCfg, InboundORTB)
	if err != nil {
		log.Printf("HandleFastORTB: no fill for placement %s after all demand sources", req.PlacementID)
		h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
		h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))
		ctx.SetStatusCode(fasthttp.StatusNoContent)
		return
	}

	if !resp.NoFill && resp.BidResp != nil {
		if win, bidder, werr := h.extractWinningBid(resp.BidResp, adsCfg); werr == nil && win != nil {
			auctionID := resp.AuctionID
			if auctionID == "" && resp.BidResp != nil {
				auctionID = resp.BidResp.ID
			}
			resolvedNURL := resolveAuctionMacros(win.NURL, win, auctionID, bidder)
			resolvedBURL := resolveAuctionMacros(win.BURL, win, auctionID, bidder)
			hasAdM := win.AdM != ""

			if resolvedNURL != "" && hasAdM {
				h.fireWinNotice(resolvedNURL)
			}
			if resolvedBURL != "" {
				h.pendingBURLs.Store(impressionKey{AuctionID: auctionID, BidID: win.BidID}, pendingBURL{
					URL:       resolvedBURL,
					ExpiresAt: time.Now().Add(5 * time.Minute),
				})
			}

			for si := range resp.BidResp.SeatBid {
				sb := &resp.BidResp.SeatBid[si]
				for bi := range sb.Bid {
					b := &sb.Bid[bi]
					if isEmptyAdM(b.AdM) {
						b.AdM = ""
					}
					if b.ID == win.BidID && (bidder == "" || sb.Seat == bidder) {
						b.BURL = ""
						resp.BURL = resolvedBURL
						if hasAdM {
							b.NURL = ""
						}
					}
				}
			}
		}
	}

	if !resp.NoFill {
		h.videoStats.incFillBatch(adsCfg.PublisherID, adsCfg.AdvertiserID)
		h.videoStats.incDimFill(adsCfg, req, resp, resolveDemandType(adsCfg))
		h.recordOpportunityMetric(req, adsCfg, resp)
		h.recordBidReport(adsCfg, req, resp, "win")
	}
	h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
	h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	encoder := json.NewEncoder(&trimTrailingNewlineWriter{writer: ctx})
	if merr := encoder.Encode(resp.BidResp); merr != nil {
		writeFastTextError(ctx, fasthttp.StatusInternalServerError, "marshal bid response failed")
		return
	}
}

func (h *VideoPipelineHandler) HandleFastTracking(ctx *fasthttp.RequestCtx) {
	q := ctx.QueryArgs()
	auctionIDRaw := q.Peek("auction_id")
	bidIDRaw := q.Peek("bid_id")
	eventRaw := q.Peek("event")
	if len(auctionIDRaw) == 0 || len(bidIDRaw) == 0 || len(eventRaw) == 0 {
		writeFastTextError(ctx, fasthttp.StatusBadRequest, "auction_id, bid_id, and event are required")
		return
	}

	priceVal := parseFastFloat(q.Peek("price"))
	ev := TrackingEvent{
		AuctionID:   copyFastArg(auctionIDRaw),
		BidID:       copyFastArg(bidIDRaw),
		Bidder:      fastArgString(q, "bidder"),
		PlacementID: fastArgString(q, "placement_id"),
		Event:       fastEventType(eventRaw),
		CrID:        fastArgString(q, "crid"),
		Price:       priceVal,
		ADomain:     fastArgString(q, "adom"),
		ReceivedAt:  time.Now(),
	}

	h.tracking.record(ev)
	var cfg *AdServerConfig
	if ev.PlacementID != "" {
		if resolvedCfg, err := h.resolveAdServerConfig(ev.PlacementID); err == nil {
			cfg = resolvedCfg
		}
	}
	var dk *auctionDimKey
	if ev.AuctionID != "" {
		h.videoStats.mu.Lock()
		dk = h.videoStats.auctionDims[ev.AuctionID]
		h.videoStats.mu.Unlock()
	}
	h.recordTrackingMetric(ev, cfg, dk)

	if ev.Event == EventComplete {
		if cfg != nil {
			h.videoStats.incComplete(cfg.PublisherID)
			h.videoStats.incAdvertiserComplete(cfg.AdvertiserID)
		}
		h.videoStats.incDimComplete(ev.AuctionID)
	}

	h.serveFastPixelGIF(ctx)
}

func (h *VideoPipelineHandler) HandleFastImpression(ctx *fasthttp.RequestCtx) {
	q := ctx.QueryArgs()
	auctionIDRaw := q.Peek("auction_id")
	bidIDRaw := q.Peek("bid_id")
	if len(auctionIDRaw) == 0 || len(bidIDRaw) == 0 {
		writeFastTextError(ctx, fasthttp.StatusBadRequest, "auction_id and bid_id are required")
		return
	}
	auctionID := copyFastArg(auctionIDRaw)
	bidID := copyFastArg(bidIDRaw)
	now := time.Now()
	key := impressionKey{AuctionID: auctionID, BidID: bidID}

	if h.firedImpressions.SeenOrAdd(key, now.Unix()) {
		if count, emit := shouldLogSampledCounter(&fastImpressionDuplicateLogCount, 50); emit {
			log.Printf("HandleFastImpression: duplicate beacon for auction=%s bid=%s from %s — skipped [sampled 1/%d, count=%d]", auctionID, bidID, maskIP(extractClientIPFast(ctx)), 50, count)
		}
		h.serveFastPixelGIF(ctx)
		return
	}

	dk := h.videoStats.incDimImpression(auctionID)
	placementID := ""
	if dk != nil {
		placementID = dk.Placement
	}
	if placementID == "" {
		placementID = fastArgString(q, "placement_id")
	}
	bidder := ""
	if dk != nil {
		bidder = dk.Bidder
	}
	if bidder == "" {
		bidder = fastArgString(q, "bidder")
	}
	priceVal := 0.0
	if dk != nil {
		priceVal = dk.PriceCPM
	} else {
		priceVal = parseFastFloat(q.Peek("price"))
	}
	crID := fastArgString(q, "crid")
	adomain := fastArgString(q, "adom")

	if count, emit := shouldLogSampledCounter(&fastImpressionBeaconLogCount, 200); emit {
		log.Printf("HandleFastImpression: beacon fired auction=%s bid=%s placement=%s from ip=%s [sampled 1/%d, count=%d]", auctionID, bidID, placementID, maskIP(extractClientIPFast(ctx)), 200, count)
	}

	h.tracking.record(TrackingEvent{
		AuctionID:   auctionID,
		BidID:       bidID,
		Bidder:      bidder,
		PlacementID: placementID,
		Event:       EventImpression,
		CrID:        crID,
		Price:       priceVal,
		ADomain:     adomain,
		ReceivedAt:  now,
	})

	h.metricsEng.RecordImps(metrics.ImpLabels{VideoImps: true})

	var cfg *AdServerConfig
	if dk != nil {
		h.videoStats.incImpressionBatch(dk.PublisherID, dk.AdvertiserID, dk.PriceCPM)
	} else if placementID != "" {
		if resolvedCfg, err := h.resolveAdServerConfig(placementID); err == nil {
		cfg = resolvedCfg
		h.videoStats.incImpressionBatch(resolvedCfg.PublisherID, resolvedCfg.AdvertiserID, priceVal)
		}
	}
	h.recordImpressionMetric(placementID, auctionID, bidID, bidder, crID, priceVal, cfg, dk)

	if val, ok := h.pendingBURLs.LoadAndDelete(key); ok {
		if pb, ok := val.(pendingBURL); ok && now.Before(pb.ExpiresAt) {
			h.fireBillingNotice(pb.URL)
		}
	}

	h.serveFastPixelGIF(ctx)
}

func (h *VideoPipelineHandler) HandleFastTrackingEvents(ctx *fasthttp.RequestCtx) {
	if !requireFastDashAuth(ctx) {
		return
	}
	events := h.tracking.all()
	q := ctx.QueryArgs()
	offset := parseFastIntDefault(q.Peek("offset"), 0)
	limit := parseFastIntDefault(q.Peek("limit"), 1000)
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 1000
	}
	total := len(events)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	page := events[offset:end]

	resp := struct {
		Total  int             `json:"total"`
		Offset int             `json:"offset"`
		Limit  int             `json:"limit"`
		Events []TrackingEvent `json:"events"`
	}{Total: total, Offset: offset, Limit: limit, Events: page}
	writeFastJSONValue(ctx, fasthttp.StatusOK, resp)
}

func (h *VideoPipelineHandler) HandleFastVideoStats(ctx *fasthttp.RequestCtx) {
	if !requireFastDashAuth(ctx) {
		return
	}
	payload := h.snapshotVideoMetrics()
	ctx.Response.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	writeFastJSONValue(ctx, fasthttp.StatusOK, payload)
}

func (h *VideoPipelineHandler) HandleFastResetStats(ctx *fasthttp.RequestCtx) {
	if !requireFastDashAuth(ctx) {
		return
	}
	if !bytes.Equal(ctx.Method(), fastHTTPMethodPost) {
		writeFastTextError(ctx, fasthttp.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := h.resetVideoMetrics(); err != nil {
		writeFastTextError(ctx, fasthttp.StatusInternalServerError, err.Error())
		return
	}
	writeFastJSONBytes(ctx, fasthttp.StatusOK, []byte(`{"ok":true}`))
}

func (h *VideoPipelineHandler) HandleFastDashboardConfig(ctx *fasthttp.RequestCtx) {
	if !requireFastDashAuth(ctx) {
		return
	}
	var resp dashboardConfigResponse
	if h.cfg != nil {
		resp.AuctionTimeouts.Default = int(h.cfg.AuctionTimeouts.Default)
		resp.AuctionTimeouts.Max = int(h.cfg.AuctionTimeouts.Max)
		resp.HTTPClient.MaxIdleConns = h.cfg.Client.MaxIdleConns
		resp.HTTPClient.MaxIdleConnsPerHost = h.cfg.Client.MaxIdleConnsPerHost
		resp.HTTPClient.IdleConnTimeout = h.cfg.Client.IdleConnTimeout
		resp.Compression.Response.EnableGzip = h.cfg.Compression.Response.GZIP
		resp.Compression.Request.EnableGzip = h.cfg.Compression.Request.GZIP
	}
	ctx.Response.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	writeFastJSONValue(ctx, fasthttp.StatusOK, resp)
}

func (h *VideoPipelineHandler) HandleFastAdServerConfig(ctx *fasthttp.RequestCtx, placementID string) {
	if !requireFastDashAuth(ctx) {
		return
	}
	method := ctx.Method()
	switch {
	case bytes.Equal(method, fastHTTPMethodGet):
		h.configStore.mu.RLock()
		configs := make([]AdServerConfig, 0, len(h.configStore.configs))
		for _, c := range h.configStore.configs {
			configs = append(configs, *c)
		}
		h.configStore.mu.RUnlock()
		writeFastJSONValue(ctx, fasthttp.StatusOK, configs)
	case bytes.Equal(method, fastHTTPMethodPost):
		var cfg AdServerConfig
		body := ctx.PostBody()
		if len(body) > 65536 {
			writeFastTextError(ctx, fasthttp.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		if err := json.Unmarshal(body, &cfg); err != nil {
			writeFastTextError(ctx, fasthttp.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		if cfg.PlacementID == "" {
			writeFastTextError(ctx, fasthttp.StatusBadRequest, "placement_id is required")
			return
		}
		h.configStore.set(&cfg)
		writeFastJSONValueWithStatus(ctx, fasthttp.StatusCreated, cfg)
	case bytes.Equal(method, fastHTTPMethodDelete):
		if placementID == "" {
			writeFastTextError(ctx, fasthttp.StatusBadRequest, "placement_id is required")
			return
		}
		if decoded, err := url.PathUnescape(placementID); err == nil {
			placementID = decoded
		}
		h.configStore.remove(placementID)
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	default:
		writeFastTextError(ctx, fasthttp.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *VideoPipelineHandler) parsePlayerRequestFast(ctx *fasthttp.RequestCtx) (*PlayerRequest, error) {
	pr := &PlayerRequest{}
	if bytes.Equal(ctx.Method(), fastHTTPMethodPost) {
		body := ctx.PostBody()
		if len(body) > 65536 {
			return nil, fmt.Errorf("decode body: request too large")
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, pr); err != nil {
				return nil, fmt.Errorf("decode body: %w", err)
			}
		}
	}

	q := ctx.QueryArgs()
	setPersistentString := func(key string, dst *string) {
		if v := fastArgString(q, key); v != "" {
			*dst = v
		}
	}
	setTransientString := func(key string, dst *string) {
		if v := fastArgView(q, key); v != "" {
			*dst = v
		}
	}
	setURLString := func(key string, dst *string) {
		raw := q.Peek(key)
		if len(raw) > 0 {
			v := bytesToStringView(raw)
			if bytes.IndexByte(raw, '%') >= 0 {
			if dec, err := url.PathUnescape(v); err == nil {
				v = dec
			}
			}
			*dst = v
		}
	}

	if v := fastArgString(q, "placement_id"); v != "" {
		pr.PlacementID = v
	} else if v := fastArgString(q, "sid"); v != "" {
		pr.PlacementID = v
	}
	setTransientString("publisher_id", &pr.PublisherID)
	setPersistentString("app_bundle", &pr.AppBundle)
	setPersistentString("domain", &pr.Domain)
	setURLString("page_url", &pr.PageURL)
	setTransientString("gdpr", &pr.GDPR)
	setTransientString("gdpr_consent", &pr.Consent)
	setTransientString("us_privacy", &pr.CCPA)
	setTransientString("gpp", &pr.GPP)
	setTransientString("gpp_sid", &pr.GPPSID)

	pr.Width = parseFastInt64Default(q.Peek("w"), pr.Width)
	pr.Height = parseFastInt64Default(q.Peek("h"), pr.Height)
	pr.MinDuration = parseFastIntDefault(q.Peek("min_dur"), pr.MinDuration)
	pr.MaxDuration = parseFastIntDefault(q.Peek("max_dur"), pr.MaxDuration)

	setTransientString("ip", &pr.IP)
	setTransientString("ip6", &pr.IPv6)
	setTransientString("ua", &pr.UA)
	setTransientString("app_name", &pr.AppName)
	setURLString("app_store_url", &pr.AppStoreURL)
	setTransientString("device_make", &pr.DeviceMake)
	setTransientString("device_model", &pr.DeviceModel)
	pr.DeviceType = parseFastIntDefault(q.Peek("device_type"), pr.DeviceType)
	setTransientString("os", &pr.DeviceOS)
	setTransientString("ifa", &pr.IFA)
	setTransientString("ifa_type", &pr.IFAType)
	setPersistentString("country_code", &pr.CountryCode)
	pr.DNT = int8(parseFastIntDefault(q.Peek("dnt"), int(pr.DNT)))
	setTransientString("ct_genre", &pr.ContentGenre)
	setTransientString("ct_lang", &pr.ContentLang)
	setTransientString("ct_rating", &pr.ContentRating)
	pr.LiveStream = int8(parseFastIntDefault(q.Peek("ct_livestream"), int(pr.LiveStream)))
	pr.ContentLen = parseFastInt64Default(q.Peek("ct_len"), pr.ContentLen)
	setTransientString("app_id", &pr.AppID)
	setTransientString("osv", &pr.OSVersion)
	pr.LMT = int8(parseFastIntDefault(q.Peek("lmt"), int(pr.LMT)))
	setTransientString("language", &pr.Language)
	setTransientString("user_id", &pr.UserID)
	setTransientString("ct_title", &pr.ContentTitle)
	setTransientString("ct_series", &pr.ContentSeries)
	setTransientString("ct_season", &pr.ContentSeason)
	setTransientString("ct_url", &pr.ContentURL)
	setTransientString("ct_cat", &pr.ContentCat)
	pr.ContentProdQ = parseFastIntDefault(q.Peek("ct_prodq"), pr.ContentProdQ)
	setTransientString("site_name", &pr.SiteName)
	setTransientString("site_cat", &pr.SiteCat)
	setTransientString("site_keywords", &pr.SiteKeywords)
	setTransientString("page_ref", &pr.PageRef)
	setTransientString("app_ver", &pr.AppVer)
	pr.Skip = int8(parseFastIntDefault(q.Peek("skip"), int(pr.Skip)))
	pr.StartDelay = parseFastIntDefault(q.Peek("start_delay"), pr.StartDelay)
	if v := q.Peek("secure"); len(v) > 0 {
		pr.Secure = int8(parseFastIntDefault(v, int(pr.Secure)))
	} else if ctx.IsTLS() || bytes.EqualFold(ctx.Request.Header.Peek("X-Forwarded-Proto"), fastHTTPSBytes) {
		pr.Secure = 1
	}
	pr.TMax = parseFastInt64Default(q.Peek("tmax"), pr.TMax)
	setTransientString("bcat", &pr.BCat)
	setTransientString("badv", &pr.BAdv)

	if pr.IP == "" || pr.IPv6 == "" {
		if xfwd := ctx.Request.Header.Peek("X-Forwarded-For"); len(xfwd) > 0 {
			if idx := bytes.IndexByte(xfwd, ','); idx >= 0 {
				xfwd = xfwd[:idx]
			}
			ip := bytesToStringView(bytes.TrimSpace(xfwd))
			if pr.IP == "" {
				pr.IP = ip
			}
			if pr.IPv6 == "" && strings.Contains(ip, ":") {
				pr.IPv6 = ip
			}
		} else if pr.IP == "" {
			host, _, err := net.SplitHostPort(ctx.RemoteAddr().String())
			if err != nil {
				host = ctx.RemoteAddr().String()
			}
			if pr.IP == "" {
				pr.IP = host
			}
			if pr.IPv6 == "" && strings.Contains(host, ":") {
				pr.IPv6 = host
			}
		}
	}
	if pr.IP == "" && pr.IPv6 != "" {
		pr.IP = pr.IPv6
	}
	if pr.IP == "" {
		pr.IP = extractClientIPFast(ctx)
	}
	if pr.UA == "" {
		pr.UA = bytesToStringView(ctx.Request.Header.UserAgent())
	}

	if pr.PlacementID == "" {
		return nil, fmt.Errorf("placement_id is required")
	}

	return pr, nil
}

func (h *VideoPipelineHandler) writeFastVASTResponse(ctx *fasthttp.RequestCtx, vastXML string) {
	hdr := &ctx.Response.Header
	hdr.Set("Content-Type", "application/xml; charset=utf-8")
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("Connection", "keep-alive")
	ctx.SetStatusCode(fasthttp.StatusOK)
	if vastXML == emptyVASTString {
		hdr.Set("Content-Length", emptyVASTLenStr)
		ctx.SetBody(emptyVASTBytes)
		return
	}
	ctx.SetBodyString(vastXML)
}

func (h *VideoPipelineHandler) serveFastPixelGIF(ctx *fasthttp.RequestCtx) {
	hdr := &ctx.Response.Header
	hdr.Set("Content-Type", "image/gif")
	hdr.Set("Content-Length", pixelGIFContentLength)
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("Connection", "keep-alive")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody(pixelGIF)
}

func requestBaseURLFast(ctx *fasthttp.RequestCtx) string {
	scheme := bytesToStringView(ctx.URI().Scheme())
	if scheme == "" {
		scheme = "http"
	}
	if forwarded := bytesToStringView(ctx.Request.Header.Peek("X-Forwarded-Proto")); forwarded != "" {
		scheme = forwarded
	}
	host := bytesToStringView(ctx.Host())
	if host == "" {
		host = bytesToStringView(ctx.URI().Host())
	}
	return scheme + "://" + host
}

func extractClientIPFast(ctx *fasthttp.RequestCtx) string {
	if xff := string(ctx.Request.Header.Peek("X-Forwarded-For")); xff != "" {
		if idx := strings.Index(xff, ","); idx > 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	if xri := string(ctx.Request.Header.Peek("X-Real-IP")); xri != "" {
		return xri
	}
	if ip := ctx.RemoteIP(); ip != nil {
		return ip.String()
	}
	return ctx.RemoteAddr().String()
}

func requireFastDashAuth(ctx *fasthttp.RequestCtx) bool {
	return FastDashboardAuth(ctx)
}

func writeFastTextError(ctx *fasthttp.RequestCtx, status int, message string) {
	ctx.SetStatusCode(status)
	ctx.SetContentType("text/plain; charset=utf-8")
	ctx.SetBodyString(message)
}

func writeFastJSONValue(ctx *fasthttp.RequestCtx, status int, value interface{}) {
	data, err := json.Marshal(value)
	if err != nil {
		writeFastTextError(ctx, fasthttp.StatusInternalServerError, err.Error())
		return
	}
	writeFastJSONBytes(ctx, status, data)
}

func writeFastJSONValueWithStatus(ctx *fasthttp.RequestCtx, status int, value interface{}) {
	writeFastJSONValue(ctx, status, value)
}

func writeFastJSONBytes(ctx *fasthttp.RequestCtx, status int, data []byte) {
	ctx.SetStatusCode(status)
	ctx.SetContentType("application/json")
	ctx.SetBody(data)
}

func fastArgString(q *fasthttp.Args, key string) string {
	return copyFastArg(q.Peek(key))
}

func fastArgView(q *fasthttp.Args, key string) string {
	return bytesToStringView(q.Peek(key))
}

func copyFastArg(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	return string(raw)
}

func bytesToStringView(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(raw), len(raw))
}

type trimTrailingNewlineWriter struct {
	writer interface{ Write([]byte) (int, error) }

	pendingNewline bool
}

func (w *trimTrailingNewlineWriter) Write(p []byte) (int, error) {
	written := len(p)
	if len(p) == 0 {
		return 0, nil
	}
	if w.pendingNewline {
		if _, err := w.writer.Write([]byte{'\n'}); err != nil {
			return 0, err
		}
		w.pendingNewline = false
	}
	if p[len(p)-1] == '\n' {
		w.pendingNewline = true
		p = p[:len(p)-1]
	}
	if len(p) == 0 {
		return written, nil
	}
	_, err := w.writer.Write(p)
	if err != nil {
		return 0, err
	}
	return written, nil
}

func fastEventType(raw []byte) EventType {
	switch {
	case bytes.Equal(raw, []byte("impression")):
		return EventImpression
	case bytes.Equal(raw, []byte("start")):
		return EventStart
	case bytes.Equal(raw, []byte("firstQuartile")):
		return EventFirstQuartile
	case bytes.Equal(raw, []byte("midpoint")):
		return EventMidpoint
	case bytes.Equal(raw, []byte("thirdQuartile")):
		return EventThirdQuartile
	case bytes.Equal(raw, []byte("complete")):
		return EventComplete
	case bytes.Equal(raw, []byte("click")):
		return EventClick
	default:
		return EventType(copyFastArg(raw))
	}
}

func parseFastIntDefault(raw []byte, fallback int) int {
	if len(raw) == 0 {
		return fallback
	}
	if parsed, err := strconv.Atoi(string(raw)); err == nil {
		return parsed
	}
	return fallback
}

func parseFastInt64Default(raw []byte, fallback int64) int64 {
	if len(raw) == 0 {
		return fallback
	}
	if parsed, err := strconv.ParseInt(string(raw), 10, 64); err == nil {
		return parsed
	}
	return fallback
}

func parseFastFloat(raw []byte) float64 {
	if len(raw) == 0 {
		return 0
	}
	parsed, _ := strconv.ParseFloat(string(raw), 64)
	return parsed
}