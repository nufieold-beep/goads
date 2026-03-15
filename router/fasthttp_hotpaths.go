package router

import (
	"net/http"
	"strings"

	"github.com/prebid/prebid-server/v4/endpoints"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	applyHTTPResponseHeaders(w, req)
	if req.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	r.Router.ServeHTTP(w, req)
}

func (r *Router) fastHandler() fasthttp.RequestHandler {
	fallback := r.fastHTTPFallback
	if fallback == nil {
		fallback = fasthttpadaptor.NewFastHTTPHandler(r.Router)
	}

	return func(ctx *fasthttp.RequestCtx) {
		applyFastHTTPResponseHeaders(ctx)
		if ctx.IsOptions() {
			ctx.SetStatusCode(fasthttp.StatusNoContent)
			return
		}

		if r.dispatchFastHotPath(ctx) {
			return
		}

		fallback(ctx)
	}
}

func (r *Router) dispatchFastHotPath(ctx *fasthttp.RequestCtx) bool {
	path := string(ctx.Path())
	method := string(ctx.Method())

	switch path {
	case "/":
		if method == fasthttp.MethodGet {
			ctx.Redirect("/dashboard", fasthttp.StatusFound)
			return true
		}
	case "/status":
		if method == fasthttp.MethodGet {
			if r.fastStatus != nil {
				r.fastStatus(ctx)
				return true
			}
		}
	case "/version":
		if method == fasthttp.MethodGet {
			ctx.SetStatusCode(fasthttp.StatusOK)
			ctx.SetContentType("application/json")
			ctx.SetBody(r.versionResponse)
			return true
		}
	case "/dashboard/login":
		if method == fasthttp.MethodGet {
			endpoints.HandleFastDashboardLoginGet(ctx)
			return true
		}
		if method == fasthttp.MethodPost {
			endpoints.HandleFastDashboardLoginPost(ctx)
			return true
		}
	case "/dashboard/logout":
		if method == fasthttp.MethodPost {
			endpoints.HandleFastDashboardLogout(ctx)
			return true
		}
	case "/dashboard", "/dashboard/bidder", "/dashboard/audience", "/dashboard/optimization", "/dashboard/quality", "/dashboard/backend":
		if method == fasthttp.MethodGet {
			if !endpoints.FastDashboardAuth(ctx) {
				return true
			}
			endpoints.HandleFastDashboardPage(ctx)
			return true
		}
	case "/dashboard/stats":
		if method == fasthttp.MethodGet {
			if !endpoints.FastDashboardAuth(ctx) {
				return true
			}
			endpoints.HandleFastDashboardStats(ctx, r.MetricsEngine)
			return true
		}
	case "/video/vast":
		if r.videoPipeline == nil {
			return false
		}
		if method == fasthttp.MethodGet || method == fasthttp.MethodPost {
			r.withFastAdLimit(ctx, r.videoPipeline.HandleFastVAST)
			return true
		}
	case "/video/ortb":
		if r.videoPipeline == nil {
			return false
		}
		if method == fasthttp.MethodGet || method == fasthttp.MethodPost {
			r.withFastAdLimit(ctx, r.videoPipeline.HandleFastORTB)
			return true
		}
	case "/video/impression":
		if r.videoPipeline == nil {
			return false
		}
		if method == fasthttp.MethodGet {
			r.videoPipeline.HandleFastImpression(ctx)
			return true
		}
	case "/video/tracking":
		if r.videoPipeline == nil {
			return false
		}
		if method == fasthttp.MethodGet {
			r.videoPipeline.HandleFastTracking(ctx)
			return true
		}
	case "/video/tracking/events":
		if r.videoPipeline == nil {
			return false
		}
		if method == fasthttp.MethodGet {
			r.videoPipeline.HandleFastTrackingEvents(ctx)
			return true
		}
	case "/video/adserver":
		if r.videoPipeline == nil {
			return false
		}
		if method == fasthttp.MethodGet || method == fasthttp.MethodPost {
			r.videoPipeline.HandleFastAdServerConfig(ctx, "")
			return true
		}
	case "/dashboard/stats/video":
		if r.videoPipeline == nil {
			return false
		}
		if method == fasthttp.MethodGet {
			r.videoPipeline.HandleFastVideoStats(ctx)
			return true
		}
	case "/dashboard/stats/video/overview":
		if r.videoPipeline == nil {
			return false
		}
		if method == fasthttp.MethodGet {
			r.videoPipeline.HandleFastVideoOverviewStats(ctx)
			return true
		}
	case "/dashboard/stats/reset":
		if r.videoPipeline == nil {
			return false
		}
		if method == fasthttp.MethodPost {
			r.videoPipeline.HandleFastResetStats(ctx)
			return true
		}
	case "/dashboard/config":
		if r.videoPipeline == nil {
			return false
		}
		if method == fasthttp.MethodGet {
			r.videoPipeline.HandleFastDashboardConfig(ctx)
			return true
		}
	}

	if method == fasthttp.MethodDelete && strings.HasPrefix(path, "/video/adserver/") {
		if r.videoPipeline == nil {
			return false
		}
		placementID := strings.TrimPrefix(path, "/video/adserver/")
		r.videoPipeline.HandleFastAdServerConfig(ctx, placementID)
		return true
	}

	return false
}

func (r *Router) withFastAdLimit(ctx *fasthttp.RequestCtx, next fasthttp.RequestHandler) {
	if r.fastAdRequestSem == nil {
		next(ctx)
		return
	}
	select {
	case r.fastAdRequestSem <- struct{}{}:
		defer func() { <-r.fastAdRequestSem }()
		next(ctx)
	default:
		ctx.Response.Header.Set("Retry-After", "1")
		ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBodyString("too many requests")
	}
}

func applyHTTPResponseHeaders(w http.ResponseWriter, req *http.Request) {
	h := w.Header()
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Pragma", "no-cache")
	h.Set("Expires", "0")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
	h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	applyHTTPCORSHeaders(h, req.Header.Get("Origin"))
}

func applyFastHTTPResponseHeaders(ctx *fasthttp.RequestCtx) {
	h := &ctx.Response.Header
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Pragma", "no-cache")
	h.Set("Expires", "0")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
	h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	applyFastHTTPCORSHeaders(h, string(ctx.Request.Header.Peek("Origin")))
}

func applyHTTPCORSHeaders(h http.Header, origin string) {
	if origin != "" {
		h.Set("Access-Control-Allow-Origin", origin)
		h.Add("Vary", "Origin")
	}
	h.Set("Access-Control-Allow-Credentials", "true")
	h.Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Max-Age", "600")
}

func applyFastHTTPCORSHeaders(h *fasthttp.ResponseHeader, origin string) {
	if origin != "" {
		h.Set("Access-Control-Allow-Origin", origin)
		h.Add("Vary", "Origin")
	}
	h.Set("Access-Control-Allow-Credentials", "true")
	h.Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Max-Age", "600")
}