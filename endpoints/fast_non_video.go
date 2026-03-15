package endpoints

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	metricsConf "github.com/prebid/prebid-server/v4/metrics/config"
	"github.com/valyala/fasthttp"

	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/util/jsonutil"
)

func NewFastStatusHandler(response string, cfg *config.Configuration) fasthttp.RequestHandler {
	checks := buildDependencyChecks(cfg)

	if response == "" && len(checks) == 0 {
		return func(ctx *fasthttp.RequestCtx) {
			ctx.SetStatusCode(fasthttp.StatusNoContent)
		}
	}

	if len(checks) == 0 {
		responseBytes := []byte(response)
		return func(ctx *fasthttp.RequestCtx) {
			ctx.SetStatusCode(fasthttp.StatusOK)
			ctx.SetBody(responseBytes)
		}
	}

	return func(ctx *fasthttp.RequestCtx) {
		payload, healthy := runDependencyChecks(response, checks)
		data, err := jsonutil.Marshal(payload)
		if err != nil {
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			ctx.SetContentType("application/json")
			ctx.SetBodyString(`{"error":"failed to encode status"}`)
			return
		}
		ctx.SetContentType("application/json")
		if healthy {
			ctx.SetStatusCode(fasthttp.StatusOK)
		} else {
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		}
		ctx.SetBody(data)
	}
}

func FastDashboardAuth(ctx *fasthttp.RequestCtx) bool {
	if isValidDashSessionToken(string(ctx.Request.Header.Cookie(dashSessionCookie))) {
		return true
	}
	if fastRequestWantsJSON(ctx) {
		ctx.SetStatusCode(fasthttp.StatusUnauthorized)
		ctx.SetContentType("application/json")
		ctx.SetBodyString(`{"error":"unauthorized"}`)
		return false
	}
	ctx.Redirect("/dashboard/login", fasthttp.StatusFound)
	return false
}

func HandleFastDashboardLoginGet(ctx *fasthttp.RequestCtx) {
	if isValidDashSessionToken(string(ctx.Request.Header.Cookie(dashSessionCookie))) {
		ctx.Redirect("/dashboard", fasthttp.StatusFound)
		return
	}
	data, err := os.ReadFile("static/login.html")
	if err != nil {
		writeFastTextError(ctx, fasthttp.StatusNotFound, "dashboard not found")
		return
	}
	ctx.Response.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	ctx.Response.Header.Set("Pragma", "no-cache")
	ctx.Response.Header.Set("Expires", "0")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("text/html; charset=utf-8")
	ctx.SetBody(data)
}

func HandleFastDashboardLoginPost(ctx *fasthttp.RequestCtx) {
	var username string
	var password string
	contentType := string(ctx.Request.Header.ContentType())
	if strings.Contains(contentType, "application/json") {
		var creds struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.Unmarshal(ctx.PostBody(), &creds); err != nil {
			writeFastJSONBytes(ctx, fasthttp.StatusBadRequest, []byte(`{"error":"bad request"}`))
			return
		}
		username = creds.Username
		password = creds.Password
	} else {
		username = string(ctx.PostArgs().Peek("username"))
		password = string(ctx.PostArgs().Peek("password"))
	}

	if username != dashAdminUser || password != dashAdminPass {
		if strings.Contains(contentType, "application/json") {
			writeFastJSONBytes(ctx, fasthttp.StatusUnauthorized, []byte(`{"error":"invalid credentials"}`))
		} else {
			ctx.Redirect("/dashboard/login?error=1", fasthttp.StatusFound)
		}
		return
	}

	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		writeFastTextError(ctx, fasthttp.StatusInternalServerError, "server error")
		return
	}
	token := hex.EncodeToString(b)
	dashSessionsMu.Lock()
	dashSessions[token] = time.Now().Add(dashSessionTTL)
	dashSessionsMu.Unlock()
	safeGo(saveDashSessions)

	isSecure := ctx.IsTLS() || strings.EqualFold(string(ctx.Request.Header.Peek("X-Forwarded-Proto")), "https")
	sameSite := "Lax"
	if isSecure {
		sameSite = "Strict"
	}
	ctx.Response.Header.Add("Set-Cookie", buildFastDashSessionCookie(token, int(dashSessionTTL.Seconds()), isSecure, sameSite))

	if strings.Contains(contentType, "application/json") {
		writeFastJSONBytes(ctx, fasthttp.StatusOK, []byte(`{"ok":true}`))
		return
	}
	ctx.Redirect("/dashboard", fasthttp.StatusFound)
}

func HandleFastDashboardLogout(ctx *fasthttp.RequestCtx) {
	token := string(ctx.Request.Header.Cookie(dashSessionCookie))
	if token != "" {
		dashSessionsMu.Lock()
		delete(dashSessions, token)
		dashSessionsMu.Unlock()
		safeGo(saveDashSessions)
	}
	ctx.Response.Header.Add("Set-Cookie", buildFastDashSessionCookie("", -1, true, "Strict"))
	ctx.Redirect("/dashboard/login", fasthttp.StatusFound)
}

func HandleFastDashboardPage(ctx *fasthttp.RequestCtx) {
	data, err := os.ReadFile("static/dashboard.html")
	if err != nil {
		writeFastTextError(ctx, fasthttp.StatusNotFound, "dashboard not found")
		return
	}
	ctx.Response.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	ctx.Response.Header.Set("Pragma", "no-cache")
	ctx.Response.Header.Set("Expires", "0")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("text/html; charset=utf-8")
	ctx.SetBody(data)
}

func HandleFastDashboardStats(ctx *fasthttp.RequestCtx, metricsEngine *metricsConf.DetailedMetricsEngine) {
	stats := buildDashboardStats(metricsEngine)
	data, err := jsonutil.Marshal(stats)
	if err != nil {
		writeFastTextError(ctx, fasthttp.StatusInternalServerError, "failed to encode stats")
		return
	}
	ctx.Response.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	writeFastJSONBytes(ctx, fasthttp.StatusOK, data)
}

func fastRequestWantsJSON(ctx *fasthttp.RequestCtx) bool {
	accept := strings.ToLower(string(ctx.Request.Header.Peek("Accept")))
	contentType := strings.ToLower(string(ctx.Request.Header.Peek("Content-Type")))
	return strings.Contains(accept, "application/json") || strings.Contains(contentType, "application/json")
}

func buildFastDashSessionCookie(token string, maxAge int, secure bool, sameSite string) string {
	b := strings.Builder{}
	b.WriteString(dashSessionCookie)
	b.WriteString("=")
	b.WriteString(token)
	b.WriteString("; Path=/; HttpOnly; SameSite=")
	b.WriteString(sameSite)
	b.WriteString("; Max-Age=")
	b.WriteString(strconv.Itoa(maxAge))
	if secure {
		b.WriteString("; Secure")
	}
	return b.String()
}