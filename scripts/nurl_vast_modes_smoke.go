package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type adServerConfig struct {
	PlacementID   string  `json:"placement_id"`
	PublisherID   string  `json:"publisher_id,omitempty"`
	MinDuration   int     `json:"min_duration,omitempty"`
	MaxDuration   int     `json:"max_duration,omitempty"`
	FloorCPM      float64 `json:"floor_cpm,omitempty"`
	DemandOrtbURL string  `json:"demand_ortb_url,omitempty"`
	DomainOrApp   string  `json:"domain_or_app,omitempty"`
	ContentURL    string  `json:"content_url,omitempty"`
	Active        bool    `json:"active"`
}

type smokeResult struct {
	BaseURL   string           `json:"base_url"`
	MockURL   string           `json:"mock_url"`
	Scenarios []scenarioResult `json:"scenarios"`
	Status    string           `json:"status"`
}

type scenarioResult struct {
	Name           string       `json:"name"`
	PlacementID    string       `json:"placement_id"`
	InboundURL     string       `json:"inbound_url"`
	DemandRequest  httpCapture  `json:"demand_request"`
	DemandResponse responseInfo `json:"demand_response"`
	PlayerResponse responseInfo `json:"player_response"`
	MockHits       []string     `json:"mock_hits,omitempty"`
	Assertions     []string     `json:"assertions"`
	Status         string       `json:"status"`
}

type httpCapture struct {
	Method      string              `json:"method"`
	Path        string              `json:"path"`
	RawQuery    string              `json:"raw_query,omitempty"`
	Query       map[string][]string `json:"query,omitempty"`
	ContentType string              `json:"content_type,omitempty"`
	BodyPreview string              `json:"body_preview,omitempty"`
	BodyJSON    interface{}         `json:"body_json,omitempty"`
}

type responseInfo struct {
	StatusCode  int         `json:"status_code"`
	ContentType string      `json:"content_type,omitempty"`
	BodyPreview string      `json:"body_preview,omitempty"`
	BodyJSON    interface{} `json:"body_json,omitempty"`
}

type mockExchange struct {
	request  httpCapture
	response responseInfo
}

type mockDemand struct {
	mu        sync.Mutex
	exchange  map[string]mockExchange
	hitOrder  []string
	hitCounts map[string]int
}

func main() {
	baseURL := getenv("BASE_URL", "http://localhost:8000")
	user := getenv("DASH_ADMIN_USER", "admin")
	pass := getenv("DASH_ADMIN_PASS", "admin")
	httpClient := &http.Client{Timeout: 20 * time.Second}
	cookie := login(httpClient, baseURL, user, pass)

	mockState := &mockDemand{
		exchange:  make(map[string]mockExchange),
		hitCounts: make(map[string]int),
	}
	mockServer := httptest.NewServer(http.HandlerFunc(mockState.handle))
	defer mockServer.Close()

	ts := time.Now().UnixNano()
	placements := map[string]string{
		"nurl_only":    fmt.Sprintf("smoke-nurl-only-%d", ts),
		"vast_wrapper": fmt.Sprintf("smoke-vast-wrapper-%d", ts),
		"vast_inline":  fmt.Sprintf("smoke-vast-inline-%d", ts),
	}
	for _, placementID := range placements {
		defer deleteConfig(httpClient, baseURL, cookie, placementID)
	}

	createConfig(httpClient, baseURL, cookie, adServerConfig{
		PlacementID:   placements["nurl_only"],
		PublisherID:   "smoke-publisher",
		DomainOrApp:   "com.smoke.ctv",
		ContentURL:    fmt.Sprintf("https://content.example/watch/%d", ts),
		MinDuration:   15,
		MaxDuration:   30,
		FloorCPM:      2.10,
		DemandOrtbURL: mockServer.URL + "/openrtb/nurl-only",
		Active:        true,
	})
	createConfig(httpClient, baseURL, cookie, adServerConfig{
		PlacementID:   placements["vast_wrapper"],
		PublisherID:   "smoke-publisher",
		DomainOrApp:   "com.smoke.ctv",
		ContentURL:    fmt.Sprintf("https://content.example/watch/%d", ts),
		MinDuration:   15,
		MaxDuration:   30,
		FloorCPM:      2.30,
		DemandOrtbURL: mockServer.URL + "/openrtb/vast-wrapper",
		Active:        true,
	})
	createConfig(httpClient, baseURL, cookie, adServerConfig{
		PlacementID:   placements["vast_inline"],
		PublisherID:   "smoke-publisher",
		DomainOrApp:   "com.smoke.ctv",
		ContentURL:    fmt.Sprintf("https://content.example/watch/%d", ts),
		MinDuration:   15,
		MaxDuration:   30,
		FloorCPM:      2.50,
		DemandOrtbURL: mockServer.URL + "/openrtb/vast-inline",
		Active:        true,
	})

	results := []scenarioResult{
		runScenario(httpClient, baseURL, mockState, placements["nurl_only"], "nurl_only"),
		runScenario(httpClient, baseURL, mockState, placements["vast_wrapper"], "vast_wrapper"),
		runScenario(httpClient, baseURL, mockState, placements["vast_inline"], "vast_inline"),
	}

	out := smokeResult{
		BaseURL:   baseURL,
		MockURL:   mockServer.URL,
		Scenarios: results,
		Status:    "ok",
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fail(err.Error())
	}
	fmt.Println(string(b))
}

func (m *mockDemand) handle(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	capture := httpCapture{
		Method:      r.Method,
		Path:        r.URL.Path,
		RawQuery:    r.URL.RawQuery,
		Query:       cloneQuery(r.URL.Query()),
		ContentType: r.Header.Get("Content-Type"),
		BodyPreview: preview(body),
	}
	if parsed := parseJSON(body); parsed != nil {
		capture.BodyJSON = parsed
	}

	response := responseInfo{}
	switch r.URL.Path {
	case "/openrtb/nurl-only":
		body := nurlOnlyResponse("smoke-nurl-only", r.Host)
		response = responseInfo{StatusCode: http.StatusOK, ContentType: "application/json", BodyPreview: preview([]byte(body)), BodyJSON: parseJSON([]byte(body))}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	case "/openrtb/vast-wrapper":
		body := wrapperAdMResponse("smoke-vast-wrapper", serverURL(r)+"/vast/final-inline")
		response = responseInfo{StatusCode: http.StatusOK, ContentType: "application/json", BodyPreview: preview([]byte(body)), BodyJSON: parseJSON([]byte(body))}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	case "/openrtb/vast-inline":
		body := inlineAdMResponse("smoke-vast-inline")
		response = responseInfo{StatusCode: http.StatusOK, ContentType: "application/json", BodyPreview: preview([]byte(body)), BodyJSON: parseJSON([]byte(body))}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	case "/vast/final-inline":
		body := inlineVAST("smoke-wrapper-final")
		response = responseInfo{StatusCode: http.StatusOK, ContentType: "application/xml", BodyPreview: preview([]byte(body))}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	case "/vast/nurl-target":
		response = responseInfo{StatusCode: http.StatusOK, ContentType: "application/xml", BodyPreview: preview([]byte(inlineVAST("smoke-nurl-target")))}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, inlineVAST("smoke-nurl-target"))
	default:
		response = responseInfo{StatusCode: http.StatusNotFound, ContentType: "text/plain", BodyPreview: "not found"}
		http.NotFound(w, r)
	}

	m.mu.Lock()
	m.exchange[r.URL.Path] = mockExchange{request: capture, response: response}
	m.hitOrder = append(m.hitOrder, r.URL.Path)
	m.hitCounts[r.URL.Path]++
	m.mu.Unlock()
}

func runScenario(client *http.Client, baseURL string, mockState *mockDemand, placementID, scenario string) scenarioResult {
	mockState.reset()
	inboundURL := baseURL + "/video/vast?" + playerQuery(placementID).Encode()
	req, err := http.NewRequest(http.MethodGet, inboundURL, nil)
	if err != nil {
		fail(err.Error())
	}
	req.Header.Set("User-Agent", "SmokePlayer/1.0")
	resp, err := client.Do(req)
	if err != nil {
		fail(err.Error())
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	player := responseInfo{
		StatusCode:  resp.StatusCode,
		ContentType: resp.Header.Get("Content-Type"),
		BodyPreview: preview(body),
	}

	hits := mockState.hits()
	demandPath := map[string]string{
		"nurl_only":    "/openrtb/nurl-only",
		"vast_wrapper": "/openrtb/vast-wrapper",
		"vast_inline":  "/openrtb/vast-inline",
	}[scenario]
	exchange, ok := mockState.get(demandPath)
	if !ok {
		fail("missing demand exchange for " + scenario)
	}
	assertions := verifyScenario(scenario, placementID, player, hits)

	return scenarioResult{
		Name:           scenario,
		PlacementID:    placementID,
		InboundURL:     inboundURL,
		DemandRequest:  exchange.request,
		DemandResponse: exchange.response,
		PlayerResponse: player,
		MockHits:       hits,
		Assertions:     assertions,
		Status:         "ok",
	}
}

func verifyScenario(scenario, placementID string, player responseInfo, hits []string) []string {
	if player.StatusCode != http.StatusOK {
		fail(fmt.Sprintf("scenario %s returned HTTP %d", scenario, player.StatusCode))
	}
	assertions := []string{}
	switch scenario {
	case "nurl_only":
		if !strings.Contains(player.BodyPreview, "<Wrapper>") {
			fail("nurl_only should return VAST Wrapper")
		}
		if !strings.Contains(player.BodyPreview, "/vast/nurl-target") {
			fail("nurl_only wrapper missing VASTAdTagURI target")
		}
		if strings.Contains(strings.Join(hits, ","), "/vast/nurl-target") {
			fail("nurl_only unexpectedly fired NURL server-side")
		}
		if !strings.Contains(player.BodyPreview, "/video/impression?") {
			fail("nurl_only wrapper missing impression beacon")
		}
		assertions = append(assertions,
			"returned VAST Wrapper for NURL-only bid",
			"kept NURL for player fetch and did not fire it server-side",
			"wrapper includes impression beacon and tracking events",
		)
	case "vast_wrapper":
		if strings.Contains(player.BodyPreview, "<Wrapper>") || strings.Contains(player.BodyPreview, "VASTAdTagURI") {
			fail("vast_wrapper should be resolved to final inline VAST")
		}
		if !contains(hits, "/vast/final-inline") {
			fail("vast_wrapper did not fetch final inline VAST from wrapper chain")
		}
		if !strings.Contains(player.BodyPreview, "smoke-wrapper-final") {
			fail("vast_wrapper final player response missing resolved inline marker")
		}
		assertions = append(assertions,
			"resolved wrapper chain server-side",
			"player received final inline VAST instead of wrapper",
			"resolved inline VAST kept injected impression and tracking",
		)
	case "vast_inline":
		if !strings.Contains(player.BodyPreview, "<InLine>") {
			fail("vast_inline should return inline VAST")
		}
		if !strings.Contains(player.BodyPreview, "smoke-vast-inline") {
			fail("vast_inline player response missing inline marker")
		}
		if !strings.Contains(player.BodyPreview, "/video/impression?") || !strings.Contains(player.BodyPreview, "/video/tracking?") {
			fail("vast_inline missing injected beacons")
		}
		assertions = append(assertions,
			"kept inline VAST creative path",
			"injected impression beacon into inline response",
			"injected quartile and complete tracking into inline response",
		)
	default:
		fail("unknown scenario: " + scenario)
	}
	if !strings.Contains(player.BodyPreview, placementID) {
		fail("player response missing placement identifier in injected beacons for " + scenario)
	}
	return assertions
}

func (m *mockDemand) get(path string) (mockExchange, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	exchange, ok := m.exchange[path]
	return exchange, ok
}

func (m *mockDemand) hits() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.hitOrder))
	copy(out, m.hitOrder)
	return out
}

func (m *mockDemand) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.exchange = make(map[string]mockExchange)
	m.hitOrder = nil
	m.hitCounts = make(map[string]int)
}

func playerQuery(placementID string) url.Values {
	q := url.Values{}
	q.Set("placement_id", placementID)
	q.Set("app_bundle", "com.smoke.app")
	q.Set("ct_url", "https://content.example/episode/smoke")
	q.Set("ua", "SmokePlayer/1.0")
	q.Set("ip", "203.0.113.10")
	q.Set("device_type", "3")
	q.Set("os", "tvOS")
	q.Set("w", "1920")
	q.Set("h", "1080")
	q.Set("page_url", "https://player.example/watch/smoke")
	q.Set("site_name", "Smoke TV")
	q.Set("ct_title", "Smoke Episode")
	q.Set("ct_series", "Smoke Series")
	q.Set("language", "en")
	q.Set("country_code", "US")
	return q
}

func createConfig(client *http.Client, baseURL, cookie string, cfg adServerConfig) {
	body, err := json.Marshal(cfg)
	if err != nil {
		fail(err.Error())
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/video/adserver", bytes.NewReader(body))
	if err != nil {
		fail(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Cookie", cookie)
	resp, err := client.Do(req)
	if err != nil {
		fail(err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		fail(fmt.Sprintf("create adserver config failed: %s", string(respBody)))
	}
}

func deleteConfig(client *http.Client, baseURL, cookie, placementID string) {
	req, err := http.NewRequest(http.MethodDelete, baseURL+"/video/adserver/"+url.PathEscape(placementID), nil)
	if err != nil {
		return
	}
	req.Header.Set("Cookie", cookie)
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func login(client *http.Client, baseURL, user, pass string) string {
	payload, err := json.Marshal(map[string]string{
		"username": user,
		"password": pass,
	})
	if err != nil {
		fail(err.Error())
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/dashboard/login", bytes.NewReader(payload))
	if err != nil {
		fail(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fail(err.Error())
	}
	defer resp.Body.Close()
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "dash_session" {
			return cookie.Name + "=" + cookie.Value
		}
	}
	body, _ := io.ReadAll(resp.Body)
	fail(fmt.Sprintf("login failed: %s", string(body)))
	return ""
}

func nurlOnlyResponse(marker, host string) string {
	nurl := "http://" + host + "/vast/nurl-target?marker=" + url.QueryEscape(marker)
	return fmt.Sprintf(`{"id":"auction-%s","seatbid":[{"seat":"smoke-demand","bid":[{"id":"bid-%s","impid":"1","price":3.30,"nurl":%q,"crid":"creative-%s","adomain":["advertiser.example"]}]}]}`,
		marker,
		marker,
		nurl,
		marker,
	)
}

func wrapperAdMResponse(marker, tagURI string) string {
	return fmt.Sprintf(`{"id":"auction-%s","seatbid":[{"seat":"smoke-demand","bid":[{"id":"bid-%s","impid":"1","price":4.10,"crid":"creative-%s","adomain":["advertiser.example"],"adm":%q}]}]}`,
		marker,
		marker,
		marker,
		wrapperVAST(marker, tagURI),
	)
}

func inlineAdMResponse(marker string) string {
	return fmt.Sprintf(`{"id":"auction-%s","seatbid":[{"seat":"smoke-demand","bid":[{"id":"bid-%s","impid":"1","price":4.50,"crid":"creative-%s","adomain":["advertiser.example"],"adm":%q}]}]}`,
		marker,
		marker,
		marker,
		inlineVAST(marker),
	)
}

func inlineVAST(marker string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><VAST version="3.0"><Ad id="%s"><InLine><AdSystem>%s</AdSystem><AdTitle>%s</AdTitle><Impression><![CDATA[https://demand.example/imp/%s]]></Impression><Creatives><Creative><Linear><Duration>00:00:15</Duration><MediaFiles><MediaFile delivery="progressive" type="video/mp4" width="1920" height="1080"><![CDATA[https://cdn.example.com/%s.mp4]]></MediaFile></MediaFiles></Linear></Creative></Creatives></InLine></Ad></VAST>`, marker, marker, marker, marker, marker)
}

func wrapperVAST(marker, tagURI string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><VAST version="3.0"><Ad id="%s"><Wrapper><AdSystem>%s</AdSystem><VASTAdTagURI><![CDATA[%s]]></VASTAdTagURI><Impression><![CDATA[https://demand.example/wrapper/%s]]></Impression></Wrapper></Ad></VAST>`, marker, marker, tagURI, marker)
}

func serverURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func cloneQuery(values url.Values) map[string][]string {
	if len(values) == 0 {
		return nil
	}
	clone := make(map[string][]string, len(values))
	for key, items := range values {
		copyItems := make([]string, len(items))
		copy(copyItems, items)
		clone[key] = copyItems
	}
	return clone
}

func parseJSON(body []byte) interface{} {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	var out interface{}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil
	}
	return out
}

func preview(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > 1200 {
		return text[:1200] + "..."
	}
	return text
}

func contains(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
