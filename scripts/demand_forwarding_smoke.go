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
	PlacementID    string  `json:"placement_id"`
	PublisherID    string  `json:"publisher_id,omitempty"`
	MinDuration    int     `json:"min_duration,omitempty"`
	MaxDuration    int     `json:"max_duration,omitempty"`
	FloorCPM       float64 `json:"floor_cpm,omitempty"`
	DemandVASTURL  string  `json:"demand_vast_url,omitempty"`
	DemandOrtbURL  string  `json:"demand_ortb_url,omitempty"`
	DomainOrApp    string  `json:"domain_or_app,omitempty"`
	ContentURL     string  `json:"content_url,omitempty"`
	VideoPlacement string  `json:"video_placement_type,omitempty"`
	Active         bool    `json:"active"`
}

type scenarioResult struct {
	Name           string       `json:"name"`
	PlacementID    string       `json:"placement_id"`
	InboundURL     string       `json:"inbound_url"`
	DemandRequest  httpCapture  `json:"demand_request"`
	DemandResponse responseInfo `json:"demand_response"`
	PlayerResponse responseInfo `json:"player_response"`
	Assertions     []string     `json:"assertions"`
	Status         string       `json:"status"`
}

type smokeResult struct {
	BaseURL   string           `json:"base_url"`
	MockURL   string           `json:"mock_url"`
	Scenarios []scenarioResult `json:"scenarios"`
	Status    string           `json:"status"`
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
	mu       sync.Mutex
	exchange map[string]mockExchange
}

func main() {
	baseURL := getenv("BASE_URL", "http://localhost:8000")
	user := getenv("DASH_ADMIN_USER", "admin")
	pass := getenv("DASH_ADMIN_PASS", "admin")
	httpClient := &http.Client{Timeout: 20 * time.Second}
	cookie := login(httpClient, baseURL, user, pass)

	mockState := &mockDemand{exchange: make(map[string]mockExchange)}
	mockServer := httptest.NewServer(http.HandlerFunc(mockState.handle))
	defer mockServer.Close()

	ts := time.Now().UnixNano()
	placements := []string{
		fmt.Sprintf("smoke-vast-vast-%d", ts),
		fmt.Sprintf("smoke-vast-ortb-%d", ts),
		fmt.Sprintf("smoke-ortb-vast-%d", ts),
		fmt.Sprintf("smoke-ortb-ortb-%d", ts),
	}
	for _, placementID := range placements {
		defer deleteConfig(httpClient, baseURL, cookie, placementID)
	}

	createConfig(httpClient, baseURL, cookie, adServerConfig{
		PlacementID:   placements[0],
		PublisherID:   "smoke-publisher",
		DomainOrApp:   "com.smoke.ctv",
		ContentURL:    fmt.Sprintf("https://content.example/watch/%d", ts),
		MinDuration:   15,
		MaxDuration:   30,
		FloorCPM:      2.10,
		DemandVASTURL: mockServer.URL + "/vast/vast-to-vast?bundle={app_bundle}&ua={ua}&ip={uip}&device_type={device_type}&cb={cb}",
		Active:        true,
	})
	createConfig(httpClient, baseURL, cookie, adServerConfig{
		PlacementID:   placements[1],
		PublisherID:   "smoke-publisher",
		DomainOrApp:   "com.smoke.ctv",
		ContentURL:    fmt.Sprintf("https://content.example/watch/%d", ts),
		MinDuration:   15,
		MaxDuration:   30,
		FloorCPM:      2.25,
		DemandOrtbURL: mockServer.URL + "/openrtb/vast-to-ortb",
		Active:        true,
	})
	createConfig(httpClient, baseURL, cookie, adServerConfig{
		PlacementID:   placements[2],
		PublisherID:   "smoke-publisher",
		DomainOrApp:   "com.smoke.ctv",
		ContentURL:    fmt.Sprintf("https://content.example/watch/%d", ts),
		MinDuration:   15,
		MaxDuration:   30,
		FloorCPM:      2.35,
		DemandVASTURL: mockServer.URL + "/vast/ortb-to-vast?bundle={app_bundle}&ua={ua}&ip={uip}&device_type={device_type}&cb={cb}",
		Active:        true,
	})
	createConfig(httpClient, baseURL, cookie, adServerConfig{
		PlacementID:   placements[3],
		PublisherID:   "smoke-publisher",
		DomainOrApp:   "com.smoke.ctv",
		ContentURL:    fmt.Sprintf("https://content.example/watch/%d", ts),
		MinDuration:   15,
		MaxDuration:   30,
		FloorCPM:      2.50,
		DemandOrtbURL: mockServer.URL + "/openrtb/ortb-to-ortb",
		Active:        true,
	})

	results := []scenarioResult{
		runScenario(httpClient, baseURL, mockState, "vast_to_vast", placements[0], "/video/vast", false),
		runScenario(httpClient, baseURL, mockState, "vast_to_ortb", placements[1], "/video/vast", true),
		runScenario(httpClient, baseURL, mockState, "ortb_to_vast", placements[2], "/video/ortb", false),
		runScenario(httpClient, baseURL, mockState, "ortb_to_ortb", placements[3], "/video/ortb", true),
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
	case "/vast/vast-to-vast":
		response = responseInfo{StatusCode: http.StatusOK, ContentType: "application/xml", BodyPreview: preview([]byte(vastDocument("smoke-vast-vast")))}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, vastDocument("smoke-vast-vast"))
	case "/vast/ortb-to-vast":
		response = responseInfo{StatusCode: http.StatusOK, ContentType: "application/xml", BodyPreview: preview([]byte(vastDocument("smoke-ortb-vast")))}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, vastDocument("smoke-ortb-vast"))
	case "/openrtb/vast-to-ortb":
		body := ortbResponse("smoke-vast-ortb", 4.20)
		response = responseInfo{StatusCode: http.StatusOK, ContentType: "application/json", BodyPreview: preview([]byte(body)), BodyJSON: parseJSON([]byte(body))}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	case "/openrtb/ortb-to-ortb":
		body := ortbResponse("smoke-ortb-ortb", 4.80)
		response = responseInfo{StatusCode: http.StatusOK, ContentType: "application/json", BodyPreview: preview([]byte(body)), BodyJSON: parseJSON([]byte(body))}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	default:
		response = responseInfo{StatusCode: http.StatusNotFound, ContentType: "text/plain", BodyPreview: "not found"}
		http.NotFound(w, r)
	}

	m.mu.Lock()
	m.exchange[r.URL.Path] = mockExchange{request: capture, response: response}
	m.mu.Unlock()
}

func runScenario(client *http.Client, baseURL string, mockState *mockDemand, name, placementID, endpoint string, expectsORTBRequest bool) scenarioResult {
	inboundURL := baseURL + endpoint + "?" + playerQuery(placementID).Encode()
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
	if strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		player.BodyJSON = parseJSON(body)
	}

	path := mockPathForScenario(name)
	exchange, ok := mockState.get(path)
	if !ok {
		fail(fmt.Sprintf("mock demand did not receive request for %s", name))
	}

	assertions := verifyScenario(name, placementID, exchange.request, player, expectsORTBRequest)

	return scenarioResult{
		Name:           name,
		PlacementID:    placementID,
		InboundURL:     inboundURL,
		DemandRequest:  exchange.request,
		DemandResponse: exchange.response,
		PlayerResponse: player,
		Assertions:     assertions,
		Status:         "ok",
	}
}

func (m *mockDemand) get(path string) (mockExchange, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	exchange, ok := m.exchange[path]
	return exchange, ok
}

func verifyScenario(name, placementID string, demand httpCapture, player responseInfo, expectsORTBRequest bool) []string {
	assertions := []string{}
	if player.StatusCode != http.StatusOK {
		fail(fmt.Sprintf("scenario %s returned HTTP %d", name, player.StatusCode))
	}
	if expectsORTBRequest {
		if demand.Method != http.MethodPost {
			fail(fmt.Sprintf("scenario %s expected POST to demand, got %s", name, demand.Method))
		}
		root, ok := demand.BodyJSON.(map[string]interface{})
		if !ok {
			fail(fmt.Sprintf("scenario %s expected JSON bid request body", name))
		}
		app, _ := root["app"].(map[string]interface{})
		if fmt.Sprint(app["bundle"]) != "com.smoke.app" {
			fail(fmt.Sprintf("scenario %s did not forward app.bundle correctly", name))
		}
		assertions = append(assertions, "demand received POST OpenRTB payload")
		assertions = append(assertions, "forwarded app.bundle=com.smoke.app")
	} else {
		if demand.Method != http.MethodGet {
			fail(fmt.Sprintf("scenario %s expected GET to VAST demand, got %s", name, demand.Method))
		}
		if first(demand.Query, "bundle") != "com.smoke.app" {
			fail(fmt.Sprintf("scenario %s did not substitute app bundle macro", name))
		}
		if first(demand.Query, "ip") != "203.0.113.10" {
			fail(fmt.Sprintf("scenario %s did not substitute client IP macro", name))
		}
		if first(demand.Query, "device_type") != "3" {
			fail(fmt.Sprintf("scenario %s did not substitute device type macro", name))
		}
		assertions = append(assertions, "demand received GET VAST request")
		assertions = append(assertions, "macro substitution preserved bundle, IP, and device type")
	}

	if strings.Contains(name, "vast") {
		if !strings.Contains(player.BodyPreview, "VAST") && !strings.Contains(player.BodyPreview, "seatbid") {
			fail(fmt.Sprintf("scenario %s returned empty player payload", name))
		}
	}
	if strings.Contains(name, "vast_to_vast") {
		if !strings.Contains(player.BodyPreview, "smoke-vast-vast") {
			fail("player VAST response missing smoke-vast-vast marker")
		}
		if !strings.Contains(player.BodyPreview, "/video/impression?") {
			fail("player VAST response missing injected impression tracking")
		}
		assertions = append(assertions, "player received inline VAST with injected impression beacon")
	}
	if strings.Contains(name, "vast_to_ortb") {
		if !strings.Contains(player.BodyPreview, "smoke-vast-ortb") {
			fail("player VAST response missing smoke-vast-ortb marker")
		}
		assertions = append(assertions, "player received VAST converted from OpenRTB demand bid")
	}
	if strings.Contains(name, "ortb_to_vast") {
		if !strings.Contains(player.BodyPreview, "smoke-ortb-vast") {
			fail("player ORTB response missing smoke-ortb-vast marker")
		}
		assertions = append(assertions, "player received OpenRTB response wrapping VAST demand")
	}
	if strings.Contains(name, "ortb_to_ortb") {
		if !strings.Contains(player.BodyPreview, "smoke-ortb-ortb") {
			fail("player ORTB response missing smoke-ortb-ortb marker")
		}
		assertions = append(assertions, "player received proxied OpenRTB response from demand")
	}
	return assertions
}

func mockPathForScenario(name string) string {
	switch name {
	case "vast_to_vast":
		return "/vast/vast-to-vast"
	case "vast_to_ortb":
		return "/openrtb/vast-to-ortb"
	case "ortb_to_vast":
		return "/vast/ortb-to-vast"
	case "ortb_to_ortb":
		return "/openrtb/ortb-to-ortb"
	default:
		fail("unknown scenario: " + name)
		return ""
	}
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

func ortbResponse(marker string, price float64) string {
	return fmt.Sprintf(`{"id":"auction-%s","seatbid":[{"seat":"smoke-demand","bid":[{"id":"bid-%s","impid":"1","price":%.2f,"crid":"creative-%s","adomain":["advertiser.example"],"adm":%q}]}]}`,
		marker,
		marker,
		price,
		marker,
		vastDocument(marker),
	)
}

func vastDocument(marker string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><VAST version="3.0"><Ad id="%s"><InLine><AdSystem>%s</AdSystem><AdTitle>%s</AdTitle><Impression><![CDATA[https://demand.example/imp/%s]]></Impression><Creatives><Creative><Linear><Duration>00:00:15</Duration><MediaFiles><MediaFile delivery="progressive" type="video/mp4" width="1920" height="1080"><![CDATA[https://cdn.example.com/%s.mp4]]></MediaFile></MediaFiles></Linear></Creative></Creatives></InLine></Ad></VAST>`, marker, marker, marker, marker, marker)
}

func preview(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > 900 {
		return text[:900] + "..."
	}
	return text
}

func first(values map[string][]string, key string) string {
	if values == nil {
		return ""
	}
	items := values[key]
	if len(items) == 0 {
		return ""
	}
	return items[0]
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
