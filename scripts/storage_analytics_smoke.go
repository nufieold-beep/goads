package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/lib/pq"
)

type result struct {
	PublisherID         string `json:"publisher_id"`
	AdvertiserID        string `json:"advertiser_id"`
	CampaignID          string `json:"campaign_id"`
	SourceID            string `json:"source_id"`
	PostgresEntityCount int64  `json:"postgres_entity_count"`
	ClickHouseImpCount  int64  `json:"clickhouse_impression_count"`
	AuctionID           string `json:"auction_id"`
	BidID               string `json:"bid_id"`
	Status              string `json:"status"`
}

func main() {
	baseURL := getenv("BASE_URL", "http://localhost:8000")
	dashUser := getenv("DASH_ADMIN_USER", "admin")
	dashPass := os.Getenv("DASH_ADMIN_PASS")
	pgDSN := os.Getenv("DASH_DB_DSN")
	chDSN := os.Getenv("CLICKHOUSE_DSN")
	chTable := getenv("CLICKHOUSE_VIDEO_TABLE", "video_event_facts")
	if dashPass == "" || pgDSN == "" || chDSN == "" {
		fail("DASH_ADMIN_PASS, DASH_DB_DSN, and CLICKHOUSE_DSN must be set")
	}

	httpClient := &http.Client{Timeout: 15 * time.Second}
	pg, err := sql.Open("postgres", pgDSN)
	if err != nil {
		fail(err.Error())
	}
	defer pg.Close()
	ch, err := sql.Open("clickhouse", chDSN)
	if err != nil {
		fail(err.Error())
	}
	defer ch.Close()

	ctxQuery := func(db *sql.DB, query string, args ...interface{}) int64 {
		var count int64
		if err := db.QueryRow(query, args...).Scan(&count); err != nil {
			fail(err.Error())
		}
		return count
	}

	cookie := login(httpClient, baseURL, dashUser, dashPass)
	ts := time.Now().UnixNano()
	publisher := createEntity(httpClient, baseURL+"/dashboard/publishers", cookie, map[string]interface{}{
		"name":          fmt.Sprintf("Smoke Publisher %d", ts),
		"domain":        fmt.Sprintf("publisher-%d.example", ts),
		"contact_email": fmt.Sprintf("ops+%d@example.com", ts),
		"status":        "active",
	})
	advertiser := createEntity(httpClient, baseURL+"/dashboard/advertisers", cookie, map[string]interface{}{
		"name":          fmt.Sprintf("Smoke Advertiser %d", ts),
		"domain":        fmt.Sprintf("advertiser-%d.example", ts),
		"category":      "IAB1",
		"contact_email": fmt.Sprintf("demand+%d@example.com", ts),
		"status":        "active",
	})
	campaign := createEntity(httpClient, baseURL+"/dashboard/campaigns", cookie, map[string]interface{}{
		"name":              fmt.Sprintf("Smoke Campaign %d", ts),
		"advertiser_id":     advertiser["id"],
		"publisher_id":      publisher["id"],
		"status":            "active",
		"integration_type":  "open_rtb",
		"ortb_version":      "2.6",
		"ortb_endpoint_url": fmt.Sprintf("https://demand.example/openrtb/%d", ts),
		"vast_tag_url":      fmt.Sprintf("https://demand.example/vast/%d", ts),
		"floor_cpm":         3.25,
		"protocols":         []int{2, 3, 5, 6, 7, 8},
		"apis":              []int{2, 7},
	})
	source := createEntity(httpClient, baseURL+"/dashboard/video", cookie, map[string]interface{}{
		"name":          fmt.Sprintf("Smoke Source %d", ts),
		"publisher_id":  publisher["id"],
		"environment":   "ctv",
		"placement":     "instream",
		"domain_or_app": fmt.Sprintf("ctv-%d.example", ts),
		"content_url":   fmt.Sprintf("https://content.example/watch/%d", ts),
		"seller_domain": "seller.example",
		"targeting_ext": map[string]interface{}{"channel": "smoke", "tier": "test"},
		"min_duration":  15,
		"max_duration":  30,
		"floor_cpm":     1.75,
		"bidders":       []string{"appnexus", "ix"},
		"active":        true,
		"timeout_ms":    500,
		"campaign_id":   campaign["id"],
		"demand_links":  []interface{}{campaign["id"]},
	})

	pgCount := ctxQuery(pg, `SELECT count(*) FROM dashboard_entities WHERE kind IN ('publishers','advertisers','campaigns','adunits','ad_server_configs')`)

	auctionID := fmt.Sprintf("smoke-auction-%d", ts)
	bidID := fmt.Sprintf("smoke-bid-%d", ts)
	fireImpression(httpClient, baseURL, auctionID, bidID, fmt.Sprint(source["id"]), 4.2)
	time.Sleep(2 * time.Second)
	chCount := ctxQuery(ch, fmt.Sprintf(`SELECT count() FROM %s WHERE event_type = 'impression' AND auction_id = ? AND bid_id = ?`, chTable), auctionID, bidID)

	out := result{
		PublisherID:         fmt.Sprint(publisher["id"]),
		AdvertiserID:        fmt.Sprint(advertiser["id"]),
		CampaignID:          fmt.Sprint(campaign["id"]),
		SourceID:            fmt.Sprint(source["id"]),
		PostgresEntityCount: pgCount,
		ClickHouseImpCount:  chCount,
		AuctionID:           auctionID,
		BidID:               bidID,
		Status:              "ok",
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
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

func login(client *http.Client, baseURL, user, pass string) string {
	values := url.Values{}
	values.Set("username", user)
	values.Set("password", pass)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/dashboard/login", strings.NewReader(values.Encode()))
	if err != nil {
		fail(err.Error())
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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

func createEntity(client *http.Client, endpoint, cookie string, payload map[string]interface{}) map[string]interface{} {
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
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
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fail(fmt.Sprintf("request %s failed: %s", endpoint, string(respBody)))
	}
	out := map[string]interface{}{}
	if err := json.Unmarshal(respBody, &out); err != nil {
		fail(err.Error())
	}
	return out
}

func fireImpression(client *http.Client, baseURL, auctionID, bidID, placementID string, price float64) {
	q := url.Values{}
	q.Set("auction_id", auctionID)
	q.Set("bid_id", bidID)
	q.Set("placement_id", placementID)
	q.Set("bidder", "smoke-bidder")
	q.Set("price", fmt.Sprintf("%.2f", price))
	req, err := http.NewRequest(http.MethodGet, baseURL+"/video/impression?"+q.Encode(), nil)
	if err != nil {
		fail(err.Error())
	}
	resp, err := client.Do(req)
	if err != nil {
		fail(err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		fail(fmt.Sprintf("impression beacon failed: %s", string(body)))
	}
}