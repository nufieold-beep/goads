package endpoints

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
)

func TestSupplyPartnerListUsesRecentRequestWindowsForAvgQPS(t *testing.T) {
	handler := NewSupplyPartnerHandler("")
	created := handler.store.create(&SupplyPartner{
		ID:             "pub-1",
		Name:           "Publisher 1",
		DeliveryStatus: "Live",
		Active:         true,
	})
	handler.SetStatsProvider(func() VideoStatsPayload {
		return VideoStatsPayload{
			ByPublisher: map[string]*VideoStats{
				created.ID: {AdRequests: 180000, Opportunities: 10, Impressions: 5},
			},
			PublisherRequestsLastDay: map[string]int64{
				created.ID: 86400,
			},
			PublisherRequestsLastHour: map[string]int64{
				created.ID: 3600,
			},
			StartedAt: time.Now().Add(-48 * time.Hour).Unix(),
		}
	})

	req := httptest.NewRequest("GET", "/dashboard/supply-partners", nil)
	recorder := httptest.NewRecorder()
	handler.List()(recorder, req, httprouter.Params{})

	if recorder.Code != 200 {
		t.Fatalf("expected 200 OK, got %d with body %s", recorder.Code, recorder.Body.String())
	}

	var payload struct {
		Entries []*SupplyPartner `json:"entries"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(payload.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(payload.Entries))
	}
	if payload.Entries[0].AvgQpsYesterday != 1 {
		t.Fatalf("expected avg qps yesterday 1 from recent 24h requests, got %d", payload.Entries[0].AvgQpsYesterday)
	}
	if payload.Entries[0].AvgQpsLastHour != 1 {
		t.Fatalf("expected avg qps last hour 1 from recent 1h requests, got %d", payload.Entries[0].AvgQpsLastHour)
	}
	if payload.Entries[0].AvgQpsYesterday == 2 || payload.Entries[0].AvgQpsLastHour == 50 {
		t.Fatalf("expected recent-window qps, not cumulative fallback")
	}
}

func TestSupplyPartnerListUsesFixedWindowSecondsForRecentQPS(t *testing.T) {
	handler := NewSupplyPartnerHandler("")
	created := handler.store.create(&SupplyPartner{
		Name:           "Publisher Fixed Window",
		DeliveryStatus: "Live",
		Active:         true,
	})
	handler.SetStatsProvider(func() VideoStatsPayload {
		return VideoStatsPayload{
			ByPublisher: map[string]*VideoStats{
				created.ID: {AdRequests: 86400, Opportunities: 10, Impressions: 5},
			},
			PublisherRequestsLastDay: map[string]int64{
				created.ID: 86400,
			},
			PublisherRequestsLastHour: map[string]int64{
				created.ID: 3600,
			},
			StartedAt: time.Now().Add(-30 * time.Minute).Unix(),
		}
	})

	req := httptest.NewRequest("GET", "/dashboard/supply-partners", nil)
	recorder := httptest.NewRecorder()
	handler.List()(recorder, req, httprouter.Params{})

	if recorder.Code != 200 {
		t.Fatalf("expected 200 OK, got %d with body %s", recorder.Code, recorder.Body.String())
	}

	var payload struct {
		Entries []*SupplyPartner `json:"entries"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(payload.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(payload.Entries))
	}
	if payload.Entries[0].AvgQpsYesterday != 1 {
		t.Fatalf("expected avg qps yesterday 1 using full 24h window, got %d", payload.Entries[0].AvgQpsYesterday)
	}
	if payload.Entries[0].AvgQpsLastHour != 1 {
		t.Fatalf("expected avg qps last hour 1 using full 1h window, got %d", payload.Entries[0].AvgQpsLastHour)
	}
	if payload.Entries[0].AvgQpsYesterday == 48 || payload.Entries[0].AvgQpsLastHour == 2 {
		t.Fatalf("expected fixed recent windows, not uptime-clamped division")
	}
}

func TestDemandPartnerListFallsBackToCumulativeRequestsWithoutRecentWindows(t *testing.T) {
	handler := NewDemandPartnerHandler("")
	created := handler.store.create(&DemandPartner{
		ID:             "adv-1",
		Name:           "Advertiser 1",
		DeliveryStatus: "Live",
		Active:         true,
	})
	handler.SetStatsProvider(func() VideoStatsPayload {
		return VideoStatsPayload{
			ByAdvertiser: map[string]*VideoStats{
				created.ID: {AdRequests: 7200, Opportunities: 10, Impressions: 5},
			},
			AdvertiserRequestsLastDay:  map[string]int64{},
			AdvertiserRequestsLastHour: map[string]int64{},
			StartedAt:                  time.Now().Add(-30 * time.Minute).Unix(),
		}
	})

	req := httptest.NewRequest("GET", "/dashboard/demand-partners", nil)
	recorder := httptest.NewRecorder()
	handler.List()(recorder, req, httprouter.Params{})

	if recorder.Code != 200 {
		t.Fatalf("expected 200 OK, got %d with body %s", recorder.Code, recorder.Body.String())
	}

	var payload struct {
		Entries []*DemandPartner `json:"entries"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(payload.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(payload.Entries))
	}
	if payload.Entries[0].AvgQpsYesterday != 4 {
		t.Fatalf("expected fallback avg qps yesterday 4, got %d", payload.Entries[0].AvgQpsYesterday)
	}
	if payload.Entries[0].AvgQpsLastHour != 4 {
		t.Fatalf("expected fallback avg qps last hour 4, got %d", payload.Entries[0].AvgQpsLastHour)
	}
}

func TestSupplyPartnerListBuildsFromPublisherMasterData(t *testing.T) {
	handler := NewSupplyPartnerHandler("")
	handler.SetPublisherListProvider(func() []*Publisher {
		return []*Publisher{{
			ID:        "pub-master-1",
			Name:      "Master Publisher",
			Status:    "active",
			CreatedAt: time.Unix(100, 0).UTC(),
			UpdatedAt: time.Unix(200, 0).UTC(),
		}}
	})
	handler.SetStatsProvider(func() VideoStatsPayload {
		return VideoStatsPayload{
			ByPublisher: map[string]*VideoStats{
				"pub-master-1": {AdRequests: 7200, Opportunities: 10, Impressions: 5},
			},
			PublisherRequestsLastDay: map[string]int64{
				"pub-master-1": 86400,
			},
			PublisherRequestsLastHour: map[string]int64{
				"pub-master-1": 3600,
			},
			StartedAt: time.Now().Add(-48 * time.Hour).Unix(),
		}
	})

	req := httptest.NewRequest("GET", "/dashboard/supply-partners", nil)
	recorder := httptest.NewRecorder()
	handler.List()(recorder, req, httprouter.Params{})

	if recorder.Code != 200 {
		t.Fatalf("expected 200 OK, got %d with body %s", recorder.Code, recorder.Body.String())
	}

	var payload struct {
		Entries []*SupplyPartner `json:"entries"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(payload.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(payload.Entries))
	}
	if payload.Entries[0].ID != "pub-master-1" || payload.Entries[0].Name != "Master Publisher" {
		t.Fatalf("expected synced publisher identity, got %+v", payload.Entries[0])
	}
	if payload.Entries[0].DeliveryStatus != "Live" || !payload.Entries[0].Active {
		t.Fatalf("expected synced publisher status to map to active/live, got %+v", payload.Entries[0])
	}
	if payload.Entries[0].AvgQpsYesterday != 1 || payload.Entries[0].AvgQpsLastHour != 1 {
		t.Fatalf("expected live stats overlay on synced publisher entry, got %+v", payload.Entries[0])
	}
}

func TestDemandPartnerListBuildsFromAdvertiserMasterData(t *testing.T) {
	handler := NewDemandPartnerHandler("")
	handler.SetAdvertiserListProvider(func() []*Advertiser {
		return []*Advertiser{{
			ID:        "adv-master-1",
			Name:      "Master Advertiser",
			Status:    "paused",
			CreatedAt: time.Unix(100, 0).UTC(),
			UpdatedAt: time.Unix(200, 0).UTC(),
		}}
	})
	handler.SetStatsProvider(func() VideoStatsPayload {
		return VideoStatsPayload{
			ByAdvertiser: map[string]*VideoStats{
				"adv-master-1": {AdRequests: 14400, Opportunities: 10, Impressions: 5},
			},
			AdvertiserRequestsLastDay: map[string]int64{
				"adv-master-1": 172800,
			},
			AdvertiserRequestsLastHour: map[string]int64{
				"adv-master-1": 7200,
			},
			StartedAt: time.Now().Add(-48 * time.Hour).Unix(),
		}
	})

	req := httptest.NewRequest("GET", "/dashboard/demand-partners", nil)
	recorder := httptest.NewRecorder()
	handler.List()(recorder, req, httprouter.Params{})

	if recorder.Code != 200 {
		t.Fatalf("expected 200 OK, got %d with body %s", recorder.Code, recorder.Body.String())
	}

	var payload struct {
		Entries []*DemandPartner `json:"entries"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(payload.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(payload.Entries))
	}
	if payload.Entries[0].ID != "adv-master-1" || payload.Entries[0].Name != "Master Advertiser" {
		t.Fatalf("expected synced advertiser identity, got %+v", payload.Entries[0])
	}
	if payload.Entries[0].DeliveryStatus != "Paused" || payload.Entries[0].Active {
		t.Fatalf("expected synced advertiser status to map to paused/inactive, got %+v", payload.Entries[0])
	}
	if payload.Entries[0].AvgQpsYesterday != 2 || payload.Entries[0].AvgQpsLastHour != 2 {
		t.Fatalf("expected live stats overlay on synced advertiser entry, got %+v", payload.Entries[0])
	}
}
