package endpoints

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
)

func TestBidReportListSortsByPriceDescending(t *testing.T) {
	handler := NewBidReportHandler("")
	handler.store.create(&BidReportEntry{
		CampaignID: "campaign-low",
		Price:      1.25,
		EventType:  "bid",
		EventTime:  time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC),
	})
	handler.store.create(&BidReportEntry{
		CampaignID: "campaign-high",
		Price:      4.75,
		EventType:  "win",
		EventTime:  time.Date(2026, 3, 15, 11, 0, 0, 0, time.UTC),
	})

	req := httptest.NewRequest("GET", "/dashboard/reports?sort_by=price&sort_dir=desc", nil)
	recorder := httptest.NewRecorder()

	handler.List()(recorder, req, httprouter.Params{})

	if recorder.Code != 200 {
		t.Fatalf("expected 200 OK, got %d with body %s", recorder.Code, recorder.Body.String())
	}

	var payload struct {
		Total   int               `json:"total"`
		Entries []*BidReportEntry `json:"entries"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if payload.Total != 2 || len(payload.Entries) != 2 {
		t.Fatalf("expected 2 entries, got total=%d len=%d", payload.Total, len(payload.Entries))
	}
	if payload.Entries[0].CampaignID != "campaign-high" {
		t.Fatalf("expected highest price first, got %#v", payload.Entries[0])
	}
	if payload.Entries[1].CampaignID != "campaign-low" {
		t.Fatalf("expected lowest price second, got %#v", payload.Entries[1])
	}
}

func TestBidReportListSortsByEventTimeAscending(t *testing.T) {
	handler := NewBidReportHandler("")
	handler.store.create(&BidReportEntry{
		RequestID: "later",
		Price:     2.0,
		EventType: "bid",
		EventTime: time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC),
	})
	handler.store.create(&BidReportEntry{
		RequestID: "earlier",
		Price:     2.0,
		EventType: "bid",
		EventTime: time.Date(2026, 3, 15, 9, 30, 0, 0, time.UTC),
	})

	req := httptest.NewRequest("GET", "/dashboard/reports?sort_by=event_time&sort_dir=asc", nil)
	recorder := httptest.NewRecorder()

	handler.List()(recorder, req, httprouter.Params{})

	if recorder.Code != 200 {
		t.Fatalf("expected 200 OK, got %d with body %s", recorder.Code, recorder.Body.String())
	}

	var payload struct {
		Entries []*BidReportEntry `json:"entries"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(payload.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(payload.Entries))
	}
	if payload.Entries[0].RequestID != "earlier" {
		t.Fatalf("expected earliest event first, got %#v", payload.Entries[0])
	}
	if payload.Entries[1].RequestID != "later" {
		t.Fatalf("expected latest event second, got %#v", payload.Entries[1])
	}
}