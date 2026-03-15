package endpoints

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
)

func TestCampaignCreateTriggersOnChange(t *testing.T) {
	handler := NewCampaignHandler("")
	called := make(chan struct{}, 1)
	handler.SetOnChange(func() {
		select {
		case called <- struct{}{}:
		default:
		}
	})

	req := httptest.NewRequest("POST", "/dashboard/campaigns", strings.NewReader(`{
		"name":"Demand Campaign",
		"advertiser_id":"adv-1",
		"status":"active",
		"integration_type":"open_rtb",
		"ortb_endpoint_url":"https://demand.example/openrtb"
	}`))
	recorder := httptest.NewRecorder()

	handler.Create()(recorder, req, httprouter.Params{})

	if recorder.Code != 201 {
		t.Fatalf("expected 201 Created, got %d with body %s", recorder.Code, recorder.Body.String())
	}

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("expected onChange to be triggered after campaign create")
	}

	entries := handler.Store().list()
	if len(entries) != 1 {
		t.Fatalf("expected 1 campaign in store, got %d", len(entries))
	}
	if entries[0].OrtbEndpointURL != "https://demand.example/openrtb" {
		t.Fatalf("expected created campaign to be stored, got %#v", entries[0])
	}
}