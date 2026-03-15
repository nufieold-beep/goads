package endpoints

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testPipelineHandler() *VideoPipelineHandler {
	return &VideoPipelineHandler{
		configStore: newAdServerConfigStore(""),
		bufPool: sync.Pool{
			New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) },
		},
		demandClient: &http.Client{},
	}
}

func TestSecureStringEquals(t *testing.T) {
	if !secureStringEquals("secret", "secret") {
		t.Fatal("expected equal strings to match")
	}
	if secureStringEquals("secret", "Secret") {
		t.Fatal("expected different strings to mismatch")
	}
	if secureStringEquals("short", "longer") {
		t.Fatal("expected different-length strings to mismatch")
	}
}

func TestValidateOutboundPublicURL(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr bool
	}{
		{name: "public https host", rawURL: "https://example.com/openrtb", wantErr: false},
		{name: "private ipv4 rejected", rawURL: "http://127.0.0.1/openrtb", wantErr: true},
		{name: "localhost rejected", rawURL: "https://localhost/tag", wantErr: true},
		{name: "single label rejected", rawURL: "https://internal/tag", wantErr: true},
		{name: "bad scheme rejected", rawURL: "ftp://example.com/tag", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateOutboundPublicURL(tc.rawURL)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for %q", tc.rawURL)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.rawURL, err)
			}
		})
	}
}

func TestAdServerConfigEndpointRejectsPrivateDemandURLs(t *testing.T) {
	h := testPipelineHandler()
	req := httptest.NewRequest(http.MethodPost, "/video/adserver", strings.NewReader(`{
		"placement_id":"placement-1",
		"demand_ortb_url":"http://127.0.0.1:8080/openrtb"
	}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.AdServerConfigEndpoint()(rec, req, httprouter.Params{})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if !strings.Contains(rec.Body.String(), "invalid demand_ortb_url") {
		t.Fatalf("expected demand URL validation error, got %q", rec.Body.String())
	}
}

func TestFetchVASTRejectsPrivateURL(t *testing.T) {
	h := testPipelineHandler()
	_, err := h.fetchVAST(context.Background(), "http://127.0.0.1:8080/vast", "PlayerUA/1.0")
	if err == nil {
		t.Fatal("expected private VAST URL to be rejected")
	}
	if !strings.Contains(err.Error(), "invalid demand VAST url") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPostToDemandORTBRejectsPrivateURLBeforeNetwork(t *testing.T) {
	h := testPipelineHandler()
	var calls int32
	h.demandClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":"auction-1"}`))}, nil
	})}

	_, err := h.postToDemandORTB(context.Background(), &PlayerRequest{PlacementID: "placement-1"}, &AdServerConfig{
		PlacementID:   "placement-1",
		PublisherID:   "pub-1",
		Active:        true,
		DemandOrtbURL: "http://127.0.0.1:9000/openrtb",
	})
	if err == nil {
		t.Fatal("expected private ORTB URL to be rejected")
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("expected no outbound network call, got %d", calls)
	}
	if !strings.Contains(err.Error(), "invalid demand ORTB url") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNoticeURLsRejectPrivateHostsWithoutDispatch(t *testing.T) {
	h := testPipelineHandler()
	var calls int32
	h.demandClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})}

	h.fireWinNotice("http://127.0.0.1:8080/win")
	h.fireBillingNotice("http://127.0.0.1:8080/bill")
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("expected no outbound notice dispatch, got %d calls", calls)
	}
}