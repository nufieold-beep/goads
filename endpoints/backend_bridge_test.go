package endpoints

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prebid/prebid-server/v4/config"
)

func TestBackendBridgeForwardsJSONRequest(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Fatalf("expected PATCH, got %s", r.Method)
		}
		if r.URL.Path != "/orders/42" {
			t.Fatalf("expected path /orders/42, got %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("expand"); got != "items" {
			t.Fatalf("expected query expand=items, got %s", got)
		}
		if got := r.Header.Get("X-Trace-Id"); got != "trace-123" {
			t.Fatalf("expected X-Trace-Id header, got %s", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":42,"state":"updated"}`))
	}))
	defer upstream.Close()

	handler := NewBackendBridgeHandler(upstream.Client(), config.BackendBridge{
		Enabled:          true,
		BaseURL:          upstream.URL,
		TimeoutMS:        1000,
		MaxResponseBytes: 1024,
		AllowedMethods:   []string{"PATCH"},
		AllowedPaths:     []string{"/orders"},
	})

	req := httptest.NewRequest(http.MethodPost, "/dashboard/backend/bridge", strings.NewReader(`{
		"method":"PATCH",
		"path":"/orders/42",
		"query":{"expand":"items"},
		"headers":{"X-Trace-Id":"trace-123"},
		"body":{"state":"paid"}
	}`))
	res := httptest.NewRecorder()

	handler(res, req, nil)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var payload backendBridgeResponse
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !payload.Success {
		t.Fatalf("expected success response, got %+v", payload)
	}
	if payload.Status != http.StatusCreated {
		t.Fatalf("expected upstream status 201, got %d", payload.Status)
	}
	data, ok := payload.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("expected JSON object data, got %#v", payload.Data)
	}
	if data["state"] != "updated" {
		t.Fatalf("expected state=updated, got %#v", data["state"])
	}
}

func TestBackendBridgeRejectsAbsolutePath(t *testing.T) {
	handler := NewBackendBridgeHandler(http.DefaultClient, config.BackendBridge{
		Enabled: true,
		BaseURL: "https://backend.internal",
	})

	req := httptest.NewRequest(http.MethodPost, "/dashboard/backend/bridge", strings.NewReader(`{
		"path":"https://evil.example/steal"
	}`))
	res := httptest.NewRecorder()

	handler(res, req, nil)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", res.Code)
	}
}

func TestBackendBridgeReturnsRawTextResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("queued"))
	}))
	defer upstream.Close()

	handler := NewBackendBridgeHandler(upstream.Client(), config.BackendBridge{
		Enabled:      true,
		BaseURL:      upstream.URL,
		AllowedPaths: []string{"/jobs"},
	})

	req := httptest.NewRequest(http.MethodPost, "/dashboard/backend/bridge", strings.NewReader(`{"path":"/jobs"}`))
	res := httptest.NewRecorder()

	handler(res, req, nil)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var payload backendBridgeResponse
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if payload.RawBody != "queued" {
		t.Fatalf("expected raw_body queued, got %+v", payload)
	}
	if payload.Status != http.StatusAccepted {
		t.Fatalf("expected upstream status 202, got %d", payload.Status)
	}
}

func TestBackendBridgeRejectsDisallowedMethod(t *testing.T) {
	handler := NewBackendBridgeHandler(http.DefaultClient, config.BackendBridge{
		Enabled:        true,
		BaseURL:        "https://backend.internal",
		AllowedMethods: []string{"POST"},
	})

	req := httptest.NewRequest(http.MethodPost, "/dashboard/backend/bridge", strings.NewReader(`{
		"method":"DELETE",
		"path":"/orders/42"
	}`))
	res := httptest.NewRecorder()

	handler(res, req, nil)

	if res.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", res.Code)
	}
}

func TestBackendBridgeRejectsDisallowedPath(t *testing.T) {
	handler := NewBackendBridgeHandler(http.DefaultClient, config.BackendBridge{
		Enabled:      true,
		BaseURL:      "https://backend.internal",
		AllowedPaths: []string{"/orders", "/jobs"},
	})

	req := httptest.NewRequest(http.MethodPost, "/dashboard/backend/bridge", strings.NewReader(`{
		"path":"/admin/secrets"
	}`))
	res := httptest.NewRecorder()

	handler(res, req, nil)

	if res.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", res.Code)
	}
}
