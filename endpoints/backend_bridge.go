package endpoints

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/prebid/prebid-server/v4/config"
)

const (
	defaultBackendBridgeTimeout   = 5 * time.Second
	defaultBackendBridgeBodyLimit = int64(1 << 20)
	maxBackendBridgeRequestBody   = int64(1 << 20)
)

type backendBridgeRequest struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   map[string]string `json:"query,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

type backendBridgeResponse struct {
	Success bool              `json:"success"`
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Data    interface{}       `json:"data,omitempty"`
	RawBody string            `json:"raw_body,omitempty"`
	Error   string            `json:"error,omitempty"`
}

type backendBridgePolicy struct {
	allowedMethods map[string]struct{}
	allowedPaths   []string
}

// NewBackendBridgeHandler builds a controlled JSON bridge to a configured backend base URL.
func NewBackendBridgeHandler(client *http.Client, cfg config.BackendBridge) httprouter.Handle {
	baseURL := strings.TrimSpace(cfg.BaseURL)
	if client == nil || !cfg.Enabled || baseURL == "" {
		return func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
			writeBackendBridgeJSON(w, http.StatusNotFound, backendBridgeResponse{
				Success: false,
				Status:  http.StatusNotFound,
				Error:   "backend bridge is disabled",
			})
		}
	}

	timeout := defaultBackendBridgeTimeout
	if cfg.TimeoutMS > 0 {
		timeout = time.Duration(cfg.TimeoutMS) * time.Millisecond
	}

	maxResponseBytes := cfg.MaxResponseBytes
	if maxResponseBytes <= 0 {
		maxResponseBytes = defaultBackendBridgeBodyLimit
	}

	policy := newBackendBridgePolicy(cfg)

	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if r.Method != http.MethodPost {
			writeBackendBridgeJSON(w, http.StatusMethodNotAllowed, backendBridgeResponse{
				Success: false,
				Status:  http.StatusMethodNotAllowed,
				Error:   "only POST is allowed",
			})
			return
		}

		var req backendBridgeRequest
		decoder := json.NewDecoder(io.LimitReader(r.Body, maxBackendBridgeRequestBody))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			writeBackendBridgeJSON(w, http.StatusBadRequest, backendBridgeResponse{
				Success: false,
				Status:  http.StatusBadRequest,
				Error:   "invalid JSON payload: " + err.Error(),
			})
			return
		}

		method, err := normalizeBackendBridgeMethod(req.Method)
		if err != nil {
			writeBackendBridgeJSON(w, http.StatusBadRequest, backendBridgeResponse{
				Success: false,
				Status:  http.StatusBadRequest,
				Error:   err.Error(),
			})
			return
		}

		if err := policy.validate(method, req.Path); err != nil {
			writeBackendBridgeJSON(w, http.StatusForbidden, backendBridgeResponse{
				Success: false,
				Status:  http.StatusForbidden,
				Error:   err.Error(),
			})
			return
		}

		targetURL, err := buildBackendBridgeURL(baseURL, req.Path, req.Query)
		if err != nil {
			writeBackendBridgeJSON(w, http.StatusBadRequest, backendBridgeResponse{
				Success: false,
				Status:  http.StatusBadRequest,
				Error:   err.Error(),
			})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		upstreamReq, err := http.NewRequestWithContext(ctx, method, targetURL.String(), bytes.NewReader(req.Body))
		if err != nil {
			writeBackendBridgeJSON(w, http.StatusInternalServerError, backendBridgeResponse{
				Success: false,
				Status:  http.StatusInternalServerError,
				Error:   "failed to construct upstream request",
			})
			return
		}

		copyBackendBridgeHeaders(upstreamReq.Header, req.Headers)
		if len(req.Body) > 0 && upstreamReq.Header.Get("Content-Type") == "" {
			upstreamReq.Header.Set("Content-Type", "application/json")
		}

		resp, err := client.Do(upstreamReq)
		if err != nil {
			writeBackendBridgeJSON(w, http.StatusBadGateway, backendBridgeResponse{
				Success: false,
				Status:  http.StatusBadGateway,
				Error:   "upstream request failed: " + err.Error(),
			})
			return
		}
		defer resp.Body.Close()

		body, err := readBackendBridgeResponseBody(resp.Body, maxResponseBytes)
		if err != nil {
			writeBackendBridgeJSON(w, http.StatusBadGateway, backendBridgeResponse{
				Success: false,
				Status:  http.StatusBadGateway,
				Error:   err.Error(),
			})
			return
		}

		bridgeResp := backendBridgeResponse{
			Success: resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices,
			Status:  resp.StatusCode,
			Headers: flattenBackendBridgeHeaders(resp.Header),
		}

		if isJSONContentType(resp.Header.Get("Content-Type")) && len(body) > 0 {
			var payload interface{}
			if err := json.Unmarshal(body, &payload); err == nil {
				bridgeResp.Data = payload
			} else {
				bridgeResp.RawBody = string(body)
			}
		} else {
			bridgeResp.RawBody = string(body)
		}

		if !bridgeResp.Success && bridgeResp.Error == "" && bridgeResp.RawBody == "" {
			bridgeResp.Error = http.StatusText(resp.StatusCode)
		}

		writeBackendBridgeJSON(w, http.StatusOK, bridgeResp)
	}
}

func normalizeBackendBridgeMethod(method string) (string, error) {
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		method = http.MethodPost
	}

	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return method, nil
	default:
		return "", fmt.Errorf("unsupported method %q", method)
	}
}

func newBackendBridgePolicy(cfg config.BackendBridge) backendBridgePolicy {
	policy := backendBridgePolicy{}
	if len(cfg.AllowedMethods) > 0 {
		policy.allowedMethods = make(map[string]struct{}, len(cfg.AllowedMethods))
		for _, method := range cfg.AllowedMethods {
			method = strings.ToUpper(strings.TrimSpace(method))
			if method == "" {
				continue
			}
			policy.allowedMethods[method] = struct{}{}
		}
	}
	if len(cfg.AllowedPaths) > 0 {
		policy.allowedPaths = make([]string, 0, len(cfg.AllowedPaths))
		for _, path := range cfg.AllowedPaths {
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}
			if !strings.HasPrefix(path, "/") {
				path = "/" + path
			}
			policy.allowedPaths = append(policy.allowedPaths, path)
		}
	}
	return policy
}

func (p backendBridgePolicy) validate(method, requestPath string) error {
	if len(p.allowedMethods) > 0 {
		if _, ok := p.allowedMethods[method]; !ok {
			return fmt.Errorf("method %q is not allowed", method)
		}
	}

	if len(p.allowedPaths) == 0 {
		return nil
	}

	requestPath = strings.TrimSpace(requestPath)
	if requestPath == "" {
		return fmt.Errorf("path is required")
	}

	rel, err := url.Parse(requestPath)
	if err != nil {
		return fmt.Errorf("invalid path")
	}
	path := rel.Path
	if path == "" {
		path = "/"
	}

	for _, allowedPrefix := range p.allowedPaths {
		if strings.HasPrefix(path, allowedPrefix) {
			return nil
		}
	}

	return fmt.Errorf("path %q is not allowed", path)
}

func buildBackendBridgeURL(baseURL, requestPath string, query map[string]string) (*url.URL, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid backend bridge base_url")
	}

	requestPath = strings.TrimSpace(requestPath)
	if requestPath == "" {
		return nil, fmt.Errorf("path is required")
	}

	rel, err := url.Parse(requestPath)
	if err != nil {
		return nil, fmt.Errorf("invalid path")
	}
	if rel.IsAbs() || rel.Host != "" {
		return nil, fmt.Errorf("path must be relative to the configured backend")
	}

	target := base.ResolveReference(rel)
	values := target.Query()
	for key, value := range query {
		values.Set(key, value)
	}
	target.RawQuery = values.Encode()

	return target, nil
}

func copyBackendBridgeHeaders(dst http.Header, src map[string]string) {
	for key, value := range src {
		trimmedKey := strings.TrimSpace(key)
		if trimmedKey == "" {
			continue
		}
		switch http.CanonicalHeaderKey(trimmedKey) {
		case "Host", "Content-Length":
			continue
		default:
			dst.Set(trimmedKey, value)
		}
	}
}

func flattenBackendBridgeHeaders(headers http.Header) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	flat := make(map[string]string, len(headers))
	for key, values := range headers {
		flat[key] = strings.Join(values, ",")
	}
	return flat
}

func readBackendBridgeResponseBody(body io.Reader, maxBytes int64) ([]byte, error) {
	limited := io.LimitReader(body, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("failed reading upstream response")
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("upstream response exceeds %d bytes", maxBytes)
	}
	return data, nil
}

func isJSONContentType(contentType string) bool {
	contentType = strings.ToLower(contentType)
	return strings.Contains(contentType, "application/json") || strings.Contains(contentType, "+json")
}

func writeBackendBridgeJSON(w http.ResponseWriter, status int, payload backendBridgeResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
