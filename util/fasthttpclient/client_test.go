package fasthttpclient

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRoundTripperPOST(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		defer r.Body.Close()
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if got := r.Header.Get("X-Test"); got != "value" {
			t.Fatalf("unexpected header %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"body":"` + string(body) + `"}`))
	}))
	defer server.Close()

	client := NewClient(5*time.Second, TransportConfig{
		Name:                "test-post",
		DialTimeout:         time.Second,
		KeepAlive:           time.Second,
		MaxConnsPerHost:     32,
		MaxIdleConnDuration: time.Minute,
		ReadTimeout:         5 * time.Second,
		WriteTimeout:        5 * time.Second,
	})

	req, err := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString("payload"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Test", "value")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if string(body) != `{"body":"payload"}` {
		t.Fatalf("unexpected response %s", string(body))
	}
}

func TestRoundTripperAutoDecompressesGzip(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept-Encoding"); got != "gzip" {
			t.Fatalf("unexpected accept-encoding %q", got)
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		_, _ = gz.Write([]byte("hello gzip"))
		_ = gz.Close()
	}))
	defer server.Close()

	client := NewClient(5*time.Second, TransportConfig{
		Name:                "test-gzip-auto",
		DialTimeout:         time.Second,
		KeepAlive:           time.Second,
		MaxConnsPerHost:     32,
		MaxIdleConnDuration: time.Minute,
		ReadTimeout:         5 * time.Second,
		WriteTimeout:        5 * time.Second,
	})

	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "" {
		t.Fatalf("expected decompressed response header to be cleared")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if string(body) != "hello gzip" {
		t.Fatalf("unexpected body %q", string(body))
	}
}

func TestRoundTripperKeepsExplicitGzipCompressed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept-Encoding"); got != "gzip" {
			t.Fatalf("unexpected accept-encoding %q", got)
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		_, _ = gz.Write([]byte("manual gzip"))
		_ = gz.Close()
	}))
	defer server.Close()

	client := NewClient(5*time.Second, TransportConfig{
		Name:                "test-gzip-explicit",
		DialTimeout:         time.Second,
		KeepAlive:           time.Second,
		MaxConnsPerHost:     32,
		MaxIdleConnDuration: time.Minute,
		ReadTimeout:         5 * time.Second,
		WriteTimeout:        5 * time.Second,
	})

	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("expected compressed response to be preserved")
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("new gzip reader: %v", err)
	}
	defer gz.Close()
	body, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read gzip response: %v", err)
	}
	if string(body) != "manual gzip" {
		t.Fatalf("unexpected body %q", string(body))
	}
}