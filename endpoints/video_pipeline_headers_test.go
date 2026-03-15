package endpoints

import (
	"net/http"
	"testing"

	"github.com/prebid/openrtb/v20/openrtb2"
)

func TestApplyOutboundHeadersAddsDeviceHeaders(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://demand.example/openrtb", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	bidReq := &openrtb2.BidRequest{
		Device: &openrtb2.Device{
			UA:         "PlayerUA/1.0",
			IP:         "203.0.113.10",
		},
	}

	applyOutboundHeaders(req, bidReq, "2.6")

	assertHeader(t, req, "Content-Type", "application/json;charset=utf-8")
	assertHeader(t, req, "Accept-Encoding", "gzip")
	assertHeader(t, req, "Connection", "keep-alive")
	assertHeader(t, req, "X-OpenRTB-Version", "2.6")
	assertHeader(t, req, "User-Agent", "PlayerUA/1.0")
	assertHeader(t, req, "X-Device-User-Agent", "PlayerUA/1.0")
	assertHeader(t, req, "X-Forwarded-For", "203.0.113.10")
	assertHeader(t, req, "X-Device-IP", "203.0.113.10")
}

func assertHeader(t *testing.T, req *http.Request, key, want string) {
	t.Helper()
	if got := req.Header.Get(key); got != want {
		t.Fatalf("header %s = %q, want %q", key, got, want)
	}
}