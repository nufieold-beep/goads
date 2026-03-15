package fasthttpclient

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

type TransportConfig struct {
	Name                string
	DialTimeout         time.Duration
	KeepAlive           time.Duration
	MaxConnsPerHost     int
	MaxIdleConnDuration time.Duration
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	TLSConfig           *tls.Config
	ReadBufferSize      int
	WriteBufferSize     int
}

type roundTripper struct {
	client *fasthttp.Client
}

type responseBody struct {
	reader  *bytes.Reader
	release sync.Once
	onClose func()
}

type gzipResponseBody struct {
	reader  *gzip.Reader
	release sync.Once
	onClose func()
}

func NewRoundTripper(cfg TransportConfig) http.RoundTripper {
	dialer := &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: cfg.KeepAlive,
	}

	client := &fasthttp.Client{
		Name:                cfg.Name,
		Dial: func(addr string) (net.Conn, error) {
			return dialer.Dial("tcp", addr)
		},
		MaxConnsPerHost:     cfg.MaxConnsPerHost,
		MaxIdleConnDuration: cfg.MaxIdleConnDuration,
		ReadTimeout:         cfg.ReadTimeout,
		WriteTimeout:        cfg.WriteTimeout,
		TLSConfig:           cfg.TLSConfig,
		ReadBufferSize:      cfg.ReadBufferSize,
		WriteBufferSize:     cfg.WriteBufferSize,
	}

	return &roundTripper{client: client}
}

func NewClient(timeout time.Duration, cfg TransportConfig) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: NewRoundTripper(cfg),
	}
}

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, fmt.Errorf("fasthttp transport: nil request")
	}
	if req.Context() != nil {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		default:
		}
	}

	fastReq := fasthttp.AcquireRequest()
	fastResp := fasthttp.AcquireResponse()
	release := func() {
		fasthttp.ReleaseRequest(fastReq)
		fasthttp.ReleaseResponse(fastResp)
	}

	fastReq.Header.SetMethod(req.Method)
	fastReq.SetRequestURI(req.URL.String())
	if req.Host != "" {
		fastReq.SetHost(req.Host)
		fastReq.Header.SetHost(req.Host)
	}
	if req.Close {
		fastReq.SetConnectionClose()
	}

	autoGzip := shouldAutoDecompress(req)
	for headerName, values := range req.Header {
		for _, value := range values {
			fastReq.Header.Add(headerName, value)
		}
	}
	if autoGzip {
		fastReq.Header.Set("Accept-Encoding", "gzip")
	}

	if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			release()
			return nil, fmt.Errorf("fasthttp transport: read request body: %w", err)
		}
		fastReq.SetBodyRaw(body)
	}

	if timeout, ok := requestTimeout(req); ok {
		if timeout <= 0 {
			release()
			return nil, req.Context().Err()
		}
		if err := rt.client.DoTimeout(fastReq, fastResp, timeout); err != nil {
			release()
			if req.Context() != nil && req.Context().Err() != nil {
				return nil, req.Context().Err()
			}
			return nil, err
		}
	} else {
		if err := rt.client.Do(fastReq, fastResp); err != nil {
			release()
			if req.Context() != nil && req.Context().Err() != nil {
				return nil, req.Context().Err()
			}
			return nil, err
		}
	}

	httpResp, err := toHTTPResponse(req, fastResp, autoGzip, release)
	if err != nil {
		release()
		return nil, err
	}
	return httpResp, nil
}

func shouldAutoDecompress(req *http.Request) bool {
	if req.Method == http.MethodHead {
		return false
	}
	if req.Header.Get("Accept-Encoding") != "" {
		return false
	}
	return req.Header.Get("Range") == ""
}

func requestTimeout(req *http.Request) (time.Duration, bool) {
	if req == nil || req.Context() == nil {
		return 0, false
	}
	deadline, ok := req.Context().Deadline()
	if !ok {
		return 0, false
	}
	return time.Until(deadline), true
}

func toHTTPResponse(req *http.Request, fastResp *fasthttp.Response, autoGzip bool, release func()) (*http.Response, error) {
	statusCode := fastResp.StatusCode()
	headers := make(http.Header, fastResp.Header.Len())
	fastResp.Header.VisitAll(func(key, value []byte) {
		headers.Add(string(key), string(value))
	})

	bodyBytes := fastResp.Body()
	body := &responseBody{
		reader:  bytes.NewReader(bodyBytes),
		onClose: release,
	}

	contentLength := int64(len(bodyBytes))
	uncompressed := false
	var responseBody io.ReadCloser = body

	if autoGzip && strings.EqualFold(headers.Get("Content-Encoding"), "gzip") {
		gzipReader, err := gzip.NewReader(bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, fmt.Errorf("fasthttp transport: build gzip reader: %w", err)
		}
		responseBody = &gzipResponseBody{
			reader:  gzipReader,
			onClose: release,
		}
		headers.Del("Content-Encoding")
		headers.Del("Content-Length")
		contentLength = -1
		uncompressed = true
	}

	statusText := http.StatusText(statusCode)
	if statusText == "" {
		statusText = "status"
	}

	return &http.Response{
		Status:           strconv.Itoa(statusCode) + " " + statusText,
		StatusCode:       statusCode,
		Proto:            "HTTP/1.1",
		ProtoMajor:       1,
		ProtoMinor:       1,
		Header:           headers,
		Body:             responseBody,
		ContentLength:    contentLength,
		Close:            strings.EqualFold(headers.Get("Connection"), "close"),
		Uncompressed:     uncompressed,
		Request:          req,
		TransferEncoding: nil,
		Trailer:          make(http.Header),
	}, nil
}

func (b *responseBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *responseBody) Close() error {
	b.release.Do(b.onClose)
	return nil
}

func (b *gzipResponseBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *gzipResponseBody) Close() error {
	err := b.reader.Close()
	b.release.Do(b.onClose)
	return err
}