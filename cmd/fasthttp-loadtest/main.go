package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

type headerFlags []string

func (h *headerFlags) String() string {
	return strings.Join(*h, ",")
}

func (h *headerFlags) Set(value string) error {
	*h = append(*h, value)
	return nil
}

func main() {
	var targetURL string
	var method string
	var body string
	var concurrency int
	var duration time.Duration
	var timeout time.Duration
	var headers headerFlags

	flag.StringVar(&targetURL, "url", "", "Target URL to exercise")
	flag.StringVar(&method, "method", fasthttp.MethodGet, "HTTP method")
	flag.StringVar(&body, "body", "", "Optional request body")
	flag.IntVar(&concurrency, "concurrency", 200, "Number of concurrent workers")
	flag.DurationVar(&duration, "duration", 15*time.Second, "How long to run the load test")
	flag.DurationVar(&timeout, "timeout", 5*time.Second, "Per-request timeout")
	flag.Var(&headers, "header", "Repeatable request header in 'Key: Value' form")
	flag.Parse()

	if strings.TrimSpace(targetURL) == "" {
		fmt.Fprintln(os.Stderr, "-url is required")
		os.Exit(2)
	}
	if concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "-concurrency must be > 0")
		os.Exit(2)
	}
	if duration <= 0 {
		fmt.Fprintln(os.Stderr, "-duration must be > 0")
		os.Exit(2)
	}

	client := &fasthttp.Client{
		ReadTimeout:         timeout,
		WriteTimeout:        timeout,
		MaxConnsPerHost:     concurrency * 2,
		MaxIdleConnDuration: 30 * time.Second,
		ReadBufferSize:      8192,
		WriteBufferSize:     8192,
	}

	deadline := time.Now().Add(duration)
	var totalRequests uint64
	var totalErrors uint64
	var totalBytes uint64
	var totalLatencyNs uint64
	var maxLatencyNs uint64
	statusCounts := make([]uint64, 600)
	var statusMu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for worker := 0; worker < concurrency; worker++ {
		go func() {
			defer wg.Done()
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(resp)

			req.Header.SetMethod(method)
			req.SetRequestURI(targetURL)
			if body != "" {
				req.SetBodyString(body)
			}
			for _, raw := range headers {
				parts := strings.SplitN(raw, ":", 2)
				if len(parts) != 2 {
					continue
				}
				req.Header.Set(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
			}

			for time.Now().Before(deadline) {
				resp.Reset()
				started := time.Now()
				err := client.DoTimeout(req, resp, timeout)
				latency := time.Since(started)

				atomic.AddUint64(&totalRequests, 1)
				atomic.AddUint64(&totalLatencyNs, uint64(latency))
				atomic.AddUint64(&totalBytes, uint64(len(resp.Body())))
				updateMax(&maxLatencyNs, uint64(latency))

				if err != nil {
					atomic.AddUint64(&totalErrors, 1)
					continue
				}

				status := resp.StatusCode()
				if status >= 0 && status < len(statusCounts) {
					statusMu.Lock()
					statusCounts[status]++
					statusMu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	requests := atomic.LoadUint64(&totalRequests)
	errors := atomic.LoadUint64(&totalErrors)
	bytesRead := atomic.LoadUint64(&totalBytes)
	totalLatency := atomic.LoadUint64(&totalLatencyNs)
	maxLatency := atomic.LoadUint64(&maxLatencyNs)

	if requests == 0 {
		fmt.Println("No requests completed.")
		return
	}

	seconds := duration.Seconds()
	avgLatency := time.Duration(totalLatency / requests)
	throughput := float64(requests) / seconds
	mbps := float64(bytesRead) / seconds / (1024 * 1024)

	fmt.Printf("Target: %s\n", targetURL)
	fmt.Printf("Method: %s\n", method)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Duration: %s\n", duration)
	fmt.Printf("Requests: %d\n", requests)
	fmt.Printf("Errors: %d\n", errors)
	fmt.Printf("Throughput: %.2f req/s\n", throughput)
	fmt.Printf("Average latency: %s\n", avgLatency)
	fmt.Printf("Max latency: %s\n", time.Duration(maxLatency))
	fmt.Printf("Response throughput: %.2f MiB/s\n", mbps)

	type statusCount struct {
		code  int
		count uint64
	}
	var nonZero []statusCount
	for code, count := range statusCounts {
		if count > 0 {
			nonZero = append(nonZero, statusCount{code: code, count: count})
		}
	}
	sort.Slice(nonZero, func(i, j int) bool { return nonZero[i].code < nonZero[j].code })
	if len(nonZero) > 0 {
		fmt.Println("Status codes:")
		for _, entry := range nonZero {
			fmt.Printf("  %d: %d\n", entry.code, entry.count)
		}
	}
}

func updateMax(dst *uint64, value uint64) {
	for {
		current := atomic.LoadUint64(dst)
		if value <= current {
			return
		}
		if atomic.CompareAndSwapUint64(dst, current, value) {
			return
		}
	}
}