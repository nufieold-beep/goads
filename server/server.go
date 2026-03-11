package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/logger"
	"github.com/prebid/prebid-server/v4/metrics"
	metricsconfig "github.com/prebid/prebid-server/v4/metrics/config"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// Listen blocks forever, serving PBS requests on the given port. This will block forever, until the process is shut down.
func Listen(cfg *config.Configuration, handler http.Handler, adminHandler http.Handler, metrics *metricsconfig.DetailedMetricsEngine) (err error) {
	stopSignals := make(chan os.Signal, 1)
	signal.Notify(stopSignals, syscall.SIGTERM, syscall.SIGINT)

	// Run the servers. Fan any process-stopper signals out to each server for graceful shutdowns.
	stopAdmin := make(chan os.Signal)
	stopMain := make(chan os.Signal)
	stopPrometheus := make(chan os.Signal)
	stopChannels := []chan<- os.Signal{stopMain}
	done := make(chan struct{})
	useFast := os.Getenv("USE_FASTHTTP") == "1"

	if cfg.UnixSocketEnable && len(cfg.UnixSocketName) > 0 { // start the unix_socket server if config enable-it.
		addr := cfg.UnixSocketName
		var socketListener net.Listener
		socketListener, err = newUnixListener(addr, metrics)
		if err != nil {
			logger.Errorf("Error listening for Unix-Socket connections on path %s: %v for socket server", addr, err)
			return
		}
		if useFast {
			fsrv := newFastHTTPServer(handler, cfg.Compression.Response)
			go shutdownAfterSignalsFast(fsrv, stopMain, done)
			go runFastHTTPServer(fsrv, "UnixSocket", socketListener)
		} else {
			mainServer := newSocketServer(cfg, handler)
			go shutdownAfterSignals(mainServer, stopMain, done)
			go runServer(mainServer, "UnixSocket", socketListener)
		}
	} else { // start the TCP server
		addr := cfg.Host + ":" + strconv.Itoa(cfg.Port)
		var mainListener net.Listener
		mainListener, err = newTCPListener(addr, metrics)
		if err != nil {
			logger.Errorf("Error listening for TCP connections on %s: %v for main server", addr, err)
			return
		}
		if useFast {
			fsrv := newFastHTTPServer(handler, cfg.Compression.Response)
			go shutdownAfterSignalsFast(fsrv, stopMain, done)
			go runFastHTTPServer(fsrv, "Main", mainListener)
		} else {
			mainServer := newMainServer(cfg, handler)
			go shutdownAfterSignals(mainServer, stopMain, done)
			go runServer(mainServer, "Main", mainListener)
		}
	}

	if cfg.Admin.Enabled {
		stopChannels = append(stopChannels, stopAdmin)
		adminServer := newAdminServer(cfg, adminHandler)
		go shutdownAfterSignals(adminServer, stopAdmin, done)

		var adminListener net.Listener
		if adminListener, err = newTCPListener(adminServer.Addr, nil); err != nil {
			logger.Errorf("Error listening for TCP connections on %s: %v for admin server", adminServer.Addr, err)
			return
		}
		go runServer(adminServer, "Admin", adminListener)
	}

	if cfg.Metrics.Prometheus.Port != 0 {
		var (
			prometheusListener net.Listener
			prometheusServer   = newPrometheusServer(cfg, metrics)
		)
		stopChannels = append(stopChannels, stopPrometheus)
		go shutdownAfterSignals(prometheusServer, stopPrometheus, done)
		if prometheusListener, err = newTCPListener(prometheusServer.Addr, nil); err != nil {
			logger.Errorf("Error listening for TCP connections on %s: %v for prometheus server", prometheusServer.Addr, err)
			return
		}

		go runServer(prometheusServer, "Prometheus", prometheusListener)
	}

	wait(stopSignals, done, stopChannels...)

	return
}

func newAdminServer(cfg *config.Configuration, handler http.Handler) *http.Server {
	host := cfg.Host
	if host == "" {
		host = "127.0.0.1"
	}
	return &http.Server{
		Addr:    host + ":" + strconv.Itoa(cfg.AdminPort),
		Handler: handler,
	}
}

func newMainServer(cfg *config.Configuration, handler http.Handler) *http.Server {
	serverHandler := getCompressionEnabledHandler(handler, cfg.Compression.Response)

	return &http.Server{
		Addr:              cfg.Host + ":" + strconv.Itoa(cfg.Port),
		Handler:           serverHandler,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 15, // 32 KB
	}

}

func newFastHTTPServer(handler http.Handler, compressionInfo config.CompressionInfo) *fasthttp.Server {
	wrapped := getCompressionEnabledHandler(handler, compressionInfo)
	return &fasthttp.Server{
		Handler:            fasthttpadaptor.NewFastHTTPHandler(wrapped),
		ReadTimeout:        10 * time.Second,
		WriteTimeout:       10 * time.Second,
		IdleTimeout:        120 * time.Second,
		MaxRequestBodySize: 2 << 20, // 2 MB — ad requests are small
		Concurrency:        16384,   // max concurrent connections
		ReadBufferSize:     8192,
		WriteBufferSize:    8192,
		DisableKeepalive:   false,
		TCPKeepalive:       true,
		TCPKeepalivePeriod: 30 * time.Second,
		ReduceMemoryUsage:  false,
	}
}

func newSocketServer(cfg *config.Configuration, handler http.Handler) *http.Server {
	serverHandler := getCompressionEnabledHandler(handler, cfg.Compression.Response)

	return &http.Server{
		Addr:              cfg.UnixSocketName,
		Handler:           serverHandler,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}
}

func getCompressionEnabledHandler(h http.Handler, compressionInfo config.CompressionInfo) http.Handler {
	if compressionInfo.GZIP {
		h = gziphandler.GzipHandler(h)
	}
	return h
}

func runServer(server *http.Server, name string, listener net.Listener) (err error) {
	if server == nil {
		err = fmt.Errorf(">> Server is a nil_ptr.")
		logger.Errorf("%s server quit with error: %v", name, err)
		return
	} else if listener == nil {
		err = fmt.Errorf(">> Listener is a nil.")
		logger.Errorf("%s server quit with error: %v", name, err)
		return
	}

	logger.Infof("%s server starting on: %s", name, server.Addr)
	if err = server.Serve(listener); err != nil {
		logger.Errorf("%s server quit with error: %v", name, err)
	}
	return
}

func runFastHTTPServer(server *fasthttp.Server, name string, listener net.Listener) (err error) {
	if server == nil {
		err = fmt.Errorf(">> Server is a nil_ptr.")
		logger.Errorf("%s server quit with error: %v", name, err)
		return
	} else if listener == nil {
		err = fmt.Errorf(">> Listener is a nil.")
		logger.Errorf("%s server quit with error: %v", name, err)
		return
	}

	logger.Infof("%s server starting on: %s", name, listener.Addr())
	if err = server.Serve(listener); err != nil {
		logger.Errorf("%s server quit with error: %v", name, err)
	}
	return
}

func newTCPListener(address string, metrics metrics.MetricsEngine) (net.Listener, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("Error listening for TCP connections on %s: %v", address, err)
	}

	// This cast is in Go's core libs as Server.ListenAndServe(), so it _should_ be safe, but just in case it changes in a future version...
	if casted, ok := ln.(*net.TCPListener); ok {
		ln = &tcpKeepAliveListener{casted}
	} else {
		logger.Warnf("net.Listen(\"tcp\", \"addr\") didn't return a TCPListener as it did in Go 1.9. Things will probably work fine... but this should be investigated.")
	}

	if metrics != nil {
		ln = &monitorableListener{ln, metrics}
	}

	return ln, nil
}

func newUnixListener(address string, metrics metrics.MetricsEngine) (net.Listener, error) {
	ln, err := net.Listen("unix", address)
	if err != nil {
		return nil, fmt.Errorf("Error listening for Unix-Socket connections on path %s: %v", address, err)
	}

	if casted, ok := ln.(*net.UnixListener); ok {
		ln = &unixListener{casted}
	} else {
		logger.Warnf("net.Listen(\"unix\", \"addr\") didn't return an UnixListener.")
	}

	if metrics != nil {
		ln = &monitorableListener{ln, metrics}
	}

	return ln, nil
}

func wait(inbound <-chan os.Signal, done <-chan struct{}, outbound ...chan<- os.Signal) {
	sig := <-inbound

	for i := 0; i < len(outbound); i++ {
		go sendSignal(outbound[i], sig)
	}

	for i := 0; i < len(outbound); i++ {
		<-done
	}
}

func shutdownAfterSignalsFast(server *fasthttp.Server, stopper <-chan os.Signal, done chan<- struct{}) {
	sig := <-stopper
	logger.Infof("Stopping fasthttp server because of signal: %s", sig.String())
	server.Shutdown()
	var s struct{}
	done <- s
}

func shutdownAfterSignals(server *http.Server, stopper <-chan os.Signal, done chan<- struct{}) {
	sig := <-stopper

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var s struct{}
	logger.Infof("Stopping %s because of signal: %s", server.Addr, sig.String())
	if err := server.Shutdown(ctx); err != nil {
		logger.Errorf("Failed to shutdown %s: %v", server.Addr, err)
	}
	done <- s
}

func sendSignal(to chan<- os.Signal, sig os.Signal) {
	to <- sig
}
