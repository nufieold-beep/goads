package filelogger

import (
	"fmt"
	"sync"
	"time"

	cglog "github.com/chasex/glog"
	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/analytics"
	"github.com/prebid/prebid-server/v4/logger"
	"github.com/prebid/prebid-server/v4/util/jsonutil"
)

type RequestType string

const (
	COOKIE_SYNC        RequestType = "/cookie_sync"
	AUCTION            RequestType = "/openrtb2/auction"
	VIDEO              RequestType = "/openrtb2/video"
	SETUID             RequestType = "/set_uid"
	AMP                RequestType = "/openrtb2/amp"
	NOTIFICATION_EVENT RequestType = "/event"
)

type Logger interface {
	Debug(v ...interface{})
	Flush()
}

type FileLogger struct {
	Logger    Logger
	entries   chan string
	done      chan struct{}
	closeOnce sync.Once
}

func (f *FileLogger) LogAuctionObject(ao *analytics.AuctionObject) {
	f.enqueue(jsonifyAuctionObject(ao))
}

func (f *FileLogger) LogVideoObject(vo *analytics.VideoObject) {
	f.enqueue(jsonifyVideoObject(vo))
}

func (f *FileLogger) LogSetUIDObject(so *analytics.SetUIDObject) {
	f.enqueue(jsonifySetUIDObject(so))
}

func (f *FileLogger) LogCookieSyncObject(cso *analytics.CookieSyncObject) {
	f.enqueue(jsonifyCookieSync(cso))
}

func (f *FileLogger) LogAmpObject(ao *analytics.AmpObject) {
	if ao == nil {
		return
	}
	f.enqueue(jsonifyAmpObject(ao))
}

func (f *FileLogger) LogNotificationEventObject(ne *analytics.NotificationEvent) {
	if ne == nil {
		return
	}
	f.enqueue(jsonifyNotificationEventObject(ne))
}

func (f *FileLogger) Shutdown() {
	f.closeOnce.Do(func() {
		logger.Infof("[FileLogger] Shutdown, trying to flush buffer")
		close(f.entries)
		<-f.done
	})
}

func NewFileLogger(filename string) (analytics.Module, error) {
	options := cglog.LogOptions{File: filename, Flag: cglog.LstdFlags, Level: cglog.Ldebug, Mode: cglog.R_Day}
	if logger, err := cglog.New(options); err == nil {
		fileLogger := &FileLogger{
			Logger:  logger,
			entries: make(chan string, 1024),
			done:    make(chan struct{}),
		}
		go fileLogger.run()
		return fileLogger, nil
	} else {
		return nil, err
	}
}

func (f *FileLogger) enqueue(entry string) {
	select {
	case f.entries <- entry:
	default:
		logger.Warnf("[FileLogger] queue is full, dropping analytics log entry")
	}
}

func (f *FileLogger) run() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	defer close(f.done)

	for {
		select {
		case entry, ok := <-f.entries:
			if !ok {
				f.Logger.Flush()
				return
			}
			f.Logger.Debug(entry)
		case <-ticker.C:
			f.Logger.Flush()
		}
	}
}

func jsonifyAuctionObject(ao *analytics.AuctionObject) string {
	var logEntry *logAuction
	if ao != nil {
		var request *openrtb2.BidRequest
		if ao.RequestWrapper != nil {
			request = ao.RequestWrapper.BidRequest
		}
		logEntry = &logAuction{Status: ao.Status, Errors: ao.Errors, Request: request, Response: ao.Response, Account: ao.Account, StartTime: ao.StartTime, HookExecutionOutcome: ao.HookExecutionOutcome}
	}

	b, err := jsonutil.Marshal(&struct {
		Type RequestType `json:"type"`
		*logAuction
	}{Type: AUCTION, logAuction: logEntry})
	if err == nil {
		return string(b)
	}
	return fmt.Sprintf("Transactional Logs Error: Auction object badly formed %v", err)
}

func jsonifyVideoObject(vo *analytics.VideoObject) string {
	var logEntry *logVideo
	if vo != nil {
		var request *openrtb2.BidRequest
		if vo.RequestWrapper != nil {
			request = vo.RequestWrapper.BidRequest
		}
		logEntry = &logVideo{Status: vo.Status, Errors: vo.Errors, Request: request, Response: vo.Response, VideoRequest: vo.VideoRequest, VideoResponse: vo.VideoResponse, StartTime: vo.StartTime}
	}

	b, err := jsonutil.Marshal(&struct {
		Type RequestType `json:"type"`
		*logVideo
	}{Type: VIDEO, logVideo: logEntry})
	if err == nil {
		return string(b)
	}
	return fmt.Sprintf("Transactional Logs Error: Video object badly formed %v", err)
}

func jsonifyCookieSync(cso *analytics.CookieSyncObject) string {
	var logEntry *logUserSync
	if cso != nil {
		logEntry = &logUserSync{Status: cso.Status, Errors: cso.Errors, BidderStatus: cso.BidderStatus}
	}

	b, err := jsonutil.Marshal(&struct {
		Type RequestType `json:"type"`
		*logUserSync
	}{Type: COOKIE_SYNC, logUserSync: logEntry})
	if err == nil {
		return string(b)
	}
	return fmt.Sprintf("Transactional Logs Error: Cookie sync object badly formed %v", err)
}

func jsonifySetUIDObject(so *analytics.SetUIDObject) string {
	var logEntry *logSetUID
	if so != nil {
		logEntry = &logSetUID{Status: so.Status, Bidder: so.Bidder, UID: so.UID, Errors: so.Errors, Success: so.Success}
	}

	b, err := jsonutil.Marshal(&struct {
		Type RequestType `json:"type"`
		*logSetUID
	}{Type: SETUID, logSetUID: logEntry})
	if err == nil {
		return string(b)
	}
	return fmt.Sprintf("Transactional Logs Error: Set UID object badly formed %v", err)
}

func jsonifyAmpObject(ao *analytics.AmpObject) string {
	var logEntry *logAMP
	if ao != nil {
		var request *openrtb2.BidRequest
		if ao.RequestWrapper != nil {
			request = ao.RequestWrapper.BidRequest
		}
		logEntry = &logAMP{Status: ao.Status, Errors: ao.Errors, Request: request, AuctionResponse: ao.AuctionResponse, AmpTargetingValues: ao.AmpTargetingValues, Origin: ao.Origin, StartTime: ao.StartTime, HookExecutionOutcome: ao.HookExecutionOutcome}
	}

	b, err := jsonutil.Marshal(&struct {
		Type RequestType `json:"type"`
		*logAMP
	}{Type: AMP, logAMP: logEntry})
	if err == nil {
		return string(b)
	}
	return fmt.Sprintf("Transactional Logs Error: Amp object badly formed %v", err)
}

func jsonifyNotificationEventObject(ne *analytics.NotificationEvent) string {
	var logEntry *logNotificationEvent
	if ne != nil {
		logEntry = &logNotificationEvent{Request: ne.Request, Account: ne.Account}
	}

	b, err := jsonutil.Marshal(&struct {
		Type RequestType `json:"type"`
		*logNotificationEvent
	}{Type: NOTIFICATION_EVENT, logNotificationEvent: logEntry})
	if err == nil {
		return string(b)
	}
	return fmt.Sprintf("Transactional Logs Error: NotificationEvent object badly formed %v", err)
}