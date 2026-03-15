package endpoints

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	clickHouseDSNEnv        = "CLICKHOUSE_DSN"
	clickHouseVideoTableEnv = "CLICKHOUSE_VIDEO_TABLE"
	defaultClickHouseTable  = "video_event_facts"
)

type videoMetricEvent struct {
	EventTime     time.Time
	EventType     string
	PlacementID   string
	PublisherID   string
	AdvertiserID  string
	Bidder        string
	AppID         string
	Country       string
	Device        string
	Format        string
	DemandChannel string
	AuctionID     string
	BidID         string
	CrID          string
	PriceCPM      float64
	RevenueUSD    float64
}

type clickHouseVideoMetricsStore struct {
	db    *sql.DB
	table string
	queue chan videoMetricEvent
	done  chan struct{}
	wg    sync.WaitGroup
}

func newClickHouseVideoMetricsStoreFromEnv() *clickHouseVideoMetricsStore {
	dsn := strings.TrimSpace(os.Getenv(clickHouseDSNEnv))
	if dsn == "" {
		return nil
	}
	table := strings.TrimSpace(os.Getenv(clickHouseVideoTableEnv))
	if table == "" {
		table = defaultClickHouseTable
	}
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		log.Printf("clickhouse metrics: open failed: %v", err)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Printf("clickhouse metrics: ping failed: %v", err)
		_ = db.Close()
		return nil
	}
	store := &clickHouseVideoMetricsStore{
		db:    db,
		table: table,
		queue: make(chan videoMetricEvent, 4096),
		done:  make(chan struct{}),
	}
	if err := store.ensureSchema(ctx); err != nil {
		log.Printf("clickhouse metrics: schema failed: %v", err)
		_ = db.Close()
		return nil
	}
	store.wg.Add(1)
	safeGo(store.run)
	log.Printf("clickhouse metrics: enabled (table %s)", table)
	return store
}

func (s *clickHouseVideoMetricsStore) ensureSchema(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			event_time DateTime64(3),
			event_date Date MATERIALIZED toDate(event_time),
			event_type LowCardinality(String),
			placement_id String,
			publisher_id String,
			advertiser_id String,
			bidder LowCardinality(String),
			app_id String,
			country LowCardinality(String),
			device LowCardinality(String),
			format LowCardinality(String),
			demand_channel LowCardinality(String),
			auction_id String,
			bid_id String,
			crid String,
			price_cpm Float64,
			revenue_usd Float64
		) ENGINE = MergeTree
		PARTITION BY toYYYYMM(event_date)
		ORDER BY (event_date, event_type, placement_id, publisher_id, advertiser_id, bidder, auction_id, bid_id)
	`, s.table)
	_, err := s.db.ExecContext(ctx, query)
	return err
}

func (s *clickHouseVideoMetricsStore) run() {
	defer s.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	batch := make([]videoMetricEvent, 0, 256)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.insertBatch(ctx, batch); err != nil {
			log.Printf("clickhouse metrics: insert batch failed: %v", err)
		}
		batch = batch[:0]
	}
	for {
		select {
		case ev := <-s.queue:
			batch = append(batch, ev)
			if len(batch) >= 256 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.done:
			flush()
			return
		}
	}
}

func (s *clickHouseVideoMetricsStore) Record(ev videoMetricEvent) {
	if s == nil || s.db == nil {
		return
	}
	if ev.EventTime.IsZero() {
		ev.EventTime = time.Now().UTC()
	}
	select {
	case s.queue <- ev:
		return
	case <-time.After(50 * time.Millisecond):
		ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
		defer cancel()
		if err := s.insertBatch(ctx, []videoMetricEvent{ev}); err != nil {
			log.Printf("clickhouse metrics: fallback insert failed: %v", err)
		}
	}
}

func (s *clickHouseVideoMetricsStore) insertBatch(ctx context.Context, batch []videoMetricEvent) error {
	if len(batch) == 0 {
		return nil
	}
	var builder strings.Builder
	args := make([]interface{}, 0, len(batch)*16)
	builder.WriteString("INSERT INTO ")
	builder.WriteString(s.table)
	builder.WriteString(" (event_time,event_type,placement_id,publisher_id,advertiser_id,bidder,app_id,country,device,format,demand_channel,auction_id,bid_id,crid,price_cpm,revenue_usd) VALUES ")
	for i, ev := range batch {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		args = append(args,
			ev.EventTime.UTC(),
			ev.EventType,
			ev.PlacementID,
			ev.PublisherID,
			ev.AdvertiserID,
			ev.Bidder,
			ev.AppID,
			ev.Country,
			ev.Device,
			ev.Format,
			ev.DemandChannel,
			ev.AuctionID,
			ev.BidID,
			ev.CrID,
			ev.PriceCPM,
			ev.RevenueUSD,
		)
	}
	_, err := s.db.ExecContext(ctx, builder.String(), args...)
	return err
}

func (s *clickHouseVideoMetricsStore) Snapshot(ctx context.Context) (VideoStatsPayload, error) {
	result := VideoStatsPayload{
		ByPublisher:     map[string]*VideoStats{},
		ByAdvertiser:    map[string]*VideoStats{},
		ByBidder:        map[string]*VideoStats{},
		ByApp:           map[string]*VideoStats{},
		ByPlacement:     map[string]*VideoStats{},
		ByCountry:       map[string]*VideoStats{},
		ByDevice:        map[string]*VideoStats{},
		ByFormat:        map[string]*VideoStats{},
		ByDemandChannel: map[string]*VideoStats{},
	}
	if s == nil || s.db == nil {
		return result, nil
	}
	var err error
	if result.ByPublisher, err = s.queryGrouped(ctx, "publisher_id"); err != nil {
		return result, err
	}
	if result.ByAdvertiser, err = s.queryGrouped(ctx, "advertiser_id"); err != nil {
		return result, err
	}
	if result.ByBidder, err = s.queryGrouped(ctx, "bidder"); err != nil {
		return result, err
	}
	if result.ByApp, err = s.queryGrouped(ctx, "app_id"); err != nil {
		return result, err
	}
	if result.ByPlacement, err = s.queryGrouped(ctx, "placement_id"); err != nil {
		return result, err
	}
	if result.ByCountry, err = s.queryGrouped(ctx, "country"); err != nil {
		return result, err
	}
	if result.ByDevice, err = s.queryGrouped(ctx, "device"); err != nil {
		return result, err
	}
	if result.ByFormat, err = s.queryGrouped(ctx, "format"); err != nil {
		return result, err
	}
	if result.ByDemandChannel, err = s.queryGrouped(ctx, "demand_channel"); err != nil {
		return result, err
	}
	if result.Total, err = s.queryTotal(ctx); err != nil {
		return result, err
	}
	if result.StartedAt, err = s.queryStartedAt(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func (s *clickHouseVideoMetricsStore) queryGrouped(ctx context.Context, column string) (map[string]*VideoStats, error) {
	allowed := map[string]bool{
		"publisher_id":    true,
		"advertiser_id":   true,
		"bidder":          true,
		"app_id":          true,
		"placement_id":    true,
		"country":         true,
		"device":          true,
		"format":          true,
		"demand_channel":  true,
	}
	if !allowed[column] {
		return nil, fmt.Errorf("unsupported column %q", column)
	}
	query := fmt.Sprintf(`
		SELECT
			if(%[1]s = '', '(unknown)', %[1]s) AS dim,
			countIf(event_type = 'request') AS ad_requests,
			countIf(event_type = 'opportunity') AS opportunities,
			countIf(event_type = 'impression') AS impressions,
			countIf(event_type = 'complete') AS completes,
			sumIf(revenue_usd, event_type = 'impression') AS revenue
		FROM %s
		GROUP BY dim
	`, column, s.table)
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*VideoStats{}
	for rows.Next() {
		var key string
		stats := &VideoStats{}
		if err := rows.Scan(&key, &stats.AdRequests, &stats.Opportunities, &stats.Impressions, &stats.Completes, &stats.Revenue); err != nil {
			return nil, err
		}
		if stats.Impressions > 0 {
			stats.VCR = float64(stats.Completes) / float64(stats.Impressions) * 100
		}
		out[key] = stats
	}
	return out, rows.Err()
}

func (s *clickHouseVideoMetricsStore) queryTotal(ctx context.Context) (VideoStats, error) {
	query := fmt.Sprintf(`
		SELECT
			countIf(event_type = 'request') AS ad_requests,
			countIf(event_type = 'opportunity') AS opportunities,
			countIf(event_type = 'impression') AS impressions,
			countIf(event_type = 'complete') AS completes,
			sumIf(revenue_usd, event_type = 'impression') AS revenue
		FROM %s
	`, s.table)
	stats := VideoStats{}
	if err := s.db.QueryRowContext(ctx, query).Scan(&stats.AdRequests, &stats.Opportunities, &stats.Impressions, &stats.Completes, &stats.Revenue); err != nil {
		return stats, err
	}
	if stats.Impressions > 0 {
		stats.VCR = float64(stats.Completes) / float64(stats.Impressions) * 100
	}
	return stats, nil
}

func (s *clickHouseVideoMetricsStore) queryStartedAt(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`SELECT toUnixTimestamp(min(event_time)) FROM %s`, s.table)
	var started sql.NullInt64
	if err := s.db.QueryRowContext(ctx, query).Scan(&started); err != nil {
		return 0, err
	}
	if !started.Valid {
		return time.Now().Unix(), nil
	}
	return started.Int64, nil
}

func (s *clickHouseVideoMetricsStore) Reset(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`TRUNCATE TABLE %s`, s.table))
	return err
}

func (s *clickHouseVideoMetricsStore) Close() {
	if s == nil {
		return
	}
	close(s.done)
	s.wg.Wait()
	_ = s.db.Close()
}

func metricAppID(pr *PlayerRequest, cfg *AdServerConfig) string {
	if pr != nil {
		if pr.AppBundle != "" {
			return pr.AppBundle
		}
		if pr.Domain != "" {
			return pr.Domain
		}
	}
	if cfg != nil && cfg.DomainOrApp != "" {
		return cfg.DomainOrApp
	}
	return "(unknown)"
}

func metricCountry(pr *PlayerRequest) string {
	if pr != nil && pr.CountryCode != "" {
		return pr.CountryCode
	}
	return "(unknown)"
}

func metricDevice(pr *PlayerRequest) string {
	if pr == nil {
		return "Unknown"
	}
	return deviceTypeLabel(pr.DeviceType)
}

func metricFormat(cfg *AdServerConfig) string {
	if cfg != nil && cfg.VideoPlacementType != "" {
		return cfg.VideoPlacementType
	}
	return "(unknown)"
}

func (h *VideoPipelineHandler) recordRequestMetric(pr *PlayerRequest, cfg *AdServerConfig) {
	if h.clickHouseMetrics == nil || cfg == nil {
		return
	}
	h.clickHouseMetrics.Record(videoMetricEvent{
		EventTime:     time.Now().UTC(),
		EventType:     "request",
		PlacementID:   cfg.PlacementID,
		PublisherID:   cfg.PublisherID,
		AdvertiserID:  cfg.AdvertiserID,
		AppID:         metricAppID(pr, cfg),
		Country:       metricCountry(pr),
		Device:        metricDevice(pr),
		Format:        metricFormat(cfg),
		DemandChannel: demandChannelLabel(resolveDemandType(cfg)),
	})
}

func (h *VideoPipelineHandler) recordOpportunityMetric(pr *PlayerRequest, cfg *AdServerConfig, resp *DemandResponse) {
	if h.clickHouseMetrics == nil || cfg == nil || resp == nil || resp.NoFill {
		return
	}
	bidder := resp.Bidder
	if bidder == "" {
		bidder = "Unknown"
	}
	h.clickHouseMetrics.Record(videoMetricEvent{
		EventTime:     time.Now().UTC(),
		EventType:     "opportunity",
		PlacementID:   cfg.PlacementID,
		PublisherID:   cfg.PublisherID,
		AdvertiserID:  cfg.AdvertiserID,
		Bidder:        bidder,
		AppID:         metricAppID(pr, cfg),
		Country:       metricCountry(pr),
		Device:        metricDevice(pr),
		Format:        metricFormat(cfg),
		DemandChannel: demandChannelLabel(resolveDemandType(cfg)),
		AuctionID:     resp.AuctionID,
		PriceCPM:      resp.WinPrice,
	})
}

func (h *VideoPipelineHandler) recordImpressionMetric(placementID, auctionID, bidID, bidder, crid string, priceCPM float64, cfg *AdServerConfig, dk *auctionDimKey) {
	if h.clickHouseMetrics == nil {
		return
	}
	ev := videoMetricEvent{
		EventTime:    time.Now().UTC(),
		EventType:    "impression",
		PlacementID:  placementID,
		PublisherID:  "unknown",
		AuctionID:    auctionID,
		BidID:        bidID,
		Bidder:       bidder,
		CrID:         crid,
		PriceCPM:     priceCPM,
		RevenueUSD:   priceCPM / 1000,
		Country:      "(unknown)",
		Device:       "Unknown",
		Format:       "(unknown)",
		DemandChannel:"Unknown",
		AppID:        "(unknown)",
	}
	if cfg != nil {
		ev.PublisherID = cfg.PublisherID
		ev.AdvertiserID = cfg.AdvertiserID
		ev.Format = metricFormat(cfg)
		ev.AppID = metricAppID(nil, cfg)
		ev.DemandChannel = demandChannelLabel(resolveDemandType(cfg))
	}
	if dk != nil {
		ev.PublisherID = dk.PublisherID
		ev.AdvertiserID = dk.AdvertiserID
		ev.Bidder = dk.Bidder
		ev.AppID = dk.App
		ev.Country = dk.Country
		ev.Device = dk.Device
		ev.Format = dk.Format
		ev.DemandChannel = dk.DemandCh
		ev.PriceCPM = dk.PriceCPM
		ev.RevenueUSD = dk.PriceCPM / 1000
	}
	h.clickHouseMetrics.Record(ev)
}

func (h *VideoPipelineHandler) recordTrackingMetric(ev TrackingEvent, cfg *AdServerConfig, dk *auctionDimKey) {
	if h.clickHouseMetrics == nil {
		return
	}
	record := videoMetricEvent{
		EventTime:    ev.ReceivedAt.UTC(),
		EventType:    string(ev.Event),
		PlacementID:  ev.PlacementID,
		PublisherID:  "unknown",
		AuctionID:    ev.AuctionID,
		BidID:        ev.BidID,
		Bidder:       ev.Bidder,
		CrID:         ev.CrID,
		PriceCPM:     ev.Price,
		Country:      "(unknown)",
		Device:       "Unknown",
		Format:       "(unknown)",
		DemandChannel:"Unknown",
		AppID:        "(unknown)",
	}
	if cfg != nil {
		record.PublisherID = cfg.PublisherID
		record.AdvertiserID = cfg.AdvertiserID
		record.Format = metricFormat(cfg)
		record.AppID = metricAppID(nil, cfg)
		record.DemandChannel = demandChannelLabel(resolveDemandType(cfg))
	}
	if dk != nil {
		record.PublisherID = dk.PublisherID
		record.AdvertiserID = dk.AdvertiserID
		record.Bidder = dk.Bidder
		record.AppID = dk.App
		record.Country = dk.Country
		record.Device = dk.Device
		record.Format = dk.Format
		record.DemandChannel = dk.DemandCh
		record.PriceCPM = dk.PriceCPM
	}
	h.clickHouseMetrics.Record(record)
}

func (h *VideoPipelineHandler) snapshotVideoMetrics() VideoStatsPayload {
	if h.clickHouseMetrics != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		payload, err := h.clickHouseMetrics.Snapshot(ctx)
		if err == nil {
			return payload
		}
		log.Printf("clickhouse metrics: snapshot failed, falling back to memory: %v", err)
	}
	return h.videoStats.snapshot()
}

func (h *VideoPipelineHandler) resetVideoMetrics() error {
	h.videoStats.reset()
	if h.clickHouseMetrics == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return h.clickHouseMetrics.Reset(ctx)
}