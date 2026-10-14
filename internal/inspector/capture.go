package inspector

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
)

// CaptureConfig enables a single bounded capture session for this process.
// Use DefaultCaptureConfig and set Selector to opt in. There is no request API
// to enable capture, reset its budget, or extend its deadline.
type CaptureConfig struct {
	Selector   string
	Duration   time.Duration
	MaxSamples int
	Rate       int
	LabelBytes int
}

func DefaultCaptureConfig() CaptureConfig {
	return CaptureConfig{Duration: 5 * time.Minute, MaxSamples: 1000, Rate: 10, LabelBytes: 4096}
}

type sampleCapture struct {
	cfg      CaptureConfig
	matchers []*labels.Matcher
	logger   *slog.Logger
	clock    func() time.Time
	deadline time.Time

	mu          sync.Mutex
	attempts    int
	windowStart time.Time
	windowCount int

	matched   *prometheus.CounterVec
	captured  *prometheus.CounterVec
	skipped   *prometheus.CounterVec
	truncated *prometheus.CounterVec
}

func newSampleCapture(cfg CaptureConfig, reg prometheus.Registerer, identityLabels []string, logger *slog.Logger, clock func() time.Time) (*sampleCapture, error) {
	if cfg.Duration <= 0 || cfg.MaxSamples <= 0 || cfg.Rate <= 0 || cfg.LabelBytes < 2 {
		return nil, fmt.Errorf("capture duration, max samples and rate must be positive; capture label bytes must be at least 2")
	}
	matchers, err := parser.NewParser(parser.Options{}).ParseMetricSelector(cfg.Selector)
	if err != nil {
		return nil, fmt.Errorf("invalid capture selector: %w", err)
	}
	// Like a PromQL vector selector, require at least one matcher that cannot
	// match an absent/empty label. This also rejects an empty selector.
	constrained := false
	for _, m := range matchers {
		constrained = constrained || !m.Matches("")
	}
	if !constrained {
		return nil, fmt.Errorf("capture selector must contain at least one non-empty matcher")
	}
	now := clock()
	c := &sampleCapture{
		cfg: cfg, matchers: matchers, logger: logger, clock: clock,
		deadline: now.Add(cfg.Duration), windowStart: now,
		matched: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "remote_write_inspector_capture_matched_samples_total",
			Help: "Decoded samples matching the capture selector, including samples skipped after limits are reached.",
		}, appendLabels(identityLabels, "sample_type")),
		captured: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "remote_write_inspector_capture_captured_samples_total",
			Help: "Matching samples successfully handed to the capture log handler; not a durable storage acknowledgement.",
		}, appendLabels(identityLabels, "sample_type")),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "remote_write_inspector_capture_skipped_samples_total",
			Help: "Matching samples not captured, by reason. Undecoded requests are accounted for by requests_total instead.",
		}, appendLabels(identityLabels, "sample_type", "reason")),
		truncated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "remote_write_inspector_capture_truncated_samples_total",
			Help: "Captured samples whose ordered label array was truncated to the byte limit.",
		}, identityLabels),
	}
	deadline := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "remote_write_inspector_capture_deadline_timestamp_seconds",
		Help: "Capture deadline as Unix time; present only when capture is configured. A restart starts a new session.",
	})
	deadline.Set(float64(c.deadline.UnixMilli()) / 1000)
	for _, collector := range []prometheus.Collector{c.matched, c.captured, c.skipped, c.truncated, deadline} {
		if err := reg.Register(collector); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Match raw labels without sorting, deduplicating or normalizing them. For an
// invalid duplicate name, all occurrences must satisfy its matchers; an absent
// label has the usual Prometheus empty-string semantics.
func (c *sampleCapture) matches(ls []prompb.Label) bool {
	for _, m := range c.matchers {
		found := false
		for _, l := range ls {
			if l.Name == m.Name {
				found = true
				if !m.Matches(l.Value) {
					return false
				}
			}
		}
		if !found && !m.Matches("") {
			return false
		}
	}
	return true
}

func (c *sampleCapture) inspect(identity RequestIdentity, ts *prompb.TimeSeries, series seriesDetails, receivedAt time.Time) {
	if !c.matches(ts.Labels) {
		return
	}
	if n := len(ts.Histograms); n > 0 {
		c.matched.WithLabelValues(identity.LabelValues("histogram")...).Add(float64(n))
		c.skipped.WithLabelValues(identity.LabelValues("histogram", "unsupported_sample_type")...).Add(float64(n))
	}
	if len(ts.Samples) == 0 {
		return
	}
	c.matched.WithLabelValues(identity.LabelValues("float")...).Add(float64(len(ts.Samples)))
	for idx, sample := range ts.Samples {
		now := c.clock()
		if reason := c.reserve(now); reason != "" {
			// These budgets never recover within a session. Account for the
			// remainder at once so expired capture adds no per-sample work.
			if reason == "duration_limit" || reason == "sample_limit" {
				c.skipped.WithLabelValues(identity.LabelValues("float", reason)...).Add(float64(len(ts.Samples) - idx))
				return
			}
			c.skip(identity, reason)
			continue
		}
		ctx := context.Background()
		handler := c.logger.Handler()
		if !handler.Enabled(ctx, slog.LevelInfo) {
			c.skip(identity, "log_disabled")
			continue
		}
		ls, truncated := boundedLogLabels(ts.Labels, c.cfg.LabelBytes)
		record := slog.NewRecord(now, slog.LevelInfo, "captured remote-write sample", 0)
		record.Add(
			slog.Group("event", "action", "sample_capture"),
			"series_fingerprint", seriesFingerprint(series.hash),
			"received_at", receivedAt.UTC().Format(time.RFC3339Nano),
			"sample_type", "float",
			"sample_timestamp_ms", sample.Timestamp,
			"sample_value_bits", "0x"+seriesFingerprint(math.Float64bits(sample.Value)),
			"labels", ls, "labels_count", len(ts.Labels), "labels_truncated", truncated,
		)
		if isFinite(sample.Value) {
			record.Add("sample_value", sample.Value)
		}
		record.Add(identity.Attrs()...)
		if err := handler.Handle(ctx, record); err != nil {
			c.skip(identity, "log_error")
			continue
		}
		c.captured.WithLabelValues(identity.LabelValues("float")...).Inc()
		if truncated {
			c.truncated.WithLabelValues(identity.LabelValues()...).Inc()
		}
	}
}

func (c *sampleCapture) skip(identity RequestIdentity, reason string) {
	c.skipped.WithLabelValues(identity.LabelValues("float", reason)...).Inc()
}

// Reserve before formatting/logging. Budgets are global across tenants and
// concurrent requests. Failed/disabled log writes consume a reservation too,
// so a failing output cannot cause unbounded log attempts.
func (c *sampleCapture) reserve(now time.Time) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !now.Before(c.deadline) {
		return "duration_limit"
	}
	if c.attempts >= c.cfg.MaxSamples {
		return "sample_limit"
	}
	if now.Sub(c.windowStart) >= time.Second {
		c.windowStart = now
		c.windowCount = 0
	}
	if c.windowCount >= c.cfg.Rate {
		return "rate_limit"
	}
	c.windowCount++
	c.attempts++
	return ""
}
