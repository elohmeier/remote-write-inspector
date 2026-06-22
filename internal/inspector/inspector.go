package inspector

import (
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
)

const (
	reasonMissingMetricName                = "missing_metric_name"
	reasonEmptyMetricName                  = "empty_metric_name"
	reasonInvalidMetricName                = "invalid_metric_name"
	reasonEmptyLabelName                   = "empty_label_name"
	reasonInvalidLabelName                 = "invalid_label_name"
	reasonEmptyLabelValue                  = "empty_label_value"
	reasonDuplicateLabelName               = "duplicate_label_name"
	reasonOutOfOrderLabels                 = "out_of_order_labels"
	reasonExcessiveLabelCount              = "excessive_label_count"
	reasonExcessiveLabelValueLength        = "excessive_label_value_length"
	reasonStaleSample                      = "stale_sample"
	reasonFutureSample                     = "future_sample"
	reasonDuplicateTimestampDifferentValue = "duplicate_timestamp_different_value"
	reasonCrossPathCollision               = "cross_path_collision"
	reasonDiagnosticWriterIDMissing        = "diagnostic_writer_id_missing"
)

var (
	metricNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelNameRE  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

type Inspector struct {
	cfg     Config
	ids     *IdentitySet
	metrics *metrics
	logger  *slog.Logger
	clock   func() time.Time
	samples *stateCache[sampleKey, sampleValue]
	paths   *stateCache[pathKey, string]

	cacheMetricsMtx  sync.Mutex
	cacheMetricsLast time.Time
}

type InspectionResult struct {
	TotalSamples int
	BadData      bool
}

type sampleKey struct {
	tenant string
	series uint64
	ts     int64
}

type sampleValue struct {
	valueBits uint64
}

type pathKey struct {
	tenant string
	series uint64
}

func New(cfg Config, reg prometheus.Registerer, logger *slog.Logger) (*Inspector, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	ids, err := NewIdentitySet(cfg.IdentityNames, cfg.MaxIdentityLength)
	if err != nil {
		return nil, err
	}
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	if logger == nil {
		logger = slog.Default()
	}
	clock := time.Now
	m, err := newMetrics(reg, ids.LabelNames(), cfg.TopSeriesSize, cfg.TopSeriesWindow, clock)
	if err != nil {
		return nil, err
	}
	sampleCacheSize := cfg.CacheSize
	if cfg.DisableDuplicateSampleDetection {
		sampleCacheSize = 1
	}
	pathCacheSize := cfg.CacheSize
	if cfg.DisableCrossPathCollisionDetection {
		pathCacheSize = 1
	}
	return &Inspector{
		cfg:     cfg,
		ids:     ids,
		metrics: m,
		logger:  logger.With("component", "remote-write-inspector"),
		clock:   clock,
		samples: newStateCache[sampleKey, sampleValue](sampleCacheSize, cfg.CacheTTL, clock, sampleCacheShard),
		paths:   newStateCache[pathKey, string](pathCacheSize, cfg.CacheTTL, clock, pathCacheShard),
	}, nil
}

func (i *Inspector) IdentityFromHeaders(h http.Header) RequestIdentity {
	return i.ids.Resolve(h)
}

func (i *Inspector) ObserveDecodeError(reason string) {
	i.metrics.decodeErrors.WithLabelValues(reason).Inc()
}

func (i *Inspector) ObserveRequest(identity RequestIdentity, result string, bodyBytes int) {
	i.metrics.requests.WithLabelValues(identity.LabelValues(result)...).Inc()
	i.metrics.requestBytes.Observe(float64(bodyBytes))
}

func (i *Inspector) Inspect(identity RequestIdentity, req *prompb.WriteRequest) InspectionResult {
	start := i.clock()
	defer func() {
		i.metrics.validationDuration.Observe(i.clock().Sub(start).Seconds())
		i.updateCacheMetrics()
	}()

	nowMs := i.clock().UnixMilli()
	result := InspectionResult{}

	for _, ts := range req.Timeseries {
		result.TotalSamples += len(ts.Samples) + len(ts.Histograms)
		series := seriesDetails{
			name: seriesName(ts.Labels),
			host: hostName(ts.Labels),
			hash: canonicalHash(ts.Labels),
		}
		seriesReasons := i.validateSeries(ts.Labels)
		for reason := range seriesReasons {
			result.BadData = true
			i.observeBadSeries(identity, reason, ts.Labels, series)
		}

		if len(seriesReasons) == 0 {
			if !i.cfg.DisableCrossPathCollisionDetection && i.detectCrossPathCollision(identity, series.hash) {
				result.BadData = true
				i.observeBadSeries(identity, reasonCrossPathCollision, ts.Labels, series)
			}
			if i.isDiagnosticMetric(series.name) && identity.HasField("writer_id") && !identity.HasKnown("writer_id") {
				result.BadData = true
				i.observeBadSeries(identity, reasonDiagnosticWriterIDMissing, ts.Labels, series)
			}
		}

		for _, sample := range ts.Samples {
			badSample := false
			if i.isStale(sample.Timestamp, nowMs) {
				badSample = true
				result.BadData = true
				i.observeBadSample(identity, reasonStaleSample, ts.Labels, series, sample.Timestamp, sample.Value)
			}
			if i.isFuture(sample.Timestamp, nowMs) {
				badSample = true
				result.BadData = true
				i.observeBadSample(identity, reasonFutureSample, ts.Labels, series, sample.Timestamp, sample.Value)
			}
			if len(seriesReasons) == 0 && !badSample && !i.cfg.DisableDuplicateSampleDetection && i.detectDuplicateSample(identity, series.hash, sample.Timestamp, sample.Value) {
				result.BadData = true
				i.observeBadSample(identity, reasonDuplicateTimestampDifferentValue, ts.Labels, series, sample.Timestamp, sample.Value)
			}
		}

		for _, histogram := range ts.Histograms {
			if i.isStale(histogram.Timestamp, nowMs) {
				result.BadData = true
				i.observeBadSample(identity, reasonStaleSample, ts.Labels, series, histogram.Timestamp, 0)
			}
			if i.isFuture(histogram.Timestamp, nowMs) {
				result.BadData = true
				i.observeBadSample(identity, reasonFutureSample, ts.Labels, series, histogram.Timestamp, 0)
			}
		}
	}

	i.metrics.samples.WithLabelValues(identity.LabelValues()...).Add(float64(result.TotalSamples))
	return result
}

type seriesDetails struct {
	name string
	host string
	hash uint64
}

func (i *Inspector) validateSeries(labels []prompb.Label) map[string]struct{} {
	var reasons map[string]struct{}
	addReason := func(reason string) {
		if reasons == nil {
			reasons = make(map[string]struct{}, 1)
		}
		reasons[reason] = struct{}{}
	}

	if len(labels) == 0 {
		addReason(reasonMissingMetricName)
		return reasons
	}
	if len(labels) > i.cfg.MaxLabels {
		addReason(reasonExcessiveLabelCount)
	}

	metricSeen := false
	inNameOrder := true
	prevName := ""
	for idx, label := range labels {
		if idx > 0 && label.Name < prevName {
			addReason(reasonOutOfOrderLabels)
			inNameOrder = false
		}
		if idx > 0 && label.Name == prevName {
			addReason(reasonDuplicateLabelName)
		}
		prevName = label.Name

		if label.Name == "" {
			addReason(reasonEmptyLabelName)
			continue
		}

		if !labelNameRE.MatchString(label.Name) {
			addReason(reasonInvalidLabelName)
		}
		if len(label.Value) > i.cfg.MaxLabelValueLength {
			addReason(reasonExcessiveLabelValueLength)
		}

		if label.Name == metricNameLabel {
			metricSeen = true
			if label.Value == "" {
				addReason(reasonEmptyMetricName)
			} else if !metricNameRE.MatchString(label.Value) {
				addReason(reasonInvalidMetricName)
			}
			continue
		}

		if label.Value == "" {
			addReason(reasonEmptyLabelValue)
		}
	}
	if !inNameOrder {
		seen := make(map[string]struct{}, len(labels))
		for _, label := range labels {
			if _, ok := seen[label.Name]; ok {
				addReason(reasonDuplicateLabelName)
				break
			}
			seen[label.Name] = struct{}{}
		}
	}
	if !metricSeen {
		addReason(reasonMissingMetricName)
	}
	return reasons
}

func (i *Inspector) isStale(ts, nowMs int64) bool {
	return ts < nowMs-i.cfg.StaleCutoff.Milliseconds()
}

func (i *Inspector) isFuture(ts, nowMs int64) bool {
	return ts > nowMs+i.cfg.FutureSkew.Milliseconds()
}

func (i *Inspector) detectDuplicateSample(identity RequestIdentity, series uint64, ts int64, value float64) bool {
	key := sampleKey{tenant: identity.Get("tenant"), series: series, ts: ts}
	valueBits := math.Float64bits(value)

	existing, ok, evicted := i.samples.GetOrPut(key, sampleValue{valueBits: valueBits})
	if ok {
		if existing.valueBits != valueBits {
			return true
		}
		return false
	}
	if evicted > 0 {
		i.metrics.cacheEvictions.WithLabelValues("sample_conflicts").Add(float64(evicted))
	}
	return false
}

func (i *Inspector) detectCrossPathCollision(identity RequestIdentity, series uint64) bool {
	inputPath := identity.Get("input_path")
	if inputPath == unknownIdentity {
		return false
	}
	key := pathKey{tenant: identity.Get("tenant"), series: series}

	existing, ok, evicted := i.paths.GetOrPut(key, inputPath)
	if ok {
		return existing != inputPath
	}
	if evicted > 0 {
		i.metrics.cacheEvictions.WithLabelValues("cross_path").Add(float64(evicted))
	}
	return false
}

func (i *Inspector) observeBadSeries(identity RequestIdentity, reason string, labels []prompb.Label, series seriesDetails) {
	i.metrics.badSeries.WithLabelValues(identity.LabelValues(reason)...).Inc()
	i.metrics.topSeries.Observe(identity, reason, series.name, series.host)
	i.sampleLog("bad_series", identity, reason, labels, series, 0, 0)
}

func (i *Inspector) observeBadSample(identity RequestIdentity, reason string, labels []prompb.Label, series seriesDetails, ts int64, value float64) {
	i.metrics.badSamples.WithLabelValues(identity.LabelValues(reason)...).Inc()
	i.metrics.topSeries.Observe(identity, reason, series.name, series.host)
	i.sampleLog("bad_sample", identity, reason, labels, series, ts, value)
}

func (i *Inspector) sampleLog(kind string, identity RequestIdentity, reason string, labels []prompb.Label, series seriesDetails, ts int64, value float64) {
	if i.cfg.LogSampleRate <= 0 || rand.Float64() > i.cfg.LogSampleRate {
		return
	}
	attrs := []any{
		"event", kind,
		"reason", reason,
		"series_name", series.name,
		"host_name", series.host,
		"series_hash", series.hash,
		"canonical_labels", canonicalString(labels, 2048),
	}
	attrs = append(attrs, identity.Attrs()...)
	if ts != 0 {
		attrs = append(attrs, "timestamp", ts, "value_hash", math.Float64bits(value))
	}
	i.logger.Warn("detected remote-write data quality issue", attrs...)
}

func (i *Inspector) isDiagnosticMetric(name string) bool {
	for _, prefix := range i.cfg.DiagnosticMetricPrefixes {
		if prefix != "" && len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

func (i *Inspector) updateCacheMetrics() {
	now := i.clock()
	i.cacheMetricsMtx.Lock()
	if !i.cacheMetricsLast.IsZero() && now.Sub(i.cacheMetricsLast) < time.Second {
		i.cacheMetricsMtx.Unlock()
		return
	}
	i.cacheMetricsLast = now
	i.cacheMetricsMtx.Unlock()

	i.metrics.cacheEntries.WithLabelValues("sample_conflicts").Set(float64(i.samples.Len()))
	i.metrics.cacheEntries.WithLabelValues("cross_path").Set(float64(i.paths.Len()))
}

func sampleCacheShard(key sampleKey) uint64 {
	return key.series ^ uint64(key.ts) ^ hashString64(key.tenant)
}

func pathCacheShard(key pathKey) uint64 {
	return key.series ^ hashString64(key.tenant)
}

func hashString64(s string) uint64 {
	h := uint64(14695981039346656037)
	for idx := 0; idx < len(s); idx++ {
		h ^= uint64(s[idx])
		h *= 1099511628211
	}
	return h
}
