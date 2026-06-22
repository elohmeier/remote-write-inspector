package inspector

import (
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	requests           *prometheus.CounterVec
	samples            *prometheus.CounterVec
	badSeries          *prometheus.CounterVec
	badSamples         *prometheus.CounterVec
	decodeErrors       *prometheus.CounterVec
	cacheEntries       *prometheus.GaugeVec
	cacheEvictions     *prometheus.CounterVec
	requestBytes       prometheus.Histogram
	decodeDuration     prometheus.Histogram
	validationDuration prometheus.Histogram
	topSeries          *topSeriesCollector
}

func newMetrics(reg prometheus.Registerer, identityLabels []string, topSeriesSize int, topSeriesWindow time.Duration, clock func() time.Time) (*metrics, error) {
	m := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "remote_write_inspector_requests_total",
			Help: "Total remote-write requests inspected.",
		}, appendLabels(identityLabels, "result")),
		samples: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "remote_write_inspector_samples_total",
			Help: "Total remote-write samples and histograms inspected.",
		}, identityLabels),
		badSeries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "remote_write_inspector_bad_series_total",
			Help: "Total series with detected data quality problems.",
		}, appendLabels(identityLabels, "reason")),
		badSamples: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "remote_write_inspector_bad_samples_total",
			Help: "Total samples with detected data quality problems.",
		}, appendLabels(identityLabels, "reason")),
		decodeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "remote_write_inspector_decode_errors_total",
			Help: "Total remote-write decode errors.",
		}, []string{"reason"}),
		cacheEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "remote_write_inspector_cache_entries",
			Help: "Current entries in stateful detector caches.",
		}, []string{"cache"}),
		cacheEvictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "remote_write_inspector_cache_evictions_total",
			Help: "Total stateful detector cache evictions.",
		}, []string{"cache"}),
		requestBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "remote_write_inspector_request_bytes",
			Help:    "Compressed remote-write request body size in bytes.",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 16),
		}),
		decodeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "remote_write_inspector_decode_duration_seconds",
			Help:    "Time spent decoding remote-write requests.",
			Buckets: prometheus.DefBuckets,
		}),
		validationDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "remote_write_inspector_validation_duration_seconds",
			Help:    "Time spent validating decoded remote-write requests.",
			Buckets: prometheus.DefBuckets,
		}),
		topSeries: newTopSeriesCollector(identityLabels, topSeriesSize, topSeriesWindow, clock),
	}

	collectors := []prometheus.Collector{
		m.requests,
		m.samples,
		m.badSeries,
		m.badSamples,
		m.decodeErrors,
		m.cacheEntries,
		m.cacheEvictions,
		m.requestBytes,
		m.decodeDuration,
		m.validationDuration,
		m.topSeries,
	}
	for _, collector := range collectors {
		if err := reg.Register(collector); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func appendLabels(labels []string, extra ...string) []string {
	out := make([]string, 0, len(labels)+len(extra))
	out = append(out, labels...)
	out = append(out, extra...)
	return out
}

type topSeriesKey struct {
	identityValues string
	reason         string
	seriesName     string
	hostName       string
}

type topSeriesEntry struct {
	values     []string
	reason     string
	seriesName string
	hostName   string
	count      float64
}

type topSeriesCollector struct {
	mtx            sync.Mutex
	desc           *prometheus.Desc
	identityLabels []string
	maxSeries      int
	window         time.Duration
	clock          func() time.Time
	windowStart    time.Time
	entries        map[topSeriesKey]*topSeriesEntry
}

func newTopSeriesCollector(identityLabels []string, maxSeries int, window time.Duration, clock func() time.Time) *topSeriesCollector {
	return &topSeriesCollector{
		desc: prometheus.NewDesc(
			"remote_write_inspector_top_series",
			"Top series observed with detected data quality problems in the current window.",
			appendLabels(identityLabels, "reason", "series_name", "host_name"),
			nil,
		),
		identityLabels: identityLabels,
		maxSeries:      maxSeries,
		window:         window,
		clock:          clock,
		windowStart:    clock(),
		entries:        make(map[topSeriesKey]*topSeriesEntry),
	}
}

func (c *topSeriesCollector) Observe(identity RequestIdentity, reason, seriesName, hostName string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.resetExpiredLocked(c.clock())

	key := topSeriesKey{
		identityValues: joinIdentityValues(identity.values),
		reason:         reason,
		seriesName:     seriesName,
		hostName:       hostName,
	}
	entry, ok := c.entries[key]
	if !ok {
		entry = &topSeriesEntry{
			values:     identity.LabelValues(reason, seriesName, hostName),
			reason:     reason,
			seriesName: seriesName,
			hostName:   hostName,
		}
		c.entries[key] = entry
	}
	entry.count++

	if len(c.entries) > c.maxSeries*50 {
		c.pruneLocked()
	}
}

func (c *topSeriesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *topSeriesCollector) Collect(ch chan<- prometheus.Metric) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.resetExpiredLocked(c.clock())

	entries := make([]*topSeriesEntry, 0, len(c.entries))
	for _, entry := range c.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count == entries[j].count {
			return entries[i].seriesName < entries[j].seriesName
		}
		return entries[i].count > entries[j].count
	})
	if len(entries) > c.maxSeries {
		entries = entries[:c.maxSeries]
	}
	for _, entry := range entries {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, entry.count, entry.values...)
	}
}

func (c *topSeriesCollector) resetExpiredLocked(now time.Time) {
	if now.Sub(c.windowStart) < c.window {
		return
	}
	c.windowStart = now
	c.entries = make(map[topSeriesKey]*topSeriesEntry)
}

func (c *topSeriesCollector) pruneLocked() {
	entries := make([]*topSeriesEntry, 0, len(c.entries))
	for _, entry := range c.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].count > entries[j].count
	})
	keep := c.maxSeries * 25
	if keep < c.maxSeries {
		keep = c.maxSeries
	}
	if len(entries) <= keep {
		return
	}
	next := make(map[topSeriesKey]*topSeriesEntry, keep)
	for _, entry := range entries[:keep] {
		key := topSeriesKey{
			identityValues: joinIdentityValues(entry.values[:len(c.identityLabels)]),
			reason:         entry.reason,
			seriesName:     entry.seriesName,
			hostName:       entry.hostName,
		}
		next[key] = entry
	}
	c.entries = next
}

func joinIdentityValues(values []string) string {
	if len(values) == 0 {
		return ""
	}
	total := len(values) - 1
	for _, value := range values {
		total += len(value)
	}
	buf := make([]byte, 0, total)
	for idx, value := range values {
		if idx > 0 {
			buf = append(buf, '\xff')
		}
		buf = append(buf, value...)
	}
	return string(buf)
}
