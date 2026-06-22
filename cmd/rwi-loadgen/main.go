package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

const remoteWriteVersion = "0.1.0"

type config struct {
	targetURL          string
	duration           time.Duration
	workers            int
	rate               int
	timeout            time.Duration
	seriesPerRequest   int
	samplesPerSeries   int
	activeSeries       int
	tenantCount        int
	sinkCount          int
	pathCount          int
	writerCount        int
	staleRatio         float64
	futureRatio        float64
	invalidSeriesRatio float64
	conflictRatio      float64
	progressInterval   time.Duration
}

type generatedRequest struct {
	body         []byte
	tenant       string
	sink         string
	path         string
	writer       string
	series       int
	samples      int
	bodyBytes    int
	decodedBytes int
}

type loadStats struct {
	requests        uint64
	successes       uint64
	transportErrors uint64
	marshalErrors   uint64
	series          uint64
	samples         uint64
	bodyBytes       uint64
	decodedBytes    uint64
	latencyNS       uint64
	statusCodes     [600]uint64
	latencies       latencyHistogram
}

type snapshot struct {
	requests        uint64
	successes       uint64
	transportErrors uint64
	marshalErrors   uint64
	series          uint64
	samples         uint64
	bodyBytes       uint64
	decodedBytes    uint64
	latencyNS       uint64
	statusCodes     [600]uint64
	latencyCounts   []uint64
}

type latencyHistogram struct {
	bounds []time.Duration
	counts []uint64
}

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}

	stats := &loadStats{latencies: newLatencyHistogram()}
	client := newHTTPClient(cfg)
	start := time.Now()

	fmt.Fprintf(os.Stderr, "target=%s duration=%s workers=%d rate=%s series/request=%d samples/series=%d active_series=%d\n",
		cfg.targetURL, durationOrSignal(cfg.duration), cfg.workers, rateLabel(cfg.rate), cfg.seriesPerRequest, cfg.samplesPerSeries, cfg.activeSeries)

	var requestSeq uint64
	var wg sync.WaitGroup
	for workerID := 0; workerID < cfg.workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, cfg, client, stats, &requestSeq, workerID)
		}(workerID)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	if cfg.progressInterval > 0 {
		printProgress(ctx, done, stats, start, cfg.progressInterval)
	} else {
		<-done
	}

	printSummary(os.Stdout, stats.snapshot(), time.Since(start))
}

func parseFlags() config {
	cfg := config{
		targetURL:          "http://localhost:8080/receive",
		duration:           30 * time.Second,
		workers:            runtime.NumCPU(),
		timeout:            10 * time.Second,
		seriesPerRequest:   1000,
		samplesPerSeries:   1,
		activeSeries:       100000,
		tenantCount:        1,
		sinkCount:          1,
		pathCount:          1,
		writerCount:        1,
		progressInterval:   5 * time.Second,
		staleRatio:         0,
		futureRatio:        0,
		invalidSeriesRatio: 0,
		conflictRatio:      0,
	}

	flagSet := flagSet()
	flagSet.StringVar(&cfg.targetURL, "url", cfg.targetURL, "Remote-write receive URL.")
	flagSet.DurationVar(&cfg.duration, "duration", cfg.duration, "Load test duration. Set to 0 to run until interrupted.")
	flagSet.IntVar(&cfg.workers, "workers", cfg.workers, "Concurrent request workers.")
	flagSet.IntVar(&cfg.rate, "rate", cfg.rate, "Target requests per second across all workers. Set to 0 for unlimited.")
	flagSet.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "Per-request HTTP timeout.")
	flagSet.IntVar(&cfg.seriesPerRequest, "series-per-request", cfg.seriesPerRequest, "Time series per remote-write request.")
	flagSet.IntVar(&cfg.samplesPerSeries, "samples-per-series", cfg.samplesPerSeries, "Samples per generated time series.")
	flagSet.IntVar(&cfg.activeSeries, "active-series", cfg.activeSeries, "Number of unique series IDs to cycle through.")
	flagSet.IntVar(&cfg.tenantCount, "tenant-count", cfg.tenantCount, "Number of X-Scope-OrgID values to cycle through.")
	flagSet.IntVar(&cfg.sinkCount, "sink-count", cfg.sinkCount, "Number of X-Obs-Pipeline-Sink values to cycle through.")
	flagSet.IntVar(&cfg.pathCount, "path-count", cfg.pathCount, "Number of X-Obs-Input-Path values to cycle through.")
	flagSet.IntVar(&cfg.writerCount, "writer-count", cfg.writerCount, "Number of X-Obs-Writer-ID values to cycle through.")
	flagSet.Float64Var(&cfg.staleRatio, "stale-ratio", cfg.staleRatio, "Fraction of samples generated older than the default stale cutoff.")
	flagSet.Float64Var(&cfg.futureRatio, "future-ratio", cfg.futureRatio, "Fraction of samples generated beyond the default future skew.")
	flagSet.Float64Var(&cfg.invalidSeriesRatio, "invalid-series-ratio", cfg.invalidSeriesRatio, "Fraction of series generated with an invalid metric name.")
	flagSet.Float64Var(&cfg.conflictRatio, "conflict-ratio", cfg.conflictRatio, "Fraction of samples generated with repeated timestamps and changing values.")
	flagSet.DurationVar(&cfg.progressInterval, "progress-interval", cfg.progressInterval, "Progress print interval. Set to 0 to disable.")
	_ = flagSet.Parse(os.Args[1:])

	return cfg
}

func flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: %s [flags]\n\n", os.Args[0])
		fs.PrintDefaults()
	}
	return fs
}

func (cfg config) validate() error {
	parsed, err := url.Parse(cfg.targetURL)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("url scheme must be http or https")
	}
	if parsed.Host == "" {
		return fmt.Errorf("url host is required")
	}
	if cfg.duration < 0 {
		return fmt.Errorf("duration must be non-negative")
	}
	if cfg.workers <= 0 {
		return fmt.Errorf("workers must be positive")
	}
	if cfg.rate < 0 {
		return fmt.Errorf("rate must be non-negative")
	}
	if cfg.timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if cfg.seriesPerRequest <= 0 {
		return fmt.Errorf("series-per-request must be positive")
	}
	if cfg.samplesPerSeries <= 0 {
		return fmt.Errorf("samples-per-series must be positive")
	}
	if cfg.activeSeries <= 0 {
		return fmt.Errorf("active-series must be positive")
	}
	if cfg.tenantCount <= 0 || cfg.sinkCount <= 0 || cfg.pathCount <= 0 || cfg.writerCount <= 0 {
		return fmt.Errorf("identity counts must be positive")
	}
	if err := validateRatio("stale-ratio", cfg.staleRatio); err != nil {
		return err
	}
	if err := validateRatio("future-ratio", cfg.futureRatio); err != nil {
		return err
	}
	if err := validateRatio("invalid-series-ratio", cfg.invalidSeriesRatio); err != nil {
		return err
	}
	if err := validateRatio("conflict-ratio", cfg.conflictRatio); err != nil {
		return err
	}
	if cfg.staleRatio+cfg.futureRatio > 1 {
		return fmt.Errorf("stale-ratio plus future-ratio must not exceed 1")
	}
	if cfg.progressInterval < 0 {
		return fmt.Errorf("progress-interval must be non-negative")
	}
	return nil
}

func validateRatio(name string, value float64) error {
	if value < 0 || value > 1 {
		return fmt.Errorf("%s must be between 0 and 1", name)
	}
	return nil
}

func newHTTPClient(cfg config) *http.Client {
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        cfg.workers * 4,
		MaxIdleConnsPerHost: cfg.workers * 4,
		MaxConnsPerHost:     cfg.workers * 4,
		IdleConnTimeout:     90 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   cfg.timeout,
	}
}

func runWorker(ctx context.Context, cfg config, client *http.Client, stats *loadStats, requestSeq *uint64, workerID int) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*9973))
	pacer := newPacer(cfg.rate, cfg.workers)
	defer pacer.Stop()

	for {
		if !pacer.Wait(ctx) {
			return
		}
		if ctx.Err() != nil {
			return
		}

		seq := atomic.AddUint64(requestSeq, 1) - 1
		generated, err := generateRequest(cfg, seq, rng)
		if err != nil {
			atomic.AddUint64(&stats.marshalErrors, 1)
			continue
		}

		start := time.Now()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.targetURL, bytes.NewReader(generated.body))
		if err != nil {
			atomic.AddUint64(&stats.transportErrors, 1)
			continue
		}
		setRemoteWriteHeaders(req, generated)

		resp, err := client.Do(req)
		latency := time.Since(start)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			stats.observeTransportError(latency)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		stats.observeResponse(resp.StatusCode, generated, latency)
	}
}

func generateRequest(cfg config, seq uint64, rng *rand.Rand) (generatedRequest, error) {
	nowMs := time.Now().UnixMilli()
	request := prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, cfg.seriesPerRequest),
	}

	startSeries := int((seq * uint64(cfg.seriesPerRequest)) % uint64(cfg.activeSeries))
	for idx := range request.Timeseries {
		seriesID := (startSeries + idx) % cfg.activeSeries
		metricName := "loadgen_metric_" + strconv.Itoa(seriesID%128)
		if rng.Float64() < cfg.invalidSeriesRatio {
			metricName = "bad metric " + strconv.Itoa(seriesID)
		}

		samples := make([]prompb.Sample, cfg.samplesPerSeries)
		freshBase := nowMs - int64(cfg.samplesPerSeries)
		for sampleIdx := range samples {
			ts := freshBase + int64(sampleIdx)
			value := deterministicValue(seriesID, ts, sampleIdx)

			roll := rng.Float64()
			switch {
			case roll < cfg.staleRatio:
				ts = nowMs - int64(25*time.Hour/time.Millisecond) - int64(sampleIdx)
			case roll < cfg.staleRatio+cfg.futureRatio:
				ts = nowMs + int64(11*time.Minute/time.Millisecond) + int64(sampleIdx)
			}

			if rng.Float64() < cfg.conflictRatio {
				ts = (nowMs / 1000) * 1000
				value = rng.Float64() * 1_000_000
			}

			samples[sampleIdx] = prompb.Sample{
				Timestamp: ts,
				Value:     value,
			}
		}

		request.Timeseries[idx] = prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: metricName},
				{Name: "host_name", Value: "host-" + strconv.Itoa(seriesID)},
				{Name: "job", Value: "rwi-loadgen"},
				{Name: "series_id", Value: strconv.Itoa(seriesID)},
			},
			Samples: samples,
		}
	}

	body, err := proto.Marshal(&request)
	if err != nil {
		return generatedRequest{}, fmt.Errorf("marshal write request: %w", err)
	}
	compressed := snappy.Encode(nil, body)

	tenantIndex := int(seq % uint64(cfg.tenantCount))
	sinkIndex := int(seq % uint64(cfg.sinkCount))
	cycleLen := (cfg.activeSeries + cfg.seriesPerRequest - 1) / cfg.seriesPerRequest
	pathIndex := int((seq / uint64(cycleLen)) % uint64(cfg.pathCount))
	writerIndex := int(seq % uint64(cfg.writerCount))

	return generatedRequest{
		body:         compressed,
		tenant:       "tenant-" + strconv.Itoa(tenantIndex),
		sink:         "sink-" + strconv.Itoa(sinkIndex),
		path:         "path-" + strconv.Itoa(pathIndex),
		writer:       "writer-" + strconv.Itoa(writerIndex),
		series:       cfg.seriesPerRequest,
		samples:      cfg.seriesPerRequest * cfg.samplesPerSeries,
		bodyBytes:    len(compressed),
		decodedBytes: len(body),
	}, nil
}

func deterministicValue(seriesID int, ts int64, sampleIdx int) float64 {
	return float64(seriesID%100000) + float64(ts%1000)/1000 + float64(sampleIdx)/1000000
}

func setRemoteWriteHeaders(req *http.Request, generated generatedRequest) {
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", remoteWriteVersion)
	req.Header.Set("X-Scope-OrgID", generated.tenant)
	req.Header.Set("X-Obs-Pipeline-Sink", generated.sink)
	req.Header.Set("X-Obs-Input-Path", generated.path)
	req.Header.Set("X-Obs-Writer-ID", generated.writer)
}

type pacer struct {
	ticker *time.Ticker
}

func newPacer(totalRate int, workers int) pacer {
	if totalRate <= 0 {
		return pacer{}
	}
	perWorkerRate := float64(totalRate) / float64(workers)
	if perWorkerRate <= 0 {
		perWorkerRate = 1
	}
	interval := time.Duration(float64(time.Second) / perWorkerRate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	return pacer{ticker: time.NewTicker(interval)}
}

func (p pacer) Wait(ctx context.Context) bool {
	if p.ticker == nil {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-p.ticker.C:
		return true
	}
}

func (p pacer) Stop() {
	if p.ticker != nil {
		p.ticker.Stop()
	}
}

func (s *loadStats) observeResponse(statusCode int, generated generatedRequest, latency time.Duration) {
	atomic.AddUint64(&s.requests, 1)
	if statusCode >= 200 && statusCode < 300 {
		atomic.AddUint64(&s.successes, 1)
	}
	if statusCode >= 0 && statusCode < len(s.statusCodes) {
		atomic.AddUint64(&s.statusCodes[statusCode], 1)
	}
	atomic.AddUint64(&s.series, uint64(generated.series))
	atomic.AddUint64(&s.samples, uint64(generated.samples))
	atomic.AddUint64(&s.bodyBytes, uint64(generated.bodyBytes))
	atomic.AddUint64(&s.decodedBytes, uint64(generated.decodedBytes))
	atomic.AddUint64(&s.latencyNS, uint64(latency.Nanoseconds()))
	s.latencies.observe(latency)
}

func (s *loadStats) observeTransportError(latency time.Duration) {
	atomic.AddUint64(&s.requests, 1)
	atomic.AddUint64(&s.transportErrors, 1)
	atomic.AddUint64(&s.latencyNS, uint64(latency.Nanoseconds()))
	s.latencies.observe(latency)
}

func (s *loadStats) snapshot() snapshot {
	out := snapshot{
		requests:        atomic.LoadUint64(&s.requests),
		successes:       atomic.LoadUint64(&s.successes),
		transportErrors: atomic.LoadUint64(&s.transportErrors),
		marshalErrors:   atomic.LoadUint64(&s.marshalErrors),
		series:          atomic.LoadUint64(&s.series),
		samples:         atomic.LoadUint64(&s.samples),
		bodyBytes:       atomic.LoadUint64(&s.bodyBytes),
		decodedBytes:    atomic.LoadUint64(&s.decodedBytes),
		latencyNS:       atomic.LoadUint64(&s.latencyNS),
		latencyCounts:   s.latencies.snapshot(),
	}
	for idx := range s.statusCodes {
		out.statusCodes[idx] = atomic.LoadUint64(&s.statusCodes[idx])
	}
	return out
}

func newLatencyHistogram() latencyHistogram {
	return latencyHistogram{
		bounds: []time.Duration{
			100 * time.Microsecond,
			250 * time.Microsecond,
			500 * time.Microsecond,
			time.Millisecond,
			2 * time.Millisecond,
			5 * time.Millisecond,
			10 * time.Millisecond,
			25 * time.Millisecond,
			50 * time.Millisecond,
			100 * time.Millisecond,
			250 * time.Millisecond,
			500 * time.Millisecond,
			time.Second,
			2 * time.Second,
			5 * time.Second,
			10 * time.Second,
		},
		counts: make([]uint64, 17),
	}
}

func (h *latencyHistogram) observe(duration time.Duration) {
	idx := sort.Search(len(h.bounds), func(idx int) bool {
		return duration <= h.bounds[idx]
	})
	atomic.AddUint64(&h.counts[idx], 1)
}

func (h *latencyHistogram) snapshot() []uint64 {
	out := make([]uint64, len(h.counts))
	for idx := range h.counts {
		out[idx] = atomic.LoadUint64(&h.counts[idx])
	}
	return out
}

func printProgress(ctx context.Context, done <-chan struct{}, stats *loadStats, start time.Time, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			<-done
			return
		case <-ticker.C:
			snap := stats.snapshot()
			elapsed := time.Since(start)
			fmt.Fprintf(os.Stderr, "elapsed=%s requests=%d rps=%.1f samples/s=%.1f decoded=%.1fMiB/s wire=%.1fMiB/s success=%d statuses=%s p95~%s\n",
				elapsed.Round(time.Second),
				snap.requests,
				perSecond(snap.requests, elapsed),
				perSecond(snap.samples, elapsed),
				mibPerSecond(snap.decodedBytes, elapsed),
				mibPerSecond(snap.bodyBytes, elapsed),
				snap.successes,
				statusSummary(snap),
				percentileLabel(snap.latencyCounts, 0.95),
			)
		}
	}
}

func printSummary(w io.Writer, snap snapshot, elapsed time.Duration) {
	fmt.Fprintf(w, "duration: %s\n", elapsed.Round(time.Millisecond))
	fmt.Fprintf(w, "requests: %d (%.1f/s)\n", snap.requests, perSecond(snap.requests, elapsed))
	fmt.Fprintf(w, "successes: %d\n", snap.successes)
	fmt.Fprintf(w, "series: %d (%.1f/s)\n", snap.series, perSecond(snap.series, elapsed))
	fmt.Fprintf(w, "samples: %d (%.1f/s)\n", snap.samples, perSecond(snap.samples, elapsed))
	fmt.Fprintf(w, "decoded bytes: %d (%.1f MiB/s)\n", snap.decodedBytes, mibPerSecond(snap.decodedBytes, elapsed))
	fmt.Fprintf(w, "compressed bytes: %d (%.1f MiB/s)\n", snap.bodyBytes, mibPerSecond(snap.bodyBytes, elapsed))
	fmt.Fprintf(w, "latency: avg=%s p50~%s p95~%s p99~%s\n",
		averageLatency(snap),
		percentileLabel(snap.latencyCounts, 0.50),
		percentileLabel(snap.latencyCounts, 0.95),
		percentileLabel(snap.latencyCounts, 0.99),
	)
	fmt.Fprintf(w, "statuses: %s\n", statusSummary(snap))
	if snap.marshalErrors > 0 {
		fmt.Fprintf(w, "marshal errors: %d\n", snap.marshalErrors)
	}
}

func perSecond(value uint64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(value) / elapsed.Seconds()
}

func mibPerSecond(value uint64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(value) / 1024 / 1024 / elapsed.Seconds()
}

func averageLatency(snap snapshot) time.Duration {
	if snap.requests == 0 {
		return 0
	}
	return time.Duration(snap.latencyNS / snap.requests)
}

var summaryLatencyBounds = newLatencyHistogram().bounds

func percentileLabel(counts []uint64, percentile float64) string {
	total := uint64(0)
	for _, count := range counts {
		total += count
	}
	if total == 0 {
		return "0s"
	}

	target := uint64(math.Ceil(float64(total) * percentile))
	if target == 0 {
		target = 1
	}

	seen := uint64(0)
	for idx, count := range counts {
		seen += count
		if seen >= target {
			if idx >= len(summaryLatencyBounds) {
				return ">=" + summaryLatencyBounds[len(summaryLatencyBounds)-1].String()
			}
			return summaryLatencyBounds[idx].String()
		}
	}
	return ">=" + summaryLatencyBounds[len(summaryLatencyBounds)-1].String()
}

func statusSummary(snap snapshot) string {
	parts := make([]string, 0, 8)
	for code := 100; code < len(snap.statusCodes); code++ {
		if snap.statusCodes[code] > 0 {
			parts = append(parts, strconv.Itoa(code)+"="+strconv.FormatUint(snap.statusCodes[code], 10))
		}
	}
	if snap.transportErrors > 0 {
		parts = append(parts, "transport_error="+strconv.FormatUint(snap.transportErrors, 10))
	}
	if snap.marshalErrors > 0 {
		parts = append(parts, "marshal_error="+strconv.FormatUint(snap.marshalErrors, 10))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ",")
}

func durationOrSignal(duration time.Duration) string {
	if duration == 0 {
		return "until-signal"
	}
	return duration.String()
}

func rateLabel(rate int) string {
	if rate == 0 {
		return "unlimited"
	}
	return strconv.Itoa(rate) + "/s"
}
