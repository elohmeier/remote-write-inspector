package inspector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/prompb"
)

func captureInspector(t *testing.T, cfg CaptureConfig, logger *slog.Logger) (*Inspector, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	ins, err := New(Config{
		Capture: &cfg, IdentityNames: []string{"tenant", "input_path"}, CacheSize: 10,
	}, reg, logger)
	if err != nil {
		t.Fatal(err)
	}
	now := ins.capture.windowStart
	ins.clock = func() time.Time { return now }
	return ins, reg
}

func captureConfig() CaptureConfig {
	cfg := DefaultCaptureConfig()
	cfg.Selector = `{__name__=~"recorded:.*",monitor_source="user-workload/thanos-ruler"}`
	return cfg
}

func captureSeries(values ...float64) prompb.TimeSeries {
	ts := prompb.TimeSeries{Labels: []prompb.Label{
		{Name: "__name__", Value: "recorded:requests"},
		{Name: "monitor_source", Value: "user-workload/thanos-ruler"},
		{Name: "replica", Value: "ruler-0"},
	}}
	for _, v := range values {
		ts.Samples = append(ts.Samples, prompb.Sample{Timestamp: 1791892800123, Value: v})
	}
	return ts
}

func captureIdentity(ins *Inspector, tenant string) RequestIdentity {
	return ins.IdentityFromHeaders(http.Header{"X-Scope-Orgid": {tenant}, "X-Obs-Input-Path": {"ruler"}})
}

func captureRecords(t *testing.T, buf *bytes.Buffer) []map[string]json.RawMessage {
	t.Helper()
	var records []map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	for {
		var record map[string]json.RawMessage
		if err := dec.Decode(&record); err != nil {
			if err == io.EOF {
				return records
			}
			t.Fatal(err)
		}
		if string(record["event"]) == `"sample_capture"` {
			records = append(records, record)
		}
	}
}

func assertCaptureCounter(t *testing.T, counter *prometheus.CounterVec, want float64, labels ...string) {
	t.Helper()
	if got := testutil.ToFloat64(counter.WithLabelValues(labels...)); got != want {
		t.Fatalf("counter %v = %v, want %v", labels, got, want)
	}
}

func TestCaptureOffByDefault(t *testing.T) {
	var buf bytes.Buffer
	reg := prometheus.NewRegistry()
	ins, err := New(Config{CacheSize: 10}, reg, slog.New(slog.NewJSONHandler(&buf, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ins.Inspect(ins.IdentityFromHeaders(nil), writeRequest(captureSeries(1)))
	if ins.capture != nil || len(captureRecords(t, &buf)) != 0 {
		t.Fatal("capture enabled by default")
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "remote_write_inspector_capture_") {
			t.Fatalf("capture metric exposed while disabled: %s", family.GetName())
		}
	}
}

func TestCapturePreservesEvidence(t *testing.T) {
	var buf bytes.Buffer
	ins, _ := captureInspector(t, captureConfig(), slog.New(slog.NewJSONHandler(&buf, nil)))
	values := []float64{1.25, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff0000000000002)}
	ts := captureSeries(values...)
	// Capture invalid label sets verbatim, including their ordering and repeats.
	ts.Labels = append([]prompb.Label{{Name: "z", Value: "first"}}, ts.Labels...)
	ts.Labels = append(ts.Labels, prompb.Label{Name: "replica", Value: "ruler-0"})
	before := append([]prompb.Label(nil), ts.Labels...)
	received := ins.clock().Add(-time.Second)
	result := ins.inspectReceived(captureIdentity(ins, "test"), writeRequest(ts), received)
	if !result.BadData || result.TotalSamples != len(values) {
		t.Fatalf("normal diagnostics changed: %+v", result)
	}
	records := captureRecords(t, &buf)
	if len(records) != len(values) {
		t.Fatalf("got %d records, want %d", len(records), len(values))
	}
	for idx, record := range records {
		var ls []logLabel
		if err := json.Unmarshal(record["labels"], &ls); err != nil {
			t.Fatal(err)
		}
		wantLabels := make([]logLabel, 0, len(ts.Labels))
		for _, l := range ts.Labels {
			wantLabels = append(wantLabels, logLabel{Name: l.Name, Value: l.Value})
		}
		if !reflect.DeepEqual(ls, wantLabels) {
			t.Fatalf("labels changed: %v", ls)
		}
		for field, want := range map[string]string{
			"event": `"sample_capture"`, "sample_type": `"float"`, "tenant": `"test"`, "input_path": `"ruler"`,
			"component": `"remote-write-inspector"`, "labels_truncated": "false", "labels_count": "5",
			"sample_timestamp_ms": "1791892800123", "sample_value_bits": fmt.Sprintf(`"0x%016x"`, math.Float64bits(values[idx])),
			"received_at":        fmt.Sprintf("%q", received.UTC().Format(time.RFC3339Nano)),
			"series_fingerprint": fmt.Sprintf("%q", seriesFingerprint(canonicalHash(ts.Labels))),
		} {
			if string(record[field]) != want {
				t.Errorf("%s = %s, want %s", field, record[field], want)
			}
		}
		if isFinite(values[idx]) {
			var v float64
			if err := json.Unmarshal(record["sample_value"], &v); err != nil || math.Float64bits(v) != math.Float64bits(values[idx]) {
				t.Errorf("numeric value not preserved: %s, %v", record["sample_value"], err)
			}
		} else if _, ok := record["sample_value"]; ok {
			t.Error("non-finite value emitted as JSON number")
		}
	}
	if !reflect.DeepEqual(ts.Labels, before) {
		t.Fatal("capture mutated input")
	}
	assertCaptureCounter(t, ins.capture.matched, 5, "test", "ruler", "float")
	assertCaptureCounter(t, ins.capture.captured, 5, "test", "ruler", "float")
}

func TestCaptureSelector(t *testing.T) {
	for _, tc := range []struct {
		name, selector string
		labels         []prompb.Label
		want           bool
	}{
		{"name", "recorded:requests", captureSeries().Labels, true},
		{"regex", `{__name__=~"recorded:.*",replica!="ruler-1"}`, captureSeries().Labels, true},
		{"anchored_regex", `{__name__=~"recorded"}`, captureSeries().Labels, false},
		{"missing_equal_empty", `{__name__="recorded:requests",absent=""}`, captureSeries().Labels, true},
		{"missing_negative", `{__name__="recorded:requests",absent!=""}`, captureSeries().Labels, false},
		{"negative_regex", `{__name__="recorded:requests",replica!~"ruler-.*"}`, captureSeries().Labels, false},
		{"duplicates_agree", `{replica="ruler-0"}`, []prompb.Label{{Name: "replica", Value: "ruler-0"}, {Name: "replica", Value: "ruler-0"}}, true},
		{"duplicates_disagree", `{replica="ruler-0"}`, []prompb.Label{{Name: "replica", Value: "ruler-0"}, {Name: "replica", Value: "ruler-1"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := captureConfig()
			cfg.Selector = tc.selector
			ins, _ := captureInspector(t, cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
			if got := ins.capture.matches(tc.labels); got != tc.want {
				t.Fatalf("matches = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCaptureRejectsInvalidConfig(t *testing.T) {
	for _, mutate := range []func(*CaptureConfig){
		func(c *CaptureConfig) { c.Selector = "" },
		func(c *CaptureConfig) { c.Selector = "{}" },
		func(c *CaptureConfig) { c.Selector = `{replica=~".*"}` },
		func(c *CaptureConfig) { c.Selector = `{replica="x"` },
		func(c *CaptureConfig) { c.Selector = `rate(requests[5m])` },
		func(c *CaptureConfig) { c.Duration = 0 },
		func(c *CaptureConfig) { c.MaxSamples = -1 },
		func(c *CaptureConfig) { c.Rate = 0 },
		func(c *CaptureConfig) { c.LabelBytes = 1 },
	} {
		cfg := captureConfig()
		mutate(&cfg)
		if _, err := New(Config{Capture: &cfg, CacheSize: 1}, prometheus.NewRegistry(), nil); err == nil {
			t.Errorf("accepted invalid config: %+v", cfg)
		}
	}
}

func TestCaptureLimitsAndAccounting(t *testing.T) {
	var buf bytes.Buffer
	cfg := captureConfig()
	cfg.Rate, cfg.MaxSamples, cfg.Duration, cfg.LabelBytes = 2, 3, 3*time.Second, 2
	ins, reg := captureInspector(t, cfg, slog.New(slog.NewJSONHandler(&buf, nil)))
	now := ins.clock()
	ins.clock = func() time.Time { return now }
	identity := captureIdentity(ins, "test")
	ins.Inspect(identity, writeRequest(captureSeries(1, 2, 3))) // 2 captured; 1 rate-limited.
	now = now.Add(time.Second)
	ins.Inspect(identity, writeRequest(captureSeries(4, 5))) // 1 captured; 1 budget-limited.
	now = ins.capture.deadline
	ins.Inspect(identity, writeRequest(captureSeries(6, 7, 8))) // Exact deadline is excluded, including a whole batch.
	// Nonmatching samples must not contribute to the capture ledger.
	unmatched := captureSeries(7)
	unmatched.Labels[0].Value = "other"
	ins.Inspect(identity, writeRequest(unmatched))
	assertCaptureCounter(t, ins.capture.matched, 8, "test", "ruler", "float")
	assertCaptureCounter(t, ins.capture.captured, 3, "test", "ruler", "float")
	for _, reason := range []string{"rate_limit", "sample_limit"} {
		assertCaptureCounter(t, ins.capture.skipped, 1, "test", "ruler", "float", reason)
	}
	assertCaptureCounter(t, ins.capture.skipped, 3, "test", "ruler", "float", "duration_limit")
	assertCaptureCounter(t, ins.capture.truncated, 3, "test", "ruler")
	records := captureRecords(t, &buf)
	if len(records) != 3 {
		t.Fatalf("got %d records", len(records))
	}
	for _, record := range records {
		if string(record["labels"]) != "[]" || string(record["labels_truncated"]) != "true" || string(record["labels_count"]) != "3" {
			t.Fatalf("unbounded or unmarked labels: %s", record)
		}
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range families {
		if f.GetName() == "remote_write_inspector_capture_deadline_timestamp_seconds" {
			found = true
			if got := f.Metric[0].Gauge.GetValue(); got != float64(ins.capture.deadline.UnixMilli())/1000 {
				t.Fatalf("deadline = %v", got)
			}
		}
	}
	if !found {
		t.Fatal("missing deadline metric")
	}
}

func TestCaptureConcurrentGlobalLimits(t *testing.T) {
	for _, limit := range []string{"sample_limit", "rate_limit"} {
		t.Run(limit, func(t *testing.T) {
			var buf bytes.Buffer
			cfg := captureConfig()
			cfg.MaxSamples, cfg.Rate = 7, 100
			if limit == "rate_limit" {
				cfg.MaxSamples, cfg.Rate = 100, 7
			}
			ins, _ := captureInspector(t, cfg, slog.New(slog.NewJSONHandler(&buf, nil)))
			var wg sync.WaitGroup
			for idx := range 50 {
				wg.Go(func() {
					ins.Inspect(captureIdentity(ins, fmt.Sprint(idx%2)), writeRequest(captureSeries(1)))
				})
			}
			wg.Wait()
			if n := len(captureRecords(t, &buf)); n != 7 {
				t.Fatalf("global limit exceeded: %d records", n)
			}
			for _, tenant := range []string{"0", "1"} {
				captured := testutil.ToFloat64(ins.capture.captured.WithLabelValues(tenant, "ruler", "float"))
				skipped := testutil.ToFloat64(ins.capture.skipped.WithLabelValues(tenant, "ruler", "float", limit))
				assertCaptureCounter(t, ins.capture.matched, 25, tenant, "ruler", "float")
				if captured+skipped != 25 {
					t.Fatalf("incomplete accounting for %s: captured=%v skipped=%v", tenant, captured, skipped)
				}
			}
		})
	}
}

type captureFailWriter struct{}

func (captureFailWriter) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }

func TestCaptureLogFailuresConsumeBudget(t *testing.T) {
	for reason, handler := range map[string]slog.Handler{
		"log_disabled": slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}),
		"log_error":    slog.NewJSONHandler(captureFailWriter{}, nil),
	} {
		t.Run(reason, func(t *testing.T) {
			cfg := captureConfig()
			cfg.MaxSamples = 1
			ins, _ := captureInspector(t, cfg, slog.New(handler))
			ins.Inspect(captureIdentity(ins, "test"), writeRequest(captureSeries(1, 2)))
			assertCaptureCounter(t, ins.capture.matched, 2, "test", "ruler", "float")
			assertCaptureCounter(t, ins.capture.captured, 0, "test", "ruler", "float")
			assertCaptureCounter(t, ins.capture.skipped, 1, "test", "ruler", "float", reason)
			assertCaptureCounter(t, ins.capture.skipped, 1, "test", "ruler", "float", "sample_limit")
		})
	}
}

func TestCaptureHistogramsAccountedAsUnsupported(t *testing.T) {
	var buf bytes.Buffer
	ins, _ := captureInspector(t, captureConfig(), slog.New(slog.NewJSONHandler(&buf, nil)))
	ts := captureSeries(1)
	ts.Histograms = []prompb.Histogram{{Timestamp: ts.Samples[0].Timestamp}}
	ins.Inspect(captureIdentity(ins, "test"), writeRequest(ts))
	assertCaptureCounter(t, ins.capture.matched, 1, "test", "ruler", "histogram")
	assertCaptureCounter(t, ins.capture.skipped, 1, "test", "ruler", "histogram", "unsupported_sample_type")
	assertCaptureCounter(t, ins.capture.captured, 1, "test", "ruler", "float")
	if len(captureRecords(t, &buf)) != 1 {
		t.Fatal("unexpected histogram log")
	}
}

type captureArrivalReader struct {
	io.Reader
	read func()
}

func (r captureArrivalReader) Read(p []byte) (int, error) {
	r.read()
	return r.Reader.Read(p)
}

func TestCaptureHTTPArrivalAndCoverage(t *testing.T) {
	var buf bytes.Buffer
	ins, _ := captureInspector(t, captureConfig(), slog.New(slog.NewJSONHandler(&buf, nil)))
	arrival := ins.clock()
	now := arrival
	ins.clock = func() time.Time { return now }
	ts := captureSeries(1)
	ts.Samples[0].Timestamp = arrival.UnixMilli()
	payload := encodeWriteRequest(t, writeRequest(ts))
	req := httptest.NewRequest(http.MethodPost, "/receive", captureArrivalReader{
		Reader: bytes.NewReader(payload), read: func() { now = arrival.Add(time.Second) },
	})
	req.Header.Set("X-Scope-OrgID", "test")
	req.Header.Set("X-Obs-Input-Path", "ruler")
	res := httptest.NewRecorder()
	ins.receiveHTTP(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("response = %d", res.Code)
	}
	assertCaptureCounter(t, ins.metrics.requests, 1, "test", "ruler", "ok")
	records := captureRecords(t, &buf)
	if len(records) != 1 || string(records[0]["received_at"]) != fmt.Sprintf("%q", arrival.UTC().Format(time.RFC3339Nano)) {
		t.Fatalf("arrival timestamp lost: %s", buf.String())
	}
	// Undecoded traffic cannot contribute known sample counts.
	for range ins.cfg.MaxInFlightRequests {
		if !ins.tryAdmit() {
			t.Fatal("unexpected admission failure")
		}
	}
	res = httptest.NewRecorder()
	ins.receiveHTTP(res, httptest.NewRequest(http.MethodPost, "/receive", bytes.NewReader(payload)))
	if res.Code != http.StatusNoContent {
		t.Fatalf("overloaded response = %d", res.Code)
	}
	for range ins.cfg.MaxInFlightRequests {
		ins.release()
	}
	assertCaptureCounter(t, ins.metrics.requests, 1, "unknown", "unknown", "overloaded")
	res = httptest.NewRecorder()
	ins.receiveHTTP(res, httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader("invalid")))
	if res.Code != http.StatusBadRequest {
		t.Fatalf("invalid response = %d", res.Code)
	}
	assertCaptureCounter(t, ins.metrics.requests, 1, "unknown", "unknown", "decode_error")
	assertCaptureCounter(t, ins.capture.matched, 0, "unknown", "unknown", "float")
	if len(captureRecords(t, &buf)) != 1 {
		t.Fatal("undecoded request was captured")
	}
}
