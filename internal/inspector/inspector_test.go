package inspector

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/prompb"
)

func TestValidateSeriesReasons(t *testing.T) {
	ins := newTestInspector(t, Config{MaxLabels: 2, MaxLabelValueLength: 3})

	for _, tc := range []struct {
		name   string
		labels []prompb.Label
		reason string
	}{
		{
			name:   "missing metric",
			labels: nil,
			reason: reasonMissingMetricName,
		},
		{
			name:   "empty metric",
			labels: []prompb.Label{{Name: metricNameLabel, Value: ""}},
			reason: reasonEmptyMetricName,
		},
		{
			name:   "invalid metric",
			labels: []prompb.Label{{Name: metricNameLabel, Value: "bad metric"}},
			reason: reasonInvalidMetricName,
		},
		{
			name:   "empty label name",
			labels: []prompb.Label{{Name: metricNameLabel, Value: "up"}, {Name: "", Value: "x"}},
			reason: reasonEmptyLabelName,
		},
		{
			name:   "invalid label name",
			labels: []prompb.Label{{Name: metricNameLabel, Value: "up"}, {Name: "bad-label", Value: "x"}},
			reason: reasonInvalidLabelName,
		},
		{
			name:   "empty label value",
			labels: []prompb.Label{{Name: metricNameLabel, Value: "up"}, {Name: "job", Value: ""}},
			reason: reasonEmptyLabelValue,
		},
		{
			name:   "duplicate label",
			labels: []prompb.Label{{Name: metricNameLabel, Value: "up"}, {Name: "job", Value: "a"}, {Name: "job", Value: "b"}},
			reason: reasonDuplicateLabelName,
		},
		{
			name:   "out of order labels",
			labels: []prompb.Label{{Name: metricNameLabel, Value: "up"}, {Name: "z", Value: "a"}, {Name: "a", Value: "b"}},
			reason: reasonOutOfOrderLabels,
		},
		{
			name:   "excessive label count",
			labels: []prompb.Label{{Name: metricNameLabel, Value: "up"}, {Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
			reason: reasonExcessiveLabelCount,
		},
		{
			name:   "excessive value length",
			labels: []prompb.Label{{Name: metricNameLabel, Value: "up"}, {Name: "job", Value: "abcd"}},
			reason: reasonExcessiveLabelValueLength,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reasons := ins.validateSeries(tc.labels)
			if _, ok := reasons[tc.reason]; !ok {
				t.Fatalf("expected reason %q in %#v", tc.reason, reasons)
			}
		})
	}
}

func TestDuplicateTimestampDifferentValue(t *testing.T) {
	reg := prometheus.NewRegistry()
	ins := newTestInspectorWithRegistry(t, reg, Config{IdentityNames: []string{"tenant"}})
	id := identityFor(ins, http.Header{"X-Scope-Orgid": []string{"tenant-a"}})
	ts := time.Now().UnixMilli()
	req := writeRequest(sampleSeries("temperature_celsius", "host-a", sample(ts, 1)))

	if result := ins.Inspect(id, req); result.BadData {
		t.Fatalf("first sample should not be bad data")
	}
	if result := ins.Inspect(id, req); result.BadData {
		t.Fatalf("same value and timestamp should not be bad data")
	}
	req.Timeseries[0].Samples[0].Value = 2
	if result := ins.Inspect(id, req); !result.BadData {
		t.Fatalf("different value at same timestamp should be bad data")
	}

	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP remote_write_inspector_bad_samples_total Total samples with detected data quality problems.
# TYPE remote_write_inspector_bad_samples_total counter
remote_write_inspector_bad_samples_total{reason="duplicate_timestamp_different_value",tenant="tenant-a"} 1
`), "remote_write_inspector_bad_samples_total"); err != nil {
		t.Fatalf("bad sample metric mismatch: %v", err)
	}
}

func TestDuplicateTimestampDifferentValueCanBeDisabled(t *testing.T) {
	ins := newTestInspector(t, Config{
		IdentityNames:                   []string{"tenant"},
		DisableDuplicateSampleDetection: true,
	})
	id := identityFor(ins, http.Header{"X-Scope-Orgid": []string{"tenant-a"}})
	req := writeRequest(sampleSeries("temperature_celsius", "host-a", sample(time.Now().UnixMilli(), 1)))
	ins.Inspect(id, req)

	req.Timeseries[0].Samples[0].Value = 2
	if result := ins.Inspect(id, req); result.BadData {
		t.Fatalf("duplicate timestamp conflict was detected despite disabled detector")
	}
}

func TestStaleFutureAndCrossPath(t *testing.T) {
	reg := prometheus.NewRegistry()
	ins := newTestInspectorWithRegistry(t, reg, Config{IdentityNames: []string{"tenant", "input_path"}, StaleCutoff: time.Hour, FutureSkew: time.Minute})
	now := time.UnixMilli(10_000_000)
	ins.clock = func() time.Time { return now }

	idA := identityFor(ins, http.Header{"X-Scope-Orgid": []string{"tenant-a"}, "X-Obs-Input-Path": []string{"path-a"}})
	idB := identityFor(ins, http.Header{"X-Scope-Orgid": []string{"tenant-a"}, "X-Obs-Input-Path": []string{"path-b"}})

	reqA := writeRequest(sampleSeries("requests_total", "host-a", sample(now.UnixMilli(), 1)))
	if result := ins.Inspect(idA, reqA); result.BadData {
		t.Fatalf("first path should not collide")
	}
	if result := ins.Inspect(idB, reqA); !result.BadData {
		t.Fatalf("second path for same canonical series should collide")
	}

	reqTime := writeRequest(sampleSeries("requests_total", "host-b",
		sample(now.Add(-2*time.Hour).UnixMilli(), 1),
		sample(now.Add(2*time.Minute).UnixMilli(), 1),
	))
	if result := ins.Inspect(idA, reqTime); !result.BadData {
		t.Fatalf("stale and future samples should be bad data")
	}

	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP remote_write_inspector_bad_samples_total Total samples with detected data quality problems.
# TYPE remote_write_inspector_bad_samples_total counter
remote_write_inspector_bad_samples_total{input_path="path-a",reason="future_sample",tenant="tenant-a"} 1
remote_write_inspector_bad_samples_total{input_path="path-a",reason="stale_sample",tenant="tenant-a"} 1
`), "remote_write_inspector_bad_samples_total"); err != nil {
		t.Fatalf("bad sample metric mismatch: %v", err)
	}
	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP remote_write_inspector_bad_series_total Total series with detected data quality problems.
# TYPE remote_write_inspector_bad_series_total counter
remote_write_inspector_bad_series_total{input_path="path-b",reason="cross_path_collision",tenant="tenant-a"} 1
`), "remote_write_inspector_bad_series_total"); err != nil {
		t.Fatalf("bad series metric mismatch: %v", err)
	}
}

func TestCrossPathCollisionCanBeDisabled(t *testing.T) {
	ins := newTestInspector(t, Config{
		IdentityNames:                      []string{"tenant", "input_path"},
		DisableCrossPathCollisionDetection: true,
	})
	idA := identityFor(ins, http.Header{"X-Scope-Orgid": []string{"tenant-a"}, "X-Obs-Input-Path": []string{"path-a"}})
	idB := identityFor(ins, http.Header{"X-Scope-Orgid": []string{"tenant-a"}, "X-Obs-Input-Path": []string{"path-b"}})
	req := writeRequest(sampleSeries("requests_total", "host-a", sample(time.Now().UnixMilli(), 1)))

	ins.Inspect(idA, req)
	if result := ins.Inspect(idB, req); result.BadData {
		t.Fatalf("cross-path collision was detected despite disabled detector")
	}
}

func TestDiagnosticMetricRequiresWriterID(t *testing.T) {
	reg := prometheus.NewRegistry()
	ins := newTestInspectorWithRegistry(t, reg, Config{IdentityNames: []string{"tenant", "writer_id"}})
	id := identityFor(ins, http.Header{"X-Scope-Orgid": []string{"tenant-a"}})

	result := ins.Inspect(id, writeRequest(sampleSeries("remote_write_inspector_requests_total", "host-a", sample(1000, 1))))
	if !result.BadData {
		t.Fatalf("diagnostic metric without writer_id should be bad data")
	}

	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP remote_write_inspector_bad_series_total Total series with detected data quality problems.
# TYPE remote_write_inspector_bad_series_total counter
remote_write_inspector_bad_series_total{reason="diagnostic_writer_id_missing",tenant="tenant-a",writer_id="unknown"} 1
`), "remote_write_inspector_bad_series_total"); err != nil {
		t.Fatalf("bad series metric mismatch: %v", err)
	}
}

func TestDiagnosticMetricDoesNotRequireWriterIDWhenDisabled(t *testing.T) {
	reg := prometheus.NewRegistry()
	ins := newTestInspectorWithRegistry(t, reg, Config{})
	id := identityFor(ins, http.Header{})

	result := ins.Inspect(id, writeRequest(sampleSeries("remote_write_inspector_requests_total", "host-a", sample(time.Now().UnixMilli(), 1))))
	if result.BadData {
		t.Fatalf("diagnostic metric should not require writer_id when writer_id identity is disabled")
	}
}

func TestReceiveHTTP(t *testing.T) {
	reg := prometheus.NewRegistry()
	srv, err := NewServer(Config{
		ListenAddress:   "127.0.0.1:0",
		IdentityNames:   []string{"tenant", "pipeline_sink", "input_path"},
		LogSampleRate:   0,
		MaxBodyBytes:    1024,
		StaleCutoff:     time.Hour,
		FutureSkew:      time.Minute,
		TopSeriesSize:   5,
		TopSeriesWindow: time.Minute,
	}, reg, slog.New(slog.NewTextHandler(ioDiscard{}, nil)))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/receive", bytes.NewReader(encodeWriteRequest(t, writeRequest(sampleSeries("up", "host-a", sample(time.Now().UnixMilli(), 1))))))
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	req.Header.Set("X-Obs-Pipeline-Sink", "sink-a")
	req.Header.Set("X-Obs-Input-Path", "path-a")
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("valid request status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	badReq := httptest.NewRequest(http.MethodPost, "/receive", bytes.NewReader(encodeWriteRequest(t, writeRequest(sampleSeries("bad metric", "host-a", sample(time.Now().UnixMilli(), 1))))))
	badReq.Header.Set("X-Scope-OrgID", "tenant-a")
	badReq.Header.Set("X-Obs-Pipeline-Sink", "sink-a")
	badReq.Header.Set("X-Obs-Input-Path", "path-a")
	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, badReq)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("bad data request status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	corruptReq := httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader("not-snappy"))
	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, corruptReq)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("corrupt request status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	largeReq := httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader(strings.Repeat("x", 2048)))
	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, largeReq)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large request status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}

	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP remote_write_inspector_decode_errors_total Total remote-write decode errors.
# TYPE remote_write_inspector_decode_errors_total counter
remote_write_inspector_decode_errors_total{reason="snappy"} 1
`), "remote_write_inspector_decode_errors_total"); err != nil {
		t.Fatalf("decode metric mismatch: %v", err)
	}
	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP remote_write_inspector_requests_total Total remote-write requests inspected.
# TYPE remote_write_inspector_requests_total counter
remote_write_inspector_requests_total{input_path="path-a",pipeline_sink="sink-a",result="bad_data",tenant="tenant-a"} 1
remote_write_inspector_requests_total{input_path="path-a",pipeline_sink="sink-a",result="ok",tenant="tenant-a"} 1
remote_write_inspector_requests_total{input_path="unknown",pipeline_sink="unknown",result="decode_error",tenant="unknown"} 1
remote_write_inspector_requests_total{input_path="unknown",pipeline_sink="unknown",result="too_large",tenant="unknown"} 1
`), "remote_write_inspector_requests_total"); err != nil {
		t.Fatalf("request metric mismatch: %v", err)
	}
}

func TestReceiveHTTPNoIdentityByDefault(t *testing.T) {
	reg := prometheus.NewRegistry()
	srv, err := NewServer(Config{
		ListenAddress: "127.0.0.1:0",
		LogSampleRate: 0,
		MaxBodyBytes:  1024,
		StaleCutoff:   time.Hour,
		FutureSkew:    time.Minute,
	}, reg, slog.New(slog.NewTextHandler(ioDiscard{}, nil)))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/receive", bytes.NewReader(encodeWriteRequest(t, writeRequest(sampleSeries("up", "host-a", sample(time.Now().UnixMilli(), 1))))))
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	req.Header.Set("X-Obs-Pipeline-Sink", "sink-a")
	req.Header.Set("X-Obs-Input-Path", "path-a")
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("valid request status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP remote_write_inspector_requests_total Total remote-write requests inspected.
# TYPE remote_write_inspector_requests_total counter
remote_write_inspector_requests_total{result="ok"} 1
`), "remote_write_inspector_requests_total"); err != nil {
		t.Fatalf("request metric mismatch: %v", err)
	}
}

func BenchmarkInspectLargeBatch(b *testing.B) {
	ins := newTestInspectorWithRegistry(b, prometheus.NewRegistry(), Config{
		IdentityNames: []string{"tenant", "pipeline_sink", "input_path"},
		CacheSize:     100000,
	})
	id := identityFor(ins, http.Header{
		"X-Scope-Orgid":       []string{"tenant-a"},
		"X-Obs-Pipeline-Sink": []string{"sink-a"},
		"X-Obs-Input-Path":    []string{"path-a"},
	})
	now := time.Now().UnixMilli()
	req := &prompb.WriteRequest{Timeseries: make([]prompb.TimeSeries, 0, 1000)}
	for n := 0; n < 1000; n++ {
		req.Timeseries = append(req.Timeseries, prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: metricNameLabel, Value: "requests_total"},
				{Name: "host_name", Value: "host-" + strconv.Itoa(n)},
				{Name: "job", Value: "app"},
			},
			Samples: []prompb.Sample{{Timestamp: now + int64(n), Value: float64(n)}},
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ins.Inspect(id, req)
	}
}

func newTestInspector(t testing.TB, cfg Config) *Inspector {
	t.Helper()
	return newTestInspectorWithRegistry(t, prometheus.NewRegistry(), cfg)
}

func newTestInspectorWithRegistry(t testing.TB, reg *prometheus.Registry, cfg Config) *Inspector {
	t.Helper()
	cfg.LogSampleRate = 0
	cfg.TopSeriesSize = 10
	cfg.TopSeriesWindow = time.Minute
	ins, err := New(cfg, reg, slog.New(slog.NewTextHandler(ioDiscard{}, nil)))
	if err != nil {
		t.Fatalf("new inspector: %v", err)
	}
	return ins
}

func identityFor(ins *Inspector, h http.Header) RequestIdentity {
	return ins.IdentityFromHeaders(h)
}

func writeRequest(series ...prompb.TimeSeries) *prompb.WriteRequest {
	return &prompb.WriteRequest{Timeseries: series}
}

func sampleSeries(name, host string, samples ...prompb.Sample) prompb.TimeSeries {
	return prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: metricNameLabel, Value: name},
			{Name: "host_name", Value: host},
		},
		Samples: samples,
	}
}

func sample(ts int64, value float64) prompb.Sample {
	return prompb.Sample{Timestamp: ts, Value: value}
}

func encodeWriteRequest(t *testing.T, req *prompb.WriteRequest) []byte {
	t.Helper()
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return snappy.Encode(nil, body)
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
