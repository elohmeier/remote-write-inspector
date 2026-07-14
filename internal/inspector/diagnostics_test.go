package inspector

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

func TestFindingLogSamplerBoundsFirstExamplesAndResets(t *testing.T) {
	now := time.Unix(100, 0)
	sampler := newFindingLogSampler(time.Minute, 1, now)
	first := findingLogKey{kind: "bad_sample", reason: "test_reason", seriesHash: 1}
	second := findingLogKey{kind: "bad_sample", reason: "test_reason", seriesHash: 2}

	if !sampler.ShouldLog(now, first, 0) {
		t.Fatalf("first distinct finding should be logged")
	}
	if sampler.ShouldLog(now, first, 0) {
		t.Fatalf("repeat finding should not be logged when repeat rate is zero")
	}
	if sampler.ShouldLog(now, second, 0) {
		t.Fatalf("second distinct finding should be bounded by first-example limit")
	}
	if !sampler.ShouldLog(now.Add(time.Minute), first, 0) {
		t.Fatalf("first finding should be logged after the window resets")
	}
}

func TestBoundedLogLabelsPreservesOrderAndSignalsTruncation(t *testing.T) {
	labels := []prompb.Label{
		{Name: metricNameLabel, Value: "synthetic_metric_total"},
		{Name: "duplicate", Value: "first"},
		{Name: "duplicate", Value: "second"},
	}

	all, truncated := boundedLogLabels(labels, 4096)
	if truncated {
		t.Fatalf("labels unexpectedly truncated")
	}
	for idx, want := range labels {
		if all[idx].Name != want.Name || all[idx].Value != want.Value {
			t.Fatalf("label %d = %#v, want %#v", idx, all[idx], want)
		}
	}

	partial, truncated := boundedLogLabels(labels, 100)
	if !truncated {
		t.Fatalf("expected bounded labels to report truncation")
	}
	if len(partial) == 0 || len(partial) >= len(labels) {
		t.Fatalf("partial label count = %d, want between 1 and %d", len(partial), len(labels)-1)
	}
}
