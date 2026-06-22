package inspector

import (
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/prompb"
)

const (
	metricNameLabel = "__name__"
	unknownSeries   = "unknown"
)

type labelPair struct {
	name  string
	value string
}

func canonicalHash(labels []prompb.Label) uint64 {
	if labelsInCanonicalOrder(labels) {
		return hashLabels(labels)
	}
	pairs := sortedPairs(labels)
	var d xxhash.Digest
	d.Reset()
	for _, pair := range pairs {
		_, _ = d.WriteString(pair.name)
		_, _ = d.WriteString("\x00")
		_, _ = d.WriteString(pair.value)
		_, _ = d.WriteString("\x00")
	}
	return d.Sum64()
}

func labelsInCanonicalOrder(labels []prompb.Label) bool {
	for idx := 1; idx < len(labels); idx++ {
		prev := labels[idx-1]
		curr := labels[idx]
		if curr.Name < prev.Name {
			return false
		}
		if curr.Name == prev.Name && curr.Value < prev.Value {
			return false
		}
	}
	return true
}

func hashLabels(labels []prompb.Label) uint64 {
	var d xxhash.Digest
	d.Reset()
	for _, label := range labels {
		_, _ = d.WriteString(label.Name)
		_, _ = d.WriteString("\x00")
		_, _ = d.WriteString(label.Value)
		_, _ = d.WriteString("\x00")
	}
	return d.Sum64()
}

func canonicalString(labels []prompb.Label, maxBytes int) string {
	pairs := sortedPairs(labels)
	var b strings.Builder
	for idx, pair := range pairs {
		if idx > 0 {
			b.WriteByte(',')
		}
		b.WriteString(pair.name)
		b.WriteByte('=')
		b.WriteString(pair.value)
		if maxBytes > 0 && b.Len() >= maxBytes {
			out := b.String()
			return out[:maxBytes]
		}
	}
	return b.String()
}

func sortedPairs(labels []prompb.Label) []labelPair {
	pairs := make([]labelPair, 0, len(labels))
	for _, label := range labels {
		pairs = append(pairs, labelPair{name: label.Name, value: label.Value})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].name == pairs[j].name {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].name < pairs[j].name
	})
	return pairs
}

func seriesName(labels []prompb.Label) string {
	for _, label := range labels {
		if label.Name == metricNameLabel && label.Value != "" {
			return label.Value
		}
	}
	return unknownSeries
}

func hostName(labels []prompb.Label) string {
	for _, label := range labels {
		switch label.Name {
		case "host_name", "host", "hostname", "instance":
			if label.Value != "" {
				return label.Value
			}
		}
	}
	return unknownIdentity
}
