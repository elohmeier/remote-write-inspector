package inspector

import (
	"fmt"
	"sort"

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

func seriesFingerprint(hash uint64) string {
	return fmt.Sprintf("%016x", hash)
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
