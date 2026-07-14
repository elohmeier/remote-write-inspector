package inspector

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

type findingLogKey struct {
	identityValues string
	kind           string
	reason         string
	seriesHash     uint64
}

type findingLogSampler struct {
	mtx         sync.Mutex
	window      time.Duration
	firstLimit  int
	windowStart time.Time
	seen        map[findingLogKey]struct{}
}

func newFindingLogSampler(window time.Duration, firstLimit int, now time.Time) *findingLogSampler {
	return &findingLogSampler{
		window:      window,
		firstLimit:  firstLimit,
		windowStart: now,
		seen:        make(map[findingLogKey]struct{}),
	}
}

func (s *findingLogSampler) ShouldLog(now time.Time, key findingLogKey, repeatRate float64) bool {
	s.mtx.Lock()
	if now.Sub(s.windowStart) >= s.window {
		s.windowStart = now
		s.seen = make(map[findingLogKey]struct{})
	}
	_, seen := s.seen[key]
	if !seen && len(s.seen) < s.firstLimit {
		s.seen[key] = struct{}{}
		s.mtx.Unlock()
		return true
	}
	s.mtx.Unlock()

	return repeatRate > 0 && rand.Float64() <= repeatRate
}

type logLabel struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func boundedLogLabels(labels []prompb.Label, maxBytes int) ([]logLabel, bool) {
	out := make([]logLabel, 0, len(labels))
	used := 2 // JSON array brackets.
	for _, label := range labels {
		candidate := logLabel{Name: label.Name, Value: label.Value}
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return out, true
		}
		separator := 0
		if len(out) > 0 {
			separator = 1
		}
		if used+separator+len(encoded) > maxBytes {
			return out, true
		}
		out = append(out, candidate)
		used += separator + len(encoded)
	}
	return out, false
}
