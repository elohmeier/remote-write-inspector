package inspector

import (
	"fmt"
	"strings"
	"time"
)

type TopSeriesLabel struct {
	Source string
	Output string
}

type Config struct {
	ListenAddress             string
	MaxBodyBytes              int64
	IdentityNames             []string
	MaxIdentityLength         int
	StaleCutoff               time.Duration
	FutureSkew                time.Duration
	MaxLabels                 int
	MaxLabelValueLength       int
	CacheSize                 int
	CacheMemoryBytes          int64
	CacheTTL                  time.Duration
	TopSeriesSize             int
	TopSeriesWindow           time.Duration
	TopSeriesLabels           []TopSeriesLabel
	LogSampleRate             float64
	LogFirstExamplesPerWindow int
	LogLabelBytes             int
	DiagnosticMetricPrefixes  []string

	DisableDuplicateSampleDetection    bool
	DisableCrossPathCollisionDetection bool
}

func (c *Config) normalize() error {
	if c.ListenAddress == "" {
		c.ListenAddress = ":8080"
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 32 << 20
	}
	if c.MaxIdentityLength <= 0 {
		c.MaxIdentityLength = 128
	}
	if c.StaleCutoff <= 0 {
		c.StaleCutoff = 23 * time.Hour
	}
	if c.FutureSkew <= 0 {
		c.FutureSkew = 10 * time.Minute
	}
	if c.MaxLabels <= 0 {
		c.MaxLabels = 128
	}
	if c.MaxLabelValueLength <= 0 {
		c.MaxLabelValueLength = 4096
	}
	if c.CacheMemoryBytes > 0 {
		c.CacheSize = cacheSizeFromMemoryBudget(c.CacheMemoryBytes)
	} else if c.CacheSize <= 0 {
		c.CacheSize = 500000
	}
	if c.CacheTTL <= 0 {
		c.CacheTTL = 25 * time.Hour
	}
	if c.TopSeriesSize <= 0 {
		c.TopSeriesSize = 20
	}
	if c.TopSeriesWindow <= 0 {
		c.TopSeriesWindow = 5 * time.Minute
	}
	if err := c.normalizeTopSeriesLabels(); err != nil {
		return err
	}
	if c.LogSampleRate < 0 || c.LogSampleRate > 1 {
		return fmt.Errorf("log sample rate must be between 0 and 1")
	}
	if c.LogFirstExamplesPerWindow < 0 {
		return fmt.Errorf("log first examples per window must not be negative")
	}
	if c.LogLabelBytes <= 0 {
		c.LogLabelBytes = 4096
	}
	if len(c.DiagnosticMetricPrefixes) == 0 {
		c.DiagnosticMetricPrefixes = []string{"remote_write_inspector_", "obspipeline_"}
	}
	return nil
}

func ParseTopSeriesLabel(value string) (TopSeriesLabel, error) {
	source, output, mapped := strings.Cut(strings.TrimSpace(value), "=")
	source = strings.TrimSpace(source)
	output = strings.TrimSpace(output)
	if source == "" {
		return TopSeriesLabel{}, fmt.Errorf("top-series label source must not be empty")
	}
	if !mapped {
		output = "source_" + source
	}
	if output == "" {
		return TopSeriesLabel{}, fmt.Errorf("top-series label output must not be empty")
	}
	return TopSeriesLabel{Source: source, Output: output}, nil
}

func (c *Config) normalizeTopSeriesLabels() error {
	reserved := map[string]struct{}{
		"__name__":            {},
		"component":           {},
		"event":               {},
		"host_name":           {},
		"incoming_value":      {},
		"incoming_value_bits": {},
		"labels":              {},
		"labels_truncated":    {},
		"previous_value":      {},
		"previous_value_bits": {},
		"reason":              {},
		"sample_timestamp_ms": {},
		"sample_type":         {},
		"series_fingerprint":  {},
		"series_name":         {},
		"value_delta":         {},
	}
	for _, identity := range c.IdentityNames {
		reserved[identity] = struct{}{}
	}

	sources := make(map[string]struct{}, len(c.TopSeriesLabels))
	outputs := make(map[string]struct{}, len(c.TopSeriesLabels))
	for idx := range c.TopSeriesLabels {
		projection := &c.TopSeriesLabels[idx]
		if projection.Output == "" {
			projection.Output = "source_" + projection.Source
		}
		if !labelNameRE.MatchString(projection.Source) {
			return fmt.Errorf("invalid top-series source label %q", projection.Source)
		}
		if !labelNameRE.MatchString(projection.Output) {
			return fmt.Errorf("invalid top-series output label %q", projection.Output)
		}
		if _, ok := sources[projection.Source]; ok {
			return fmt.Errorf("duplicate top-series source label %q", projection.Source)
		}
		if _, ok := outputs[projection.Output]; ok {
			return fmt.Errorf("duplicate top-series output label %q", projection.Output)
		}
		if _, ok := reserved[projection.Output]; ok {
			return fmt.Errorf("top-series output label %q is reserved", projection.Output)
		}
		sources[projection.Source] = struct{}{}
		outputs[projection.Output] = struct{}{}
	}
	return nil
}

func cacheSizeFromMemoryBudget(bytes int64) int {
	const (
		statefulCaches         = 2
		estimatedBytesPerEntry = 160
	)
	entries := bytes / (statefulCaches * estimatedBytesPerEntry)
	if entries < 1 {
		return 1
	}
	maxInt := int64(^uint(0) >> 1)
	if entries > maxInt {
		return int(maxInt)
	}
	return int(entries)
}
