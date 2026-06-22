package inspector

import (
	"fmt"
	"time"
)

type Config struct {
	ListenAddress            string
	MaxBodyBytes             int64
	IdentityNames            []string
	MaxIdentityLength        int
	StaleCutoff              time.Duration
	FutureSkew               time.Duration
	MaxLabels                int
	MaxLabelValueLength      int
	CacheSize                int
	CacheTTL                 time.Duration
	TopSeriesSize            int
	TopSeriesWindow          time.Duration
	LogSampleRate            float64
	DiagnosticMetricPrefixes []string
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
	if c.CacheSize <= 0 {
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
	if c.LogSampleRate < 0 || c.LogSampleRate > 1 {
		return fmt.Errorf("log sample rate must be between 0 and 1")
	}
	if len(c.DiagnosticMetricPrefixes) == 0 {
		c.DiagnosticMetricPrefixes = []string{"remote_write_inspector_", "obspipeline_"}
	}
	return nil
}
