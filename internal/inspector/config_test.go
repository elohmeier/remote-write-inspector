package inspector

import (
	"strings"
	"testing"
)

func TestCacheMemoryBudgetOverridesCacheSize(t *testing.T) {
	cfg := Config{
		CacheSize:        12345,
		CacheMemoryBytes: 320000,
	}
	if err := cfg.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if cfg.CacheSize != 1000 {
		t.Fatalf("cache size = %d, want 1000", cfg.CacheSize)
	}
}

func TestParseTopSeriesLabel(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		want  TopSeriesLabel
	}{
		{name: "default output", input: "service_env", want: TopSeriesLabel{Source: "service_env", Output: "source_service_env"}},
		{name: "explicit output", input: "requestType=source_request_type", want: TopSeriesLabel{Source: "requestType", Output: "source_request_type"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTopSeriesLabel(tc.input)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got != tc.want {
				t.Fatalf("projection = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestTopSeriesLabelValidation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		projections []TopSeriesLabel
		identities  []string
		wantError   string
	}{
		{
			name: "duplicate source",
			projections: []TopSeriesLabel{
				{Source: "service_env", Output: "source_service_env"},
				{Source: "service_env", Output: "environment"},
			},
			wantError: "duplicate top-series source label",
		},
		{
			name: "duplicate output",
			projections: []TopSeriesLabel{
				{Source: "service_env", Output: "environment"},
				{Source: "dc_substage", Output: "environment"},
			},
			wantError: "duplicate top-series output label",
		},
		{
			name:        "fixed collision",
			projections: []TopSeriesLabel{{Source: "service_env", Output: "reason"}},
			wantError:   "is reserved",
		},
		{
			name:        "identity collision",
			projections: []TopSeriesLabel{{Source: "source_tenant", Output: "tenant"}},
			identities:  []string{"tenant"},
			wantError:   "is reserved",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{TopSeriesLabels: tc.projections, IdentityNames: tc.identities}
			err := cfg.normalize()
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("normalize error = %v, want containing %q", err, tc.wantError)
			}
		})
	}
}
