package inspector

import "testing"

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
