package inspector

import (
	"net/http"
	"strings"
	"testing"
)

func TestIdentitySetResolve(t *testing.T) {
	set, err := NewIdentitySet([]string{"tenant", "pipeline_sink", "input_path", "writer_id"}, 8)
	if err != nil {
		t.Fatalf("new identity set: %v", err)
	}

	h := http.Header{}
	h.Set("X-Scope-OrgID", "tenant-a")
	h.Set("X-Remote-Write-Inspector-Tenant", "tenant-b")
	h.Set("X-RWI-Pipeline-Sink", "sink-a")
	h.Set("X-Obs-Input-Path", "path-a")
	h.Set("X-Obs-Writer-ID", "writer-with-long-name")

	id := set.Resolve(h)
	if got := id.Get("tenant"); got != "tenant-a" {
		t.Fatalf("tenant header precedence mismatch: got %q", got)
	}
	if got := id.Get("pipeline_sink"); got != "sink-a" {
		t.Fatalf("pipeline sink fallback mismatch: got %q", got)
	}
	if got := id.Get("input_path"); got != "path-a" {
		t.Fatalf("input path mismatch: got %q", got)
	}
	if got := id.Get("writer_id"); got != "writer-w" {
		t.Fatalf("writer id truncation mismatch: got %q", got)
	}
}

func TestIdentitySetEmptyByDefault(t *testing.T) {
	set, err := NewIdentitySet(nil, 128)
	if err != nil {
		t.Fatalf("new identity set: %v", err)
	}
	if labels := set.LabelNames(); len(labels) != 0 {
		t.Fatalf("default identity labels = %#v, want none", labels)
	}
	id := set.Resolve(http.Header{"X-Scope-Orgid": []string{"tenant-a"}})
	if values := id.LabelValues(); len(values) != 0 {
		t.Fatalf("default identity values = %#v, want none", values)
	}
	if got := id.Get("tenant"); got != unknownIdentity {
		t.Fatalf("disabled tenant identity should read as unknown, got %q", got)
	}
}

func TestIdentitySetUnknownAndValidation(t *testing.T) {
	set, err := NewIdentitySet([]string{"tenant"}, 128)
	if err != nil {
		t.Fatalf("new identity set: %v", err)
	}
	if got := set.Resolve(http.Header{}).Get("tenant"); got != unknownIdentity {
		t.Fatalf("empty headers should resolve to unknown, got %q", got)
	}

	if _, err := NewIdentitySet([]string{"tenant", "tenant"}, 128); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate identity error, got %v", err)
	}
	if _, err := NewIdentitySet([]string{"free_form"}, 128); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported identity error, got %v", err)
	}
}
