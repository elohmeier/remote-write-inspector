package inspector

import (
	"fmt"
	"net/http"
	"strings"
)

const unknownIdentity = "unknown"

type IdentityDefinition struct {
	Name    string
	Headers []string
}

type IdentitySet struct {
	defs              []IdentityDefinition
	names             []string
	maxIdentityLength int
}

type RequestIdentity struct {
	names  []string
	values []string
	byName map[string]string
}

func NewIdentitySet(names []string, maxIdentityLength int) (*IdentitySet, error) {
	seen := make(map[string]struct{}, len(names))
	defs := make([]IdentityDefinition, 0, len(names))
	for _, name := range names {
		def, ok := identityDefinition(name)
		if !ok {
			return nil, fmt.Errorf("unsupported identity shorthand %q", name)
		}
		if _, ok := seen[def.Name]; ok {
			return nil, fmt.Errorf("duplicate identity shorthand %q", def.Name)
		}
		seen[def.Name] = struct{}{}
		defs = append(defs, def)
	}

	return &IdentitySet{
		defs:              defs,
		names:             namesFromDefs(defs),
		maxIdentityLength: maxIdentityLength,
	}, nil
}

func identityDefinition(name string) (IdentityDefinition, bool) {
	switch name {
	case "tenant":
		return IdentityDefinition{Name: name, Headers: []string{"X-Scope-OrgID", "X-Remote-Write-Inspector-Tenant", "X-RWI-Tenant"}}, true
	case "pipeline_sink":
		return IdentityDefinition{Name: name, Headers: []string{"X-Obs-Pipeline-Sink", "X-Remote-Write-Inspector-Pipeline-Sink", "X-RWI-Pipeline-Sink"}}, true
	case "input_path":
		return IdentityDefinition{Name: name, Headers: []string{"X-Obs-Input-Path", "X-Remote-Write-Inspector-Input-Path", "X-RWI-Input-Path"}}, true
	case "writer_id":
		return IdentityDefinition{Name: name, Headers: []string{"X-Obs-Writer-ID", "X-Remote-Write-Inspector-Writer-ID", "X-RWI-Writer-ID"}}, true
	default:
		return IdentityDefinition{}, false
	}
}

func namesFromDefs(defs []IdentityDefinition) []string {
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	return names
}

func (s *IdentitySet) LabelNames() []string {
	return append([]string(nil), s.names...)
}

func (s *IdentitySet) Resolve(h http.Header) RequestIdentity {
	values := make([]string, 0, len(s.defs))
	byName := make(map[string]string, len(s.defs))
	for _, def := range s.defs {
		value := unknownIdentity
		for _, header := range def.Headers {
			raw := strings.TrimSpace(h.Get(header))
			if raw != "" {
				value = truncateIdentityValue(raw, s.maxIdentityLength)
				break
			}
		}
		values = append(values, value)
		byName[def.Name] = value
	}

	return RequestIdentity{
		names:  s.names,
		values: values,
		byName: byName,
	}
}

func truncateIdentityValue(value string, maxLen int) string {
	if value == "" {
		return unknownIdentity
	}
	if maxLen <= 0 || len(value) <= maxLen {
		return value
	}
	return value[:maxLen]
}

func (i RequestIdentity) LabelValues(extra ...string) []string {
	values := make([]string, 0, len(i.values)+len(extra))
	values = append(values, i.values...)
	values = append(values, extra...)
	return values
}

func (i RequestIdentity) Get(name string) string {
	if v, ok := i.byName[name]; ok {
		return v
	}
	return unknownIdentity
}

func (i RequestIdentity) HasKnown(name string) bool {
	return i.Get(name) != unknownIdentity
}

func (i RequestIdentity) HasField(name string) bool {
	_, ok := i.byName[name]
	return ok
}

func (i RequestIdentity) Attrs() []any {
	attrs := make([]any, 0, len(i.names)*2)
	for idx, name := range i.names {
		attrs = append(attrs, name, i.values[idx])
	}
	return attrs
}
