# remote-write-inspector

[![CI](https://github.com/elohmeier/remote-write-inspector/actions/workflows/ci.yml/badge.svg)](https://github.com/elohmeier/remote-write-inspector/actions/workflows/ci.yml)
[![Release](https://github.com/elohmeier/remote-write-inspector/actions/workflows/release.yml/badge.svg)](https://github.com/elohmeier/remote-write-inspector/actions/workflows/release.yml)
[![GitHub release](https://img.shields.io/github/v/release/elohmeier/remote-write-inspector?sort=semver)](https://github.com/elohmeier/remote-write-inspector/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/elohmeier/remote-write-inspector.svg)](https://pkg.go.dev/github.com/elohmeier/remote-write-inspector)
[![Go Report Card](https://goreportcard.com/badge/github.com/elohmeier/remote-write-inspector)](https://goreportcard.com/report/github.com/elohmeier/remote-write-inspector)
[![GHCR](https://img.shields.io/badge/ghcr.io-elohmeier%2Fremote--write--inspector-blue)](https://github.com/elohmeier/remote-write-inspector/pkgs/container/remote-write-inspector)

`remote-write-inspector` is a diagnostic Prometheus remote-write receiver for
data-quality troubleshooting. It accepts Thanos-compatible remote-write traffic,
exports bounded diagnostics, emits sampled structured logs, and returns `204 No
Content` for successfully decoded requests so it can be used without turning bad
data into ingestion backpressure.

The receiver is intentionally best-effort. `--max-in-flight-requests` bounds
concurrent body allocation, decoding, and inspection. Excess requests are
counted with `result="overloaded"` and acknowledged with `204 No Content` so a
diagnostic outage cannot backpressure the primary metrics path.

## Usage

```bash
go run ./cmd/remote-write-inspector --listen-address=:8080
```

Or run the GHCR image:

```bash
docker run --rm -p 8080:8080 ghcr.io/elohmeier/remote-write-inspector:main
```

Enable identity labels explicitly when needed:

```bash
go run ./cmd/remote-write-inspector \
  --identity=tenant \
  --identity=pipeline_sink \
  --identity=input_path
```

Endpoints:

- `POST /api/v1/receive`
- `POST /receive`
- `GET /metrics`
- `GET /-/healthy`
- `GET /-/ready`

Remote-write payloads must be snappy-compressed protobuf
`prompb.WriteRequest` bodies.

## Identity Headers

Identity labels are enabled with shorthand-only `--identity` flags. If no
identity flags are provided, no request headers are read for identity and the
exported metrics use no identity labels.

Header lookup order:

| Identity | Headers |
| --- | --- |
| `tenant` | `X-Scope-OrgID`, `X-Remote-Write-Inspector-Tenant`, `X-RWI-Tenant` |
| `pipeline_sink` | `X-Obs-Pipeline-Sink`, `X-Remote-Write-Inspector-Pipeline-Sink`, `X-RWI-Pipeline-Sink` |
| `input_path` | `X-Obs-Input-Path`, `X-Remote-Write-Inspector-Input-Path`, `X-RWI-Input-Path` |
| `writer_id` | `X-Obs-Writer-ID`, `X-Remote-Write-Inspector-Writer-ID`, `X-RWI-Writer-ID` |

For enabled identities, missing values become `unknown`. Label names are fixed
at process startup from the enabled shorthands.

## Diagnostics

Detected reasons include invalid label and metric shape, stale/future samples,
duplicate timestamp with different value, cross-path canonical labelset
collisions, and diagnostic metric series without a stable `writer_id`.

Metrics use the `remote_write_inspector_*` namespace. High-cardinality series
details are limited to the bounded `remote_write_inspector_top_series` gauge;
counters only use the configured identity labels plus fixed labels such as
`reason` and `result`.

`remote_write_inspector_top_series` represents exact canonical series. Each
row contains a stable 16-character `series_fingerprint`, so series with the
same metric and host are no longer merged. Selected source-series labels can
be promoted into the bounded metric and its structured finding logs:

```bash
go run ./cmd/remote-write-inspector \
  --identity=tenant \
  --top-series-label=cluster \
  --top-series-label=service_env \
  --top-series-label=namespace \
  --top-series-label=requestType=source_request_type
```

A bare source label is exported with a `source_` prefix. For example,
`namespace` becomes `source_namespace`; this avoids collisions with labels
added when the inspector itself is scraped. An explicit `source=output`
mapping can normalize source label names. Missing source labels use `unknown`.
Duplicate sources, output names, invalid Prometheus label names, and collisions
with fixed or identity labels are rejected at startup.

Finding logs include the fingerprint, the promoted fields, and an ordered,
size-bounded `labels` array. The array preserves duplicate and out-of-order
labels for invalid-series investigations. Duplicate-timestamp conflicts also
include the previous and incoming IEEE-754 bit patterns, finite numeric values,
and their delta.

The event discriminator is a JSON object: `{"event":{"action":"bad_series"}}`
or `{"event":{"action":"bad_sample"}}`. Capture logs use the same shape with
`action="sample_capture"`. Consumers of earlier versions must migrate from the
scalar `event` field to `event.action`. Keeping `event` as an object allows
collectors to add fields such as `event.created` and avoids conflicts with ECS
Elasticsearch mappings. The remaining diagnostic fields retain their existing
format; this is not a complete ECS log envelope.

The first distinct findings in each `--top-series-window` are logged
deterministically. `--log-first-examples-per-window` bounds those logs; after a
finding's first example, `--log-sample-rate` controls repeat examples.
`--log-label-bytes` bounds the encoded label array. Set both logging controls to
zero to disable finding examples completely. Source labels and sample values
are application-controlled and may be sensitive, so protect log access and
retention accordingly.

## Bounded Sample Capture

Use `--capture-selector` to capture selected float samples, including valid
samples that produce no findings. Capture is off by default and operates
independently of finding-log sampling:

```bash
go run ./cmd/remote-write-inspector \
  --identity=tenant \
  --identity=input_path \
  --capture-selector='{__name__=~"recorded:.*",monitor_source="user-workload/thanos-ruler"}' \
  --capture-duration=5m \
  --capture-max-samples=1000 \
  --capture-rate=10 \
  --capture-label-bytes=4096
```

The selector uses Prometheus metric-selector syntax (metric name and/or label
matchers, not a PromQL expression). It matches source-series labels, not HTTP
identity headers. At least one matcher must exclude an empty/missing value.
Missing labels have the usual empty-string matching semantics. For invalid
duplicate label names, every occurrence must satisfy that label's matchers.

| Flag | Default | Scope |
| --- | --- | --- |
| `--capture-selector` | empty (disabled) | One selector per process |
| `--capture-duration` | `5m` | Deadline from inspector startup |
| `--capture-max-samples` | `1000` | Total log attempts per process |
| `--capture-rate` | `10` | Log attempts per one-second window per process |
| `--capture-label-bytes` | `4096` | Encoded JSON label-array bytes per record |

Duration, sample count, and rate must be positive; label bytes must be at least
2. The other capture flags have no effect without a selector. Budgets are
shared across identities and concurrent requests. The rate limit uses fixed
one-second windows starting at startup or the first attempt after a window
expires; it allows bursts within a window. There is no queue or retry for
skipped samples. Restarting the process starts a new session with new budgets.

Each structured log has `event.action="sample_capture"` and includes:

- `received_at`: UTC time when the HTTP handler received the request, before
  reading or decoding its body. This is the inspector's arrival time, not the
  sender's dispatch time or the destination receiver's arrival time.
- `sample_timestamp_ms`, `sample_type="float"`, and `sample_value_bits`: the
  original timestamp and exact IEEE-754 bits in hexadecimal. `sample_value`
  is also included for finite values. The bits preserve NaN payloads (including
  stale markers), infinities, and signed zero.
- `labels`: the original ordered label array, preserving duplicates and replica
  labels, up to the byte limit. `labels_count` is the original label count;
  `labels_truncated` marks omitted labels. The first label that would exceed
  the limit and all following labels are omitted.
- `series_fingerprint`: the canonical fingerprint of the complete label set,
  even when the logged array is truncated. Retain complete labels when checking
  series equality; a fingerprint alone is not proof.
- Configured request identities, such as `tenant` and `input_path`.

Capture metrics are registered only when enabled. Counters use configured
identity labels; sample counters also use `sample_type` (`float` or `histogram`):

| Metric | Meaning |
| --- | --- |
| `remote_write_inspector_capture_matched_samples_total` | Decoded samples matching the selector, including after capture limits are reached |
| `remote_write_inspector_capture_captured_samples_total` | Samples successfully handed to the log handler |
| `remote_write_inspector_capture_skipped_samples_total` | Matching samples skipped, with a `reason` label |
| `remote_write_inspector_capture_truncated_samples_total` | Subset of captured samples with truncated labels; identity labels only |
| `remote_write_inspector_capture_deadline_timestamp_seconds` | Session deadline as Unix time; no labels |

Skip reasons are `duration_limit`, `sample_limit`, `rate_limit`, `log_disabled`,
`log_error`, and `unsupported_sample_type`. Failed or disabled log writes
consume an attempt from the count and rate budgets. Native histogram samples
are counted as matched and skipped with `unsupported_sample_type`; exemplars
and metadata are not captured or included in these sample counters.

For completed inspections, matched samples equal captured plus skipped samples
per identity and sample type. Truncated samples are a subset of captured
samples. A scrape during an active inspection can observe a temporary gap.
Capture does not change validation findings or HTTP response semantics.

This is bounded evidence from traffic that reaches the inspector and is
decoded. Overloaded, oversized, or undecodable requests have unknown matching
sample counts: consult `remote_write_inspector_requests_total` as well as the
capture counters. A `204` can acknowledge an uninspected overload request.
Mirror drops and downstream ingestion outcomes require separate evidence.
Successful logging does not acknowledge durable log storage; retain the logs
and metrics together when using a capture for an investigation.

## Load Testing

The repo includes a synthetic remote-write load generator that sends real
snappy-compressed `prompb.WriteRequest` payloads:

```bash
go run ./cmd/rwi-loadgen \
  -url=http://localhost:8080/receive \
  -duration=1m \
  -workers=8 \
  -rate=200 \
  -series-per-request=1000 \
  -samples-per-series=1 \
  -active-series=100000
```

Set `-rate=0` for an unlimited saturation run. The generator prints request
rate, sample rate, status counts, decoded protobuf throughput, compressed wire
throughput, and approximate latency percentiles.

Useful scenarios:

```bash
# Baseline valid traffic at a controlled rate.
go run ./cmd/rwi-loadgen -duration=2m -workers=8 -rate=500

# Saturate the local receiver/generator pair.
go run ./cmd/rwi-loadgen -duration=30s -workers=16 -rate=0

# Exercise cache churn with more active series.
go run ./cmd/rwi-loadgen -duration=1m -workers=8 -rate=300 -active-series=1000000

# Generate bad samples and invalid metric names.
go run ./cmd/rwi-loadgen -duration=1m -rate=200 \
  -stale-ratio=0.01 \
  -future-ratio=0.005 \
  -invalid-series-ratio=0.001

# Trigger cross-path collisions when the receiver enables input_path identity.
go run ./cmd/remote-write-inspector --identity=tenant --identity=input_path
go run ./cmd/rwi-loadgen -duration=1m -rate=200 -path-count=2

# Trigger duplicate timestamp conflicts. Smaller active-series values make
# repeats happen sooner.
go run ./cmd/rwi-loadgen -duration=1m -rate=500 -active-series=10000 -conflict-ratio=0.01
```

Watch the receiver's own metrics while testing:

```promql
rate(remote_write_inspector_requests_total[1m])
rate(remote_write_inspector_samples_total[1m])
histogram_quantile(0.99, rate(remote_write_inspector_decode_duration_seconds_bucket[1m]))
histogram_quantile(0.99, rate(remote_write_inspector_validation_duration_seconds_bucket[1m]))
histogram_quantile(0.99, rate(remote_write_inspector_request_duration_seconds_bucket[1m]))
remote_write_inspector_in_flight_requests
remote_write_inspector_max_in_flight_requests
remote_write_inspector_cache_entries
remote_write_inspector_cache_capacity_entries
rate(remote_write_inspector_cache_operations_total[1m])
rate(remote_write_inspector_cache_evictions_total[1m])
rate(remote_write_inspector_decode_errors_total[1m])
```

## High-Volume Tuning

Stateful detectors need bounded memory. You can size the duplicate-sample and
cross-path caches by explicit entries:

```bash
go run ./cmd/remote-write-inspector --cache-size=5000000
```

Or by an approximate total memory budget for both stateful caches:

```bash
go run ./cmd/remote-write-inspector --cache-memory-bytes=8589934592
```

`--cache-memory-bytes` overrides `--cache-size`, uses a conservative entry
estimate, and divides the total budget only among active stateful detectors.
The cross-path detector allocates no cache unless `input_path` identity is
enabled. Exact duplicate-timestamp detection still scales with the number of
samples retained inside `--cache-ttl`; at very high ingress rates, a long TTL
can require impractical memory.

For high-throughput stateless inspection, keep label, timestamp freshness, and
diagnostic metric checks enabled while disabling the expensive stateful caches:

```bash
go run ./cmd/remote-write-inspector \
  --disable-duplicate-sample-detection \
  --disable-cross-path-collision-detection
```
