# remote-write-inspector

`remote-write-inspector` is a diagnostic Prometheus remote-write receiver for
data-quality troubleshooting. It accepts Thanos-compatible remote-write traffic,
exports bounded diagnostics, emits sampled structured logs, and returns `204 No
Content` for successfully decoded requests so it can be used without turning bad
data into ingestion backpressure.

## Usage

```bash
go run ./cmd/remote-write-inspector --listen-address=:8080
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
remote_write_inspector_cache_entries
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

`--cache-memory-bytes` overrides `--cache-size` and uses a conservative entry
estimate. Exact duplicate-timestamp detection still scales with the number of
samples retained inside `--cache-ttl`; at very high ingress rates, a long TTL
can require impractical memory.

For high-throughput stateless inspection, keep label, timestamp freshness, and
diagnostic metric checks enabled while disabling the expensive stateful caches:

```bash
go run ./cmd/remote-write-inspector \
  --disable-duplicate-sample-detection \
  --disable-cross-path-collision-detection
```
