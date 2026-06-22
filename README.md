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
