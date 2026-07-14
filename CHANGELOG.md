# Changelog

## Unreleased

### Breaking changes

* change `remote_write_inspector_top_series` from metric/host aggregation to
  exact canonical series identified by `series_fingerprint`
* replace sampled canonical-label strings in finding logs with bounded ordered
  label arrays and explicit sample-conflict evidence

### Features

* add repeatable `--top-series-label=source[=output]` projections for bounded
  metrics and structured logs
* add deterministic, bounded first-example logging per diagnostics window

## 0.1.0 (2026-06-22)


### Features

* initial public release ([15094b5](https://github.com/elohmeier/remote-write-inspector/commit/15094b536a3f493584873f820d3f6485ceb4d022))
