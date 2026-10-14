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

## [0.2.0](https://github.com/elohmeier/remote-write-inspector/compare/v0.1.0...v0.2.0) (2026-10-14)


### Features

* add bounded remote-write sample capture ([2dc08ae](https://github.com/elohmeier/remote-write-inspector/commit/2dc08ae24425704fda0907d81bd17b7edc0b092a))
* make remote write inspection overload-safe ([4bd5b19](https://github.com/elohmeier/remote-write-inspector/commit/4bd5b194ad0a3b82353b86771a1a1c184d17bbe4))
* make series diagnostics actionable ([b5b91f1](https://github.com/elohmeier/remote-write-inspector/commit/b5b91f1f5262dd5a8ef955bb70c003569a3a9eb4))


### Bug Fixes

* emit structured event actions in diagnostic logs ([b1bfc9e](https://github.com/elohmeier/remote-write-inspector/commit/b1bfc9eafb21001e19cec1856b92e1860cf6ab13))

## 0.1.0 (2026-06-22)


### Features

* initial public release ([15094b5](https://github.com/elohmeier/remote-write-inspector/commit/15094b536a3f493584873f820d3f6485ceb4d022))
