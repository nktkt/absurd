# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] — 2026-05-12

### Added
- Typed task handlers via `absurd_sdk::task()` factory, `Client::register_task`, `Client::spawn_typed`, and `TaskDefinition::spawn`
- `#[task]` attribute macro (in new `absurd-macros` crate; re-exported as `absurd_sdk::macros::task`)
- Pluggable `Hooks` with `before_spawn` and `wrap_task_execution`
- `ShutdownHandle` for graceful worker shutdown, wired through `WorkerOptions.shutdown`
- Lease watchdog with warn/fatal timers reset by heartbeats and checkpoint writes
- TLS support behind `rustls` and `native-tls` Cargo features
- Stepwise migrations bundled at compile time (`absurd-sdk::migrations`)
- `absurd-axum` integration crate (`AbsurdState`, `spawn_response`)
- Structured retry strategies: `RetryStrategyKind` enum + `RetryStrategy::exponential/linear/fixed` factories
- New CLI commands: `cleanup`, `queue-policy`, `set-queue-policy`, `list-detach-candidates`, `drop-detached-partition`, `cron-enable`, `cron-disable`
- GitHub Actions CI: fmt, clippy, unit tests, integration tests against a Postgres service

### Changed
- Workspace bumped to 1.0.0
- `Client::spawn` now runs the `before_spawn` hook before invoking `absurd.spawn_task`
- Worker drains in-flight tasks on shutdown signal

### Notes
- Bundled schema target version is `main` (mirrors upstream `earendil-works/absurd`)
- The pre-1.0 stepwise chain has a gap at 0.0.7→0.0.8 inherited from upstream; migrate from a more recent version

## [0.1.0] — 2026-05-12

### Added
- Initial Rust port: `absurd-sdk` (async client + worker) and `absurdctl` (CLI)
- Bundled `absurd.sql` schema, `step` / `sleep_for` / `await_event`, `spawn` / `retry_task` / `cancel_task` / `await_task_result`, `WorkerOptions`, `WorkBatchOptions`, end-to-end example

[1.0.0]: https://github.com/nktkt/absurd/releases/tag/v1.0.0
[0.1.0]: https://github.com/nktkt/absurd/releases/tag/v0.1.0
