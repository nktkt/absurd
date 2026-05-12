# Roadmap

This is a living document. Items are roughly ordered by priority within each
milestone; checked items are already in `main`.

The goal is to keep behavioral parity with [`earendil-works/absurd`](https://github.com/earendil-works/absurd) while taking advantage of Rust's type system and async story.

## 1.0 — Shipped

- Typed task handlers, `register_task` / `spawn_typed`, and `absurd_sdk::task()` builders backed by `Hooks` (`before_spawn`, `wrap_task_execution`).
- Lease watchdog with structured `WatchdogObserver` callbacks, plus graceful worker shutdown via `ShutdownHandle`.
- TLS via `rustls` / `native-tls` feature flags and stepwise, fully-offline migrations (`include_str!`-bundled migration index).
- `absurd-axum` integration crate (`AbsurdState`, `spawn_response`) and the `#[absurd::task]` proc-macro from `absurd-macros` (re-exported as `absurd_sdk::macros::task`).
- CLI parity for day-to-day ops: `cleanup`, `queue-policy get/set`, `list-detach-candidates`, `drop-detached-partition`, `cron-enable`, `cron-disable`, on top of the 0.1.x baseline.

## 0.1.x — Foundation (current line)

- [x] Cargo workspace with `absurd-sdk` + `absurdctl`
- [x] Bundled `absurd.sql` schema embedded at compile time
- [x] Async client with connection pool (`deadpool-postgres`)
- [x] `step` / `sleep_for` / `sleep_until` / `await_event`
- [x] Worker loop with concurrency, batch size, claim timeouts, heartbeats
- [x] `spawn` / `retry_task` / `cancel_task` / `await_task_result`
- [x] CLI: `init`, `migrate`, `schema-version`, `create-queue`, `drop-queue`, `list-queues`, `spawn-task`, `retry-task`, `cancel-task`, `print-schema`
- [x] Typed task handlers (`TaskHandler<P, R>`) — done via `register_task` / `spawn_typed` and `absurd_sdk::task()`
- [x] Hooks: `before_spawn`, `wrap_task_execution` — done in `absurd_sdk::Hooks`
- [x] Lease watchdog — done in `worker.rs` (`LeaseWatchdog`, `WatchdogObserver`)
- [x] Unit tests around queue-name validation, retry strategies, `ShutdownHandle` — done in `crates/absurd-sdk/tests/options.rs`
- [x] Integration tests against a real Postgres — Done (via CI services); see `.github/workflows/ci.yml` `integration` job with a Postgres service container
- [x] GitHub Actions CI: `cargo fmt --check`, `clippy -D warnings`, `cargo test` — done in `.github/workflows/ci.yml`
- [ ] Publish `absurd-sdk` + `absurdctl` to crates.io

## 0.2.x — Operational completeness

- [x] TLS support: feature flags for `rustls` and `native-tls`
- [x] CLI `cleanup` (calls `absurd.cleanup_all_queues` / `cleanup_tasks` / `cleanup_events`)
- [x] CLI `queue-policy get/set` (parity with Python tool)
- [x] CLI `list-detach-candidates` and `drop-detached-partition`
- [x] CLI `cron` subcommands (`cron-enable`, `cron-disable`)
- [x] Stepwise migrations: walk `0.x.y → 0.x.z` SQL files like the Python `absurdctl` does (bundled migration index)
- [x] Bundled migration index works offline — everything is `include_str!` at compile time
- [x] Structured retry strategies as Rust enums (`RetryStrategyKind` + `RetryStrategy::exponential` / `linear` / `fixed`)
- [x] Worker shutdown via `ShutdownHandle`; graceful drain of in-flight runs

## 0.3.x — Ergonomics & docs

- [x] `#[absurd::task]` proc-macro for typed handler registration — done in `absurd-macros` crate, re-exported as `absurd_sdk::macros::task`
- [ ] Tracing spans on every task run (queue, task_id, run_id, attempt as fields) — the worker logs but doesn't open a span yet
- [ ] OpenTelemetry exporter wired up by default behind a feature flag
- [ ] Cookbook examples: HTTP webhook fan-out, payment retry pattern, long-running LLM agent
- [ ] `mdbook` documentation site mirroring upstream `docs/`
- [ ] Comparison page (vs. PGMQ / Cadence / Temporal / Inngest / DBOS), translated from upstream

## 0.4.x — Ecosystem

- [ ] Port of `habitat` (web UI) to Axum + a server-rendered front end
- [x] `absurd-axum` integration crate: spawn tasks from HTTP handlers with shared pool (`AbsurdState`, `spawn_response`)
- [ ] `absurd-cron`-style scheduled-task layer
- [ ] Benchmarks comparing claim throughput vs. the Go and Python SDKs

## Tracking

Larger pieces of work will get GitHub issues with the `roadmap` label once the
repo has issue triage set up. Until then, this file is the source of truth.
Contributions welcome — open an issue describing the use case before sending a
PR for a non-trivial item.
