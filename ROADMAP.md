# Roadmap

This is a living document. Items are roughly ordered by priority within each
milestone; checked items are already in `main`.

The goal is to keep behavioral parity with [`earendil-works/absurd`](https://github.com/earendil-works/absurd) while taking advantage of Rust's type system and async story.

## 0.1.x — Foundation (current line)

- [x] Cargo workspace with `absurd-sdk` + `absurdctl`
- [x] Bundled `absurd.sql` schema embedded at compile time
- [x] Async client with connection pool (`deadpool-postgres`)
- [x] `step` / `sleep_for` / `sleep_until` / `await_event`
- [x] Worker loop with concurrency, batch size, claim timeouts, heartbeats
- [x] `spawn` / `retry_task` / `cancel_task` / `await_task_result`
- [x] CLI: `init`, `migrate`, `schema-version`, `create-queue`, `drop-queue`, `list-queues`, `spawn-task`, `retry-task`, `cancel-task`, `print-schema`
- [ ] Typed task handlers (`TaskHandler<P, R>`) — drop `Value` for `Serialize`/`Deserialize` types
- [ ] Hooks: `before_spawn`, `wrap_task_execution`
- [ ] Lease watchdog (`fatal_on_lease_timeout` warning + exit timers)
- [ ] Unit tests around queue-name validation, spawn-option normalization, checkpoint-name disambiguation
- [ ] Integration tests against a real Postgres (testcontainers or a `make test` recipe)
- [ ] GitHub Actions CI: `cargo fmt --check`, `clippy -D warnings`, `cargo test`
- [ ] Publish `absurd-sdk` + `absurdctl` to crates.io

## 0.2.x — Operational completeness

- [ ] TLS support: feature flags for `rustls` and `native-tls`
- [ ] CLI `cleanup` (calls `absurd.cleanup_all_queues` / `cleanup_tasks` / `cleanup_events`)
- [ ] CLI `queue-policy get/set` (parity with Python tool)
- [ ] CLI `list-detach-candidates` and `detach-candidate`
- [ ] CLI `cron` subcommands (`enable`, `disable`, list)
- [ ] Stepwise migrations: walk `0.x.y → 0.x.z` SQL files like the Python `absurdctl` does, with `--dry-run` previews
- [ ] Bundled migration index so offline installs work without GitHub access
- [ ] Structured retry strategies (`exponential`, `linear`, `fixed`) as Rust enums instead of free-form strings
- [ ] Worker shutdown via `CancellationToken`; graceful drain of in-flight runs

## 0.3.x — Ergonomics & docs

- [ ] `#[absurd::task]` proc-macro for typed handler registration
- [ ] Tracing spans on every task run (queue, task_id, run_id, attempt as fields)
- [ ] OpenTelemetry exporter wired up by default behind a feature flag
- [ ] Cookbook examples: HTTP webhook fan-out, payment retry pattern, long-running LLM agent
- [ ] `mdbook` documentation site mirroring upstream `docs/`
- [ ] Comparison page (vs. PGMQ / Cadence / Temporal / Inngest / DBOS), translated from upstream

## 0.4.x — Ecosystem

- [ ] Port of `habitat` (web UI) to Axum + a server-rendered front end
- [ ] `absurd-axum` integration crate: spawn tasks from HTTP handlers with shared pool
- [ ] `absurd-cron`-style scheduled-task layer
- [ ] Benchmarks comparing claim throughput vs. the Go and Python SDKs

## Tracking

Larger pieces of work will get GitHub issues with the `roadmap` label once the
repo has issue triage set up. Until then, this file is the source of truth.
Contributions welcome — open an issue describing the use case before sending a
PR for a non-trivial item.
