# Absurd — Rust

A Rust port of [`earendil-works/absurd`](https://github.com/earendil-works/absurd) — the simplest durable execution workflow system, built entirely on Postgres.

Tasks decompose into idempotent steps whose results are checkpointed in Postgres. Workers pull tasks, run them, and the SDK handles retries, durable sleeps, and event-driven suspensions for you. No Redis, no broker, no coordinator — just your existing Postgres.

This is the v1.0 release: feature-complete with typed handlers, hooks, lease watchdogs, graceful shutdown, optional TLS, and the full upstream CLI surface.

## What's in the box

| Crate            | Purpose                                                                                          |
| ---------------- | ------------------------------------------------------------------------------------------------ |
| `absurd-sdk`     | Async client + worker SDK (`step`, `await_event`, `sleep_for`, `heartbeat`, `spawn`, `await_task_result`, typed handlers, hooks, …) |
| `absurd-macros`  | `#[task]` attribute macro that generates a typed handler builder. Re-exported as `absurd_sdk::macros::task`. |
| `absurd-axum`    | Axum integration helpers for exposing Absurd from a web service.                                 |
| `absurdctl`      | CLI for schema management, queues, tasks, cleanup, partition detach, and pg_cron jobs.           |

The bundled SQL (`crates/absurd-sdk/sql/absurd.sql`) is the canonical schema from upstream, embedded into the SDK at compile time. Stepwise migrations are also bundled (`absurd_sdk::migrations`) and applied automatically by `absurdctl migrate`.

## Features

- Durable steps with automatic checkpoint replay on retry
- `await_event` with cached, race-free event delivery
- Durable `sleep_for` / `sleep_until` that survive process restarts
- Typed task handlers via `absurd_sdk::task(...)` and the `#[task]` attribute macro
- Pluggable `Hooks` with `before_spawn` and `wrap_task_execution` for tracing, auth, multi-tenancy
- Lease watchdog with warn / fatal timers — detect when a step blocks the worker
- Graceful shutdown via `ShutdownHandle` (set on `WorkerOptions.shutdown`)
- Stepwise migrations bundled at compile time and applied idempotently
- Optional TLS via feature flags: `rustls` or `native-tls`
- Single-line worker: `client.run_worker(WorkerOptions::default()).await`
- Zero external services — only Postgres

## Quickstart

```bash
# 1. Create a database and apply the schema
createdb absurd
cargo run -p absurdctl -- -d absurd init
cargo run -p absurdctl -- -d absurd create-queue default

# 2. Run an example
ABSURD_DATABASE_URL="postgresql:///absurd?host=/tmp" \
  cargo run -p absurd-sdk --example order_fulfillment

# Or the typed-handler example
ABSURD_DATABASE_URL="postgresql:///absurd?host=/tmp" \
  cargo run -p absurd-sdk --example typed_task
```

Full examples live at `crates/absurd-sdk/examples/order_fulfillment.rs` and `crates/absurd-sdk/examples/typed_task.rs`.

## SDK shape

The recommended way to define a workflow is the `#[task]` macro, which generates a sibling `<fn>_task()` builder you pass to `register_task` and `spawn_typed`:

```rust
use absurd_sdk::macros::task;
use absurd_sdk::{
    AwaitTaskResultOptions, Client, Result, SpawnOptions, TaskResultState,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct Params {
    name: String,
    times: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Output {
    greeting: String,
}

#[task(name = "greet")]
async fn greet(params: Params) -> Result<Output> {
    let greeting = std::iter::repeat(format!("hello {}!", params.name))
        .take(params.times as usize)
        .collect::<Vec<_>>()
        .join(" ");
    Ok(Output { greeting })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = Client::connect().await?;
    app.register_task(greet_task()).await?;

    let worker = app.clone();
    tokio::spawn(async move { worker.run_worker(Default::default()).await });

    let spawn = app
        .spawn_typed(
            &greet_task(),
            Params { name: "world".into(), times: 3 },
            SpawnOptions::default(),
        )
        .await?;

    let snapshot = app
        .await_task_result(
            "default",
            &spawn.task_id,
            AwaitTaskResultOptions {
                timeout: Some(Duration::from_secs(10)),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(snapshot.state, TaskResultState::Completed);
    let parsed: Output = snapshot.decode_result()?.unwrap();
    println!("{}", parsed.greeting);
    Ok(())
}
```

`step`, `await_event`, and `sleep_for` work the same way inside typed tasks. See the order-fulfillment example for steps, event waits, and worker concurrency tuning.

## CLI

```text
absurdctl init                                  # apply the bundled schema
absurdctl schema-version                        # read the recorded version
absurdctl migrate                               # apply stepwise migrations (idempotent)
absurdctl print-schema                          # write the bundled SQL to stdout

absurdctl create-queue <name> [...]             # create a queue
absurdctl list-queues
absurdctl drop-queue <name>

absurdctl spawn-task <queue> <task> [--params '<json>'] [--header k=v ...]
absurdctl retry-task <queue> <task-id> [--spawn-new]
absurdctl cancel-task <queue> <task-id>

absurdctl cleanup [--queue <q>] [--ttl-seconds N] [--limit N] [--events-only]
absurdctl queue-policy <queue>                  # print persisted policy
absurdctl set-queue-policy <queue> [--ttl ...]  # update policy fields

absurdctl list-detach-candidates [--queue <q>]  # partitions eligible for detach
absurdctl drop-detached-partition <table>

absurdctl cron-enable [...]                     # schedule pg_cron maintenance jobs
absurdctl cron-disable
```

All commands accept `-d <dbname>` (libpq style) or `--database-url <url>`. They also honor `ABSURD_DATABASE_URL` and `PGDATABASE`.

## TLS

The SDK ships with two opt-in TLS backends. Pick whichever fits your dependency tree:

```toml
absurd-sdk = { version = "1.0", features = ["rustls"] }
# or
absurd-sdk = { version = "1.0", features = ["native-tls"] }
```

Both can be enabled simultaneously; `rustls` wins at runtime when both are configured. With neither feature enabled the pool uses `NoTls` and you should connect over a trusted network or a Unix socket.

## Building

```bash
cargo build --release
./target/release/absurdctl --help
```

Requires Rust 1.85+ (workspace edition 2021, recent tokio).

## Roadmap

See [ROADMAP.md](./ROADMAP.md) for what's planned beyond v1.0 (habitat web UI port, additional integrations, observability polish).

## License

Apache-2.0. The bundled SQL schema is the upstream `earendil-works/absurd` schema, also Apache-2.0.
