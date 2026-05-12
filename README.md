# Absurd — Rust port

A Rust rewrite of [`earendil-works/absurd`](https://github.com/earendil-works/absurd) — the simplest durable execution workflow system, built entirely on Postgres.

Tasks decompose into idempotent steps whose results are checkpointed in Postgres. Workers pull tasks, run them, and the SDK handles retries, durable sleeps, and event-driven suspensions for you. No Redis, no broker, no coordinator — just your existing Postgres.

## What's in the box

| Crate         | Purpose                                                              |
| ------------- | -------------------------------------------------------------------- |
| `absurd-sdk`  | Async client + worker SDK (`step`, `await_event`, `sleep_for`, `heartbeat`, `spawn`, `await_task_result`, …) |
| `absurdctl`   | CLI to init/migrate the schema and manage queues / tasks             |

The bundled SQL (`crates/absurd-sdk/sql/absurd.sql`) is the canonical schema from upstream and is embedded into the SDK at compile time. `absurdctl init` and `absurdctl migrate` apply it to the target database.

## Features

- Durable steps with automatic checkpoint replay on failure
- `await_event` with cached, race-free event delivery
- Durable `sleep_for` / `sleep_until` that survive process restarts
- Single-line worker (`client.run_worker(WorkerOptions::default())`)
- Pluggable concurrency, batch size, claim timeout, heartbeats
- Zero external services — only Postgres

## Quickstart

```bash
# 1. Create a database and apply the schema
createdb absurd
cargo run -p absurdctl -- -d absurd init
cargo run -p absurdctl -- -d absurd create-queue default

# 2. Run the order-fulfillment example
ABSURD_DATABASE_URL="postgresql:///absurd?host=/tmp" \
  cargo run -p absurd-sdk --example order_fulfillment
```

## SDK shape

```rust
use absurd_sdk::{await_event, step, AwaitTaskResultOptions, Client, SpawnOptions};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = Client::connect().await?;

    app.register_fn("order-fulfillment", |params: Value| async move {
        // Steps are checkpointed — they don't re-run on retry.
        let payment = step("process-payment", || async {
            Ok(json!({ "amount": params["amount"].clone() }))
        }).await?;

        // Suspends the task until the event arrives (durably).
        let shipment: Value = await_event(
            &format!("shipment.packed:{}", params["order_id"])
        ).await?;

        Ok(json!({ "payment": payment, "shipment": shipment }))
    }).await?;

    // Workers pull tasks from Postgres as they have capacity.
    let worker = app.clone();
    tokio::spawn(async move { worker.run_worker(Default::default()).await });

    let spawn = app.spawn(
        "order-fulfillment",
        json!({ "order_id": "42", "amount": 9999 }),
        SpawnOptions::default(),
    ).await?;

    app.emit_event("default", "shipment.packed:42",
        json!({ "tracking_number": "TRACK123" })).await?;

    let result = app.await_task_result("default", &spawn.task_id,
        AwaitTaskResultOptions { timeout: Some(Duration::from_secs(10)), ..Default::default() }
    ).await?;
    println!("{:?}", result);
    Ok(())
}
```

## CLI

```text
absurdctl init                              # apply the bundled schema
absurdctl schema-version                    # read the recorded version
absurdctl migrate                           # re-apply (idempotent)
absurdctl create-queue <name>               # create a queue
absurdctl list-queues
absurdctl drop-queue <name>
absurdctl spawn-task <queue> <task-name> [--params '<json>'] [--header k=v ...]
absurdctl retry-task <queue> <task-id> [--spawn-new]
absurdctl cancel-task <queue> <task-id>
absurdctl print-schema                      # write the bundled SQL to stdout
```

All commands accept `-d <dbname>` (libpq style) or `--database-url <url>`. They also honor `ABSURD_DATABASE_URL` and `PGDATABASE`.

## Differences vs. the upstream Python `absurdctl`

- Less-used commands not yet ported: `cleanup`, `cron`, `detach-candidate`, queue-policy management (the SDK can call `set_queue_policy` programmatically).
- `migrate` re-applies the idempotent bundled schema; stepwise version-to-version migrations are not implemented.
- TLS is disabled (`NoTls`) in the connection pool — connect over a trusted network or a Unix socket.

## Building

```bash
cargo build --release
./target/release/absurdctl --help
```

Requires Rust 1.85+ (uses edition 2021 + recent tokio).

## License

Apache-2.0. The bundled SQL schema is the upstream `earendil-works/absurd` schema, also Apache-2.0.
