# absurd-sdk

Async client and worker SDK for [Absurd](https://github.com/nktkt/absurd) —
durable workflows on Postgres. Tasks decompose into idempotent steps whose
results are checkpointed in Postgres; workers pull tasks, execute them, and the
SDK handles retries, sleeps, and event-driven suspensions.

This crate is a Rust port of [earendil-works/absurd](https://github.com/earendil-works/absurd).

## Installation

```sh
cargo add absurd-sdk
```

## Example

```rust,no_run
use absurd_sdk::macros::task;
use absurd_sdk::{
    AwaitTaskResultOptions, Client, Result, SpawnOptions, TaskResultState,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Params { name: String }

#[derive(Serialize, Deserialize)]
struct Output { greeting: String }

#[task(name = "greet")]
async fn greet(p: Params) -> Result<Output> {
    Ok(Output { greeting: format!("hello {}!", p.name) })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = Client::connect().await?;
    app.register_task(greet_task()).await?;

    let worker = app.clone();
    tokio::spawn(async move { let _ = worker.run_worker(Default::default()).await; });

    let spawned = app
        .spawn_typed(&greet_task(), Params { name: "world".into() }, SpawnOptions::default())
        .await?;
    let snapshot = app
        .await_task_result(
            "default",
            &spawned.task_id,
            AwaitTaskResultOptions { timeout: Some(Duration::from_secs(10)), ..Default::default() },
        )
        .await?;
    assert_eq!(snapshot.state, TaskResultState::Completed);
    let out: Output = snapshot.decode_result()?.unwrap();
    println!("{}", out.greeting);
    Ok(())
}
```

See [`examples/order_fulfillment.rs`](examples/order_fulfillment.rs) for a
multi-step workflow using `step`, `await_event`, and `emit_event`.

## Features

- Durable steps with idempotent checkpoints in Postgres
- `await_event` for event-driven suspensions across worker restarts
- Durable `sleep_for` / `sleep_until` that survive crashes
- Hooks (`BeforeSpawnHook`, `WrapTaskExecutionHook`) for cross-cutting concerns
- Lease watchdog with heartbeats to reclaim tasks from dead workers
- Graceful shutdown via `ShutdownHandle`
- TLS support for Postgres connections (rustls or native-tls)

## Feature flags

- `macros` (default) — enables the `#[task]` attribute macro
- `rustls` — TLS via `tokio-postgres-rustls`
- `native-tls` — TLS via `postgres-native-tls`

## Schema

The Postgres schema is bundled into the crate at compile time
(`BUNDLED_SCHEMA_SQL`) and is normally applied to your database with the
`absurdctl init` CLI before workers connect. The same SQL string is exposed
programmatically for callers that need to install or inspect the schema from
Rust.

## Links

- Repository: <https://github.com/nktkt/absurd>

## License

Licensed under the Apache License, Version 2.0.
