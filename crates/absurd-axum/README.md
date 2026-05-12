# absurd-axum

Small [Axum](https://github.com/tokio-rs/axum) integration helpers for [`absurd-sdk`](https://github.com/nktkt/absurd) — durable workflows on Postgres.

## Installation

```sh
cargo add absurd-axum absurd-sdk axum
```

## Example

Mount a router with an `/enqueue` endpoint that spawns a task from a JSON body, using the shared `Client` as Axum state.

```rust,no_run
use absurd_axum::{spawn_response, AbsurdState};
use absurd_sdk::{Client, SpawnOptions};
use axum::{extract::State, routing::post, Json, Router};
use serde_json::Value;

async fn send_email(_ctx: absurd_sdk::Context, input: Value) -> anyhow::Result<Value> {
    // ... do the work ...
    Ok(input)
}

async fn enqueue(
    State(state): State<AbsurdState>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let result = state
        .client
        .spawn("send-email", body, SpawnOptions::default())
        .await
        .unwrap();
    spawn_response(result)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::connect(&std::env::var("DATABASE_URL")?).await?;
    client.register_task("send-email", send_email).await?;

    let worker = client.clone();
    tokio::spawn(async move { worker.run_worker().await });

    let app = Router::new()
        .route("/enqueue", post(enqueue))
        .with_state(AbsurdState::new(client));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
```

## What's in the crate

- **`AbsurdState`** — a `Clone` wrapper around `absurd_sdk::Client` suitable for use as Axum router state. Construct with `AbsurdState::new(client)` or `client.into()`.
- **`spawn_response(SpawnResult)`** — converts a spawn outcome into a JSON 200 response with `task_id`, `run_id`, `attempt`, and `created` fields.

## Scope

This crate is intentionally tiny. It only covers the boilerplate that every Axum + `absurd-sdk` app needs. Deeper integration — typed extractors for spawn parameters, custom error responses, auth middleware, route generation — belongs in user code, where it can be tailored to the app's domain.

## License

Licensed under the Apache License, Version 2.0. See <https://github.com/nktkt/absurd> for the full project.
