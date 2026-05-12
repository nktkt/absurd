//! Demonstrates the `#[task]` proc-macro and typed registration.
//!
//! Run with the bundled schema already applied (`absurdctl init`):
//!
//! ```sh
//! ABSURD_DATABASE_URL="postgresql:///absurd?host=/tmp" \
//!   cargo run -p absurd-sdk --example typed_task
//! ```

use absurd_sdk::macros::task;
use absurd_sdk::{
    AwaitTaskResultOptions, Client, CreateQueueOptions, QueueStorageMode, Result, SpawnOptions,
    TaskResultState,
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
    let greeting = std::iter::repeat_n(format!("hello {}!", params.name), params.times as usize)
        .collect::<Vec<_>>()
        .join(" ");
    Ok(Output { greeting })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let app = Client::connect().await?;
    let _ = app
        .create_queue(
            "default",
            CreateQueueOptions {
                storage_mode: QueueStorageMode::Unpartitioned,
                ..Default::default()
            },
        )
        .await;
    app.register_task(greet_task()).await?;

    let worker = app.clone();
    let handle = tokio::spawn(async move {
        let _ = worker.run_worker(Default::default()).await;
    });

    let result = app
        .spawn_typed(
            &greet_task(),
            Params {
                name: "world".into(),
                times: 3,
            },
            SpawnOptions::default(),
        )
        .await?;
    println!("spawned: {}", result.task_id);

    let snapshot = app
        .await_task_result(
            "default",
            &result.task_id,
            AwaitTaskResultOptions {
                timeout: Some(Duration::from_secs(10)),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(snapshot.state, TaskResultState::Completed);
    let parsed: Output = snapshot.decode_result()?.unwrap();
    println!("{}", parsed.greeting);

    handle.abort();
    Ok(())
}
