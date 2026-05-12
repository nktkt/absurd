//! Port of the README order-fulfillment example.
//!
//! Run a Postgres with the bundled schema applied (`absurdctl init -d <db>`)
//! and then `cargo run --example order_fulfillment`.

use absurd_sdk::{
    await_event, step, AwaitTaskResultOptions, Client, CreateQueueOptions, QueueStorageMode,
    SpawnOptions, TaskResultState,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct OrderParams {
    order_id: String,
    amount: i64,
    items: Vec<String>,
    email: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Payment {
    payment_id: String,
    amount: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let app = Client::builder()
        .database_url(
            std::env::var("ABSURD_DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://localhost/absurd".into()),
        )
        .build()
        .await?;

    // Make sure the queue exists. Safe to call repeatedly.
    let _ = app
        .create_queue(
            "default",
            CreateQueueOptions {
                storage_mode: QueueStorageMode::Unpartitioned,
                ..Default::default()
            },
        )
        .await;

    app.register_fn("order-fulfillment", |params: Value| async move {
        let params: OrderParams = serde_json::from_value(params)?;

        let payment = step("process-payment", || async {
            Ok(Payment {
                payment_id: format!("pay-{}", params.order_id),
                amount: params.amount,
            })
        })
        .await?;

        let _inventory = step("reserve-inventory", || async {
            Ok(json!({ "reserved_items": params.items }))
        })
        .await?;

        let shipment: Value = await_event(&format!("shipment.packed:{}", params.order_id)).await?;
        let tracking = shipment
            .get("tracking_number")
            .cloned()
            .unwrap_or(Value::Null);

        step("send-notification", || async {
            Ok(json!({
                "sent_to": params.email,
                "tracking_number": tracking,
            }))
        })
        .await?;

        Ok(json!({
            "order_id": params.order_id,
            "payment": payment,
            "tracking_number": tracking,
        }))
    })
    .await?;

    // Start the worker in the background.
    let worker = app.clone();
    let handle = tokio::spawn(async move {
        let _ = worker
            .run_worker(absurd_sdk::WorkerOptions {
                concurrency: 4,
                ..Default::default()
            })
            .await;
    });

    let spawn = app
        .spawn(
            "order-fulfillment",
            json!({
                "order_id": "42",
                "amount": 9999,
                "items": ["widget-1", "gadget-2"],
                "email": "customer@example.com",
            }),
            SpawnOptions::default(),
        )
        .await?;
    println!("spawned task: {}", spawn.task_id);

    app.emit_event(
        "default",
        "shipment.packed:42",
        json!({"tracking_number": "TRACK123"}),
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

    println!("result: {:?}", snapshot);
    assert_eq!(snapshot.state, TaskResultState::Completed);

    handle.abort();
    Ok(())
}
