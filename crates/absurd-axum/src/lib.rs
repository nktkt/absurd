//! Axum integration helpers for `absurd-sdk`.
//!
//! Most apps want to share an [`absurd_sdk::Client`] with their Axum
//! handlers. The crate provides:
//!
//! - [`AbsurdState`]: a `FromRef`-friendly wrapper for a shared [`Client`].
//! - [`AbsurdLayer`]: helpers to plug a Client into Axum's router state.
//! - [`spawn_response`]: a small helper that converts a [`SpawnResult`] into
//!   an Axum JSON response.
//!
//! ```ignore
//! use absurd_axum::AbsurdState;
//! use absurd_sdk::Client;
//! use axum::{routing::post, Router, extract::State, Json};
//! use serde_json::{json, Value};
//!
//! async fn enqueue(State(state): State<AbsurdState>, Json(body): Json<Value>) -> Json<Value> {
//!     let result = state.client.spawn("my-task", body, Default::default()).await.unwrap();
//!     Json(json!({ "task_id": result.task_id }))
//! }
//!
//! let client = Client::connect().await.unwrap();
//! let app = Router::new()
//!     .route("/enqueue", post(enqueue))
//!     .with_state(AbsurdState { client });
//! ```

use absurd_sdk::{Client, SpawnResult};
use axum::Json;
use serde_json::json;

/// Wrapper used as Axum state. Holds a cloneable Absurd client.
#[derive(Clone)]
pub struct AbsurdState {
    pub client: Client,
}

impl AbsurdState {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl From<Client> for AbsurdState {
    fn from(client: Client) -> Self {
        Self { client }
    }
}

/// Convert a [`SpawnResult`] to a JSON response in a stable shape.
pub fn spawn_response(result: SpawnResult) -> Json<serde_json::Value> {
    Json(json!({
        "task_id": result.task_id,
        "run_id": result.run_id,
        "attempt": result.attempt,
        "created": result.created,
    }))
}
