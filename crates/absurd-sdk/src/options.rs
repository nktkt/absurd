use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

/// Retry strategy honored by Absurd's spawn paths.
#[derive(Debug, Clone, Default)]
pub struct RetryStrategy {
    pub kind: String,
    pub base_seconds: Option<f64>,
    pub factor: Option<f64>,
    pub max_seconds: Option<f64>,
}

/// Optional cancellation envelope; values in seconds.
#[derive(Debug, Clone, Default)]
pub struct CancellationPolicy {
    pub max_duration: Option<i64>,
    pub max_delay: Option<i64>,
}

/// Options accepted by [`Client::spawn`](crate::Client::spawn).
#[derive(Debug, Clone, Default)]
pub struct SpawnOptions {
    pub queue_name: Option<String>,
    pub max_attempts: Option<i32>,
    pub retry_strategy: Option<RetryStrategy>,
    pub headers: Option<BTreeMap<String, Value>>,
    pub cancellation: Option<CancellationPolicy>,
    pub idempotency_key: Option<String>,
}

/// Outcome of a spawn (matches the schema's `spawn_task` return).
#[derive(Debug, Clone)]
pub struct SpawnResult {
    pub task_id: String,
    pub run_id: String,
    pub attempt: i32,
    pub created: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueStorageMode {
    Unpartitioned,
    Partitioned,
}

impl QueueStorageMode {
    pub fn as_str(self) -> &'static str {
        match self {
            QueueStorageMode::Unpartitioned => "unpartitioned",
            QueueStorageMode::Partitioned => "partitioned",
        }
    }
}

impl Default for QueueStorageMode {
    fn default() -> Self {
        QueueStorageMode::Unpartitioned
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueDetachMode {
    None,
    Empty,
}

impl QueueDetachMode {
    pub fn as_str(self) -> &'static str {
        match self {
            QueueDetachMode::None => "none",
            QueueDetachMode::Empty => "empty",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueuePolicyOptions {
    pub partition_lookahead: Option<String>,
    pub partition_lookback: Option<String>,
    pub cleanup_ttl: Option<String>,
    pub cleanup_limit: Option<i32>,
    pub detach_mode: Option<QueueDetachMode>,
    pub detach_min_age: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CreateQueueOptions {
    pub storage_mode: QueueStorageMode,
    pub policy: QueuePolicyOptions,
}

#[derive(Debug, Clone)]
pub struct QueuePolicy {
    pub queue_name: String,
    pub storage_mode: String,
    pub partition_lookahead: String,
    pub partition_lookback: String,
    pub cleanup_ttl: String,
    pub cleanup_limit: i32,
    pub detach_mode: String,
    pub detach_min_age: String,
}

#[derive(Debug, Clone, Default)]
pub struct RetryTaskOptions {
    pub max_attempts: Option<i32>,
    pub spawn_new: bool,
}

#[derive(Debug, Clone, Default)]
pub struct AwaitEventOptions {
    pub step_name: Option<String>,
    /// `None` = wait indefinitely. `Some(Duration::ZERO)` mirrors the Go SDK's
    /// "non-blocking; raise immediately if not present" behavior. Otherwise a
    /// positive duration is the timeout.
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, Default)]
pub struct AwaitTaskResultOptions {
    pub step_name: Option<String>,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct WorkBatchOptions {
    pub worker_id: String,
    pub claim_timeout: Duration,
    pub batch_size: i32,
}

impl Default for WorkBatchOptions {
    fn default() -> Self {
        WorkBatchOptions {
            worker_id: "worker".to_string(),
            claim_timeout: Duration::from_secs(120),
            batch_size: 1,
        }
    }
}

#[derive(Clone)]
pub struct WorkerOptions {
    pub worker_id: Option<String>,
    pub claim_timeout: Duration,
    pub batch_size: Option<i32>,
    pub concurrency: i32,
    pub poll_interval: Duration,
    pub fatal_on_lease_timeout: bool,
    pub on_error: Option<std::sync::Arc<dyn Fn(&crate::AbsurdError) + Send + Sync>>,
}

impl std::fmt::Debug for WorkerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerOptions")
            .field("worker_id", &self.worker_id)
            .field("claim_timeout", &self.claim_timeout)
            .field("batch_size", &self.batch_size)
            .field("concurrency", &self.concurrency)
            .field("poll_interval", &self.poll_interval)
            .field("fatal_on_lease_timeout", &self.fatal_on_lease_timeout)
            .finish()
    }
}

impl Default for WorkerOptions {
    fn default() -> Self {
        WorkerOptions {
            worker_id: None,
            claim_timeout: Duration::from_secs(120),
            batch_size: None,
            concurrency: 1,
            poll_interval: Duration::from_millis(250),
            fatal_on_lease_timeout: true,
            on_error: None,
        }
    }
}

/// Result state strings that match the schema enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskResultState {
    Pending,
    Running,
    Sleeping,
    Completed,
    Failed,
    Cancelled,
}

impl TaskResultState {
    pub fn from_str(value: &str) -> Self {
        match value {
            "pending" => TaskResultState::Pending,
            "running" => TaskResultState::Running,
            "sleeping" => TaskResultState::Sleeping,
            "completed" => TaskResultState::Completed,
            "failed" => TaskResultState::Failed,
            "cancelled" => TaskResultState::Cancelled,
            _ => TaskResultState::Pending,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskResultState::Completed | TaskResultState::Failed | TaskResultState::Cancelled
        )
    }
}

/// Raw task result snapshot returned by `absurd.get_task_result`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResultSnapshot {
    pub state: TaskResultState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<Value>,
}

impl TaskResultSnapshot {
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    pub fn decode_result<T: for<'de> Deserialize<'de>>(&self) -> crate::Result<Option<T>> {
        match &self.result {
            Some(v) => Ok(Some(serde_json::from_value(v.clone())?)),
            None => Ok(None),
        }
    }
}
