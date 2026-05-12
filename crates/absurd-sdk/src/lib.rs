//! Rust SDK for [Absurd](https://github.com/earendil-works/absurd) — durable
//! workflows on Postgres.
//!
//! Tasks decompose into idempotent steps whose results are checkpointed in
//! Postgres. Workers pull tasks, execute them, and the SDK takes care of
//! retries, sleeps, and event-driven suspensions.

#![allow(
    clippy::type_complexity,
    clippy::redundant_guards,
    clippy::unnecessary_cast,
    clippy::should_implement_trait,
    clippy::derivable_impls
)]

mod client;
mod context;
mod error;
mod hooks;
pub mod migrations;
mod options;
mod task;
mod tls;
mod util;
mod worker;

pub use client::{Client, ClientBuilder};
pub use context::{
    await_event, await_event_with, emit_event, heartbeat, sleep_for, sleep_until, step,
    TaskContext, CURRENT_TASK,
};
pub use error::{AbsurdError, Result, TaskStateError};
pub use hooks::{BeforeSpawnHook, Hooks, WrapTaskExecutionHook};
pub use options::{
    AwaitEventOptions, AwaitTaskResultOptions, CancellationPolicy, CleanupReport,
    CreateQueueOptions, DetachCandidate, QueueDetachMode, QueuePolicy, QueuePolicyOptions,
    QueueStorageMode, RetryStrategy, RetryStrategyKind, RetryTaskOptions, ShutdownHandle,
    SpawnOptions, SpawnResult, TaskResultSnapshot, TaskResultState, WorkBatchOptions,
    WorkerOptions,
};
pub use task::{task, FnHandler, RegisteredTask, TaskDefinition, TaskHandler, TypedHandler};

/// `#[task]` attribute macro for declarative task definitions.
///
/// Re-exported under [`macros`] so users can `use absurd_sdk::macros::task;`
/// without a naming conflict with the [`task`] function. Available when the
/// `macros` feature is enabled (on by default).
#[cfg(feature = "macros")]
pub mod macros {
    pub use absurd_macros::task;
}

/// SQL schema bundled into the SDK so callers can apply it without a separate
/// asset.
pub const BUNDLED_SCHEMA_SQL: &str = include_str!("../sql/absurd.sql");

/// Maximum allowed UTF-8 byte length for queue names (so generated identifiers
/// stay within PostgreSQL's 63-byte limit).
pub const MAX_QUEUE_NAME_LENGTH: usize = 57;

/// Default queue name used by the SDK when none is specified.
pub const DEFAULT_QUEUE_NAME: &str = "default";
