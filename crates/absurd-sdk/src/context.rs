use crate::client::{await_task_result_with_backoff, boxed_before_sleep, ClaimedTask, Client};
use crate::error::{map_state_error, AbsurdError, Result};
use crate::options::{AwaitEventOptions, AwaitTaskResultOptions, TaskResultSnapshot};
use crate::util::{duration_seconds, duration_seconds_or};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

tokio::task_local! {
    /// Task-local handle to the currently executing task. Populated by the
    /// worker before invoking a handler. Use [`current`] / [`with_current`] to
    /// access from inside a handler.
    pub static CURRENT_TASK: TaskContext;
}

/// Per-run context exposed to a task handler. Implements `Clone` so handlers
/// can move it across spawned tasks freely; cheap (all state is shared via
/// `Arc`).
#[derive(Clone)]
pub struct TaskContext {
    inner: Arc<TaskContextInner>,
}

struct TaskContextInner {
    client: Client,
    queue_name: String,
    task_id: String,
    run_id: String,
    task_name: String,
    attempt: i32,
    headers: Value,
    claim_timeout: Duration,
    wake_event: Mutex<Option<String>>,
    event_payload: Mutex<Option<Value>>,
    state: Mutex<TaskContextState>,
    lease_observer: Option<Arc<dyn LeaseObserver>>,
}

/// Observer notified whenever the SDK extends the claim lease (e.g. via
/// heartbeats or checkpoint writes). The worker uses this to reset its lease
/// watchdog.
pub trait LeaseObserver: Send + Sync + 'static {
    fn observe(&self, lease: Duration);
}

struct TaskContextState {
    checkpoint_cache: HashMap<String, Value>,
    step_name_counter: HashMap<String, usize>,
}

const MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_CLAIM_TIMEOUT: Duration = Duration::from_secs(120);

impl TaskContext {
    pub(crate) async fn new(
        client: Client,
        queue_name: String,
        task: &ClaimedTask,
        claim_timeout: Duration,
        lease_observer: Option<Arc<dyn LeaseObserver>>,
    ) -> Result<Self> {
        let headers = match &task.headers {
            Some(Value::Object(_)) => task.headers.clone().unwrap(),
            Some(Value::Null) | None => Value::Object(Default::default()),
            Some(_) => {
                return Err(AbsurdError::InvalidTaskHeaders(
                    "headers payload must be a JSON object".into(),
                ));
            }
        };

        let effective_lease = if claim_timeout.is_zero() {
            DEFAULT_CLAIM_TIMEOUT
        } else {
            claim_timeout
        };

        let ctx = TaskContext {
            inner: Arc::new(TaskContextInner {
                client: client.clone(),
                queue_name: queue_name.clone(),
                task_id: task.task_id.clone(),
                run_id: task.run_id.clone(),
                task_name: task.task_name.clone(),
                attempt: task.attempt,
                headers,
                claim_timeout: effective_lease,
                wake_event: Mutex::new(task.wake_event.clone()),
                event_payload: Mutex::new(task.event_payload.clone()),
                state: Mutex::new(TaskContextState {
                    checkpoint_cache: HashMap::new(),
                    step_name_counter: HashMap::new(),
                }),
                lease_observer,
            }),
        };

        // Prime the checkpoint cache with everything Postgres already has for
        // this task. That avoids a per-step round trip when replaying.
        let pool_client = client.pool.get().await?;
        let rows = pool_client
            .query(
                "SELECT checkpoint_name, state \
                   FROM absurd.get_task_checkpoint_states($1, $2::text::uuid, $3::text::uuid)",
                &[&queue_name, &task.task_id, &task.run_id],
            )
            .await?;
        let mut state = ctx.inner.state.lock().await;
        for row in rows {
            let name: String = row.get(0);
            let value: Option<Value> = row.try_get(1).ok().flatten();
            state
                .checkpoint_cache
                .insert(name, value.unwrap_or(Value::Null));
        }
        drop(state);
        Ok(ctx)
    }

    pub fn task_id(&self) -> &str {
        &self.inner.task_id
    }

    pub fn run_id(&self) -> &str {
        &self.inner.run_id
    }

    pub fn task_name(&self) -> &str {
        &self.inner.task_name
    }

    pub fn queue_name(&self) -> &str {
        &self.inner.queue_name
    }

    pub fn attempt(&self) -> i32 {
        self.inner.attempt
    }

    pub fn headers(&self) -> &Value {
        &self.inner.headers
    }

    pub fn client(&self) -> &Client {
        &self.inner.client
    }

    async fn next_checkpoint_name(&self, name: &str) -> String {
        let mut state = self.inner.state.lock().await;
        let count = state.step_name_counter.entry(name.to_string()).or_insert(0);
        *count += 1;
        if *count == 1 {
            name.to_string()
        } else {
            format!("{}#{}", name, count)
        }
    }

    async fn lookup_checkpoint(&self, checkpoint_name: &str) -> Result<Option<Value>> {
        {
            let state = self.inner.state.lock().await;
            if let Some(v) = state.checkpoint_cache.get(checkpoint_name) {
                return Ok(Some(v.clone()));
            }
        }
        let pool_client = self.inner.client.pool.get().await?;
        let row = pool_client
            .query_opt(
                "SELECT state FROM absurd.get_task_checkpoint_state($1, $2::text::uuid, $3)",
                &[
                    &self.inner.queue_name,
                    &self.inner.task_id,
                    &checkpoint_name,
                ],
            )
            .await?;
        match row {
            None => Ok(None),
            Some(r) => {
                let value: Option<Value> = r.try_get(0).ok().flatten();
                let normalized = value.unwrap_or(Value::Null);
                let mut state = self.inner.state.lock().await;
                state
                    .checkpoint_cache
                    .insert(checkpoint_name.to_string(), normalized.clone());
                Ok(Some(normalized))
            }
        }
    }

    async fn persist_checkpoint(&self, checkpoint_name: &str, value: Value) -> Result<()> {
        let pool_client = self.inner.client.pool.get().await?;
        pool_client
            .execute(
                "SELECT absurd.set_task_checkpoint_state($1, $2::text::uuid, $3, $4, $5::text::uuid, $6)",
                &[
                    &self.inner.queue_name,
                    &self.inner.task_id,
                    &checkpoint_name,
                    &value,
                    &self.inner.run_id,
                    &(duration_seconds_or(self.inner.claim_timeout, DEFAULT_CLAIM_TIMEOUT)
                        as i32),
                ],
            )
            .await
            .map_err(map_state_error)?;
        self.notify_lease_extended(self.inner.claim_timeout);
        let mut state = self.inner.state.lock().await;
        state
            .checkpoint_cache
            .insert(checkpoint_name.to_string(), value);
        Ok(())
    }

    fn notify_lease_extended(&self, lease: Duration) {
        if let Some(obs) = &self.inner.lease_observer {
            obs.observe(lease);
        }
    }

    async fn schedule_run(&self, wake_at: DateTime<Utc>) -> Result<()> {
        let pool_client = self.inner.client.pool.get().await?;
        pool_client
            .execute(
                "SELECT absurd.schedule_run($1, $2::text::uuid, $3)",
                &[&self.inner.queue_name, &self.inner.run_id, &wake_at],
            )
            .await?;
        Ok(())
    }

    /// Extend the current run's claim lease. Pass `Duration::ZERO` to use the
    /// initial claim timeout.
    pub async fn heartbeat(&self, d: Duration) -> Result<()> {
        let lease = if d.is_zero() {
            self.inner.claim_timeout
        } else {
            d
        };
        let pool_client = self.inner.client.pool.get().await?;
        pool_client
            .execute(
                "SELECT absurd.extend_claim($1, $2::text::uuid, $3)",
                &[
                    &self.inner.queue_name,
                    &self.inner.run_id,
                    &(duration_seconds(lease) as i32),
                ],
            )
            .await
            .map_err(map_state_error)?;
        self.notify_lease_extended(lease);
        Ok(())
    }

    pub async fn emit_event(&self, event_name: &str, payload: Value) -> Result<()> {
        if event_name.is_empty() {
            return Err(AbsurdError::other("event name must be a non-empty string"));
        }
        let pool_client = self.inner.client.pool.get().await?;
        pool_client
            .execute(
                "SELECT absurd.emit_event($1, $2, $3)",
                &[&self.inner.queue_name, &event_name, &payload],
            )
            .await?;
        Ok(())
    }

    pub async fn await_task_result(
        &self,
        queue_name: &str,
        task_id: &str,
        options: AwaitTaskResultOptions,
    ) -> Result<TaskResultSnapshot> {
        let validated = crate::util::validate_queue_name(queue_name)?;
        if validated == self.inner.queue_name {
            return Err(AbsurdError::other(
                "TaskContext.await_task_result cannot wait on tasks in the same queue (deadlocks workers). Spawn the child in a different queue.",
            ));
        }
        let step_name = options
            .step_name
            .clone()
            .unwrap_or_else(|| format!("$awaitTaskResult:{}", task_id));
        let checkpoint = self.next_checkpoint_name(&step_name).await;
        if let Some(raw) = self.lookup_checkpoint(&checkpoint).await? {
            let snapshot: TaskResultSnapshot = serde_json::from_value(raw)?;
            return Ok(snapshot);
        }
        let heartbeat_interval =
            std::cmp::max(self.inner.claim_timeout / 2, MIN_HEARTBEAT_INTERVAL);
        let this = self.clone();
        let next_heartbeat = std::sync::Arc::new(std::sync::Mutex::new(
            tokio::time::Instant::now() + heartbeat_interval,
        ));
        let before_sleep = boxed_before_sleep(move || {
            let this = this.clone();
            let next_heartbeat = next_heartbeat.clone();
            async move {
                let now = tokio::time::Instant::now();
                let next = *next_heartbeat.lock().unwrap();
                if now < next {
                    return Ok(());
                }
                *next_heartbeat.lock().unwrap() = now + heartbeat_interval;
                this.heartbeat(Duration::ZERO).await
            }
        });
        let snapshot = await_task_result_with_backoff(
            &self.inner.client,
            &validated,
            task_id,
            options.timeout,
            Some(before_sleep),
        )
        .await?;
        self.persist_checkpoint(&checkpoint, serde_json::to_value(&snapshot)?)
            .await?;
        Ok(snapshot)
    }
}

/// Run an idempotent step. The first time the task reaches this call site the
/// closure executes and its result is checkpointed; on replay the cached value
/// is returned without running the closure again.
pub async fn step<T, F, Fut>(name: &str, f: F) -> Result<T>
where
    T: Serialize + DeserializeOwned,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let ctx = current()?;
    let checkpoint_name = ctx.next_checkpoint_name(name).await;
    if let Some(raw) = ctx.lookup_checkpoint(&checkpoint_name).await? {
        let value: T = serde_json::from_value(raw)?;
        return Ok(value);
    }
    let value = f().await?;
    let raw = serde_json::to_value(&value)?;
    ctx.persist_checkpoint(&checkpoint_name, raw).await?;
    Ok(value)
}

/// Sleep for at least `d`. Suspends the task durably — if the worker dies
/// while waiting, the task resumes from this point on the next claim.
pub async fn sleep_for(step_name: &str, d: Duration) -> Result<()> {
    let wake_at =
        Utc::now() + chrono::Duration::from_std(d).unwrap_or_else(|_| chrono::Duration::seconds(0));
    sleep_until(step_name, wake_at).await
}

pub async fn sleep_until(step_name: &str, wake_at: DateTime<Utc>) -> Result<()> {
    let ctx = current()?;
    let checkpoint = ctx.next_checkpoint_name(step_name).await;
    let mut actual = wake_at;
    if let Some(raw) = ctx.lookup_checkpoint(&checkpoint).await? {
        if let Some(s) = raw.as_str() {
            if !s.is_empty() {
                actual = DateTime::parse_from_rfc3339(s)
                    .map_err(|e| AbsurdError::other(format!("invalid wake_at checkpoint: {e}")))?
                    .with_timezone(&Utc);
            }
        }
    } else {
        ctx.persist_checkpoint(&checkpoint, Value::String(actual.to_rfc3339()))
            .await?;
    }
    if Utc::now() < actual {
        ctx.schedule_run(actual).await?;
        return Err(AbsurdError::Suspended);
    }
    Ok(())
}

pub async fn await_event<T: DeserializeOwned>(event_name: &str) -> Result<T> {
    await_event_with(event_name, AwaitEventOptions::default()).await
}

pub async fn await_event_with<T: DeserializeOwned>(
    event_name: &str,
    options: AwaitEventOptions,
) -> Result<T> {
    let ctx = current()?;
    let step_name = options
        .step_name
        .clone()
        .unwrap_or_else(|| format!("$awaitEvent:{}", event_name));
    let checkpoint = ctx.next_checkpoint_name(&step_name).await;
    if let Some(raw) = ctx.lookup_checkpoint(&checkpoint).await? {
        let value: T = serde_json::from_value(raw)?;
        return Ok(value);
    }

    // Encode the timeout argument matching the SQL surface: NULL = wait
    // indefinitely; 0 = non-blocking; positive = bounded wait in seconds.
    let timeout_arg: Option<i32> = match options.timeout {
        None => None,
        Some(d) if d.is_zero() => Some(0),
        Some(d) => Some(duration_seconds(d)),
    };

    let pool_client = ctx.inner.client.pool.get().await?;
    let row = pool_client
        .query_one(
            "SELECT should_suspend, payload \
               FROM absurd.await_event($1, $2::text::uuid, $3::text::uuid, $4, $5, $6)",
            &[
                &ctx.inner.queue_name,
                &ctx.inner.task_id,
                &ctx.inner.run_id,
                &checkpoint,
                &event_name,
                &timeout_arg,
            ],
        )
        .await
        .map_err(map_state_error)?;
    let should_suspend: bool = row.get(0);
    let payload: Option<Value> = row.try_get(1).ok().flatten();
    drop(pool_client);
    if should_suspend {
        return Err(AbsurdError::Suspended);
    }
    let Some(payload) = payload else {
        // Clear stale wake state and surface a timeout.
        *ctx.inner.wake_event.lock().await = None;
        *ctx.inner.event_payload.lock().await = None;
        return Err(AbsurdError::Timeout(format!(
            "timed out waiting for event {:?}",
            event_name
        )));
    };
    {
        let mut state = ctx.inner.state.lock().await;
        state.checkpoint_cache.insert(checkpoint, payload.clone());
    }
    *ctx.inner.wake_event.lock().await = None;
    *ctx.inner.event_payload.lock().await = None;
    let value: T = serde_json::from_value(payload)?;
    Ok(value)
}

/// Heartbeat the current task. Equivalent to [`TaskContext::heartbeat`].
pub async fn heartbeat(d: Duration) -> Result<()> {
    current()?.heartbeat(d).await
}

/// Emit an event on the current task's queue.
pub async fn emit_event(event_name: &str, payload: Value) -> Result<()> {
    current()?.emit_event(event_name, payload).await
}

fn current() -> Result<TaskContext> {
    CURRENT_TASK
        .try_with(|ctx| ctx.clone())
        .map_err(|_| AbsurdError::NoTaskContext)
}
