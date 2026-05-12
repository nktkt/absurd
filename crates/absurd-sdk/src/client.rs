use crate::error::{map_state_error, AbsurdError, Result};
use crate::hooks::Hooks;
use crate::options::*;
use crate::task::{FnHandler, RegisteredTask, TaskDefinition, TaskHandler};
use crate::util::{duration_seconds, resolve_database_url, validate_queue_name};
use crate::{DEFAULT_QUEUE_NAME, MAX_QUEUE_NAME_LENGTH};
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Builder for [`Client`].
#[derive(Default)]
pub struct ClientBuilder {
    database_url: Option<String>,
    queue_name: Option<String>,
    default_max_attempts: Option<i32>,
    pool_size: Option<usize>,
    hooks: Hooks,
}

impl ClientBuilder {
    pub fn database_url(mut self, url: impl Into<String>) -> Self {
        self.database_url = Some(url.into());
        self
    }

    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = Some(name.into());
        self
    }

    pub fn default_max_attempts(mut self, n: i32) -> Self {
        self.default_max_attempts = Some(n);
        self
    }

    pub fn pool_size(mut self, n: usize) -> Self {
        self.pool_size = Some(n);
        self
    }

    pub fn hooks(mut self, hooks: Hooks) -> Self {
        self.hooks = hooks;
        self
    }

    pub async fn build(self) -> Result<Client> {
        let dsn = resolve_database_url(self.database_url.as_deref());
        let pg_config = tokio_postgres::Config::from_str(&dsn)
            .map_err(|e| AbsurdError::other(format!("invalid database URL: {e}")))?;
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = crate::tls::build_manager(pg_config, mgr_config)?;
        let mut builder = Pool::builder(mgr);
        if let Some(size) = self.pool_size {
            builder = builder.max_size(size);
        }
        let pool = builder.build()?;
        let queue_name =
            validate_queue_name(self.queue_name.as_deref().unwrap_or(DEFAULT_QUEUE_NAME))?;
        Ok(Client {
            pool,
            queue_name,
            default_max_attempts: self.default_max_attempts.unwrap_or(5),
            registry: Arc::new(RwLock::new(HashMap::new())),
            hooks: self.hooks,
        })
    }
}

/// Async Absurd client. Cloning is cheap — the underlying Postgres pool is
/// shared.
#[derive(Clone)]
pub struct Client {
    pub(crate) pool: Pool,
    pub(crate) queue_name: String,
    pub(crate) default_max_attempts: i32,
    pub(crate) registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
    pub(crate) hooks: Hooks,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Connect using `ABSURD_DATABASE_URL`/`PGDATABASE` defaults.
    pub async fn connect() -> Result<Self> {
        ClientBuilder::default().build().await
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Register a task handler under the client's default queue.
    pub async fn register<H>(&self, name: impl Into<String>, handler: H) -> Result<()>
    where
        H: TaskHandler,
    {
        self.register_with(name, handler, None::<&str>, None, None)
            .await
    }

    /// Register a task with explicit queue and default options.
    pub async fn register_with<H>(
        &self,
        name: impl Into<String>,
        handler: H,
        queue: Option<impl Into<String>>,
        default_max_attempts: Option<i32>,
        default_cancellation: Option<CancellationPolicy>,
    ) -> Result<()>
    where
        H: TaskHandler,
    {
        let name = name.into();
        if name.is_empty() {
            return Err(AbsurdError::other("task registration requires a name"));
        }
        let queue_name = match queue {
            Some(q) => validate_queue_name(&q.into())?,
            None => self.queue_name.clone(),
        };
        let handler_arc: Arc<dyn TaskHandler> = Arc::new(handler);
        let registered = RegisteredTask {
            name: name.clone(),
            queue_name,
            default_max_attempts,
            default_cancellation,
            handler: handler_arc,
        };
        self.registry.write().await.insert(name, registered);
        Ok(())
    }

    /// Register an untyped Fn closure that operates on `serde_json::Value`.
    pub async fn register_fn<F, Fut>(&self, name: impl Into<String>, f: F) -> Result<()>
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Value>> + Send + 'static,
    {
        self.register(name, FnHandler(f)).await
    }

    /// Register a strongly-typed task. Params and result types are pinned by
    /// the [`TaskDefinition`] and validated at compile time at every
    /// `spawn_task` call.
    pub async fn register_task<P, R>(&self, def: TaskDefinition<P, R>) -> Result<()>
    where
        P: Serialize + DeserializeOwned + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
    {
        if def.name.is_empty() {
            return Err(AbsurdError::other("task registration requires a name"));
        }
        let queue_name = match &def.queue_name {
            Some(q) => validate_queue_name(q)?,
            None => self.queue_name.clone(),
        };
        let registered = RegisteredTask {
            name: def.name.clone(),
            queue_name,
            default_max_attempts: def.default_max_attempts,
            default_cancellation: def.default_cancellation.clone(),
            handler: def.handler.clone(),
        };
        self.registry.write().await.insert(def.name, registered);
        Ok(())
    }

    /// Strongly-typed spawn. Serializes `params` and returns the same
    /// [`SpawnResult`] as [`spawn`](Self::spawn). Bring your own task name —
    /// or call `def.spawn(client, params, options)` once the definition is
    /// in scope.
    pub async fn spawn_typed<P, R>(
        &self,
        def: &TaskDefinition<P, R>,
        params: P,
        options: SpawnOptions,
    ) -> Result<SpawnResult>
    where
        P: Serialize + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
    {
        let value = serde_json::to_value(params)?;
        self.spawn(&def.name, value, options).await
    }

    pub(crate) async fn get_registered(&self, name: &str) -> Option<RegisteredTask> {
        self.registry.read().await.get(name).cloned()
    }

    pub async fn create_queue(&self, queue_name: &str, options: CreateQueueOptions) -> Result<()> {
        let validated = if queue_name.is_empty() {
            self.queue_name.clone()
        } else {
            validate_queue_name(queue_name)?
        };

        let client = self.pool.get().await?;
        match options.storage_mode {
            QueueStorageMode::Unpartitioned => {
                client
                    .execute("SELECT absurd.create_queue($1)", &[&validated])
                    .await?;
            }
            QueueStorageMode::Partitioned => {
                client
                    .execute(
                        "SELECT absurd.create_queue($1, $2)",
                        &[&validated, &options.storage_mode.as_str()],
                    )
                    .await?;
            }
        }
        drop(client);
        self.set_queue_policy(&validated, options.policy).await
    }

    pub async fn set_queue_policy(
        &self,
        queue_name: &str,
        options: QueuePolicyOptions,
    ) -> Result<()> {
        let validated = if queue_name.is_empty() {
            self.queue_name.clone()
        } else {
            validate_queue_name(queue_name)?
        };
        let payload = queue_policy_payload(&options);
        if payload.as_object().map(|m| m.is_empty()).unwrap_or(true) {
            return Ok(());
        }
        let client = self.pool.get().await?;
        client
            .execute(
                "SELECT absurd.set_queue_policy($1, $2::jsonb)",
                &[&validated, &payload],
            )
            .await?;
        Ok(())
    }

    pub async fn drop_queue(&self, queue_name: &str) -> Result<()> {
        let validated = validate_queue_name(queue_name)?;
        let client = self.pool.get().await?;
        client
            .execute("SELECT absurd.drop_queue($1)", &[&validated])
            .await?;
        Ok(())
    }

    pub async fn list_queues(&self) -> Result<Vec<String>> {
        let client = self.pool.get().await?;
        let rows = client
            .query("SELECT queue_name FROM absurd.list_queues()", &[])
            .await?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    /// Read the persisted queue policy. Returns `None` if the queue doesn't
    /// exist.
    pub async fn get_queue_policy(&self, queue_name: &str) -> Result<Option<QueuePolicy>> {
        let validated = validate_queue_name(queue_name)?;
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT queue_name, storage_mode, partition_lookahead::text, \
                        partition_lookback::text, cleanup_ttl::text, cleanup_limit, \
                        detach_mode, detach_min_age::text \
                   FROM absurd.get_queue_policy($1)",
                &[&validated],
            )
            .await?;
        Ok(row.map(|r| QueuePolicy {
            queue_name: r.get(0),
            storage_mode: r.get(1),
            partition_lookahead: r
                .try_get::<_, Option<String>>(2)
                .ok()
                .flatten()
                .unwrap_or_default(),
            partition_lookback: r
                .try_get::<_, Option<String>>(3)
                .ok()
                .flatten()
                .unwrap_or_default(),
            cleanup_ttl: r
                .try_get::<_, Option<String>>(4)
                .ok()
                .flatten()
                .unwrap_or_default(),
            cleanup_limit: r.try_get(5).unwrap_or(0),
            detach_mode: r.get(6),
            detach_min_age: r
                .try_get::<_, Option<String>>(7)
                .ok()
                .flatten()
                .unwrap_or_default(),
        }))
    }

    /// Run cleanup across all queues (or one specific queue). Returns per-queue
    /// counts of deleted tasks and events.
    pub async fn cleanup_queues(
        &self,
        queue_name: Option<&str>,
    ) -> Result<Vec<(String, CleanupReport)>> {
        let arg: Option<String> = match queue_name {
            None => None,
            Some(q) if q.is_empty() => None,
            Some(q) => Some(validate_queue_name(q)?),
        };
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT queue_name, tasks_deleted, events_deleted \
                   FROM absurd.cleanup_all_queues($1)",
                &[&arg],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let name: String = r.get(0);
                let tasks: i32 = r.get(1);
                let events: i32 = r.get(2);
                (
                    name,
                    CleanupReport {
                        tasks_deleted: tasks as i64,
                        events_deleted: events as i64,
                    },
                )
            })
            .collect())
    }

    /// Run task cleanup on a single queue with explicit TTL/limit.
    pub async fn cleanup_tasks(
        &self,
        queue_name: &str,
        ttl_seconds: i32,
        limit: i32,
    ) -> Result<i32> {
        let validated = validate_queue_name(queue_name)?;
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "SELECT absurd.cleanup_tasks($1, $2, $3)",
                &[&validated, &ttl_seconds, &limit],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Run event cleanup on a single queue with explicit TTL/limit.
    pub async fn cleanup_events(
        &self,
        queue_name: &str,
        ttl_seconds: i32,
        limit: i32,
    ) -> Result<i32> {
        let validated = validate_queue_name(queue_name)?;
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "SELECT absurd.cleanup_events($1, $2, $3)",
                &[&validated, &ttl_seconds, &limit],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Enumerate partitions that have aged past `detach_min_age` and are
    /// candidates for detachment.
    pub async fn list_detach_candidates(
        &self,
        queue_name: Option<&str>,
    ) -> Result<Vec<DetachCandidate>> {
        let arg: Option<String> = match queue_name {
            None => None,
            Some(q) if q.is_empty() => None,
            Some(q) => Some(validate_queue_name(q)?),
        };
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT queue_name, parent_table, partition_table \
                   FROM absurd.list_detach_candidates($1)",
                &[&arg],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| DetachCandidate {
                queue_name: r.get(0),
                parent_table: r.get(1),
                partition_table: r.get(2),
            })
            .collect())
    }

    /// Drop a previously detached partition. Returns true if the partition was
    /// dropped (or already gone).
    pub async fn drop_detached_partition(&self, partition_table: &str) -> Result<bool> {
        let client = self.pool.get().await?;
        let none: Option<&str> = None;
        let row = client
            .query_one(
                "SELECT absurd.drop_detached_partition($1, $2)",
                &[&partition_table, &none],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Install pg_cron jobs that run partition/cleanup/detach maintenance.
    pub async fn enable_cron(
        &self,
        queue_name: Option<&str>,
        partition_schedule: &str,
        cleanup_schedule: &str,
        detach_schedule: &str,
    ) -> Result<Vec<(String, i64)>> {
        let arg: Option<String> = match queue_name {
            None => None,
            Some(q) if q.is_empty() => None,
            Some(q) => Some(validate_queue_name(q)?),
        };
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT job_name, job_id FROM absurd.enable_cron($1, $2, $3, $4)",
                &[
                    &arg,
                    &partition_schedule,
                    &cleanup_schedule,
                    &detach_schedule,
                ],
            )
            .await?;
        Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    /// Tear down cron jobs installed by [`enable_cron`].
    pub async fn disable_cron(&self, queue_name: Option<&str>) -> Result<Vec<String>> {
        let arg: Option<String> = match queue_name {
            None => None,
            Some(q) if q.is_empty() => None,
            Some(q) => Some(validate_queue_name(q)?),
        };
        let client = self.pool.get().await?;
        let rows = client
            .query("SELECT job_name FROM absurd.disable_cron($1)", &[&arg])
            .await?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    pub async fn schema_version(&self) -> Result<Option<String>> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt("SELECT absurd.get_schema_version()", &[])
            .await?;
        Ok(row.and_then(|r| r.try_get::<_, Option<String>>(0).ok().flatten()))
    }

    pub async fn emit_event(
        &self,
        queue_name: &str,
        event_name: &str,
        payload: Value,
    ) -> Result<()> {
        if event_name.is_empty() {
            return Err(AbsurdError::other("event name must be a non-empty string"));
        }
        let validated = validate_queue_name(queue_name)?;
        let client = self.pool.get().await?;
        client
            .execute(
                "SELECT absurd.emit_event($1, $2, $3)",
                &[&validated, &event_name, &payload],
            )
            .await?;
        Ok(())
    }

    pub async fn spawn(
        &self,
        task_name: &str,
        params: Value,
        options: SpawnOptions,
    ) -> Result<SpawnResult> {
        let options = if let Some(hook) = self.hooks.before_spawn.clone() {
            hook(task_name, params.clone(), options).await?
        } else {
            options
        };
        let (queue, max_attempts, cancellation) = self.resolve_spawn(task_name, &options).await?;
        let opts_payload = normalize_spawn_options(&SpawnOptions {
            queue_name: Some(queue.clone()),
            max_attempts: Some(max_attempts),
            retry_strategy: options.retry_strategy.clone(),
            headers: options.headers.clone(),
            cancellation,
            idempotency_key: options.idempotency_key.clone(),
        });

        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "SELECT task_id::text, run_id::text, attempt, created \
                   FROM absurd.spawn_task($1, $2, $3, $4)",
                &[&queue, &task_name, &params, &opts_payload],
            )
            .await?;
        Ok(SpawnResult {
            task_id: row.get(0),
            run_id: row.get(1),
            attempt: row.get(2),
            created: row.get(3),
        })
    }

    pub async fn retry_task(
        &self,
        queue_name: &str,
        task_id: &str,
        options: RetryTaskOptions,
    ) -> Result<SpawnResult> {
        let validated = validate_queue_name(queue_name)?;
        let mut payload = serde_json::Map::new();
        if let Some(n) = options.max_attempts {
            payload.insert("max_attempts".into(), json!(n));
        }
        if options.spawn_new {
            payload.insert("spawn_new".into(), json!(true));
        }
        let value = Value::Object(payload);
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "SELECT task_id::text, run_id::text, attempt, created \
                   FROM absurd.retry_task($1, $2::text::uuid, $3)",
                &[&validated, &task_id, &value],
            )
            .await?;
        Ok(SpawnResult {
            task_id: row.get(0),
            run_id: row.get(1),
            attempt: row.get(2),
            created: row.get(3),
        })
    }

    pub async fn cancel_task(&self, queue_name: &str, task_id: &str) -> Result<()> {
        let validated = validate_queue_name(queue_name)?;
        let client = self.pool.get().await?;
        client
            .execute(
                "SELECT absurd.cancel_task($1, $2::text::uuid)",
                &[&validated, &task_id],
            )
            .await?;
        Ok(())
    }

    pub async fn fetch_task_result(
        &self,
        queue_name: &str,
        task_id: &str,
    ) -> Result<Option<TaskResultSnapshot>> {
        let validated = validate_queue_name(queue_name)?;
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT state, result, failure_reason \
                   FROM absurd.get_task_result($1, $2::text::uuid)",
                &[&validated, &task_id],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let state: String = row.get(0);
        let result: Option<Value> = row.try_get(1).ok().flatten();
        let failure: Option<Value> = row.try_get(2).ok().flatten();
        Ok(Some(TaskResultSnapshot {
            state: TaskResultState::from_str(&state),
            result,
            failure,
        }))
    }

    pub async fn await_task_result(
        &self,
        queue_name: &str,
        task_id: &str,
        options: AwaitTaskResultOptions,
    ) -> Result<TaskResultSnapshot> {
        let validated = validate_queue_name(queue_name)?;
        await_task_result_with_backoff(self, &validated, task_id, options.timeout, None).await
    }

    pub(crate) async fn resolve_spawn(
        &self,
        task_name: &str,
        options: &SpawnOptions,
    ) -> Result<(String, i32, Option<CancellationPolicy>)> {
        let queue;
        let mut max_attempts = self.default_max_attempts;
        let mut cancellation = None;

        if let Some(reg) = self.get_registered(task_name).await {
            queue = reg.queue_name.clone();
            if let Some(requested) = options.queue_name.as_deref() {
                if !requested.is_empty() && requested != queue {
                    return Err(AbsurdError::other(format!(
                        "task {:?} is registered for queue {:?} but spawn requested queue {:?}",
                        task_name, queue, requested
                    )));
                }
            }
            if let Some(n) = reg.default_max_attempts {
                max_attempts = n;
            }
            cancellation = reg.default_cancellation.clone();
        } else if let Some(q) = options.queue_name.as_deref() {
            if q.is_empty() {
                return Err(AbsurdError::TaskNotRegistered(task_name.to_string()));
            }
            queue = q.to_string();
        } else {
            return Err(AbsurdError::TaskNotRegistered(task_name.to_string()));
        }

        if let Some(n) = options.max_attempts {
            max_attempts = n;
        }
        if let Some(c) = options.cancellation.clone() {
            cancellation = Some(c);
        }
        let validated = validate_queue_name(&queue)?;
        Ok((validated, max_attempts, cancellation))
    }
}

fn queue_policy_payload(opts: &QueuePolicyOptions) -> Value {
    let mut m = serde_json::Map::new();
    if let Some(v) = &opts.partition_lookahead {
        m.insert("partition_lookahead".into(), json!(v));
    }
    if let Some(v) = &opts.partition_lookback {
        m.insert("partition_lookback".into(), json!(v));
    }
    if let Some(v) = &opts.cleanup_ttl {
        m.insert("cleanup_ttl".into(), json!(v));
    }
    if let Some(v) = opts.cleanup_limit {
        m.insert("cleanup_limit".into(), json!(v));
    }
    if let Some(v) = opts.detach_mode {
        m.insert("detach_mode".into(), json!(v.as_str()));
    }
    if let Some(v) = &opts.detach_min_age {
        m.insert("detach_min_age".into(), json!(v));
    }
    Value::Object(m)
}

fn normalize_spawn_options(opts: &SpawnOptions) -> Value {
    let mut m = serde_json::Map::new();
    if let Some(n) = opts.max_attempts {
        if n != 0 {
            m.insert("max_attempts".into(), json!(n));
        }
    }
    if let Some(headers) = &opts.headers {
        if !headers.is_empty() {
            m.insert("headers".into(), json!(headers));
        }
    }
    if let Some(r) = &opts.retry_strategy {
        let mut retry = serde_json::Map::new();
        retry.insert("kind".into(), json!(r.kind));
        if let Some(v) = r.base_seconds {
            retry.insert("base_seconds".into(), json!(v));
        }
        if let Some(v) = r.factor {
            retry.insert("factor".into(), json!(v));
        }
        if let Some(v) = r.max_seconds {
            retry.insert("max_seconds".into(), json!(v));
        }
        m.insert("retry_strategy".into(), Value::Object(retry));
    }
    if let Some(c) = &opts.cancellation {
        let mut cancel = serde_json::Map::new();
        if let Some(v) = c.max_duration {
            cancel.insert("max_duration".into(), json!(v));
        }
        if let Some(v) = c.max_delay {
            cancel.insert("max_delay".into(), json!(v));
        }
        if !cancel.is_empty() {
            m.insert("cancellation".into(), Value::Object(cancel));
        }
    }
    if let Some(k) = &opts.idempotency_key {
        if !k.is_empty() {
            m.insert("idempotency_key".into(), json!(k));
        }
    }
    Value::Object(m)
}

const INITIAL_BACKOFF: Duration = Duration::from_millis(50);
const MAX_BACKOFF: Duration = Duration::from_secs(1);

type BeforeSleepFut = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;
type BeforeSleep = Box<dyn FnMut() -> BeforeSleepFut + Send>;

pub(crate) async fn await_task_result_with_backoff(
    client: &Client,
    queue_name: &str,
    task_id: &str,
    timeout: Option<Duration>,
    mut before_sleep: Option<BeforeSleep>,
) -> Result<TaskResultSnapshot> {
    let started = std::time::Instant::now();
    let mut delay = INITIAL_BACKOFF;

    loop {
        let snapshot = client.fetch_task_result(queue_name, task_id).await?;
        let Some(snapshot) = snapshot else {
            return Err(AbsurdError::TaskNotFound(task_id.to_string()));
        };
        if snapshot.is_terminal() {
            return Ok(snapshot);
        }
        if let Some(t) = timeout {
            if t.is_zero() || started.elapsed() >= t {
                return Err(AbsurdError::Timeout(format!(
                    "timed out waiting for task {:?}",
                    task_id
                )));
            }
        }
        if let Some(f) = before_sleep.as_mut() {
            f().await?;
        }
        let mut wait_for = delay;
        if let Some(t) = timeout {
            let remaining = t.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(AbsurdError::Timeout(format!(
                    "timed out waiting for task {:?}",
                    task_id
                )));
            }
            if wait_for > remaining {
                wait_for = remaining;
            }
        }
        tokio::time::sleep(wait_for).await;
        delay = (delay * 2).min(MAX_BACKOFF);
    }
}

/// Convenience: build a `BeforeSleep` from a closure returning a future.
pub(crate) fn boxed_before_sleep<F, Fut>(mut f: F) -> BeforeSleep
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    Box::new(move || Box::pin(f()))
}

/// Internal helper to defer (re-schedule) a claimed run by a delay. Used by
/// the worker when it encounters an unregistered task.
pub(crate) async fn defer_claimed_run(
    pool: &Pool,
    queue_name: &str,
    run_id: &str,
    delay: Duration,
) -> Result<()> {
    let client = pool.get().await?;
    client
        .execute(
            "SELECT absurd.schedule_run($1, $2::text::uuid, absurd.current_time() + make_interval(secs => $3))",
            &[&queue_name, &run_id, &(duration_seconds(delay) as i32)],
        )
        .await?;
    Ok(())
}

pub(crate) async fn complete_run(
    pool: &Pool,
    queue_name: &str,
    run_id: &str,
    result: Value,
) -> Result<()> {
    let client = pool.get().await?;
    client
        .execute(
            "SELECT absurd.complete_run($1, $2::text::uuid, $3)",
            &[&queue_name, &run_id, &result],
        )
        .await
        .map_err(map_state_error)?;
    Ok(())
}

pub(crate) async fn fail_run(
    pool: &Pool,
    queue_name: &str,
    run_id: &str,
    name: &str,
    message: &str,
    traceback: &str,
) -> Result<()> {
    let payload = json!({
        "name": name,
        "message": message,
        "traceback": traceback,
    });
    let client = pool.get().await?;
    let none: Option<chrono::DateTime<chrono::Utc>> = None;
    client
        .execute(
            "SELECT absurd.fail_run($1, $2::text::uuid, $3, $4)",
            &[&queue_name, &run_id, &payload, &none],
        )
        .await
        .map_err(map_state_error)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
pub(crate) struct ClaimedTask {
    pub run_id: String,
    pub task_id: String,
    pub attempt: i32,
    pub task_name: String,
    pub params: Option<Value>,
    pub headers: Option<Value>,
    pub wake_event: Option<String>,
    pub event_payload: Option<Value>,
}

pub(crate) async fn claim_tasks(
    pool: &Pool,
    queue_name: &str,
    worker_id: &str,
    claim_timeout: Duration,
    batch_size: i32,
) -> Result<Vec<ClaimedTask>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT run_id::text, task_id::text, attempt, task_name, params, headers, wake_event, event_payload \
               FROM absurd.claim_task($1, $2, $3, $4)",
            &[
                &queue_name,
                &worker_id,
                &(duration_seconds(claim_timeout) as i32),
                &batch_size,
            ],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| ClaimedTask {
            run_id: r.get(0),
            task_id: r.get(1),
            attempt: r.get(2),
            task_name: r.get(3),
            params: r.try_get(4).ok().flatten(),
            headers: r.try_get(5).ok().flatten(),
            wake_event: r.try_get(6).ok().flatten(),
            event_payload: r.try_get(7).ok().flatten(),
        })
        .collect())
}

#[allow(dead_code)]
pub(crate) const _MAX_QUEUE_LEN: usize = MAX_QUEUE_NAME_LENGTH;
