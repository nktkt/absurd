use crate::client::{claim_tasks, complete_run, defer_claimed_run, fail_run, ClaimedTask, Client};
use crate::context::{LeaseObserver, TaskContext, CURRENT_TASK};
use crate::error::{AbsurdError, Result, TaskStateError};
use crate::options::{ShutdownHandle, WorkBatchOptions, WorkerOptions};
use fnv::FnvHasher;
use serde_json::Value;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

const UNKNOWN_TASK_DEFER_BASE: Duration = Duration::from_secs(15);
const UNKNOWN_TASK_DEFER_JITTER: Duration = Duration::from_secs(15);

fn unknown_task_defer_delay(run_id: &str) -> Duration {
    if UNKNOWN_TASK_DEFER_JITTER.is_zero() {
        return UNKNOWN_TASK_DEFER_BASE;
    }
    let mut hasher = FnvHasher::default();
    hasher.write(run_id.as_bytes());
    let max_seconds = UNKNOWN_TASK_DEFER_JITTER.as_secs() as u32 + 1;
    let jitter = (hasher.finish() as u32) % max_seconds;
    UNKNOWN_TASK_DEFER_BASE + Duration::from_secs(jitter as u64)
}

impl Client {
    /// Process a single batch of tasks. Returns when the batch finishes.
    pub async fn work_batch(&self, options: WorkBatchOptions) -> Result<()> {
        let opts = normalize_batch(options);
        let tasks = claim_tasks(
            &self.pool,
            &self.queue_name,
            &opts.worker_id,
            opts.claim_timeout,
            opts.batch_size,
        )
        .await?;
        for task in tasks {
            execute_task(self.clone(), task, opts.claim_timeout, false).await?;
        }
        Ok(())
    }

    /// Run a long-lived worker loop. Returns when the supplied
    /// [`ShutdownHandle`] fires; otherwise loops forever, sleeping
    /// `poll_interval` between empty polls. In-flight tasks are awaited before
    /// returning.
    pub async fn run_worker(&self, options: WorkerOptions) -> Result<()> {
        let cfg = normalize_worker(self, options);
        let sem = Arc::new(Semaphore::new(cfg.concurrency as usize));
        let shutdown = cfg.shutdown.clone();
        let mut join_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        loop {
            if let Some(h) = &shutdown {
                if h.is_shutdown() {
                    break;
                }
            }
            let permits = sem.available_permits() as i32;
            if permits == 0 {
                wait_or_shutdown(&shutdown, cfg.poll_interval).await;
                continue;
            }
            let batch_size = std::cmp::min(cfg.batch.batch_size, permits);
            let claim_fut = claim_tasks(
                &self.pool,
                &self.queue_name,
                &cfg.batch.worker_id,
                cfg.batch.claim_timeout,
                batch_size,
            );
            let claim_result = match &shutdown {
                Some(h) => tokio::select! {
                    biased;
                    _ = h.wait() => break,
                    r = claim_fut => r,
                },
                None => claim_fut.await,
            };
            let tasks = match claim_result {
                Ok(t) => t,
                Err(err) => {
                    if let Some(on_err) = &cfg.on_error {
                        on_err(&err);
                    } else {
                        tracing::error!(?err, "worker claim failed");
                    }
                    wait_or_shutdown(&shutdown, cfg.poll_interval).await;
                    continue;
                }
            };
            if tasks.is_empty() {
                wait_or_shutdown(&shutdown, cfg.poll_interval).await;
                continue;
            }
            for task in tasks {
                let permit = sem.clone().acquire_owned().await.expect("semaphore");
                let client = self.clone();
                let lease = cfg.batch.claim_timeout;
                let fatal = cfg.fatal_on_lease_timeout;
                let on_err = cfg.on_error.clone();
                join_handles.push(tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(err) = execute_task(client, task, lease, fatal).await {
                        if let Some(on_err) = on_err {
                            on_err(&err);
                        } else {
                            tracing::error!(?err, "task execution failed");
                        }
                    }
                }));
                // Trim completed handles to avoid unbounded growth.
                join_handles.retain(|h| !h.is_finished());
            }
        }
        // Drain any in-flight tasks so the caller sees a clean shutdown.
        for handle in join_handles {
            let _ = handle.await;
        }
        Ok(())
    }
}

async fn wait_or_shutdown(shutdown: &Option<ShutdownHandle>, d: Duration) {
    match shutdown {
        Some(h) => tokio::select! {
            biased;
            _ = h.wait() => {}
            _ = tokio::time::sleep(d) => {}
        },
        None => tokio::time::sleep(d).await,
    }
}

fn normalize_batch(options: WorkBatchOptions) -> WorkBatchOptions {
    let claim_timeout = if options.claim_timeout.is_zero() {
        Duration::from_secs(120)
    } else {
        options.claim_timeout
    };
    let batch_size = if options.batch_size <= 0 {
        1
    } else {
        options.batch_size
    };
    let worker_id = if options.worker_id.is_empty() {
        "worker".to_string()
    } else {
        options.worker_id
    };
    WorkBatchOptions {
        worker_id,
        claim_timeout,
        batch_size,
    }
}

struct WorkerCfg {
    batch: WorkBatchOptions,
    concurrency: i32,
    poll_interval: Duration,
    fatal_on_lease_timeout: bool,
    on_error: Option<Arc<dyn Fn(&AbsurdError) + Send + Sync>>,
    shutdown: Option<ShutdownHandle>,
}

fn normalize_worker(client: &Client, options: WorkerOptions) -> WorkerCfg {
    let worker_id = options.worker_id.unwrap_or_else(|| {
        let hostname = hostname::get()
            .ok()
            .and_then(|h| h.to_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "worker".to_string());
        format!("{}:{}", hostname, std::process::id())
    });
    let concurrency = if options.concurrency <= 0 {
        1
    } else {
        options.concurrency
    };
    let batch_size = options.batch_size.unwrap_or(concurrency);
    let _ = client;
    WorkerCfg {
        batch: WorkBatchOptions {
            worker_id,
            claim_timeout: options.claim_timeout,
            batch_size,
        },
        concurrency,
        poll_interval: options.poll_interval,
        fatal_on_lease_timeout: options.fatal_on_lease_timeout,
        on_error: options.on_error,
        shutdown: options.shutdown,
    }
}

/// Lease watchdog. Logs a warning when the claim deadline passes, and (if
/// configured) terminates the process at 2× claim_timeout to surface stuck
/// runs loudly. Reset by [`LeaseObserver::observe`].
struct LeaseWatchdog {
    label: String,
    fatal_on_timeout: bool,
    epoch: AtomicU64,
    inner: Mutex<WatchdogInner>,
}

struct WatchdogInner {
    warn: Option<tokio::task::JoinHandle<()>>,
    fatal: Option<tokio::task::JoinHandle<()>>,
    stopped: bool,
}

impl LeaseWatchdog {
    fn new(task_label: impl Into<String>, fatal_on_timeout: bool) -> Arc<Self> {
        Arc::new(LeaseWatchdog {
            label: task_label.into(),
            fatal_on_timeout,
            epoch: AtomicU64::new(0),
            inner: Mutex::new(WatchdogInner {
                warn: None,
                fatal: None,
                stopped: false,
            }),
        })
    }

    fn schedule(self: &Arc<Self>, lease: Duration) {
        if lease.is_zero() {
            return;
        }
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        let me = self.clone();
        let mut inner = self.inner.lock().unwrap();
        if inner.stopped {
            return;
        }
        if let Some(h) = inner.warn.take() {
            h.abort();
        }
        if let Some(h) = inner.fatal.take() {
            h.abort();
        }
        let label = self.label.clone();
        let lease_log = lease;
        inner.warn = Some(tokio::spawn(async move {
            tokio::time::sleep(lease_log).await;
            if me.epoch.load(Ordering::SeqCst) != epoch {
                return;
            }
            if me.inner.lock().unwrap().stopped {
                return;
            }
            tracing::warn!(
                "[absurd] task {} exceeded claim timeout of {:?}",
                label,
                lease_log
            );
        }));
        if self.fatal_on_timeout {
            let me = self.clone();
            let label = self.label.clone();
            inner.fatal = Some(tokio::spawn(async move {
                tokio::time::sleep(lease_log * 2).await;
                if me.epoch.load(Ordering::SeqCst) != epoch {
                    return;
                }
                if me.inner.lock().unwrap().stopped {
                    return;
                }
                tracing::error!(
                    "[absurd] task {} exceeded claim timeout of {:?} by more than 100%; terminating process",
                    label,
                    lease_log
                );
                std::process::exit(1);
            }));
        }
    }

    fn stop(self: &Arc<Self>) {
        let mut inner = self.inner.lock().unwrap();
        inner.stopped = true;
        if let Some(h) = inner.warn.take() {
            h.abort();
        }
        if let Some(h) = inner.fatal.take() {
            h.abort();
        }
    }
}

struct WatchdogObserver(Arc<LeaseWatchdog>);

impl LeaseObserver for WatchdogObserver {
    fn observe(&self, lease: Duration) {
        self.0.schedule(lease);
    }
}

struct WatchdogGuard {
    inner: Arc<LeaseWatchdog>,
}

impl Drop for WatchdogGuard {
    fn drop(&mut self) {
        self.inner.stop();
    }
}

async fn execute_task(
    client: Client,
    task: ClaimedTask,
    claim_timeout: Duration,
    fatal_on_lease_timeout: bool,
) -> Result<()> {
    let queue = client.queue_name().to_string();
    let watchdog = LeaseWatchdog::new(
        format!("{} ({})", task.task_name, task.task_id),
        fatal_on_lease_timeout,
    );
    watchdog.schedule(if claim_timeout.is_zero() {
        Duration::from_secs(120)
    } else {
        claim_timeout
    });
    let observer: Arc<dyn LeaseObserver> = Arc::new(WatchdogObserver(watchdog.clone()));
    let _watchdog_guard = WatchdogGuard {
        inner: watchdog.clone(),
    };
    let registration = client.get_registered(&task.task_name).await;
    let Some(reg) = registration else {
        let delay = unknown_task_defer_delay(&task.run_id);
        if let Err(err) = defer_claimed_run(&client.pool, &queue, &task.run_id, delay).await {
            tracing::warn!(?err, task_name = %task.task_name, "failed to defer unknown task; marking failed");
            let _ = fail_run(
                &client.pool,
                &queue,
                &task.run_id,
                "UnknownTask",
                &format!(
                    "failed to defer unknown task {:?} ({}): {}",
                    task.task_name, task.task_id, err
                ),
                "",
            )
            .await;
            return Ok(());
        }
        tracing::info!(
            task = %task.task_name,
            task_id = %task.task_id,
            run = %task.run_id,
            ?delay,
            "claimed unknown task; deferred"
        );
        return Ok(());
    };

    if reg.queue_name != queue {
        let msg = format!("misconfigured task {:?} (queue mismatch)", task.task_name);
        tracing::warn!("{}", msg);
        let _ = fail_run(
            &client.pool,
            &queue,
            &task.run_id,
            "QueueMismatch",
            &msg,
            "",
        )
        .await;
        return Ok(());
    }

    let ctx = match TaskContext::new(
        client.clone(),
        reg.queue_name.clone(),
        &task,
        claim_timeout,
        Some(observer),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(AbsurdError::InvalidTaskHeaders(msg)) => {
            let _ = fail_run(
                &client.pool,
                &queue,
                &task.run_id,
                "InvalidTaskHeaders",
                &msg,
                "",
            )
            .await;
            return Ok(());
        }
        Err(err) => return Err(err),
    };

    let params = task.params.unwrap_or(Value::Null);
    let handler = reg.handler.clone();
    let exec_ctx = ctx.clone();
    let wrap_hook = client.hooks.wrap_task_execution.clone();
    let result: Result<Value> = CURRENT_TASK
        .scope(exec_ctx.clone(), async move {
            if let Some(wrap) = wrap_hook {
                let exec: crate::hooks::TaskExecutor =
                    Box::new(move || Box::pin(async move { handler.handle(params).await }));
                wrap(exec_ctx, exec).await
            } else {
                handler.handle(params).await
            }
        })
        .await;

    match result {
        Ok(value) => {
            if let Err(err) = complete_run(&client.pool, &queue, &task.run_id, value).await {
                if matches!(err, AbsurdError::TaskState(_)) {
                    return Ok(());
                }
                return Err(err);
            }
            Ok(())
        }
        Err(AbsurdError::Suspended) => Ok(()),
        Err(AbsurdError::TaskState(TaskStateError::Cancelled))
        | Err(AbsurdError::TaskState(TaskStateError::AlreadyFailed)) => Ok(()),
        Err(err) => {
            tracing::warn!(?err, "task execution failed");
            let _ = fail_run(
                &client.pool,
                &queue,
                &task.run_id,
                "TaskError",
                &err.to_string(),
                "",
            )
            .await;
            Ok(())
        }
    }
}
