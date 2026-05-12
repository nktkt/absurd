use crate::context::TaskContext;
use crate::error::Result;
use crate::options::SpawnOptions;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Future returned by the [`BeforeSpawnHook`] closure.
pub type BeforeSpawnFut = Pin<Box<dyn Future<Output = Result<SpawnOptions>> + Send + 'static>>;

/// Hook called before a task is spawned. May mutate the spawn options or
/// reject the spawn by returning an error.
///
/// The hook receives the task name, the params payload, and the resolved
/// spawn options. The returned options will be used for the actual
/// `absurd.spawn_task` call.
pub type BeforeSpawnHook = Arc<dyn Fn(&str, Value, SpawnOptions) -> BeforeSpawnFut + Send + Sync>;

/// Future returned by the [`WrapTaskExecutionHook`] closure.
pub type WrapTaskFut = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;

/// Closure invoked by the worker to actually run the registered handler.
/// Wrap-task hooks call this to delegate to the underlying handler.
pub type TaskExecutor = Box<dyn FnOnce() -> WrapTaskFut + Send>;

/// Hook that wraps task execution. The hook receives the task context and a
/// closure that, when awaited, runs the registered handler. Use this for
/// cross-cutting concerns like tracing, metrics, or timing.
pub type WrapTaskExecutionHook =
    Arc<dyn Fn(TaskContext, TaskExecutor) -> WrapTaskFut + Send + Sync>;

/// Pluggable lifecycle hooks. Both hooks are optional; an empty `Hooks` is
/// the no-op default.
#[derive(Clone, Default)]
pub struct Hooks {
    pub before_spawn: Option<BeforeSpawnHook>,
    pub wrap_task_execution: Option<WrapTaskExecutionHook>,
}

impl Hooks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the before-spawn hook.
    pub fn with_before_spawn<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(&str, Value, SpawnOptions) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<SpawnOptions>> + Send + 'static,
    {
        let arc: BeforeSpawnHook =
            Arc::new(move |name, params, opts| Box::pin(hook(name, params, opts)));
        self.before_spawn = Some(arc);
        self
    }

    /// Set the wrap-task-execution hook.
    pub fn with_wrap_task_execution<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(TaskContext, TaskExecutor) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        let arc: WrapTaskExecutionHook = Arc::new(move |ctx, exec| Box::pin(hook(ctx, exec)));
        self.wrap_task_execution = Some(arc);
        self
    }
}

impl std::fmt::Debug for Hooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hooks")
            .field("before_spawn", &self.before_spawn.is_some())
            .field("wrap_task_execution", &self.wrap_task_execution.is_some())
            .finish()
    }
}
