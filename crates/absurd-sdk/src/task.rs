use crate::error::Result;
use crate::CancellationPolicy;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

/// Trait implemented by registered task handlers. Implementations receive the
/// JSON params payload and return either a JSON result or an error.
///
/// Most users won't implement this directly — register typed handlers with
/// [`Client::register_task`](crate::Client::register_task) or untyped JSON
/// handlers with [`Client::register_fn`](crate::Client::register_fn).
#[async_trait]
pub trait TaskHandler: Send + Sync + 'static {
    async fn handle(&self, params: Value) -> Result<Value>;
}

/// Adapter so `Fn(Value) -> Future<Result<Value>>` closures can be used
/// directly as handlers.
pub struct FnHandler<F>(pub F);

#[async_trait]
impl<F, Fut> TaskHandler for FnHandler<F>
where
    F: Fn(Value) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value>> + Send + 'static,
{
    async fn handle(&self, params: Value) -> Result<Value> {
        (self.0)(params).await
    }
}

/// Adapter that converts a strongly-typed handler into a `TaskHandler` by
/// serializing/deserializing params and results through `serde_json`.
pub struct TypedHandler<F, P, R> {
    func: F,
    _marker: PhantomData<fn(P) -> R>,
}

impl<F, P, R, Fut> TypedHandler<F, P, R>
where
    F: Fn(P) -> Fut + Send + Sync + 'static,
    P: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<F, P, R, Fut> TaskHandler for TypedHandler<F, P, R>
where
    F: Fn(P) -> Fut + Send + Sync + 'static,
    P: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
{
    async fn handle(&self, params: Value) -> Result<Value> {
        let typed: P = serde_json::from_value(params)?;
        let result = (self.func)(typed).await?;
        Ok(serde_json::to_value(result)?)
    }
}

#[derive(Clone)]
pub struct RegisteredTask {
    pub name: String,
    pub queue_name: String,
    pub default_max_attempts: Option<i32>,
    pub default_cancellation: Option<CancellationPolicy>,
    pub handler: Arc<dyn TaskHandler>,
}

/// Typed task definition. Returned by [`task`] and consumed by
/// [`Client::register_task`](crate::Client::register_task).
pub struct TaskDefinition<P, R> {
    pub(crate) name: String,
    pub(crate) queue_name: Option<String>,
    pub(crate) default_max_attempts: Option<i32>,
    pub(crate) default_cancellation: Option<CancellationPolicy>,
    pub(crate) handler: Arc<dyn TaskHandler>,
    pub(crate) _marker: PhantomData<fn(P) -> R>,
}

impl<P, R> TaskDefinition<P, R>
where
    P: Serialize,
    R: DeserializeOwned,
{
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Override the queue this task is registered against.
    pub fn on_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue_name = Some(queue.into());
        self
    }

    /// Override the default max-attempts cap for spawns of this task.
    pub fn with_max_attempts(mut self, n: i32) -> Self {
        self.default_max_attempts = Some(n);
        self
    }

    /// Override the default cancellation policy for spawns of this task.
    pub fn with_cancellation(mut self, cancellation: CancellationPolicy) -> Self {
        self.default_cancellation = Some(cancellation);
        self
    }
}

impl<P, R> TaskDefinition<P, R>
where
    P: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
{
    /// Strongly-typed spawn through the given client.
    pub async fn spawn(
        &self,
        client: &crate::Client,
        params: P,
        options: crate::SpawnOptions,
    ) -> crate::Result<crate::SpawnResult> {
        client.spawn_typed(self, params, options).await
    }
}

/// Helper to build a typed [`TaskDefinition`].
///
/// ```ignore
/// let order = absurd_sdk::task("order-fulfillment", |params: OrderParams| async move {
///     Ok(handle(params).await?)
/// });
/// client.register_task(order).await?;
/// ```
pub fn task<F, P, R, Fut>(name: impl Into<String>, func: F) -> TaskDefinition<P, R>
where
    F: Fn(P) -> Fut + Send + Sync + 'static,
    P: DeserializeOwned + Serialize + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
{
    TaskDefinition {
        name: name.into(),
        queue_name: None,
        default_max_attempts: None,
        default_cancellation: None,
        handler: Arc::new(TypedHandler::new(func)),
        _marker: PhantomData,
    }
}
