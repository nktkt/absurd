use crate::error::Result;
use crate::CancellationPolicy;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Trait implemented by registered task handlers. Implementations receive the
/// JSON params payload and return either a JSON result or an error.
#[async_trait]
pub trait TaskHandler: Send + Sync + 'static {
    async fn handle(&self, params: Value) -> Result<Value>;
}

/// Adapter so closures can be used directly as handlers.
pub struct FnHandler<F>(pub F);

#[async_trait]
impl<F, Fut> TaskHandler for FnHandler<F>
where
    F: Fn(Value) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<Value>> + Send + 'static,
{
    async fn handle(&self, params: Value) -> Result<Value> {
        (self.0)(params).await
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
