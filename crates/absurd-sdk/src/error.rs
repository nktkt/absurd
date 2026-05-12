use thiserror::Error;

pub type Result<T> = std::result::Result<T, AbsurdError>;

/// Error type returned by the Absurd SDK.
#[derive(Debug, Error)]
pub enum AbsurdError {
    #[error("postgres: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("pool: {0}")]
    Pool(String),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid queue name: {0}")]
    InvalidQueueName(String),

    #[error("task not found: {0}")]
    TaskNotFound(String),

    #[error("task not registered: {0}")]
    TaskNotRegistered(String),

    #[error("no task context — call from within a registered task")]
    NoTaskContext,

    #[error("invalid task headers: {0}")]
    InvalidTaskHeaders(String),

    #[error("timed out: {0}")]
    Timeout(String),

    #[error("task suspended")]
    Suspended,

    #[error("task state: {0:?}")]
    TaskState(TaskStateError),

    #[error("{0}")]
    Other(String),
}

impl AbsurdError {
    pub fn other(msg: impl Into<String>) -> Self {
        AbsurdError::Other(msg.into())
    }
}

/// Terminal task state signaled by Postgres via SQLSTATE codes AB001/AB002.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStateError {
    Cancelled,
    AlreadyFailed,
}

impl From<deadpool_postgres::PoolError> for AbsurdError {
    fn from(err: deadpool_postgres::PoolError) -> Self {
        AbsurdError::Pool(err.to_string())
    }
}

impl From<deadpool_postgres::CreatePoolError> for AbsurdError {
    fn from(err: deadpool_postgres::CreatePoolError) -> Self {
        AbsurdError::Pool(err.to_string())
    }
}

impl From<deadpool_postgres::ConfigError> for AbsurdError {
    fn from(err: deadpool_postgres::ConfigError) -> Self {
        AbsurdError::Pool(err.to_string())
    }
}

impl From<deadpool_postgres::BuildError> for AbsurdError {
    fn from(err: deadpool_postgres::BuildError) -> Self {
        AbsurdError::Pool(err.to_string())
    }
}

/// Inspect a Postgres error for the SQLSTATE codes the Absurd schema raises to
/// signal task-level termination.
pub(crate) fn map_state_error(err: tokio_postgres::Error) -> AbsurdError {
    if let Some(db) = err.as_db_error() {
        match db.code().code() {
            "AB001" => return AbsurdError::TaskState(TaskStateError::Cancelled),
            "AB002" => return AbsurdError::TaskState(TaskStateError::AlreadyFailed),
            _ => {}
        }
    }
    AbsurdError::Postgres(err)
}
