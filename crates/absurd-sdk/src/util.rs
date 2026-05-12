use crate::error::{AbsurdError, Result};
use crate::MAX_QUEUE_NAME_LENGTH;
use std::time::Duration;

/// Validate a queue name. Queue names are interpolated into table identifiers,
/// so they must stay within Postgres' 63-byte identifier limit (minus the
/// schema prefix Absurd uses).
pub fn validate_queue_name(name: &str) -> Result<String> {
    if name.is_empty() {
        return Err(AbsurdError::InvalidQueueName(
            "queue name must be provided".into(),
        ));
    }
    if name.len() > MAX_QUEUE_NAME_LENGTH {
        return Err(AbsurdError::InvalidQueueName(format!(
            "queue name {:?} is too long (max {} bytes)",
            name, MAX_QUEUE_NAME_LENGTH
        )));
    }
    Ok(name.to_string())
}

/// Convert a Duration to whole seconds, rounding up.
pub fn duration_seconds(d: Duration) -> i32 {
    if d.is_zero() {
        return 0;
    }
    let secs = d.as_secs_f64();
    secs.ceil() as i32
}

pub fn duration_seconds_or(d: Duration, fallback: Duration) -> i32 {
    if d.is_zero() {
        duration_seconds(fallback)
    } else {
        duration_seconds(d)
    }
}

/// Resolve a database URL the same way the original SDK does:
/// explicit argument → `ABSURD_DATABASE_URL` → `PGDATABASE` → localhost default.
pub fn resolve_database_url(explicit: Option<&str>) -> String {
    if let Some(value) = explicit {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if let Ok(value) = std::env::var("ABSURD_DATABASE_URL") {
        if !value.trim().is_empty() {
            return value;
        }
    }
    if let Ok(value) = std::env::var("PGDATABASE") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            if trimmed.contains("://") || trimmed.contains('=') {
                return trimmed.to_string();
            }
            return format!("dbname={}", trimmed);
        }
    }
    "postgresql://localhost/absurd".to_string()
}
