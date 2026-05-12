//! Stepwise schema migrations.
//!
//! Each migration is bundled at compile time via `include_str!`. The
//! `resolve` function walks the directed graph of `from -> to` edges to
//! produce a sequence of SQL files that take the database from its current
//! recorded version to the target.

use crate::error::{AbsurdError, Result};
use once_cell::sync::Lazy;
use std::cmp::Ordering;

/// One bundled migration step.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub from: &'static str,
    pub to: &'static str,
    pub filename: &'static str,
    pub sql: &'static str,
}

macro_rules! mig {
    ($from:literal, $to:literal, $file:literal) => {
        Migration {
            from: $from,
            to: $to,
            filename: $file,
            sql: include_str!(concat!("../sql/migrations/", $file)),
        }
    };
}

/// All bundled migrations, in lexical filename order. The schema target is
/// always `main` (matches the upstream tool's convention).
pub static MIGRATIONS: Lazy<Vec<Migration>> = Lazy::new(|| {
    vec![
        mig!("0.0.3", "0.0.4", "0.0.3-0.0.4.sql"),
        mig!("0.0.4", "0.0.5", "0.0.4-0.0.5.sql"),
        mig!("0.0.5", "0.0.6", "0.0.5-0.0.6.sql"),
        mig!("0.0.6", "0.0.7", "0.0.6-0.0.7.sql"),
        mig!("0.0.8", "0.1.0", "0.0.8-0.1.0.sql"),
        mig!("0.1.0", "0.1.1", "0.1.0-0.1.1.sql"),
        mig!("0.1.1", "0.2.0", "0.1.1-0.2.0.sql"),
        mig!("0.2.0", "0.3.0", "0.2.0-0.3.0.sql"),
        mig!("0.3.0", "main", "0.3.0-main.sql"),
    ]
});

/// The version the bundled schema represents (matches
/// `absurd.get_schema_version`).
pub const TARGET_VERSION: &str = "main";

/// Resolve a forward path from `current` to `target`. `main` is the highest
/// version; numbered semver-ish versions are ordered by major.minor.patch.
pub fn resolve(current: &str, target: &str) -> Result<Vec<Migration>> {
    if current == target {
        return Ok(Vec::new());
    }
    if compare_versions(current, target) == Ordering::Greater {
        return Err(AbsurdError::other(format!(
            "current schema {} is newer than target {}; refusing to downgrade",
            current, target
        )));
    }
    let mut steps = Vec::new();
    let mut cursor = current.to_string();
    let mut attempted = std::collections::HashSet::new();
    while cursor != target {
        if !attempted.insert(cursor.clone()) {
            return Err(AbsurdError::other(format!(
                "cycle detected while resolving migrations from {} to {}",
                current, target
            )));
        }
        let Some(step) = MIGRATIONS.iter().find(|m| m.from == cursor) else {
            return Err(AbsurdError::other(format!(
                "no bundled migration path from {} to {}",
                cursor, target
            )));
        };
        steps.push(*step);
        cursor = step.to.to_string();
    }
    Ok(steps)
}

fn parse_semver(value: &str) -> Option<(u32, u32, u32)> {
    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() < 3 {
        return None;
    }
    let major = parts[0].parse().ok()?;
    let minor = parts[1].parse().ok()?;
    // Strip pre-release suffix on the patch component.
    let patch_str = parts[2]
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap_or(parts[2]);
    let patch = patch_str.parse().ok()?;
    Some((major, minor, patch))
}

fn compare_versions(a: &str, b: &str) -> Ordering {
    match (a, b) {
        ("main", "main") => Ordering::Equal,
        ("main", _) => Ordering::Greater,
        (_, "main") => Ordering::Less,
        _ => match (parse_semver(a), parse_semver(b)) {
            (Some(x), Some(y)) => x.cmp(&y),
            _ => a.cmp(b),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_from_recent() {
        // The bundled chain has a gap between 0.0.7 and 0.0.8 (upstream
        // skipped that release), so we start at 0.0.8 which has an
        // unbroken path to `main`.
        let path = resolve("0.0.8", "main").unwrap();
        assert!(path.first().map(|m| m.from) == Some("0.0.8"));
        assert!(path.last().map(|m| m.to) == Some("main"));
    }

    #[test]
    fn no_op_when_at_target() {
        let path = resolve("main", "main").unwrap();
        assert!(path.is_empty());
    }

    #[test]
    fn rejects_downgrade() {
        let err = resolve("0.1.0", "0.0.5").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("refusing to downgrade"), "{msg}");
    }
}
