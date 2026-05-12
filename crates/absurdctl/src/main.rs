use absurd_sdk::{
    Client, CreateQueueOptions, QueueDetachMode, QueuePolicyOptions, QueueStorageMode,
    RetryStrategy, RetryTaskOptions, SpawnOptions,
};
use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use serde_json::Value;
use std::collections::BTreeMap;

/// Absurd control plane CLI (Rust port).
#[derive(Parser, Debug)]
#[command(
    name = "absurdctl",
    about = "Manage Absurd schemas, queues, and tasks",
    version
)]
struct Cli {
    #[command(flatten)]
    db: DbArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Args, Debug, Clone)]
struct DbArgs {
    /// Database URL. Falls back to ABSURD_DATABASE_URL or PGDATABASE.
    #[arg(long, env = "ABSURD_DATABASE_URL")]
    database_url: Option<String>,
    /// Convenience shorthand equivalent to dbname=<value>.
    #[arg(short = 'd', long = "database")]
    database: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Apply the bundled `absurd.sql` schema to the target database.
    Init {
        /// Print the SQL that would be applied instead of running it.
        #[arg(long)]
        dry_run: bool,
    },
    /// Print the schema version recorded in the database.
    SchemaVersion,
    /// Apply schema migrations (no-op when already at bundled version).
    Migrate {
        /// Print the SQL instead of running it.
        #[arg(long)]
        dry_run: bool,
    },
    /// Create a queue.
    CreateQueue {
        queue: String,
        #[arg(long, value_parser = ["unpartitioned", "partitioned"])]
        storage_mode: Option<String>,
        #[arg(long)]
        partition_lookahead: Option<String>,
        #[arg(long)]
        partition_lookback: Option<String>,
        #[arg(long)]
        cleanup_ttl: Option<String>,
        #[arg(long)]
        cleanup_limit: Option<i32>,
        #[arg(long, value_parser = ["none", "empty"])]
        detach_mode: Option<String>,
        #[arg(long)]
        detach_min_age: Option<String>,
    },
    /// Drop a queue.
    DropQueue { queue: String },
    /// List configured queues.
    ListQueues,
    /// Spawn a task.
    SpawnTask {
        queue: String,
        task_name: String,
        /// JSON params payload. Defaults to `null`.
        #[arg(long, default_value = "null")]
        params: String,
        #[arg(long)]
        max_attempts: Option<i32>,
        #[arg(long)]
        idempotency_key: Option<String>,
        /// Retry strategy, e.g. `exponential`. Accepted strategies depend on
        /// the schema; this is passed through unchanged.
        #[arg(long)]
        retry_kind: Option<String>,
        #[arg(long)]
        retry_base_seconds: Option<f64>,
        #[arg(long)]
        retry_factor: Option<f64>,
        #[arg(long)]
        retry_max_seconds: Option<f64>,
        /// Headers as `key=value` pairs. Values are parsed as JSON when
        /// possible; otherwise stored as strings.
        #[arg(long = "header")]
        headers: Vec<String>,
    },
    /// Retry a failed task.
    RetryTask {
        queue: String,
        task_id: String,
        #[arg(long)]
        max_attempts: Option<i32>,
        #[arg(long)]
        spawn_new: bool,
    },
    /// Cancel a task.
    CancelTask { queue: String, task_id: String },
    /// Run cleanup across queues (deletes expired tasks/events per queue
    /// policy).
    Cleanup {
        /// Limit cleanup to a specific queue.
        #[arg(long)]
        queue: Option<String>,
        /// Run a per-queue cleanup with explicit TTL seconds (overrides
        /// queue policy). Requires --queue.
        #[arg(long)]
        ttl_seconds: Option<i32>,
        /// Limit per-call deletion (used with --ttl-seconds).
        #[arg(long, default_value_t = 1000)]
        limit: i32,
        /// When --ttl-seconds is set, cleanup only events (not tasks).
        #[arg(long)]
        events_only: bool,
    },
    /// Print persisted queue policy.
    QueuePolicy { queue: String },
    /// Update queue policy fields.
    SetQueuePolicy {
        queue: String,
        #[arg(long)]
        partition_lookahead: Option<String>,
        #[arg(long)]
        partition_lookback: Option<String>,
        #[arg(long)]
        cleanup_ttl: Option<String>,
        #[arg(long)]
        cleanup_limit: Option<i32>,
        #[arg(long, value_parser = ["none", "empty"])]
        detach_mode: Option<String>,
        #[arg(long)]
        detach_min_age: Option<String>,
    },
    /// List partitions eligible for detachment.
    ListDetachCandidates {
        #[arg(long)]
        queue: Option<String>,
    },
    /// Drop a previously detached partition.
    DropDetachedPartition { partition_table: String },
    /// Enable pg_cron maintenance jobs.
    CronEnable {
        #[arg(long)]
        queue: Option<String>,
        #[arg(long, default_value = "5 * * * *")]
        partition_schedule: String,
        #[arg(long, default_value = "17 * * * *")]
        cleanup_schedule: String,
        #[arg(long, default_value = "29 * * * *")]
        detach_schedule: String,
    },
    /// Disable pg_cron maintenance jobs.
    CronDisable {
        #[arg(long)]
        queue: Option<String>,
    },
    /// Print the bundled `absurd.sql` schema to stdout.
    PrintSchema,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "warn,absurdctl=info".into()))
        .init();
    let cli = Cli::parse();
    let database_url = resolve_db_url(&cli.db);

    match cli.command {
        Command::PrintSchema => {
            print!("{}", absurd_sdk::BUNDLED_SCHEMA_SQL);
        }
        Command::Init { dry_run } => {
            if dry_run {
                print!("{}", absurd_sdk::BUNDLED_SCHEMA_SQL);
                return Ok(());
            }
            apply_sql(&database_url, absurd_sdk::BUNDLED_SCHEMA_SQL).await?;
            println!("schema applied");
        }
        Command::SchemaVersion => {
            let client = build_client(&database_url).await?;
            match client.schema_version().await? {
                Some(v) => println!("{}", v),
                None => println!("(unknown)"),
            }
        }
        Command::Migrate { dry_run } => {
            // Walk the bundled migration chain from the recorded version up
            // to `main`. If the schema isn't installed at all yet, fall back
            // to applying the full bundled schema.
            let probe_client = build_client(&database_url).await.ok();
            let current = match &probe_client {
                Some(c) => c.schema_version().await.ok().flatten(),
                None => None,
            };
            match current {
                None => {
                    if dry_run {
                        print!("{}", absurd_sdk::BUNDLED_SCHEMA_SQL);
                    } else {
                        apply_sql(&database_url, absurd_sdk::BUNDLED_SCHEMA_SQL).await?;
                        println!(
                            "schema installed at version {}",
                            absurd_sdk::migrations::TARGET_VERSION
                        );
                    }
                }
                Some(version) => {
                    let target = absurd_sdk::migrations::TARGET_VERSION;
                    let steps = absurd_sdk::migrations::resolve(&version, target)?;
                    if steps.is_empty() {
                        println!("schema already at {}", version);
                        return Ok(());
                    }
                    if dry_run {
                        for step in &steps {
                            println!("-- migration {}", step.filename);
                            print!("{}", step.sql);
                        }
                        return Ok(());
                    }
                    for step in &steps {
                        tracing::info!(from = step.from, to = step.to, "applying migration");
                        apply_sql(&database_url, step.sql).await?;
                    }
                    println!(
                        "migrated {} -> {} ({} step(s))",
                        version,
                        target,
                        steps.len()
                    );
                }
            }
        }
        Command::CreateQueue {
            queue,
            storage_mode,
            partition_lookahead,
            partition_lookback,
            cleanup_ttl,
            cleanup_limit,
            detach_mode,
            detach_min_age,
        } => {
            let client = build_client(&database_url).await?;
            let storage_mode = match storage_mode.as_deref() {
                Some("partitioned") => QueueStorageMode::Partitioned,
                _ => QueueStorageMode::Unpartitioned,
            };
            let detach = match detach_mode.as_deref() {
                Some("empty") => Some(QueueDetachMode::Empty),
                Some("none") => Some(QueueDetachMode::None),
                _ => None,
            };
            client
                .create_queue(
                    &queue,
                    CreateQueueOptions {
                        storage_mode,
                        policy: QueuePolicyOptions {
                            partition_lookahead,
                            partition_lookback,
                            cleanup_ttl,
                            cleanup_limit,
                            detach_mode: detach,
                            detach_min_age,
                        },
                    },
                )
                .await?;
            println!("created queue {:?}", queue);
        }
        Command::DropQueue { queue } => {
            let client = build_client(&database_url).await?;
            client.drop_queue(&queue).await?;
            println!("dropped queue {:?}", queue);
        }
        Command::ListQueues => {
            let client = build_client(&database_url).await?;
            for q in client.list_queues().await? {
                println!("{}", q);
            }
        }
        Command::SpawnTask {
            queue,
            task_name,
            params,
            max_attempts,
            idempotency_key,
            retry_kind,
            retry_base_seconds,
            retry_factor,
            retry_max_seconds,
            headers,
        } => {
            let client = build_client(&database_url).await?;
            let params_value: Value = serde_json::from_str(&params)
                .with_context(|| format!("invalid JSON for --params: {}", params))?;
            let mut headers_map: BTreeMap<String, Value> = BTreeMap::new();
            for kv in headers {
                let (k, v) = kv
                    .split_once('=')
                    .ok_or_else(|| anyhow::anyhow!("header {kv:?} must be key=value"))?;
                let parsed: Value =
                    serde_json::from_str(v).unwrap_or_else(|_| Value::String(v.to_string()));
                headers_map.insert(k.to_string(), parsed);
            }
            let retry = retry_kind.map(|kind| RetryStrategy {
                kind,
                base_seconds: retry_base_seconds,
                factor: retry_factor,
                max_seconds: retry_max_seconds,
            });
            let result = client
                .spawn(
                    &task_name,
                    params_value,
                    SpawnOptions {
                        queue_name: Some(queue),
                        max_attempts,
                        retry_strategy: retry,
                        headers: if headers_map.is_empty() {
                            None
                        } else {
                            Some(headers_map)
                        },
                        cancellation: None,
                        idempotency_key,
                    },
                )
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "task_id": result.task_id,
                    "run_id": result.run_id,
                    "attempt": result.attempt,
                    "created": result.created,
                }))?
            );
        }
        Command::RetryTask {
            queue,
            task_id,
            max_attempts,
            spawn_new,
        } => {
            let client = build_client(&database_url).await?;
            let result = client
                .retry_task(
                    &queue,
                    &task_id,
                    RetryTaskOptions {
                        max_attempts,
                        spawn_new,
                    },
                )
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "task_id": result.task_id,
                    "run_id": result.run_id,
                    "attempt": result.attempt,
                    "created": result.created,
                }))?
            );
        }
        Command::CancelTask { queue, task_id } => {
            let client = build_client(&database_url).await?;
            client.cancel_task(&queue, &task_id).await?;
            println!("cancelled");
        }
        Command::Cleanup {
            queue,
            ttl_seconds,
            limit,
            events_only,
        } => {
            let client = build_client(&database_url).await?;
            if let Some(ttl) = ttl_seconds {
                let q = queue.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("--queue is required when --ttl-seconds is used")
                })?;
                let deleted = if events_only {
                    client.cleanup_events(q, ttl, limit).await?
                } else {
                    client.cleanup_tasks(q, ttl, limit).await?
                };
                println!("{} deleted", deleted);
            } else {
                let report = client.cleanup_queues(queue.as_deref()).await?;
                for (q, r) in &report {
                    println!(
                        "{}\ttasks={}\tevents={}",
                        q, r.tasks_deleted, r.events_deleted
                    );
                }
                if report.is_empty() {
                    println!("nothing to clean");
                }
            }
        }
        Command::QueuePolicy { queue } => {
            let client = build_client(&database_url).await?;
            match client.get_queue_policy(&queue).await? {
                Some(p) => println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "queue_name": p.queue_name,
                        "storage_mode": p.storage_mode,
                        "partition_lookahead": p.partition_lookahead,
                        "partition_lookback": p.partition_lookback,
                        "cleanup_ttl": p.cleanup_ttl,
                        "cleanup_limit": p.cleanup_limit,
                        "detach_mode": p.detach_mode,
                        "detach_min_age": p.detach_min_age,
                    }))?
                ),
                None => println!("(no such queue)"),
            }
        }
        Command::SetQueuePolicy {
            queue,
            partition_lookahead,
            partition_lookback,
            cleanup_ttl,
            cleanup_limit,
            detach_mode,
            detach_min_age,
        } => {
            let client = build_client(&database_url).await?;
            let detach = match detach_mode.as_deref() {
                Some("empty") => Some(absurd_sdk::QueueDetachMode::Empty),
                Some("none") => Some(absurd_sdk::QueueDetachMode::None),
                _ => None,
            };
            client
                .set_queue_policy(
                    &queue,
                    absurd_sdk::QueuePolicyOptions {
                        partition_lookahead,
                        partition_lookback,
                        cleanup_ttl,
                        cleanup_limit,
                        detach_mode: detach,
                        detach_min_age,
                    },
                )
                .await?;
            println!("policy updated");
        }
        Command::ListDetachCandidates { queue } => {
            let client = build_client(&database_url).await?;
            let rows = client.list_detach_candidates(queue.as_deref()).await?;
            if rows.is_empty() {
                println!("(none)");
            } else {
                for r in rows {
                    println!(
                        "{}\t{}\t{}",
                        r.queue_name, r.parent_table, r.partition_table
                    );
                }
            }
        }
        Command::DropDetachedPartition { partition_table } => {
            let client = build_client(&database_url).await?;
            let ok = client.drop_detached_partition(&partition_table).await?;
            println!(
                "{}",
                if ok {
                    "dropped"
                } else {
                    "no-op (not detached)"
                }
            );
        }
        Command::CronEnable {
            queue,
            partition_schedule,
            cleanup_schedule,
            detach_schedule,
        } => {
            let client = build_client(&database_url).await?;
            let jobs = client
                .enable_cron(
                    queue.as_deref(),
                    &partition_schedule,
                    &cleanup_schedule,
                    &detach_schedule,
                )
                .await?;
            for (name, id) in jobs {
                println!("{}\t{}", id, name);
            }
        }
        Command::CronDisable { queue } => {
            let client = build_client(&database_url).await?;
            let jobs = client.disable_cron(queue.as_deref()).await?;
            for name in jobs {
                println!("{}", name);
            }
        }
    }
    Ok(())
}

fn resolve_db_url(args: &DbArgs) -> String {
    if let Some(url) = &args.database_url {
        if !url.trim().is_empty() {
            return url.clone();
        }
    }
    if let Some(db) = &args.database {
        let trimmed = db.trim();
        if !trimmed.is_empty() {
            return ensure_host(trimmed);
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
            return ensure_host(trimmed);
        }
    }
    "postgresql://localhost/absurd".to_string()
}

/// tokio-postgres won't fall back to PGHOST or to the platform default socket
/// the way `psql` does. Be tolerant of `-d <name>` by adding a sensible host
/// when one isn't already present.
fn ensure_host(input: &str) -> String {
    if input.contains("://") {
        return input.to_string();
    }
    if input.contains('=') {
        let has_host = input
            .split_whitespace()
            .any(|kv| kv.starts_with("host=") || kv.starts_with("hostaddr="));
        if has_host {
            return input.to_string();
        }
        let host = pg_host_default();
        return format!("host={} {}", host, input);
    }
    // Bare name: convert to a URL with the chosen host.
    let host = pg_host_default();
    format!("postgresql://{}/{}", host_url_encode(&host), input)
}

fn pg_host_default() -> String {
    if let Ok(value) = std::env::var("PGHOST") {
        if !value.trim().is_empty() {
            return value;
        }
    }
    // Homebrew/macports/EDB all use one of these socket dirs by default.
    for candidate in ["/tmp", "/var/run/postgresql"] {
        if std::path::Path::new(candidate).exists() {
            return candidate.to_string();
        }
    }
    "localhost".to_string()
}

fn host_url_encode(host: &str) -> String {
    // Crude — only path-like hosts need encoding for libpq-style URLs.
    host.replace('/', "%2F")
}

async fn build_client(database_url: &str) -> Result<Client> {
    Client::builder()
        .database_url(database_url)
        .build()
        .await
        .context("connecting to database")
}

async fn apply_sql(database_url: &str, sql: &str) -> Result<()> {
    use std::str::FromStr;
    let cfg = tokio_postgres::Config::from_str(database_url)
        .with_context(|| format!("invalid database URL: {database_url}"))?;
    let (client, conn) = cfg.connect(tokio_postgres::NoTls).await?;
    let handle = tokio::spawn(async move {
        if let Err(err) = conn.await {
            tracing::warn!(?err, "postgres connection ended");
        }
    });
    // Postgres can run the whole bundled file as a single multi-statement
    // batch with `simple_query`; `execute` is one statement only.
    if let Err(err) = client.batch_execute(sql).await {
        handle.abort();
        bail!("applying schema failed: {err}");
    }
    drop(client);
    let _ = handle.await;
    Ok(())
}
