use absurd_sdk::{
    Client, CreateQueueOptions, QueueDetachMode, QueuePolicyOptions, QueueStorageMode, RetryStrategy,
    RetryTaskOptions, SpawnOptions,
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
    /// Print the bundled `absurd.sql` schema to stdout.
    PrintSchema,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "warn,absurdctl=info".into()),
        )
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
            // The bundled schema is idempotent: re-applying it is the
            // simplest correct "migrate to latest" path for the Rust port.
            if dry_run {
                print!("{}", absurd_sdk::BUNDLED_SCHEMA_SQL);
                return Ok(());
            }
            apply_sql(&database_url, absurd_sdk::BUNDLED_SCHEMA_SQL).await?;
            let client = build_client(&database_url).await?;
            let version = client.schema_version().await?.unwrap_or_default();
            println!("schema at version {}", version);
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
                let parsed: Value = serde_json::from_str(v).unwrap_or_else(|_| Value::String(v.to_string()));
                headers_map.insert(k.to_string(), parsed);
            }
            let retry = if retry_kind.is_some() {
                Some(RetryStrategy {
                    kind: retry_kind.unwrap(),
                    base_seconds: retry_base_seconds,
                    factor: retry_factor,
                    max_seconds: retry_max_seconds,
                })
            } else {
                None
            };
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
