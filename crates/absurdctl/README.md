# absurdctl

Control-plane CLI for [Absurd](https://github.com/nktkt/absurd) — durable workflows on Postgres. Manages schemas, queues, tasks, and maintenance for an Absurd-backed database. Rust port of the original Python tool.

## Installation

From crates.io:

```sh
cargo install absurdctl
```

From source:

```sh
cargo install --path crates/absurdctl
```

## Connecting

Every command accepts the following connection options:

- `-d <dbname>` / `--database <dbname>` — convenience shorthand, equivalent to `dbname=<value>`.
- `--database-url <url>` — full libpq-style URL or key/value connection string.

If neither flag is passed, the CLI falls back to environment variables in this order:

1. `ABSURD_DATABASE_URL`
2. `PGDATABASE`

If nothing is set, it defaults to `postgresql://localhost/absurd`.

### Host auto-injection

`tokio-postgres` is stricter than `libpq`: it will not fall back to `PGHOST` or to the platform's default Unix socket directory the way `psql` does. To keep `-d <dbname>` ergonomic, the CLI auto-injects a host when only a bare dbname (or a key/value string without `host=`/`hostaddr=`) is supplied. The host is chosen in this order:

1. `PGHOST`, if set.
2. `/tmp` (Homebrew, MacPorts, EDB), if it exists.
3. `/var/run/postgresql` (Debian/Ubuntu default), if it exists.
4. `localhost` as a last resort.

A full `postgresql://...` URL is always passed through untouched.

## Command reference

### Schema

#### `init`

Apply the bundled `absurd.sql` schema to the target database. Use `--dry-run` to print the SQL instead of running it.

```sh
absurdctl -d absurd init
```

#### `migrate`

Walk the bundled migration chain from the recorded schema version up to the target version. Use `--dry-run` to print the SQL for each step.

> Note: the bundled chain inherits the gap at `0.0.7 -> 0.0.8` from upstream. When no schema is present in the database, `migrate` falls back to a full `init` and stamps the bundled version.

```sh
absurdctl -d absurd migrate
```

#### `schema-version`

Print the schema version currently recorded in the database.

```sh
absurdctl -d absurd schema-version
```

#### `print-schema`

Dump the bundled `absurd.sql` schema to stdout (useful for piping into `psql` or capturing for review).

```sh
absurdctl print-schema > absurd.sql
```

### Queues

#### `create-queue`

Create a queue. Supports storage mode (`unpartitioned` | `partitioned`) and the full set of policy fields.

```sh
absurdctl -d absurd create-queue jobs \
  --storage-mode partitioned \
  --partition-lookahead 1d \
  --partition-lookback 7d \
  --cleanup-ttl 14d \
  --cleanup-limit 1000 \
  --detach-mode empty \
  --detach-min-age 30d
```

#### `drop-queue`

Drop a queue.

```sh
absurdctl -d absurd drop-queue jobs
```

#### `list-queues`

List all configured queues, one per line.

```sh
absurdctl -d absurd list-queues
```

#### `queue-policy`

Print the persisted policy for a queue as JSON.

```sh
absurdctl -d absurd queue-policy jobs
```

#### `set-queue-policy`

Update one or more policy fields on an existing queue. Only the fields you pass are changed.

```sh
absurdctl -d absurd set-queue-policy jobs \
  --cleanup-ttl 30d \
  --detach-mode empty
```

### Tasks

#### `spawn-task`

Enqueue a task. `--params` takes a JSON string (defaults to `null`). `--header key=value` may be repeated; values that parse as JSON are stored as JSON, otherwise as strings.

```sh
absurdctl -d absurd spawn-task jobs send_email \
  --params '{"to":"alice@example.com","subject":"hi"}' \
  --max-attempts 5 \
  --idempotency-key send-alice-2026-05-12 \
  --retry-kind exponential \
  --retry-base-seconds 1.0 \
  --retry-factor 2.0 \
  --retry-max-seconds 300 \
  --header tenant=acme \
  --header priority=10
```

Prints the spawn result (`task_id`, `run_id`, `attempt`, `created`) as JSON.

#### `retry-task`

Retry a failed task. Pass `--spawn-new` to create a fresh task row instead of resuming the existing one.

```sh
absurdctl -d absurd retry-task jobs 01HXYZ... --max-attempts 3 --spawn-new
```

#### `cancel-task`

Cancel a task.

```sh
absurdctl -d absurd cancel-task jobs 01HXYZ...
```

### Maintenance

#### `cleanup`

Run cleanup across queues (deletes expired tasks/events per queue policy). With `--queue` and `--ttl-seconds`, runs a one-shot cleanup using an explicit TTL instead of the queue's persisted policy. `--events-only` restricts the explicit-TTL form to events.

```sh
# Honor each queue's persisted policy
absurdctl -d absurd cleanup

# Only one queue, persisted policy
absurdctl -d absurd cleanup --queue jobs

# Explicit TTL override, tasks + events, batched
absurdctl -d absurd cleanup --queue jobs --ttl-seconds 86400 --limit 5000

# Explicit TTL override, events only
absurdctl -d absurd cleanup --queue jobs --ttl-seconds 86400 --events-only
```

#### `list-detach-candidates`

List partitions eligible for detachment, optionally filtered to one queue.

```sh
absurdctl -d absurd list-detach-candidates --queue jobs
```

#### `drop-detached-partition`

Drop a previously detached partition table.

```sh
absurdctl -d absurd drop-detached-partition absurd_jobs_p20260101
```

#### `cron-enable`

Install `pg_cron` jobs that drive the partition, cleanup, and detach maintenance loops. Schedules are standard cron expressions and default to `5 * * * *`, `17 * * * *`, and `29 * * * *` respectively.

```sh
absurdctl -d absurd cron-enable \
  --queue jobs \
  --partition-schedule '5 * * * *' \
  --cleanup-schedule  '17 * * * *' \
  --detach-schedule   '29 * * * *'
```

#### `cron-disable`

Remove the `pg_cron` maintenance jobs. Pass `--queue` to limit to a single queue.

```sh
absurdctl -d absurd cron-disable --queue jobs
```

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

See <https://github.com/nktkt/absurd> for the project source and issue tracker.
