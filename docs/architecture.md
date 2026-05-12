# Rust SDK Architecture

This document describes how the Absurd Rust SDK (`crates/absurd-sdk`) is laid out, the contract it has with Postgres, and the runtime behaviour of the worker. It is intended for developers who want to contribute to the SDK or debug an existing integration.

## High Level

The SDK has two concerns and very little state of its own.

- **Client side**: spawning tasks, managing queues, and inspecting/controlling tasks that already exist.
- **Worker side**: claiming tasks, executing handlers, recording checkpoints, and reporting completion or failure.

All durable state — tasks, runs, checkpoints, events, queue policies, cron schedules — lives in Postgres under the `absurd` schema. The Rust process keeps only two pieces of in-memory state:

1. The **task registry** (a map from task name to handler) that the worker uses to dispatch claimed work.
2. A **per-run checkpoint cache** primed once at the start of each run so step replay does not re-read every checkpoint over the wire.

Everything else (retries, leases, scheduling, partition lifecycle) is delegated to SQL functions.

## Module Map

All paths are under `crates/absurd-sdk/src/`.

- `lib.rs` — public surface; re-exports the types that make up the SDK API.
- `client.rs` — `Client` and `ClientBuilder`. Holds the pool and exposes `spawn`, queue administration, and the low-level run primitives (`claim_tasks`, `complete_run`, `fail_run`, `defer_claimed_run`) that the worker loop is built on.
- `worker.rs` — `Client::work_batch` (single-pass) and `Client::run_worker` (long-running loop), plus the lease watchdog task that extends claims while a handler is executing.
- `context.rs` — `TaskContext` (passed to every handler) with `step`, `sleep_for`, and `await_event`. Also defines the task-local `CURRENT_TASK` that handlers can use to reach the context implicitly.
- `task.rs` — the `TaskHandler` trait, the typed `TaskDefinition<Input, Output>`, and the `task()` factory used to register handlers in the registry.
- `hooks.rs` — `Hooks` container plus `BeforeSpawnHook` and `WrapTaskExecutionHook` extension points for tracing, auth propagation, etc.
- `options.rs` — `SpawnOptions`, `WorkerOptions`, `RetryStrategy`, `ShutdownHandle`, and the other option/value structs.
- `migrations.rs` — the bundled migration chain and the resolver that decides which migrations to apply against an existing schema.
- `tls.rs` — connection-pool TLS dispatch, gated by the `tls-native`/`tls-rustls` feature flags.
- `error.rs` — `AbsurdError`, `TaskStateError`, and the mapping from Absurd SQLSTATEs (notably `AB001`, `AB002`) to typed Rust errors.
- `util.rs` — queue name validation and DSN resolution helpers shared by client and worker.

## Postgres Surface

The SDK never writes ad-hoc SQL against tables; it only calls SQL functions in the `absurd` schema. The full set the SDK invokes:

- Task lifecycle: `absurd.spawn_task`, `absurd.claim_task`, `absurd.complete_run`, `absurd.fail_run`, `absurd.schedule_run`, `absurd.extend_claim`, `absurd.cancel_task`, `absurd.retry_task`, `absurd.get_task_result`.
- Checkpoints: `absurd.set_task_checkpoint_state`, `absurd.get_task_checkpoint_state`, `absurd.get_task_checkpoint_states`.
- Events: `absurd.await_event`, `absurd.emit_event`.
- Queues: `absurd.create_queue`, `absurd.drop_queue`, `absurd.list_queues`, `absurd.get_queue_policy`, `absurd.set_queue_policy`.
- Maintenance: `absurd.cleanup_all_queues`, `absurd.cleanup_tasks`, `absurd.cleanup_events`, `absurd.list_detach_candidates`, `absurd.drop_detached_partition`.
- Cron: `absurd.enable_cron`, `absurd.disable_cron`.
- Schema: `absurd.get_schema_version`.

UUID parameters are always bound as text and cast on the server side (`$N::text::uuid`). This lets the driver stay free of the `uuid` Postgres extension and avoids forcing client libraries to negotiate a non-standard OID.

## Worker Lifecycle

`work_batch` (and `run_worker`, which is a loop around it) does the following for each claimed task:

1. **Claim** a batch via `absurd.claim_task` (returns the run row plus identifying metadata).
2. **Build a `TaskContext`**, priming the checkpoint cache from `absurd.get_task_checkpoint_states` so the entire replay history is in memory before the handler starts.
3. **Run the handler** inside `CURRENT_TASK.scope(...)` so the context is reachable from anywhere in the handler's call tree.
4. **Resolve the outcome**:
   - `Ok(output)` — call `absurd.complete_run` with the serialised output.
   - `Err(TaskStateError::Suspended)` — no-op. The handler has already called `absurd.schedule_run` (via `sleep_for` or `await_event`), and the run is now parked.
   - Any other `Err` — call `absurd.fail_run`, letting the server decide whether to retry, defer, or move to dead-letter according to the `RetryStrategy`.

Alongside this, the **lease watchdog** runs as a separate Tokio task. It extends the claim on a cadence derived from the lease length. Every heartbeat and every checkpoint write goes through a `LeaseObserver` plumbed into `TaskContext`, which resets the watchdog so handlers that make steady forward progress never lose their lease.

## Step Replay

`step`, `sleep_for`, and `await_event` all funnel into the same machinery. Each call increments a per-step-name counter and produces a unique checkpoint key: `name`, `name#2`, `name#3`, and so on. This means handlers can call `step("fetch", ...)` in a loop and each invocation deterministically maps to its own checkpoint.

Because the checkpoint cache is primed once at the start of the run, replay is local: every `step` call after a resume hits memory, not Postgres, until it reaches the first checkpoint that has not been recorded yet.

## Shutdown

`ShutdownHandle` is a thin wrapper around `tokio::sync::Notify` plus an `AtomicBool`. The atomic flag makes "are we shutting down?" cheap to check; the `Notify` wakes the worker loop immediately.

The worker loop `select!`s between `claim_tasks` and `shutdown.wait()`. When shutdown fires, the loop stops claiming new work and then joins (drains) any in-flight handler tasks before returning. This gives a clean stop: handlers either finish, suspend themselves, or report failure — never get torn down mid-execution.
