# absurd-sdk examples

## `order_fulfillment.rs`

A port of the README order-fulfillment walkthrough. Registers a
`Value`-based handler via `register_fn`, drives a small workflow that calls
`step` (`process-payment`, `reserve-inventory`, `send-notification`) and
suspends on `await_event` for a `shipment.packed:<order_id>` signal, then
runs a background worker, spawns the task, emits the awaited event, and
blocks on `await_task_result` until the task completes.

## `typed_task.rs`

The same end-to-end flow expressed with the `#[task]` proc-macro and
strongly-typed `Params` / `Output` structs. Registers the generated task
descriptor with `register_task(greet_task())`, spawns it via `spawn_typed`,
and decodes the typed result from the returned snapshot with
`decode_result()`.

## How to run

```sh
createdb absurd
cargo run -p absurdctl -- -d absurd init
cargo run -p absurdctl -- -d absurd create-queue default
ABSURD_DATABASE_URL="postgresql:///absurd?host=/tmp" \
  cargo run -p absurd-sdk --example order_fulfillment
```

The examples assume Postgres is running locally. Otherwise, set
`ABSURD_DATABASE_URL` to point at whichever cluster you want to use.
