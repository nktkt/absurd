# absurd-macros

Procedural macros for [`absurd-sdk`](https://github.com/nktkt/absurd) — provides the `#[task]` attribute macro for declaring typed background tasks.

## Use via `absurd-sdk`

You almost never need to depend on this crate directly. It is re-exported by `absurd-sdk` under its `macros` feature (enabled by default) as `absurd_sdk::macros::task`. Just add the SDK:

```sh
cargo add absurd-sdk
```

## Example

```rust
use absurd_sdk::macros::task;
use absurd_sdk::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Params { name: String }

#[derive(Serialize, Deserialize)]
struct Output { greeting: String }

#[task(name = "greet", queue = "default", max_attempts = 5)]
async fn greet(params: Params) -> Result<Output> {
    Ok(Output { greeting: format!("hello {}!", params.name) })
}
```

## What it generates

The macro leaves your original `async fn` intact and emits a sibling builder function next to it:

```rust,ignore
pub fn greet_task() -> TaskDefinition<Params, Output> { /* ... */ }
```

You can then register the task with the client, e.g. `client.register_task(greet_task())`. The builder applies any `queue` and `max_attempts` arguments you supplied to the attribute.

## Supported arguments

All arguments are optional:

- `name = "..."` — the task name used on the wire. Defaults to the function's identifier.
- `queue = "..."` — the queue the task is dispatched on.
- `max_attempts = N` — maximum retry attempts (integer literal).

## Constraints

The annotated function must:

- be `async`,
- take exactly one parameter (the task params, a plain identifier — no patterns or `self`),
- return `Result<R, E>` (typically `absurd_sdk::Result<R>`).

Violations produce a compile-time error.

## License

Licensed under the Apache License, Version 2.0. See the workspace root at <https://github.com/nktkt/absurd> for details.
