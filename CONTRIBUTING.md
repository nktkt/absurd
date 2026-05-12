# Contributing

Thanks for your interest in `absurd`. Issues and pull requests are welcome. For anything non-trivial (new features, behavior changes, larger refactors), please open an issue first so we can agree on the approach before you spend time on a patch.

## Setup

You will need:

- Rust 1.85 or newer (stable toolchain).
- PostgreSQL 14 or newer if you want to run the integration tests and examples.

```sh
git clone https://github.com/nktkt/absurd
cd absurd
cargo build --workspace
```

The workspace is laid out as:

- `crates/absurd-sdk` — the SDK.
- `crates/absurd-macros` — the `#[task]` proc-macro.
- `crates/absurdctl` — the CLI.
- `crates/absurd-axum` — the Axum integration.

## Running tests

Unit tests do not require a database:

```sh
cargo test --workspace --lib --tests
```

For end-to-end runs against a real Postgres:

```sh
createdb absurd_dev
cargo run -p absurdctl -- -d absurd_dev init
cargo run -p absurd-sdk --example order_fulfillment
```

## Style

Before sending a PR, run:

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

CI enforces both, so anything that fails locally will fail there too.

## Schema updates

The SQL files under `crates/absurd-sdk/sql/absurd.sql` and `crates/absurd-sdk/sql/migrations/` are mirrored from upstream `earendil-works/absurd`. Please don't hand-edit them. To pull a schema change, rev the bundled copy by copying the new files from upstream verbatim, then add the corresponding migration step to `crates/absurd-sdk/src/migrations.rs`.

## Feature flag matrix

When you touch anything related to the connection pool or TLS, verify the SDK builds under all three supported configurations:

```sh
cargo build -p absurd-sdk
cargo build -p absurd-sdk --features rustls
cargo build -p absurd-sdk --no-default-features --features native-tls
```

## Commit messages

Use a short summary line, then a body that explains *why* the change is being made (not just what changed). There is no required format or prefix.

## License

By submitting a pull request, you agree that your contribution is licensed under the Apache License, Version 2.0, the same license as the rest of the project.
