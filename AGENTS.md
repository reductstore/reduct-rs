# Repository Guidelines

## Project Structure & Module Organization
- Library entry is `src/lib.rs`; client surface is split into `client.rs`, `http_client.rs`, and feature modules under `src/bucket`, `src/record`, and `src/replication.rs`.
- Examples demonstrating API usage live in `examples/hallo_world.rs` and `examples/query.rs`; use them as runnable docs.
- Tests sit next to the code inside `mod tests` blocks and rely on async Tokio + `rstest`; there is no standalone `tests/` folder.
- `Cargo.toml` pins Rust 1.89 (edition 2021) and defines the `test-api-118` feature used for compatibility checks against older API behavior.

## Build, Test, and Development Commands
- `cargo fmt --all` — required by CI; keep it clean before pushing.
- `cargo check` — fast sanity compile (also wired in pre-commit).
- `cargo build --release` — produce optimized artifacts.
- Start ReductStore locally before tests: `docker run --rm --network host -e RS_API_TOKEN=TOKEN reduct/store:latest` (expects HTTP on `127.0.0.1:8383`).
- `RS_API_TOKEN=TOKEN cargo test --features default -- --test-threads=1` — main test suite; use `--features "default,test-api-118"` to exercise the backward-compat matrix (dev branch).
- `cargo run --example hallo_world` (or `query`) — run examples against the local server.

## Coding Style & Naming Conventions
- Rustfmt defaults (4-space indent) are the source of truth; run before review.
- Use snake_case for modules/functions, CamelCase for types/traits, and SCREAMING_SNAKE_CASE for consts; mirror existing builder/async patterns for new APIs.
- Prefer small, composable functions; propagate errors with `Result<T, ReductError>` like existing calls.
- Optional: install pre-commit (`pre-commit install`) to mirror CI hooks (`fmt`, `cargo-check`, whitespace fixes).
- Don't create examples or update README for minor features unless explicitly requested in an issue.

## Testing Guidelines
- Tests are async (`#[tokio::test]`) and fixture-driven via `rstest`; add new cases beside the relevant module.
- Some replication/license checks are gated by `test-with` and only run if `misc/lic.key` exists; keep them optional but working.
- Tests assume a clean server state and remove `test-*` resources; follow that pattern for new setups to avoid cross-test pollution.
- Document any new env vars or server flags needed for your tests in the PR body.
- **Always test with both `reduct/store:latest` and `reduct/store:main` Docker tags** to ensure compatibility with stable and development versions of ReductStore.

## Commit & Pull Request Guidelines
- Commit messages are short and imperative; include scope or issue/PR numbers when helpful (e.g., `Add base_url to Bucket.create_query_link (#51)`), and use `release vX.Y.Z` for tagged releases.
- PRs should describe the change, API versions or features touched, and list the commands you ran (fmt/check/tests/examples).
- Link related issues, call out breaking or backward-compat implications, and keep secrets (tokens, licenses) out of commits by using env vars instead.
- Use https://raw.githubusercontent.com/reductstore/.github/refs/heads/main/.github/pull_request_template.md for PR creation.
