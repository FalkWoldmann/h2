# h2 Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-01-20

## Active Technologies

- Rust 1.63 (MSRV); async Rus + bytes, http, futures-core/sink; tokio/tokio-util current but to be abstracted behind adapter; tracing (001-runtime-agnostic)

## Project Structure

```text
src/
tests/
```

## Commands

cargo test [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] cargo clippy

## Code Style

Rust 1.63 (MSRV); async Rus: Follow standard conventions
- Rust: keep rustfmt + clippy clean; prefer strong types and tracing over prints; add tests for protocol-visible changes

## Recent Changes

- 001-runtime-agnostic: Added Rust 1.63 (MSRV); async Rus + bytes, http, futures-core/sink; tokio/tokio-util current but to be abstracted behind adapter; tracing

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
