# Implementation Plan: Runtime-Agnostic h2

**Branch**: `001-runtime-agnostic` | **Date**: 2026-01-20 | **Spec**: specs/001-runtime-agnostic/spec.md
**Input**: Make this h2 crate agnostic over async runtimes, so we can fully decouple this from tokio. The goal is to make this library usable with other async runtimes like compio.

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Decouple h2 from a single async runtime by introducing a runtime-agnostic interface and adapters, while preserving current users’ defaults and ensuring cross-runtime parity through tests and benchmarks.

## Technical Context

<!--
  ACTION REQUIRED: Replace the content in this section with the technical details
  for the project. The structure here is presented in advisory capacity to guide
  the iteration process.
-->

**Language/Version**: Rust 1.63 (MSRV); async Rust  
**Primary Dependencies**: bytes, http, futures-core/sink; tokio/tokio-util current but to be abstracted behind adapter; tracing  
**Storage**: N/A (in-memory/protocol only)  
**Testing**: cargo test; integration suites in tests/h2-tests; fuzz/property in tests/h2-fuzz; benchmarks in benches/ (ensure alt-runtime coverage)  
**Target Platform**: Linux/macOS server environments; async executors (current default + alternative, e.g., compio)  
**Project Type**: single library crate  
**Performance Goals**: Preserve current throughput/latency; allow <5% regression when using alternative runtime; maintain allocation profile on hot paths  
**Constraints**: Maintain MSRV 1.63; avoid mandatory deps on any single runtime; keep public API backward compatible by default; minimize new dependencies  
**Scale/Scope**: Protocol-level library; supports high-concurrency HTTP/2 streams with flow control and ping/pong

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- Code Quality Discipline: rustfmt/clippy clean; adapters modular; tracing preserved or improved; no blanket `allow`; adapter API documented.
- Rigorous Testing Standards: Add unit tests for adapter surfaces; integration suites for alternative runtime; fuzz/property for framing unchanged; tests written before impl.
- User Experience Consistency: Maintain backward-compatible defaults; document any feature flags; keep error/log formats stable; update examples/docs.
- Performance & Efficiency: Benchmarks across runtimes; target <5% regression; measure allocations/locks in hot paths; prefer zero-copy buffers.
- Change Management & Documentation: Migration notes for current users; observability hooks retained; release notes call out runtime-agnostic changes; deprecation timelines if any.

## Project Structure

### Documentation (this feature)

```text
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)
<!--
  ACTION REQUIRED: Replace the placeholder tree below with the concrete layout
  for this feature. Delete unused options and expand the chosen structure with
  real paths (e.g., apps/admin, packages/something). The delivered plan must
  not include Option labels.
-->

```text
# [REMOVE IF UNUSED] Option 1: Single project (DEFAULT)
src/
├── models/
├── services/
├── cli/
└── lib/

tests/
├── contract/
├── integration/
└── unit/

# [REMOVE IF UNUSED] Option 2: Web application (when "frontend" + "backend" detected)
backend/
├── src/
│   ├── models/
│   ├── services/
│   └── api/
└── tests/

frontend/
├── src/
│   ├── components/
│   ├── pages/
│   └── services/
└── tests/

# [REMOVE IF UNUSED] Option 3: Mobile + API (when "iOS/Android" detected)
api/
└── [same as backend above]

ios/ or android/
└── [platform-specific structure: feature modules, UI flows, platform tests]
```

**Structure Decision**: Single library crate under src/ with tests/ (integration, unit, fuzz) and benches/; no additional projects required.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| None | N/A | N/A |
