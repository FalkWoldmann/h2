# Tasks: Runtime-Agnostic h2

**Input**: Design documents from `/specs/001-runtime-agnostic/`  
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Only include tests where noted; integration and benchmarks are required for runtime parity.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and baseline hygiene

- [X] T001 Ensure MSRV 1.63 is enforced and documented in Cargo.toml and README.md (Cargo.toml already set; README.md still to update if needed)
- [ ] T002 [P] Add runtime-agnostic feature flags scaffold in Cargo.toml (default runtime retained)
- [ ] T003 [P] Add skeleton runtime adapter trait module in src/runtime/mod.rs with doc comments
- [ ] T004 [P] Align rustfmt and clippy configs for new adapter modules (check ci/ and rust-toolchain hints)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T005 Define runtime capability interfaces (I/O, timers, spawn, wakers) in src/runtime/traits.rs
- [ ] T006 Add compile-time gating for default runtime (tokio) behind feature flag in Cargo.toml and src/runtime/tokio.rs
- [ ] T007 Create stub alternative runtime adapter module in src/runtime/alt.rs with capability mapping placeholders
- [ ] T008 Update protocol entry points (src/lib.rs) to accept injected runtime adapters without breaking current API
- [ ] T009 Establish baseline benchmarks in benches/main.rs capturing latency/throughput for current runtime
- [ ] T010 Wire CI to run clippy and tests for both default and alt-runtime feature sets in ci/ (add matrix)
- [ ] T011 Document adapter expectations in contracts/runtime-adapter.md and link from README.md

---

## Phase 3: User Story 1 - Use h2 with alternative runtimes (Priority: P1) 🎯 MVP

**Goal**: Enable HTTP/2 client/server operation on an alternative async runtime without pulling the default runtime.

**Independent Test**: Run h2 over an alternative runtime, completing request/response round-trip and graceful shutdown without default runtime linkage.

### Tests for User Story 1 (required)

- [ ] T012 [P] [US1] Add integration test using alternative runtime adapter in tests/h2-tests/tests/runtime_alt.rs
- [ ] T013 [P] [US1] Add adapter conformance tests for capabilities (I/O, timers, spawn) in tests/h2-tests/tests/runtime_capabilities.rs

### Implementation for User Story 1

- [ ] T014 [P] [US1] Implement alternative runtime adapter capabilities in src/runtime/alt.rs
- [ ] T015 [US1] Update connection builders to accept injected adapter in src/client.rs and src/server.rs
- [ ] T016 [US1] Add feature flag wiring for alternative runtime in src/lib.rs and Cargo.toml
- [ ] T017 [US1] Add example demonstrating alternative runtime usage in examples/alt-runtime.rs
- [ ] T018 [US1] Update quickstart with alternative runtime steps in specs/001-runtime-agnostic/quickstart.md

**Checkpoint**: Alternative runtime builds, integration tests pass, example runs without default runtime dependency.

---

## Phase 4: User Story 2 - Maintain compatibility across runtimes (Priority: P2)

**Goal**: Ensure behavior parity and performance thresholds across supported runtimes.

**Independent Test**: Cross-runtime suites and benchmarks pass for each runtime with parity thresholds met.

### Tests for User Story 2 (required)

- [ ] T019 [P] [US2] Add cross-runtime matrix tests in ci workflows to run tests/h2-tests across default and alternative runtimes
- [ ] T020 [P] [US2] Add performance benchmark comparison harness in benches/main.rs for both runtimes

### Implementation for User Story 2

- [ ] T021 [P] [US2] Add compatibility suite runner script in ci/compat.sh to execute per-runtime matrices
- [ ] T022 [US2] Add metrics collection for latency/throughput and compare against baseline in benches/main.rs
- [ ] T023 [US2] Document parity thresholds and reporting in specs/001-runtime-agnostic/research.md and README.md

**Checkpoint**: CI matrix green across runtimes; benchmarks within <5% delta from baseline.

---

## Phase 5: User Story 3 - Migrate existing users smoothly (Priority: P3)

**Goal**: Preserve current runtime users while enabling opt-in alternative runtime with clear migration.

**Independent Test**: Upgrading with defaults requires no code changes; opting into alternative runtime works per docs.

### Implementation for User Story 3

- [ ] T024 [P] [US3] Verify default runtime build path unchanged in tests/h2-tests/tests/runtime_default.rs
- [ ] T025 [US3] Add migration notes and feature flag guidance to CHANGELOG.md and README.md
- [ ] T026 [US3] Add release note entry for runtime-agnostic support in specs/001-runtime-agnostic/research.md
- [ ] T027 [US3] Ensure public API docs.rs examples compile for both runtime feature sets in docs/ or README.md snippets

**Checkpoint**: Default users unaffected; migration guidance published; examples compile under both feature sets.

---

## Phase N: Polish & Cross-Cutting Concerns

- [ ] T028 [P] Refine adapter docs with troubleshooting for missing capabilities in contracts/runtime-adapter.md
- [ ] T029 [P] Add tracing coverage verification around adapter boundaries in src/runtime/mod.rs
- [ ] T030 [P] Run cargo fmt, cargo clippy, and full test/bench matrix for both runtimes before release

---

## Dependencies & Execution Order

### Phase Dependencies

- Setup (Phase 1) → Foundational (Phase 2) → User Stories (Phases 3–5) → Polish
- User stories can proceed in priority order once Foundational is complete.

### User Story Dependencies

- US1 has no dependency on other stories once Foundational is done.
- US2 depends on US1 adapter availability to measure parity.
- US3 depends on US1 to provide alternative path and on US2 parity insights for migration notes.

### Parallel Opportunities

- T002/T003/T004 can run in parallel.  
- In US1: T012 and T013 in parallel; T014 and T017 in parallel after T005–T011.  
- In US2: T019 and T020 in parallel; T021/T022 in parallel after T019.  
- Polish tasks T028–T030 can run in parallel after story checkpoints.

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup  
2. Complete Phase 2: Foundational  
3. Complete Phase 3: User Story 1 (alternative runtime working, tests passing)  
4. Stop and validate MVP before proceeding.

### Incremental Delivery

1. Deliver US1 (runtime adapter + alt runtime tests)  
2. Deliver US2 (compatibility matrix + benchmarks)  
3. Deliver US3 (migration and docs)  
4. Apply Polish tasks.
