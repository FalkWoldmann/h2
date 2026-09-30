# Feature Specification: Runtime-Agnostic h2

**Feature Branch**: `001-runtime-agnostic`  
**Created**: 2026-01-20  
**Status**: Draft  
**Input**: Make this h2 crate agnostic over async runtimes, so we can fully decouple this from tokio. The goal is to make this library usable with other async runtimes like compio.

## User Scenarios & Testing *(mandatory)*

<!--
  IMPORTANT: User stories should be PRIORITIZED as user journeys ordered by importance.
  Each user story/journey must be INDEPENDENTLY TESTABLE - meaning if you implement just ONE of them,
  you should still have a viable MVP (Minimum Viable Product) that delivers value.
  
  Assign priorities (P1, P2, P3, etc.) to each story, where P1 is the most critical.
  Think of each story as a standalone slice of functionality that can be:
  - Developed independently
  - Tested independently
  - Deployed independently
  - Demonstrated to users independently
-->

### User Story 1 - Use h2 with alternative runtimes (Priority: P1)

Runtime integrators can adopt h2 in non-default async runtimes to run HTTP/2 clients and servers without depending on a specific executor.

**Why this priority**: Unlocks adoption in ecosystems that do not use the current default runtime, expanding compatibility and reducing vendor lock-in.

**Independent Test**: Run the crate in an alternative runtime with provided adapter, complete a request/response round-trip and graceful shutdown without linking the default runtime.

**Acceptance Scenarios**:

1. **Given** a project using an alternative async runtime, **When** h2 is integrated via the provided adapter, **Then** HTTP/2 requests and responses complete successfully without pulling in the default runtime.
2. **Given** an alternative runtime adapter, **When** concurrent streams are exercised, **Then** flow control, pings, and graceful shutdown work without executor-specific code.

---

### User Story 2 - Maintain compatibility across runtimes (Priority: P2)

Maintainers can verify h2 works across multiple supported runtimes through automated compatibility tests and benchmarks.

**Why this priority**: Prevents regressions and ensures parity of behavior across executors as the code evolves.

**Independent Test**: Run cross-runtime test suites and benchmarks; results must show parity within defined thresholds across supported runtimes.

**Acceptance Scenarios**:

1. **Given** CI configured for multiple runtimes, **When** compatibility suites run, **Then** all protocol correctness tests pass for each runtime.
2. **Given** benchmark baselines, **When** performance is measured on each supported runtime, **Then** throughput and latency stay within agreed thresholds.

---

### User Story 3 - Migrate existing users smoothly (Priority: P3)

Existing users relying on the current runtime can upgrade without breaking changes while gaining access to alternative runtime support.

**Why this priority**: Preserves stability for current users and reduces friction for adoption.

**Independent Test**: Upgrade h2 in an existing project using the current runtime; builds and runtime behavior remain unchanged or are explicitly documented when feature flags differ.

**Acceptance Scenarios**:

1. **Given** an existing project using the current runtime, **When** upgrading to the new release with default settings, **Then** it builds and functions without code changes.
2. **Given** users opting into the alternative runtime adapter, **When** they follow migration notes, **Then** they can run without the current runtime dependency.

---

[Add more user stories as needed, each with an assigned priority]

### Edge Cases

- Alternative runtime lacks required primitives (e.g., timers, TCP) — provide clear errors and documented adapter expectations.
- Mixed executors in the same process — ensure adapters do not deadlock or leak tasks when paired incorrectly.
- Blocking or long-running operations introduced via adapters — ensure backpressure and cancellation still work.
- Feature flags excluding the default runtime — crate should compile and pass targeted tests without pulling that dependency.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The library MUST expose runtime-agnostic public APIs without requiring a specific async runtime to build or link.
- **FR-002**: The library MUST offer an adapter contract that maps required runtime capabilities (I/O, timers, tasks, wakers) and validate adapters with automated tests.
- **FR-003**: The library MUST support at least two runtimes in CI (the existing default and one alternative) with passing HTTP/2 correctness suites.
- **FR-004**: The library MUST provide a migration path so existing users can upgrade without code changes when keeping the current runtime, and with documented steps when opting into alternatives.
- **FR-005**: The library MUST include user-facing documentation and examples showing configuration and usage on an alternative runtime.

### Key Entities *(include if feature involves data)*

- **Runtime Adapter**: Represents the set of capabilities an async runtime provides (network I/O, timers, spawning, waking) and the hooks the library uses to operate.
- **Compatibility Suite**: The collection of protocol and behavioral tests executed against each supported runtime to validate parity.

### Key Entities *(include if feature involves data)*

- **[Entity 1]**: [What it represents, key attributes without implementation]
- **[Entity 2]**: [What it represents, relationships to other entities]

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Building the crate with the default runtime disabled succeeds and all targeted tests pass under an alternative runtime.
- **SC-002**: Cross-runtime compatibility suites pass for at least two runtimes in CI across two consecutive runs.
- **SC-003**: Throughput and latency under an alternative runtime stay within 5% of the current baseline on representative benchmarks.
- **SC-004**: Developers can follow the documentation to integrate the crate with an alternative runtime and complete a request/response demo in under 30 minutes without referencing external sources.
