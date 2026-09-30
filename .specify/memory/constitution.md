<!--
Sync Impact Report
- Version change: 0.0.0 → 1.0.0
- Modified principles: [PRINCIPLE_1_NAME] → Code Quality Discipline; [PRINCIPLE_2_NAME] → Rigorous Testing Standards; [PRINCIPLE_3_NAME] → User Experience Consistency; [PRINCIPLE_4_NAME] → Performance & Efficiency; [PRINCIPLE_5_NAME] → Change Management & Documentation
- Added sections: Engineering Standards; Development Workflow & Quality Gates
- Removed sections: none
- Templates requiring updates: ✅ .specify/templates/plan-template.md | ✅ .specify/templates/tasks-template.md | ✅ .specify/templates/spec-template.md (reviewed, no change needed) | ✅ .specify/templates/checklist-template.md (reviewed, no change needed) | ✅ .specify/templates/agent-file-template.md (reviewed, no change needed)
- Follow-up TODOs: TODO(RATIFICATION_DATE): unknown original adoption date; replace when known
-->
# H2 Constitution

## Core Principles

### Code Quality Discipline
Maintain clear, idiomatic Rust with rustfmt and clippy clean builds; prefer strong typing and small, composable modules; forbid broad `#[allow]` without `#[expect(reason = "...")]`; instrument meaningful tracing instead of ad-hoc prints; code reviews must reject changes that erode readability or increase technical debt without a mitigation plan.

### Rigorous Testing Standards
Adopt test-first where feasible; every behavior change requires failing tests before implementation; unit tests cover state-machine edges, flow control, and error paths; integration tests validate HTTP/2 interoperability and regressions; property/fuzz tests guard framing and parsing correctness; no PR merges without automated tests for critical paths and protocol-visible changes.

### User Experience Consistency
Public APIs must preserve backward compatibility unless a MAJOR release is declared with migration notes; configuration defaults stay safe and unsurprising; errors and logs are actionable with stable formats; docs and examples update alongside behavior; naming and ergonomics stay consistent across client and server surfaces.

### Performance & Efficiency
Protect latency and throughput budgets: no changes may introduce >5% regression in benchmarks without explicit approval and remediation plan; measure allocations and locking on hot paths; prefer zero-copy and reuse buffers where practical; profile under representative loads before shipping significant protocol changes.

### Change Management & Documentation
Every change includes concise rationale, migration notes (if user-facing), and links to relevant tests or benchmarks; observability hooks (metrics/tracing) must be kept current; deprecations require timelines and shims; release notes highlight user-impacting and performance-affecting updates.

## Engineering Standards
Maintain continuous static analysis (rustfmt, clippy) and security advisories (cargo audit) in CI; benchmarks in `benches/` must cover primary request/response flows; docs.rs and README examples must compile; error messages should mention actionable next steps; avoid introducing new dependencies without justification of necessity and maintenance cost.

## Development Workflow & Quality Gates
Before implementation: define acceptance tests and benchmarks for the change scope; during implementation: keep diffs small, ensure telemetry remains intact, and update docs in the same PR; pre-merge gates: lint, tests (unit+integration), and relevant benchmarks must pass; releases require verifying backward compatibility notes and observable signal stability.

## Governance
This constitution supersedes conflicting practices. Amendments require a written proposal citing affected principles, reviewer approval from maintainers, and a migration or rollback plan when user-facing. Versioning follows: MAJOR for incompatible governance or principle removals/redefinitions; MINOR for new principles/sections or materially expanded guidance; PATCH for clarifications. Compliance is checked in PR reviews; repeated violations require remediation plans before subsequent releases.

**Version**: 1.0.0 | **Ratified**: TODO(RATIFICATION_DATE): unknown original adoption date | **Last Amended**: 2026-01-20
