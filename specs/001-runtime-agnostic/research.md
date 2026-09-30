# Research Findings

- Decision: Provide a runtime abstraction layer (trait-based) capturing I/O, timers, and task spawning; keep tokio support via adapter; add alternative runtime adapter (e.g., compio) guarded by feature flags.
- Rationale: Separates protocol logic from executor details, enables multiple runtimes without breaking current users, and keeps MSRV 1.63 constraints.
- Alternatives considered: (1) Keep tokio-only — rejects runtime-agnostic goal; (2) Use generic async traits behind cfg(feature) only — complicates API surface and harms default users; (3) Wrap executors with boxed dyn traits — adds allocation/indirection overhead on hot paths.
