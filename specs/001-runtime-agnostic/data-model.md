# Data Model

## Entities

- **Runtime Adapter**
  - Fields: capability set (I/O traits, timer, spawn, waker integration), feature flag identifier, optional performance hints
  - Relationships: used by protocol layers for I/O and timeouts; linked to compatibility suite entries
  - Validation: must supply all required capabilities; fails fast with clear errors if missing

- **Compatibility Suite**
  - Fields: runtime target, test matrix (integration, fuzz/property, benchmarks), parity thresholds
  - Relationships: executed per runtime; associated with adapters
  - Validation: requires passing protocol correctness tests and staying within performance thresholds

- **Migration Profile**
  - Fields: default runtime path, alternative runtime path, feature flags, doc references
  - Relationships: maps existing users to upgrade steps; ties to release notes and examples
  - Validation: default path must be backward compatible; alternative path must build without default runtime dependency
