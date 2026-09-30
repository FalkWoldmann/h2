# Quickstart: Runtime-Agnostic h2

1. Enable the desired runtime feature flag (default runtime stays enabled if unchanged).
2. Add the corresponding adapter dependency and feature in Cargo.toml.
3. Instantiate the runtime adapter and pass it to h2 client/server builder.
4. Run integration tests for the chosen runtime; verify HTTP/2 round trips succeed.
5. Check benchmarks to ensure latency/throughput within target thresholds (<5% regression).
