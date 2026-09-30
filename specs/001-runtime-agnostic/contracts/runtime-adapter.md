# Runtime Adapter Contract (conceptual)

## Capabilities

- Async I/O: non-blocking read/write with backpressure awareness
- Timers: sleep/delay for keepalive, ping, and flow-control timeouts
- Task spawn: schedule background work without assuming a specific executor type
- Wakers: interoperability with futures wakers and readiness notifications

## Expectations

- Must integrate without requiring the default runtime dependency
- Must be feature-flagged per runtime
- Must surface actionable errors if required capabilities are missing
- Must be covered by integration tests per runtime
