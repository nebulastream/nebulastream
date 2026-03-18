---
phase: 04-rust-sink-framework
plan: 02
subsystem: runtime
tags: [rust, async, tokio, sink, channel, lifecycle, monitoring]

# Dependency graph
requires:
  - phase: 04-rust-sink-framework/04-01
    provides: "SinkError, SinkContext, SinkMessage types"
provides:
  - "AsyncSink trait with run() for sink authors"
  - "SinkTaskHandle with stop_sink (try_send Close) and is_sink_done (sender.is_closed)"
  - "spawn_sink lifecycle function with monitoring task and error callback"
affects: [05-ffi-bridge-operator, 06-registry-validation]

# Tech tracking
tech-stack:
  added: []
  patterns: [spawn_sink lifecycle mirroring spawn_source, try_send for non-blocking Close, sender.is_closed for completion detection]

key-files:
  created:
    - nes-sources/rust/nes-source-runtime/src/sink.rs
    - nes-sources/rust/nes-source-runtime/src/sink_handle.rs
  modified:
    - nes-sources/rust/nes-source-runtime/src/lib.rs

key-decisions:
  - "stop_sink uses try_send returning bool (false=full) instead of blocking send, enabling Phase 5 repeatTask retry pattern"
  - "is_sink_done checks sender.is_closed() -- sink dropping receiver signals completion without extra state"
  - "ErrorFnPtr type redeclared in sink.rs (same signature as source.rs) rather than shared -- keeps modules independent"

patterns-established:
  - "spawn_test_sink: test-only spawn using tokio::spawn instead of source_runtime() to avoid C++ linker symbols"
  - "SinkTaskHandle holds Sender (unlike SourceTaskHandle) because stop uses channel ordering not cancellation token"

requirements-completed: [SNK-01, SNK-02, SNK-03, SNK-04]

# Metrics
duration: 3min
completed: 2026-03-16
---

# Phase 4 Plan 02: AsyncSink Trait and Lifecycle Summary

**AsyncSink trait with spawn_sink lifecycle, monitoring task error callback, and SinkTaskHandle with try_send Close and is_closed completion detection**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-16T14:39:45Z
- **Completed:** 2026-03-16T14:42:26Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- AsyncSink trait with single run(&mut self, ctx: &SinkContext) -> Result<(), SinkError> method
- SinkTaskHandle with stop_sink (try_send Close, returns false when full) and is_sink_done (sender.is_closed)
- spawn_sink creates bounded channel, SinkContext, Tokio task, monitoring task, returns boxed SinkTaskHandle
- Monitoring task calls error_fn on Err(SinkError) and Err(JoinError/panic)
- 13 new tests (6 sink_handle + 7 sink) covering full lifecycle: trait check, spawn completion, stop/close, error callback, 3-buffer FIFO ordering, cancellation

## Task Commits

Each task was committed atomically:

1. **Task 1: Create SinkTaskHandle with stop_sink and is_sink_done** - `8ba9c9942b` (feat)
2. **Task 2: Create AsyncSink trait, spawn_sink, monitoring task, and full lifecycle tests** - `e35b99d7e0` (feat)

## Files Created/Modified
- `nes-sources/rust/nes-source-runtime/src/sink_handle.rs` - SinkTaskHandle struct, stop_sink, is_sink_done, 6 unit tests
- `nes-sources/rust/nes-source-runtime/src/sink.rs` - AsyncSink trait, spawn_sink, sink_monitoring_task, ErrorCallback, spawn_test_sink, 7 unit tests
- `nes-sources/rust/nes-source-runtime/src/lib.rs` - Added pub mod sink/sink_handle, re-exports AsyncSink, SinkTaskHandle, stop_sink, is_sink_done

## Decisions Made
- stop_sink uses try_send returning bool (false=channel full) to support Phase 5 repeatTask retry pattern without blocking
- is_sink_done uses sender.is_closed() -- when sink drops receiver (by returning from run()), the sender sees closed channel
- ErrorFnPtr type alias redeclared in sink.rs with same signature as source.rs rather than sharing -- keeps sink and source modules independent for cleaner phase boundaries

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Pure Rust sink framework complete: SinkError, SinkContext, SinkMessage, AsyncSink, SinkTaskHandle, spawn_sink, stop_sink, is_sink_done
- All 24 sink tests pass (4 sink_error + 8 sink_context + 6 sink_handle + 7 sink tests -- note sink module has 7 tests: 2 compile-time + 5 functional)
- Ready for Phase 5: FFI bridge (nes-source-bindings sink FFI functions) and TokioSink C++ operator
- Research flags from STATE.md still apply: buffer retain/release and TokioSink state machine need detailed design in Phase 5 planning

---
*Phase: 04-rust-sink-framework*
*Completed: 2026-03-16*
