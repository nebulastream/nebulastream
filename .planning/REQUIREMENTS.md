# Requirements: Async Rust Sinks

**Defined:** 2026-03-16
**Core Value:** I/O authors can write async Rust sources and sinks with simple trait methods -- all complexity hidden by the framework.

## v1.1 Requirements

### Sink Framework

- [x] **SNK-01**: AsyncSink Rust trait with open/write/flush/close lifecycle methods
- [x] **SNK-02**: SinkContext providing CancellationToken for cooperative shutdown
- [x] **SNK-03**: Per-sink bounded async channel from C++ pipeline thread to Rust async task
- [x] **SNK-04**: spawn_sink/stop_sink lifecycle management on shared Tokio runtime

### Pipeline Integration

- [x] **PLN-01**: TokioSink C++ operator implementing ExecutablePipelineStage (execute/stop)
- [x] **PLN-02**: Non-blocking try_send with repeatTask retry when channel is full
- [x] **PLN-03**: BackpressureController with hysteresis thresholds (apply/release pressure)
- [x] **PLN-04**: Guaranteed flush on pipeline stop via repeatTask polling until Rust confirms drain

### Registration

- [x] **REG-01**: TokioSinkRegistry for string-name factory registration (mirrors TokioSourceRegistry)

### Validation

- [x] **VAL-01**: AsyncFileSink reference implementation using tokio::fs async file I/O
- [ ] **VAL-02**: System test using TokioSource -> pipeline -> AsyncFileSink with inline sink syntax
- [ ] **VAL-03**: Systest tool integration for TokioSink (enable sink type if needed)

## v2 Requirements

### Observability

- **OBS-01**: tracing instrumentation on sink write/flush paths
- **OBS-02**: Sink throughput metrics (buffers/sec, bytes/sec)

### Resilience

- **RES-01**: Sink-level retry with backoff for transient write failures

## Out of Scope

| Feature | Reason |
|---------|--------|
| Rewriting existing C++ sinks (NetworkSink etc.) in Rust | This adds a parallel path, not a replacement |
| Async sinks with multiple input channels | Single pipeline input per sink; fan-in is handled by the engine |
| Sink-side buffer allocation | Sinks receive buffers, they don't allocate them |
| Per-sink Tokio runtimes | All sinks share the source runtime; single I/O runtime |
| Unbounded channel between pipeline and sink | Hides backpressure failures; allows OOM |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| SNK-01 | Phase 4 | Complete |
| SNK-02 | Phase 4 | Complete |
| SNK-03 | Phase 4 | Complete |
| SNK-04 | Phase 4 | Complete |
| PLN-01 | Phase 5 | Complete |
| PLN-02 | Phase 5 | Complete |
| PLN-03 | Phase 5 | Complete |
| PLN-04 | Phase 5 | Complete |
| REG-01 | Phase 6 | Complete |
| VAL-01 | Phase 6 | Complete |
| VAL-02 | Phase 6 | Pending |
| VAL-03 | Phase 6 | Pending |

**Coverage:**
- v1.1 requirements: 12 total
- Mapped to phases: 12
- Unmapped: 0

---
*Requirements defined: 2026-03-16*
*Last updated: 2026-03-16 after roadmap creation*
