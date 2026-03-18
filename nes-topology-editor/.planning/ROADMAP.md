# Roadmap: Tokio-Based Rust I/O Framework for NebulaStream

## Milestones

- ✅ **v1.0 Tokio-Based Rust Sources** -- Phases 1-3 (shipped 2026-03-16)
- 🚧 **v1.1 Async Rust Sinks** -- Phases 4-6 (in progress)

## Phases

<details>
<summary>✅ v1.0 Tokio-Based Rust Sources (Phases 1-3) -- SHIPPED 2026-03-16</summary>

- [x] Phase 1: FFI Foundation (3/3 plans) -- completed 2026-03-15
- [x] Phase 2: Framework and Bridge (4/4 plans) -- completed 2026-03-15
- [x] Phase 3: Validation (2/2 plans) -- completed 2026-03-15

</details>

### 🚧 v1.1 Async Rust Sinks (In Progress)

**Milestone Goal:** Extend the framework with async Rust sinks -- TokioSink C++ operator routes buffers to user-defined AsyncSink implementations on the shared Tokio runtime, with backpressure, guaranteed flush, and registry.

- [ ] **Phase 4: Rust Sink Framework** - AsyncSink trait, SinkContext, bounded channel, and spawn/stop lifecycle on shared Tokio runtime
- [ ] **Phase 5: TokioSink Operator and FFI Bridge** - CXX bridge for buffer transfer, TokioSink C++ operator with backpressure hysteresis and flush-confirm protocol
- [ ] **Phase 6: Registration and Validation** - TokioSinkRegistry, AsyncFileSink reference implementation, and end-to-end system tests

## Phase Details

### Phase 4: Rust Sink Framework
**Goal**: Sink authors can implement a four-method async Rust trait and have their sink spawned on the shared Tokio runtime with a bounded channel feeding it buffers
**Depends on**: Phase 3 (v1.0 shipped -- shared Tokio runtime exists)
**Requirements**: SNK-01, SNK-02, SNK-03, SNK-04
**Success Criteria** (what must be TRUE):
  1. A Rust developer can implement AsyncSink (open/write/flush/close) and the framework spawns it as a Tokio task that receives buffers through a bounded channel
  2. SinkContext provides a CancellationToken that the sink task can select! on for cooperative shutdown
  3. spawn_sink returns a handle and stop_sink shuts down the task cleanly (channel close, drain, flush, close, task join)
  4. Unit tests with a mock AsyncSink verify the full lifecycle: open called once, write called per buffer, flush/close called on stop
**Plans**: TBD

Plans:
- [ ] 04-01: TBD
- [ ] 04-02: TBD

### Phase 5: TokioSink Operator and FFI Bridge
**Goal**: C++ pipeline worker threads can push TupleBuffers to Rust sinks without blocking, with automatic backpressure and guaranteed flush on pipeline stop
**Depends on**: Phase 4
**Requirements**: PLN-01, PLN-02, PLN-03, PLN-04
**Success Criteria** (what must be TRUE):
  1. TokioSink C++ operator receives buffers via execute() and sends them to the Rust sink task through a CXX bridge call that never blocks the pipeline worker thread
  2. When the channel is full, BackpressureController applies pressure at the hysteresis upper threshold and releases at the lower threshold, and a repeatTask retries the send until it succeeds
  3. On pipeline stop, TokioSink sends a flush sentinel through the channel, polls via repeatTask until the Rust task confirms all data is written (AtomicBool), then tears down the sink
  4. Buffer refcounts are correct across the FFI boundary -- C++ retains before sending, Rust releases on TupleBufferHandle drop -- verified by no buffer pool exhaustion under sustained load
**Plans**: TBD

Plans:
- [ ] 05-01: TBD
- [ ] 05-02: TBD

### Phase 6: Registration and Validation
**Goal**: Sink authors can register AsyncSink implementations by name and the framework is validated end-to-end with a real async file sink through the full pipeline
**Depends on**: Phase 5
**Requirements**: REG-01, VAL-01, VAL-02, VAL-03
**Success Criteria** (what must be TRUE):
  1. TokioSinkRegistry allows registering AsyncSink factories by string name and TokioSink can look up and instantiate sinks from the registry
  2. AsyncFileSink reference implementation writes TupleBuffer data to a file using tokio::fs and correctly handles open/write/flush/close lifecycle
  3. A system test runs TokioSource (generator) through a pipeline into AsyncFileSink and verifies all emitted tuples appear in the output file with correct ordering
  4. Systest tooling supports TokioSink as a sink type so automated test infrastructure can exercise the full path
**Plans**: TBD

Plans:
- [ ] 06-01: TBD
- [ ] 06-02: TBD

## Progress

**Execution Order:** Phases execute in numeric order: 4 -> 5 -> 6

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. FFI Foundation | v1.0 | 3/3 | Complete | 2026-03-15 |
| 2. Framework and Bridge | v1.0 | 4/4 | Complete | 2026-03-15 |
| 3. Validation | v1.0 | 2/2 | Complete | 2026-03-15 |
| 4. Rust Sink Framework | v1.1 | 0/? | Not started | - |
| 5. TokioSink Operator and FFI Bridge | v1.1 | 0/? | Not started | - |
| 6. Registration and Validation | v1.1 | 0/? | Not started | - |
