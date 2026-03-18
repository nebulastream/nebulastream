# Milestones

## v1.0 Tokio-Based Rust Sources (Shipped: 2026-03-16)

**Phases completed:** 3 phases, 9 plans
**Timeline:** 2026-03-15 (2 days)
**Git range:** feat(01-01) → feat(03-02)
**Stats:** 55 files changed, 7,795 insertions, ~3k Rust + ~4.4k C++ LOC

**Key accomplishments:**
1. Cargo workspace with 3 sub-crates and cxx bridge for safe Rust-C++ FFI
2. AsyncSource trait with SourceContext exposing only allocate_buffer() and emit(buf).await
3. Bridge thread with per-source emit registry, backpressure, and inflight semaphore
4. TokioSource C++ class plugging into existing SourceHandle via std::variant
5. GeneratorSource reference implementation with full async lifecycle
6. End-to-end integration tests verifying buffer ordering, backpressure, and shutdown safety

---

