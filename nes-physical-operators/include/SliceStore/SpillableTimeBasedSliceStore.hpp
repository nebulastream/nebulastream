/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <folly/Synchronized.h>
#include <SliceCacheConfiguration.hpp>

namespace NES
{

/// Out-of-core variant of DefaultTimeBasedSliceStore (Phase 4b, Increment A — single-threaded spike).
///
/// It owns a SpillBackend and unspills any non-resident slice in the trigger read paths before returning
/// it (so the handler's triggerSlices always sees resident maps). Slices are spilled by an explicit,
/// test/operator-driven forceSpill(SliceEnd) — the automatic memory-budget cold scan and live eviction are
/// deferred to Increment B.
///
/// Spill target = COLD slices only (stopped filling, not yet emitted). A slice returned by a trigger feeder
/// is recorded in `emittedKeys`: its raw HashMap* is snapshotted for the asynchronous probe, so it must NOT
/// be spilled afterwards (async-probe-drain invariant) — forceSpill rejects it.
///
/// Implementation note: the base `windows`/`slices` maps are private, so this subclass deliberately uses only
/// the public WindowSlicesStoreInterface API plus its own tracked sets (`spilledKeys` for residency + GC
/// backend-erase, `emittedKeys` for the async-probe-drain guard) and a self-tracking `createdSlices` map. It
/// reuses the inherited createSliceStoreRef + SliceCache + DefaultTimeBasedSliceStoreRef unchanged (the build
/// hot path only ever touches resident slices).
///
/// Increment B (PR-1): self-tracks every created slice in `createdSlices`
/// (folly::Synchronized<map<SliceEnd, weak_ptr<Slice>>>, weak_ptr so the base GC still frees the slice) via a
/// getSlicesOrCreate override that records AFTER the base call returns; learns the build watermark monotonically
/// (`lastSeenWatermark`) in the trigger feeders; and exposes a store-wide residentBytes() (Σ over createdSlices).
/// `createdSlices` is pruned of expired weak_ptrs in garbageCollectSlicesAndWindows and cleared in deleteState.
///
/// Increment B (PR-2): adds the SYNCHRONOUS hard-threshold eviction policy on the worker path. When getSlicesOrCreate
/// NEWLY tracks a slice and store-wide residentBytes() exceeds the hard threshold, the worker thread itself spills COLD
/// slices (sliceEnd < lastSeenWatermark, resident, not emitted, not in-flight) oldest-first until back under hard or no
/// cold candidate remains (O-B5: proceed over-budget, never throw, never block). All eviction decisions are serialized
/// by an OUTER `evictionMutex` + a per-slice `spillInProgress` reservation set; ALL RocksDB I/O (spill()) happens
/// OUTSIDE the mutex (uniform no-I/O-under-lock). forceSpill shares this serializer.
///
/// Increment B (PR-3, this layer): turns the store MULTI-THREAD-SAFE. A store-owned background `std::jthread` spiller
/// (declared LAST, so reverse-order member destruction joins it first) preemptively spills the oldest COLD slices once
/// resident memory crosses the SOFT threshold, woken by `spillerCv` (paired with `evictionMutex`) on a cache-miss
/// getSlicesOrCreate / trigger-feeder cadence, with a bounded SPILLER_POLL_INTERVAL wait_for safety net. It is started
/// lazily at the end of setBufferProvider (idempotent via `spillerStarted`) and stopped+joined FIRST in both the dtor
/// and deleteState (before backend/bufferProvider die / before the base clears state). The eviction-mutex invariant
/// (verbatim): evictionMutex covers ONLY in-memory decisions — pick-cold, confirm-not-emitted, reserve, mark-emitted,
/// clear-reservation. ALL RocksDB I/O — both spill() and unspill() — happens OUTSIDE evictionMutex, protected by the
/// per-slice spillInProgress reservation; the rare spill-vs-trigger-same-slice collision is handled by a condvar wait.
/// A trigger that wants a mid-spill slice WAITs on spillerCv under evictionMutex until the spiller finalizes, claims it
/// in emittedKeys BEFORE releasing the mutex (so the spiller can never re-pick it), then unspills it OUTSIDE the mutex —
/// a triggered slice always ends resident+emitted, never half-freed. Lock order is always evictionMutex → (createdSlices
/// | one folly set); the base slices/windows maps are NEVER taken under it.
class SpillableTimeBasedSliceStore final : public DefaultTimeBasedSliceStore
{
public:
    /// PR-3 lifecycle: stop+join the background spiller FIRST (before backend/bufferProvider are destroyed and before
    /// the base dtor runs deleteState), so the loop can never touch a half-destroyed store.
    ~SpillableTimeBasedSliceStore() override;

    SpillableTimeBasedSliceStore(
        uint64_t windowSize,
        uint64_t windowSlide,
        SliceCacheConfiguration sliceCacheConfiguration,
        std::unique_ptr<SpillBackend> backend,
        uint64_t entriesPerPage,
        uint64_t pageSize,
        uint64_t softThresholdBytes,
        uint64_t hardThresholdBytes,
        uint64_t emitLag = 0);

    /// Builds a durable RocksDB-backed SpillBackend. Defined in this translation unit so RocksDB stays a PRIVATE
    /// dependency of nes-physical-operators — the lowering rule (nes-query-compiler) passes only SpillConfig values
    /// and never links rocksdb.
    static std::unique_ptr<SpillBackend> makeRocksDBBackend(const std::string& rocksdbPath, const std::string& compression);

    /// Self-track every created slice (O-B1): delegate to the base, then record the created slice (tumbling ⇒
    /// exactly one) in `createdSlices` so the Increment-B eviction scan can enumerate cold slices without
    /// touching the private base maps. Signature mirrors the base override exactly.
    std::vector<std::shared_ptr<Slice>> getSlicesOrCreate(
        Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice) override;

    /// D3 unspill-on-read: every slice returned is unspilled (if non-resident) and recorded as emitted (its raw
    /// HashMap* will be snapshotted for the async probe → it must not be spilled afterwards).
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
    getTriggerableWindowSlices(Timestamp globalWatermark) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getAllNonTriggeredSlices() override;
    /// Random-access read (join-probe path; out of 4b scope): unspills for consistency, but does NOT mark emitted.
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(SliceEnd sliceEnd) override;
    /// Erases backend records for spilled slices that the base GC has dropped.
    void garbageCollectSlicesAndWindows(Timestamp newGlobalWaterMark) override;
    /// Erases all backend records + clears tracked sets before delegating, so an explicit deleteState() (not just
    /// destruction) leaves no orphaned on-disk records or stale residency bookkeeping.
    void deleteState() override;

    /// O1: stash the query-lifetime buffer provider used to rebuild spilled maps on unspill. Set once
    /// (called from setupSliceStoreProxy); the store unit test calls it directly.
    void setBufferProvider(std::shared_ptr<AbstractBufferProvider> provider);

    /// Increment A forced/manual spill driver: spill a provably COLD slice (the caller selects it; automatic
    /// cold detection is Increment B). No-op if the slice is unknown or already spilled. Throws SpillStoreFailure
    /// if the slice has already been emitted to the probe (async-probe-drain invariant).
    void forceSpill(SliceEnd sliceEnd);

    /// True unless the slice is currently spilled. Unknown slices report resident.
    [[nodiscard]] bool isSliceResident(SliceEnd sliceEnd) const;

    /// Resident memory footprint of one slice = Σ_workers ⌈tuples_w / entriesPerPage⌉ × pageSize. Returns 0 for a
    /// spilled slice (its maps are freed). Exact for fixed-size entries.
    [[nodiscard]] uint64_t residentBytesOf(const Slice& slice) const;

    /// Store-wide resident footprint (O-B-residentBytes): Σ over the self-tracked `createdSlices` of
    /// residentBytesOf(*slice) (0 for spilled). Read-only snapshot: locks `createdSlices` only, skips expired
    /// weak_ptrs; touches no private base map.
    [[nodiscard]] uint64_t residentBytes() const;

    /// --- PR-1/PR-2 test hooks (deterministic drive points; no production behavior; public so the unit test can call
    /// them directly, mirroring how the test already drives setBufferProvider/forceSpill). ---
    /// Seed the learned build watermark directly so a cold-but-unemitted slice is producible without driving the
    /// base trigger feeders (which couple watermark-advance to emit). Exercised by the PR-2 eviction tests
    /// (hardEvictsOldestColdFirst / hardStillOverAfterAllColdSpilled / emittedSliceNeverSpilledUnderEviction).
    void setLastSeenWatermarkForTest(Timestamp watermark);
    /// Number of currently tracked entries in `createdSlices` (incl. not-yet-pruned expired weak_ptrs).
    [[nodiscard]] std::size_t createdSlicesSizeForTest() const;
    /// PR-2: run the synchronous hard-eviction loop directly (evictUntilUnderHard()), so the policy is drivable
    /// without the background thread (added in PR-3).
    void spillColdSlicesUnderHardForTest();
    /// PR-2: monotonic count of real spills performed (eviction path + forceSpill), for tight test assertions.
    [[nodiscard]] std::size_t spillCountForTest() const;

private:
    /// For each returned slice: unspill if non-resident (dropping it from spilledKeys) and record it emitted.
    void unspillAndMarkEmitted(std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& windowSlices);

    /// Increment C late-write guard. Called from getSlicesOrCreate ONLY for a record whose window is already COLD
    /// (sliceEnd < the true frontier) under emit_lag > 0 — i.e. a late/out-of-order write into the retained band
    /// [frontier - L, frontier). The base early-returns the existing (possibly spilled) slice; the worker then writes
    /// to it via getHashMapPtrOrCreate, which terminates on a spilled slice (INVARIANT(resident)). This (a) unspills the
    /// slice on the worker thread BEFORE the write and (b) PINS it (`pinnedKeys`) so neither the background spiller nor
    /// the synchronous hard-eviction can re-spill it while its raw HashMap* is (re)cached in the per-worker SliceCache —
    /// a later cache HIT is JIT-generated and cannot be intercepted, so the slice must stay resident until it emits, at
    /// which point emittedKeys takes over and the pin is dropped. Unspill I/O happens OUTSIDE evictionMutex under a
    /// ReservationGuard (LOCKED #7). No-op for a non-spillable slice. Never marks the slice emitted (it is still filling).
    void pinAndUnspillForLateWrite(const std::shared_ptr<Slice>& slice);

    /// O-B1 self-track: record a freshly created slice in `createdSlices` keyed by its SliceEnd, holding only a
    /// weak_ptr so the base GC's shared_ptr drop still frees the slice (the eviction scan locks each weak_ptr).
    /// Returns true ONLY on a true insertion (a NEW SliceEnd); false when the base re-returned an already-tracked
    /// slice (SliceCache hit / insert race), so the caller can gate budget checks on a genuine new slice (M1).
    bool recordCreatedSlice(const std::shared_ptr<Slice>& slice);

    /// PR-2 eviction policy (single-thread this PR; the background thread is wired in PR-3). All take/release
    /// `evictionMutex` internally; RocksDB I/O (spill()) is performed OUTSIDE the mutex.
    /// Picks the oldest COLD, resident, non-emitted, non-in-flight slice, reserves it in `spillInProgress`, and
    /// returns its SliceEnd; std::nullopt if no candidate. Snapshots createdSlices under one rlock, releases it,
    /// then evaluates predicates (never nests createdSlices with the folly set locks).
    std::optional<SliceEnd> pickAndReserveColdSlice();
    /// Marks a reserved slice spilled: inserts into spilledKeys, clears its reservation. (Condvar notify is PR-3.)
    void finalizeSpill(SliceEnd end);
    /// Reserve → lock the spillable → spill (I/O OUTSIDE evictionMutex) → finalize. Returns false only when there is
    /// no cold candidate; an expired weak_ptr between reserve and lookup is treated as progress (returns true).
    bool spillOneColdSlice();
    /// Synchronous worker-path hard backstop (O-B2 hard): spill cold slices until resident <= hard or none remain.
    /// O-B5: proceeds over-budget with a NES_WARNING; never throws, never blocks.
    void evictUntilUnderHard();
    /// Release a spill reservation: erase `end` from spillInProgress under evictionMutex. Used by the RAII
    /// ReservationGuard (defined in the .cpp) to guarantee a reserved-but-not-finalized slice never leaks its
    /// reservation when the spill() I/O throws (a leak would deadlock the PR-3 condvar wait on that SliceEnd).
    /// Idempotent (set erase).
    void releaseReservation(SliceEnd end);

    /// RAII guard that releases a spill reservation on scope exit unless committed (defined in the .cpp). Declared as a
    /// private nested type so it can call releaseReservation without widening that helper's visibility.
    struct ReservationGuard;

    /// PR-3 background-spiller lifecycle + loop body. startSpiller is idempotent (spillerStarted CAS); stopSpiller is
    /// idempotent (safe to call from BOTH the dtor and deleteState). spillerLoop runs on the store-owned jthread: it
    /// blocks on spillerCv (bounded by SPILLER_POLL_INTERVAL) until resident memory crosses soft or stop is requested,
    /// then spills cold slices down to soft — NEVER holding evictionMutex across spillOneColdSlice (which manages its
    /// own locks + I/O outside the mutex).
    void startSpiller();
    void stopSpiller();
    void spillerLoop(std::stop_token stopToken);

    std::unique_ptr<SpillBackend> backend;
    folly::Synchronized<std::shared_ptr<AbstractBufferProvider>> bufferProvider;
    const uint64_t entriesPerPage;
    const uint64_t pageSize;
    /// Soft threshold drives the background spiller (PR-3 spillerLoop / getSlicesOrCreate soft signal). Now read.
    const uint64_t softThresholdBytes;
    /// Hard threshold: the synchronous worker-path backstop (PR-2 evictUntilUnderHard). Now enforced.
    const uint64_t hardThresholdBytes;
    /// Increment C emit-decouple: event-time emit lag L (ms). The EMIT cutoff in getTriggerableWindowSlices uses
    /// globalWatermark - emitLag (saturating), while quiescence/cold-pick (lastSeenWatermark) uses the TRUE
    /// globalWatermark — so completed windows in [globalWatermark - L, globalWatermark) are retained and become
    /// spillable. 0 = byte-identical to pre-Increment-C (emit wm == global wm).
    const uint64_t emitLag;
    /// SliceEnds whose maps are currently spilled — drives isSliceResident() and GC backend-erase.
    folly::Synchronized<std::set<SliceEnd>> spilledKeys;
    /// SliceEnds returned by a trigger feeder (emitted to the async probe) — forceSpill refuses these.
    folly::Synchronized<std::set<SliceEnd>> emittedKeys;

    /// O-B1 self-track: created slices keyed by SliceEnd (map sorted ascending ⇒ oldest-first scan is begin()→end()).
    /// weak_ptr so the base GC still frees the slice; the eviction scan locks each weak_ptr and skips expired entries.
    folly::Synchronized<std::map<SliceEnd, std::weak_ptr<Slice>>> createdSlices;
    /// Learned build watermark (monotonic). Stores the raw underlying value so the atomic is lock-free. A lagging
    /// value only ever under-selects cold slices (safe) — it can never cause a hot slice to be spilled (PLAN
    /// O-B-watermark safety argument), so memory_order_relaxed on the CAS-max load/store is intentional and must
    /// NOT be strengthened to seq_cst by a later reader. Bumped via CAS-max in the trigger feeders.
    std::atomic<Timestamp::Underlying> lastSeenWatermark{Timestamp(0).getRawValue()};

    /// O-B3: the OUTER eviction serializer. It guards ONLY in-memory eviction decisions — pick-cold, confirm-not-emitted,
    /// reserve (insert spillInProgress), mark-spilled, mark-emitted, clear-reservation, and the `quiesced` flag. The
    /// eviction-mutex invariant (verbatim): evictionMutex covers ONLY in-memory decisions; ALL RocksDB I/O — both spill()
    /// and unspill() — happens OUTSIDE evictionMutex, protected by the per-slice spillInProgress reservation; the rare
    /// spill-vs-trigger-same-slice collision is handled by a condvar wait. Lock order is always evictionMutex →
    /// (createdSlices | one folly set); the base slices/windows maps are NEVER taken under it. mutable: the dtor and
    /// stopSpiller take it from a const context (residentBytes() does NOT take it — it only locks createdSlices).
    mutable std::mutex evictionMutex;
    /// Spiller wakeup + trigger-wait condvar (paired with evictionMutex). The background spiller blocks on it (woken by
    /// the soft-signal at the getSlicesOrCreate / trigger cadences, bounded by SPILLER_POLL_INTERVAL); a trigger that
    /// wants a mid-spill slice WAITs on it until finalizeSpill notifies. ALWAYS used with evictionMutex held.
    std::condition_variable spillerCv;
    /// Per-slice reservation set (O-B3): a SliceEnd here has an in-flight spill; the eviction scan and forceSpill
    /// both skip it, and unspillAndMarkEmitted WAITs on spillerCv until it leaves. ALWAYS accessed under evictionMutex
    /// (a plain set — evictionMutex already guards it).
    std::set<SliceEnd> spillInProgress;
    /// Increment C: SliceEnds of retained-band slices that a LATE write has re-activated (their raw HashMap* is cached on
    /// a worker). pickAndReserveColdSlice skips these so neither the spiller nor hard-eviction re-spills a slice whose
    /// cached pointer would then dangle — the slice stays resident until it emits (then emittedKeys takes over and the
    /// pin is dropped) or is garbage-collected. ALWAYS accessed under evictionMutex (a plain set, like spillInProgress).
    std::set<SliceEnd> pinnedKeys;
    /// Set true (under evictionMutex) by stopSpiller before join: pickAndReserveColdSlice returns nullopt while it is
    /// set, so the spiller stops picking and the loop drains promptly. Read under evictionMutex.
    bool quiesced = false;
    /// Start-once guard for the background spiller (belt-and-braces with setBufferProvider's "already stashed → return").
    std::atomic<bool> spillerStarted{false};
    /// Monotonic count of real spills performed (eviction path + forceSpill). Atomic so it is safe to read from
    /// tests without evictionMutex; bumped right after a successful spill() I/O.
    std::atomic<std::size_t> spillCount{0};
    /// Store-owned background spiller. Declared LAST so reverse-order member destruction joins it FIRST — but the dtor
    /// also calls stopSpiller() explicitly so the loop is stopped before backend/bufferProvider (declared earlier) die.
    std::jthread spiller;
};

}
