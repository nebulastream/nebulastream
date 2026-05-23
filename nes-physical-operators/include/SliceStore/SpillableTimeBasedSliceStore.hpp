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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
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
/// Increment B (PR-2, this layer): adds the SYNCHRONOUS hard-threshold eviction policy on the worker path. When
/// getSlicesOrCreate NEWLY tracks a slice and store-wide residentBytes() exceeds the hard threshold, the worker
/// thread itself spills COLD slices (sliceEnd < lastSeenWatermark, resident, not emitted, not in-flight) oldest-first
/// until back under hard or no cold candidate remains (O-B5: proceed over-budget, never throw, never block). All
/// eviction decisions are serialized by an OUTER `evictionMutex` + a per-slice `spillInProgress` reservation set; ALL
/// RocksDB I/O (spill()) happens OUTSIDE the mutex (uniform no-I/O-under-lock). forceSpill now shares this serializer.
/// The background spiller (soft threshold), the trigger-wait condvar, and the store-owned lifecycle land in PR-3;
/// this layer is still single-threaded beyond the base's per-map folly::Synchronized guarantees.
class SpillableTimeBasedSliceStore final : public DefaultTimeBasedSliceStore
{
public:
    SpillableTimeBasedSliceStore(
        uint64_t windowSize,
        uint64_t windowSlide,
        SliceCacheConfiguration sliceCacheConfiguration,
        std::unique_ptr<SpillBackend> backend,
        uint64_t entriesPerPage,
        uint64_t pageSize,
        uint64_t softThresholdBytes,
        uint64_t hardThresholdBytes);

    /// Builds a durable RocksDB-backed SpillBackend. Defined in this translation unit so RocksDB stays a PRIVATE
    /// dependency of nes-physical-operators — the lowering rule (nes-query-compiler) passes only SpillConfig values
    /// and never links rocksdb.
    static std::unique_ptr<SpillBackend> makeRocksDBBackend(const std::string& rocksdbPath, const std::string& compression);

    /// Self-track every created slice (O-B1): delegate to the base, then record the created slice (tumbling ⇒
    /// exactly one) in `createdSlices` so the Increment-B eviction scan can enumerate cold slices without
    /// touching the private base maps. Signature mirrors the base override exactly.
    std::vector<std::shared_ptr<Slice>> getSlicesOrCreate(
        Timestamp timestamp,
        const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice) override;

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

    std::unique_ptr<SpillBackend> backend;
    folly::Synchronized<std::shared_ptr<AbstractBufferProvider>> bufferProvider;
    const uint64_t entriesPerPage;
    const uint64_t pageSize;
    /// Soft threshold drives the background spiller (PR-3). PR-2 does not read it yet, so it stays [[maybe_unused]]
    /// until then (-Wunused-private-field would otherwise fire).
    [[maybe_unused]] const uint64_t softThresholdBytes;
    /// Hard threshold: the synchronous worker-path backstop (PR-2 evictUntilUnderHard). Now enforced.
    const uint64_t hardThresholdBytes;
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

    /// O-B3 (PR-2): the OUTER eviction serializer. It guards ONLY in-memory eviction decisions — pick-cold,
    /// confirm-not-emitted, reserve (insert spillInProgress), mark-spilled, clear-reservation. ALL RocksDB I/O
    /// (spill()) happens OUTSIDE this mutex, protected by the per-slice spillInProgress reservation (uniform
    /// no-I/O-under-lock). Lock order is always evictionMutex → (createdSlices | one folly set); the base
    /// slices/windows maps are NEVER taken under it. mutable: residentBytes()/dtor may need it. (The condvar +
    /// background thread that complete this model land in PR-3.)
    mutable std::mutex evictionMutex;
    /// Per-slice reservation set (O-B3): a SliceEnd here has an in-flight spill; the eviction scan and forceSpill
    /// both skip it. ALWAYS accessed under evictionMutex (a plain set — evictionMutex already guards it).
    std::set<SliceEnd> spillInProgress;
    /// Monotonic count of real spills performed (eviction path + forceSpill). Atomic so it is safe to read from
    /// tests without evictionMutex; bumped right after a successful spill() I/O.
    std::atomic<std::size_t> spillCount{0};
};

}
