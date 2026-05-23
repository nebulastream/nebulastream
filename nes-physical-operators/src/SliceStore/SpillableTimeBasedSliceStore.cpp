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

#include <SliceStore/SpillableTimeBasedSliceStore.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <Aggregation/SpillableAggregationSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/RocksDBSpillBackend.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

/// O-B2 reconciled divergence 2: the background spiller's bounded wait_for is ONLY a safety net behind the condvar
/// (which is primary), so it never adds latency on the loaded path — it just re-checks the stop token and drains a
/// store that stopped creating slices but is still over soft.
namespace
{
constexpr std::chrono::milliseconds SPILLER_POLL_INTERVAL{100};
}

/// RAII reservation-release guard: erases `end` from the store's spillInProgress (under evictionMutex, via
/// releaseReservation) on scope exit UNLESS `committed` was set. Used to guarantee a reserved-but-not-finalized slice
/// never leaks its reservation when the spill() I/O throws — a leak would deadlock the PR-3 condvar wait on that
/// SliceEnd. The success path calls finalizeSpill() (which already erases the reservation) and then sets `committed`,
/// so the guard never double-erases.
struct SpillableTimeBasedSliceStore::ReservationGuard
{
    SpillableTimeBasedSliceStore& store;
    SliceEnd end;
    bool committed = false;
    ReservationGuard(const ReservationGuard&) = delete;
    ReservationGuard& operator=(const ReservationGuard&) = delete;
    ReservationGuard(ReservationGuard&&) = delete;
    ReservationGuard& operator=(ReservationGuard&&) = delete;

    ReservationGuard(SpillableTimeBasedSliceStore& store, const SliceEnd end) : store(store), end(end) { }

    ~ReservationGuard()
    {
        if (!committed)
        {
            store.releaseReservation(end);
        }
    }
};

void SpillableTimeBasedSliceStore::releaseReservation(const SliceEnd end)
{
    {
        std::unique_lock lock(evictionMutex);
        spillInProgress.erase(end);
    }
    /// PR-3: a reservation can be released WITHOUT finalizing (spill() threw, or the weak_ptr expired between reserve
    /// and lookup). A trigger WAITing on this sliceEnd in unspillAndMarkEmitted is only woken by a notify, and on these
    /// non-finalize paths finalizeSpill never runs — so notify here too, or the waiter would hang forever.
    spillerCv.notify_all();
}

SpillableTimeBasedSliceStore::SpillableTimeBasedSliceStore(
    const uint64_t windowSize,
    const uint64_t windowSlide,
    SliceCacheConfiguration sliceCacheConfiguration,
    std::unique_ptr<SpillBackend> backend,
    const uint64_t entriesPerPage,
    const uint64_t pageSize,
    const uint64_t softThresholdBytes,
    const uint64_t hardThresholdBytes,
    const uint64_t emitLag)
    : DefaultTimeBasedSliceStore(windowSize, windowSlide, std::move(sliceCacheConfiguration))
    , backend(std::move(backend))
    , entriesPerPage(entriesPerPage)
    , pageSize(pageSize)
    , softThresholdBytes(softThresholdBytes)
    , hardThresholdBytes(hardThresholdBytes)
    , emitLag(emitLag)
{
    PRECONDITION(this->backend != nullptr, "SpillableTimeBasedSliceStore requires a non-null SpillBackend");
    PRECONDITION(entriesPerPage > 0, "entriesPerPage must be > 0");
    PRECONDITION(pageSize > 0, "pageSize must be > 0");
    NES_DEBUG(
        "Constructed SpillableTimeBasedSliceStore (entriesPerPage={}, pageSize={}, softThresholdBytes={}, hardThresholdBytes={}, "
        "emitLag={})",
        entriesPerPage,
        pageSize,
        softThresholdBytes,
        hardThresholdBytes,
        emitLag);
}

SpillableTimeBasedSliceStore::~SpillableTimeBasedSliceStore()
{
    /// PR-3 teardown: stop+join the spiller BEFORE backend/bufferProvider (declared earlier ⇒ destroyed AFTER `spiller`,
    /// but we must not rely on that for correctness during the base dtor's deleteState) and before the base dtor runs.
    /// stopSpiller is idempotent — deleteState() may already have joined it.
    stopSpiller();
}

std::unique_ptr<SpillBackend>
SpillableTimeBasedSliceStore::makeRocksDBBackend(const std::string& rocksdbPath, const std::string& compression)
{
    return std::make_unique<RocksDBSpillBackend>(rocksdbPath, compression);
}

void SpillableTimeBasedSliceStore::setBufferProvider(std::shared_ptr<AbstractBufferProvider> provider)
{
    PRECONDITION(provider != nullptr, "buffer provider must not be null");
    bufferProvider.withWLock(
        [&](auto& stashed)
        {
            if (stashed != nullptr)
            {
                NES_DEBUG("Buffer provider already stashed; ignoring repeat setBufferProvider");
                return;
            }
            stashed = std::move(provider);
            NES_DEBUG("Stashed buffer provider for spillable slice store");
        });
    /// PR-3: start the store-owned background spiller lazily once the buffer provider is available (unspill-on-spiller
    /// needs it only on the trigger path, but starting here keeps the lifecycle in one place). Idempotent: the
    /// "already stashed → return" branch above plus startSpiller's CAS guard prevent a double start.
    startSpiller();
}

std::vector<std::shared_ptr<Slice>> SpillableTimeBasedSliceStore::getSlicesOrCreate(
    const Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
{
    /// The base lock is already released on return (DefaultTimeBasedSliceStore::getSlicesOrCreate), so recording the
    /// created slice afterwards never nests our `createdSlices` lock with the base maps.
    auto result = DefaultTimeBasedSliceStore::getSlicesOrCreate(timestamp, createNewSlice);
    /// tumbling window ⇒ always exactly one slice per timestamp (new or existing — this also runs on a SliceCache hit
    /// that re-returns the already-created slice), got {}.
    INVARIANT(result.size() == 1, "tumbling slice store expects exactly one slice per (timestamp), got {}", result.size());
    /// Increment C late-write guard: a record whose window is already COLD (sliceEnd < the true frontier) is a
    /// late/out-of-order write into the retained band [frontier - L, frontier). The base just early-returned the existing
    /// (possibly spilled) slice; the worker is about to write to it (and cache its raw HashMap*). Unspill + pin it here,
    /// BEFORE that write, so it cannot be (re-)spilled while cached. Gated on emitLag > 0 AND coldness ⇒ a strict no-op
    /// for L == 0 (byte-identical to pre-Increment-C) and for the normal hot/current build path.
    if (emitLag > 0 && result[0]->getSliceEnd().getRawValue() < lastSeenWatermark.load(std::memory_order_relaxed))
    {
        pinAndUnspillForLateWrite(result[0]);
    }
    /// M1: gate the hard-threshold check on a GENUINELY NEW slice. recordCreatedSlice returns false on a cache-hit /
    /// insert-race re-return of an already-tracked slice; re-running residentBytes()/eviction there would spuriously
    /// re-arm the budget against a slice already accounted for.
    if (recordCreatedSlice(result[0]))
    {
        if (const auto resident = residentBytes(); resident > hardThresholdBytes)
        {
            NES_DEBUG("Hard threshold crossed (resident={} > hard={}); synchronous spill on worker path", resident, hardThresholdBytes);
            evictUntilUnderHard();
        }
        /// PR-3 soft signal: only when NOT already over hard (the hard branch already spilled inline). A cheap compare
        /// that wakes the background spiller — it does NOT spill on the worker path, keeping the worker latency-bounded.
        else if (resident > softThresholdBytes)
        {
            NES_TRACE("Soft threshold crossed (resident={} > soft={}); waking spiller", resident, softThresholdBytes);
            spillerCv.notify_one();
        }
    }
    return result;
}

bool SpillableTimeBasedSliceStore::recordCreatedSlice(const std::shared_ptr<Slice>& slice)
{
    const auto [it, inserted] = createdSlices.wlock()->insert_or_assign(slice->getSliceEnd(), std::weak_ptr<Slice>(slice));
    if (inserted)
    {
        NES_TRACE("Recording created slice {} for self-tracking", slice->getSliceEnd());
    }
    return inserted;
}

void SpillableTimeBasedSliceStore::unspillAndMarkEmitted(
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& windowSlices)
{
    /// PR-3 trigger-WAIT (closes the spill-vs-emit / spill-vs-trigger UAF). For each returned spillable slice:
    ///   1. under evictionMutex, WAIT on spillerCv until no in-flight spill of this sliceEnd remains, THEN insert it
    ///      into emittedKeys BEFORE releasing the mutex — so no NEW spill of this slice can start after the wait (the
    ///      spiller's pickAndReserveColdSlice rejects emitted slices), and a slice mid-spill cannot be handed to the
    ///      probe half-freed (we waited the spill out);
    ///   2. OUTSIDE the mutex, if the slice is non-resident (it was fully spilled before we claimed it), unspill it
    ///      (uniform no-I/O-under-lock) — this cannot race a spiller because the slice is now emitted.
    /// The slice always ends resident+emitted. (See the eviction-mutex invariant on evictionMutex.)
    auto provider = bufferProvider.copy();
    for (auto& [windowInfo, slices] : windowSlices)
    {
        for (auto& slice : slices)
        {
            auto spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(slice);
            if (spillable == nullptr)
            {
                continue;
            }
            const auto sliceEnd = spillable->getSliceEnd();
            bool needsUnspill = false;
            {
                std::unique_lock lock(evictionMutex);
                if (spillInProgress.contains(sliceEnd))
                {
                    NES_TRACE("Trigger of slice {} waiting on in-flight spill", sliceEnd);
                    spillerCv.wait(lock, [&] { return !spillInProgress.contains(sliceEnd); });
                }
                /// Claim it BEFORE releasing the mutex: the spiller can never (re-)pick an emitted slice.
                emittedKeys.wlock()->insert(sliceEnd);
                /// Increment C: emitted now ⇒ emittedKeys protects it; drop any (now-redundant) late-write pin so
                /// pinnedKeys stays bounded by live, unemitted, late-touched slices.
                pinnedKeys.erase(sliceEnd);
                /// If it is spilled we will unspill it OUTSIDE the lock. Reserve it in spillInProgress for the duration
                /// so a concurrent residentBytes() SKIPS this slice while unspill() is rebuilding its maps (the read
                /// would otherwise race the map mutation). No spiller can pick it (it is emitted), so the reservation is
                /// purely to fence the footprint reader.
                needsUnspill = !spillable->isResident();
                if (needsUnspill)
                {
                    spillInProgress.insert(sliceEnd);
                }
            }
            if (needsUnspill)
            {
                INVARIANT(provider != nullptr, "buffer provider must be stashed before an unspill-on-read");
                NES_DEBUG("Unspilling slice {} on trigger read", sliceEnd);
                /// L1: mirror the PR-2 spill guard on the unspill path. If unspill() throws, the spillInProgress
                /// reservation (the footprint-reader fence) must STILL be released — a leak would make residentBytes()
                /// permanently skip this slice AND hang any future spillerCv.wait on this sliceEnd forever. The guard's
                /// release runs under evictionMutex (via releaseReservation) and notifies spillerCv, exactly matching the
                /// success-path cleanup below. Commit only after the spilledKeys.erase succeeds so the success path does
                /// not double-erase the reservation (the manual block below + the guard would otherwise both run).
                ReservationGuard unspillGuard{*this, sliceEnd};
                spillable->unspill(*backend, *provider); /// I/O OUTSIDE evictionMutex (slice is emitted ⇒ no spiller race)
                spilledKeys.wlock()->erase(sliceEnd);
                /// Release the footprint-reader fence + wake anyone waiting on this sliceEnd.
                {
                    std::unique_lock lock(evictionMutex);
                    spillInProgress.erase(sliceEnd);
                }
                spillerCv.notify_all();
                unspillGuard.committed = true; /// success: reservation already erased above, do not double-erase
            }
        }
    }
}

void SpillableTimeBasedSliceStore::pinAndUnspillForLateWrite(const std::shared_ptr<Slice>& slice)
{
    auto spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(slice);
    if (spillable == nullptr)
    {
        return;
    }
    const auto sliceEnd = spillable->getSliceEnd();
    bool needsUnspill = false;
    {
        std::unique_lock lock(evictionMutex);
        /// PIN first: once pinned, pickAndReserveColdSlice (background spiller + synchronous hard-eviction) skips this
        /// slice, so after we make it resident it STAYS resident until it emits — closing the re-spill race (the worker
        /// caches its raw HashMap* and a later cache HIT is JIT, uninterceptable). Idempotent (a plain set insert).
        pinnedKeys.insert(sliceEnd);
        if (spillInProgress.contains(sliceEnd))
        {
            /// A spill of this slice is in flight — wait it out before unspilling (cannot unspill mid-spill). The pin
            /// already ensures no NEW spill can start after we wake.
            NES_TRACE("Late write into slice {} waiting on in-flight spill", sliceEnd);
            spillerCv.wait(lock, [&] { return !spillInProgress.contains(sliceEnd); });
        }
        needsUnspill = !spillable->isResident();
        if (needsUnspill)
        {
            /// Reserve for the unspill duration so a concurrent residentBytes() SKIPS this slice while unspill() rebuilds
            /// its maps (same footprint-reader fence as the trigger/random-access unspill paths).
            spillInProgress.insert(sliceEnd);
        }
    }
    if (needsUnspill)
    {
        auto provider = bufferProvider.copy();
        INVARIANT(provider != nullptr, "buffer provider must be stashed before an unspill-on-write");
        NES_DEBUG("Unspilling + pinning slice {} on late write into the retained band", sliceEnd);
        /// I/O OUTSIDE evictionMutex (LOCKED #7). If unspill() throws (I/O failure), the guard releases the reservation
        /// and the exception propagates out of getSlicesOrCreate as a query error (NOT a process terminate); the pin
        /// stays (harmless — the slice remains spilled, and a later emit/GC will retry/clean it).
        ReservationGuard unspillGuard{*this, sliceEnd};
        spillable->unspill(*backend, *provider);
        spilledKeys.wlock()->erase(sliceEnd);
        {
            std::unique_lock lock(evictionMutex);
            spillInProgress.erase(sliceEnd);
        }
        spillerCv.notify_all();
        unspillGuard.committed = true; /// success: reservation already erased above, do not double-erase
    }
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
SpillableTimeBasedSliceStore::getTriggerableWindowSlices(const Timestamp globalWatermark)
{
    /// Increment C emit-decouple: drive the EMIT cutoff with a SATURATING globalWatermark - emitLag (clamp to 0 so an
    /// early-stream watermark < L cannot wrap Timestamp::operator-, which is a raw unsigned subtract), while keeping the
    /// TRUE globalWatermark for quiescence/cold-pick below. The base store retains completed windows in
    /// [globalWatermark - L, globalWatermark) — they stay cold-but-unemitted, so the spiller can finally evict them.
    /// With emitLag == 0 the lagged watermark equals the true watermark ⇒ byte-identical to pre-Increment-C.
    const auto rawWatermark = globalWatermark.getRawValue();
    const auto laggedEmitWatermark = Timestamp(rawWatermark >= emitLag ? rawWatermark - emitLag : 0);
    NES_TRACE("Emit-lagged trigger: true wm={}, emitLag={}, lagged emit wm={}", rawWatermark, emitLag, laggedEmitWatermark.getRawValue());
    auto result = DefaultTimeBasedSliceStore::getTriggerableWindowSlices(laggedEmitWatermark);
    unspillAndMarkEmitted(result);
    /// O-B-watermark: learn the build watermark monotonically (CAS-max) from the TRUE globalWatermark (NOT the lagged
    /// emit watermark) — quiescence/cold-selection must track the real frontier so the retained band is spillable.
    /// Slices ending before it are cold. A lagging value only under-selects cold slices (safe — never spills hot).
    auto current = lastSeenWatermark.load(std::memory_order_relaxed);
    const auto incoming = globalWatermark.getRawValue();
    while (incoming > current && !lastSeenWatermark.compare_exchange_weak(current, incoming, std::memory_order_relaxed))
    {
    }
    /// PR-3: a freshly advanced watermark may have just turned previously-hot slices cold — wake the spiller so it can
    /// preemptively evict them (the soft-budget background path) instead of waiting for the next slice-creation tick.
    spillerCv.notify_one();
    return result;
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> SpillableTimeBasedSliceStore::getAllNonTriggeredSlices()
{
    auto result = DefaultTimeBasedSliceStore::getAllNonTriggeredSlices();
    unspillAndMarkEmitted(result);
    /// Deliberately do NOT bump lastSeenWatermark here: this is the terminate path, where the spiller is quiesced
    /// before deleteState, so cold-selection is moot. Leaving it unchanged keeps the learned watermark a pure
    /// build-progress signal (O-B-watermark).
    return result;
}

std::optional<std::shared_ptr<Slice>> SpillableTimeBasedSliceStore::getSliceBySliceEnd(const SliceEnd sliceEnd)
{
    auto opt = DefaultTimeBasedSliceStore::getSliceBySliceEnd(sliceEnd);
    if (opt.has_value())
    {
        if (auto spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(opt.value()); spillable != nullptr)
        {
            /// PR-3: random-access read (join-probe path; out of 4b scope) must be concurrency-safe too. WAIT out any
            /// in-flight spill of this slice, then RESERVE it so neither a concurrent spiller pick nor a residentBytes()
            /// footprint read races the unspill() map rebuild. The reservation is released after the I/O.
            bool needsUnspill = false;
            {
                std::unique_lock lock(evictionMutex);
                if (spillInProgress.contains(sliceEnd))
                {
                    spillerCv.wait(lock, [&] { return !spillInProgress.contains(sliceEnd); });
                }
                needsUnspill = !spillable->isResident();
                if (needsUnspill)
                {
                    spillInProgress.insert(sliceEnd);
                }
            }
            if (needsUnspill)
            {
                auto provider = bufferProvider.copy();
                INVARIANT(provider != nullptr, "buffer provider must be stashed before an unspill-on-read");
                NES_DEBUG("Unspilling slice {} on getSliceBySliceEnd", sliceEnd);
                /// L1: same RAII fix as unspillAndMarkEmitted, but MORE reachable here — this slice is NOT emitted, so a
                /// leaked reservation on an unspill() throw would make every future access to this sliceEnd (trigger or
                /// probe) wait forever on spillerCv. The guard releases the reservation (under evictionMutex, with a
                /// notify) on the throw path; committed after the manual erase below so the success path does not
                /// double-erase.
                ReservationGuard unspillGuard{*this, sliceEnd};
                spillable->unspill(*backend, *provider);
                spilledKeys.wlock()->erase(sliceEnd);
                {
                    std::unique_lock lock(evictionMutex);
                    spillInProgress.erase(sliceEnd);
                }
                spillerCv.notify_all();
                unspillGuard.committed = true; /// success: reservation already erased above, do not double-erase
            }
        }
    }
    return opt;
}

void SpillableTimeBasedSliceStore::garbageCollectSlicesAndWindows(const Timestamp newGlobalWaterMark)
{
    DefaultTimeBasedSliceStore::garbageCollectSlicesAndWindows(newGlobalWaterMark);

    /// Collect the spilled keys the base GC has now dropped (using the base, non-unspilling lookup so the presence
    /// check does not rebuild the very slice we are dropping), then release the spilledKeys lock BEFORE touching the
    /// backend or emittedKeys. Never hold two of our locks at once — the trigger path acquires spilledKeys and
    /// emittedKeys independently, so nesting them here would be a lock-order inversion (H1).
    std::vector<SliceEnd> droppedKeys;
    {
        auto keys = spilledKeys.wlock();
        for (auto it = keys->begin(); it != keys->end();)
        {
            if (!DefaultTimeBasedSliceStore::getSliceBySliceEnd(*it).has_value())
            {
                droppedKeys.push_back(*it);
                it = keys->erase(it);
            }
            else
            {
                ++it;
            }
        }
    }
    for (const auto droppedKey : droppedKeys)
    {
        NES_TRACE("Erasing backend record for garbage-collected slice {}", droppedKey);
        backend->erase(droppedKey);
        emittedKeys.wlock()->erase(droppedKey);
    }
    /// Increment C: drop late-write pins for GC'd slices. Taken on its own — evictionMutex is never nested with the
    /// folly set locks above (the eviction-mutex lock-order invariant), so this runs after they are released.
    if (!droppedKeys.empty())
    {
        std::unique_lock lock(evictionMutex);
        for (const auto droppedKey : droppedKeys)
        {
            pinnedKeys.erase(droppedKey);
        }
    }

    /// Prune self-tracked entries whose slice the base GC has dropped (weak_ptr now expired). Taken on its own —
    /// never nested with the spilledKeys/emittedKeys locks (H1) — so it can run after they have been released.
    {
        auto tracked = createdSlices.wlock();
        for (auto it = tracked->begin(); it != tracked->end();)
        {
            if (it->second.expired())
            {
                it = tracked->erase(it);
            }
            else
            {
                ++it;
            }
        }
    }
}

void SpillableTimeBasedSliceStore::deleteState()
{
    /// PR-3: quiesce + join the background spiller FIRST, so it can never spill/reserve a slice while we are tearing
    /// down the tracked state below (and never touch backend/bufferProvider mid-clear). Idempotent with the dtor.
    NES_TRACE("Quiescing spiller before deleteState");
    stopSpiller();
    /// Drop all on-disk records and tracked state before the base clears its in-memory maps. (Locks taken one at a
    /// time — see the H1 note in garbageCollectSlicesAndWindows.)
    std::vector<SliceEnd> keysToErase;
    {
        auto keys = spilledKeys.wlock();
        keysToErase.assign(keys->begin(), keys->end());
        keys->clear();
    }
    emittedKeys.wlock()->clear();
    /// Increment C: clear late-write pins (spiller already quiesced above, so no concurrent pickAndReserveColdSlice).
    {
        std::unique_lock lock(evictionMutex);
        pinnedKeys.clear();
    }
    for (const auto sliceEnd : keysToErase)
    {
        backend->erase(sliceEnd);
    }
    /// Drop all self-tracking bookkeeping before the base clears its in-memory maps. (Spiller-quiesce is added in PR-3.)
    createdSlices.wlock()->clear();
    DefaultTimeBasedSliceStore::deleteState();
}

void SpillableTimeBasedSliceStore::forceSpill(const SliceEnd sliceEnd)
{
    /// Lock-order fix: DefaultTimeBasedSliceStore::getSliceBySliceEnd acquires the base `slices` rlock. The LOCKED rule
    /// is "base slices/windows are NEVER taken under evictionMutex". So the slice LOOKUP is done BEFORE acquiring
    /// evictionMutex; only the emitted-check + in-flight-check + reserve happen inside the mutex, and the spill() I/O is
    /// performed OUTSIDE it (uniform no-I/O-under-lock). The shared_ptr returned by the lookup keeps the slice alive
    /// across the critical section, so re-reading it after acquiring the mutex needs no second base touch.
    auto opt = DefaultTimeBasedSliceStore::getSliceBySliceEnd(sliceEnd);
    if (!opt.has_value())
    {
        NES_DEBUG("forceSpill: no slice for sliceEnd {}", sliceEnd);
        return;
    }
    auto spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(opt.value());
    INVARIANT(spillable != nullptr, "forceSpill on a non-spillable slice {}", sliceEnd);

    {
        /// M1 (Increment A TOCTOU, closed in PR-2): the emitted-check and the reservation happen atomically under
        /// evictionMutex, sharing the serializer with the eviction path so a concurrent trigger cannot mark the slice
        /// emitted between the check and the reserve. isResident() reads the slice's own flag (no base lock), so it is
        /// safe to evaluate under evictionMutex.
        std::unique_lock lock(evictionMutex);
        if (emittedKeys.rlock()->contains(sliceEnd))
        {
            throw SpillStoreFailure(
                "refusing to spill slice {} that has been emitted to the probe (async-probe-drain invariant)", sliceEnd);
        }
        /// Increment C note: forceSpill deliberately does NOT consult pinnedKeys. It is the Increment-A manual/test-only
        /// driver, never wired on a production hot path, and pinnedKeys is only populated when emitLag > 0 (the
        /// emit-decouple late-write guard). If forceSpill is ever put on a live path, it MUST add the pinnedKeys check
        /// (mirroring pickAndReserveColdSlice) or it would re-spill a cached late-touched slice → dangling SliceCache ptr.
        if (spillInProgress.contains(sliceEnd))
        {
            /// Another path already reserved this slice for an in-flight spill — do not double-write.
            NES_DEBUG("forceSpill: slice {} already has an in-flight spill, no-op", sliceEnd);
            return;
        }
        if (!spillable->isResident())
        {
            NES_DEBUG("forceSpill: slice {} is already spilled, no-op", sliceEnd);
            return;
        }
        spillInProgress.insert(sliceEnd);
    }
    /// I/O OUTSIDE evictionMutex, protected by the reservation. H2: forceSpill PRESERVES its throwing contract, so on a
    /// spill() failure we still RE-THROW — but the reservation MUST be released first (a leak would deadlock the PR-3
    /// condvar wait on this SliceEnd). The guard erases it on the throw path; on success finalizeSpill erases it and we
    /// set committed so the guard does not double-erase. (The pre-insert SpillStoreFailure for emitted slices throws
    /// BEFORE reserving, so that path needs no guard.)
    ReservationGuard guard{*this, sliceEnd};
    NES_DEBUG("Spilling slice {} ({} bytes resident)", sliceEnd, residentBytesOf(*spillable));
    spillable->spill(*backend); /// if this throws, the guard releases the reservation, then the exception propagates
    /// L3: ++spillCount advances just BEFORE finalizeSpill inserts into spilledKeys, so spillCountForTest() reads ">=
    /// finalized spills" (see spillOneColdSlice). PR-3's stress test must assert spillCountForTest() > 0, not an exact
    /// equality tied to spilledKeys size.
    ++spillCount;
    finalizeSpill(sliceEnd); /// finalizeSpill already erased the reservation
    guard.committed = true; /// do not double-erase
}

std::optional<SliceEnd> SpillableTimeBasedSliceStore::pickAndReserveColdSlice()
{
    std::unique_lock lock(evictionMutex);
    /// PR-3: once quiesced (stopSpiller before join / before deleteState) the spiller must stop picking, so the loop
    /// drains promptly and never reserves a slice the store is about to tear down.
    if (quiesced)
    {
        return std::nullopt;
    }
    const auto wm = lastSeenWatermark.load(std::memory_order_relaxed);

    /// Snapshot (end, weak) pairs under ONE createdSlices rlock, then RELEASE it before evaluating the predicates —
    /// never nest createdSlices with the folly set locks (spilledKeys/emittedKeys) and never take a base map under
    /// evictionMutex (NH1). The map is sorted ascending by SliceEnd, so this snapshot is already oldest-first.
    /// evictionMutex is held across the whole snapshot+predicate scan (PLAN-mandated): a concurrent emitted-mark or
    /// reservation cannot race the predicate evaluation between the check and spillInProgress.insert.
    std::vector<std::pair<SliceEnd, std::weak_ptr<Slice>>> snapshot;
    createdSlices.withRLock(
        [&](const auto& tracked)
        {
            snapshot.reserve(tracked.size());
            for (const auto& [end, weak] : tracked)
            {
                snapshot.emplace_back(end, weak);
            }
        });

    for (const auto& [end, weak] : snapshot)
    {
        if (end.getRawValue() >= wm)
        {
            /// Not cold (sliceEnd >= watermark). The snapshot is ascending by SliceEnd, so NO later entry can be cold
            /// either — stop scanning. (This is the ONLY break: it is the single failure that monotonically rules out
            /// all remaining candidates. Every other predicate below uses `continue`, because a later cold slice may
            /// still qualify; breaking on one of those would skip valid candidates.)
            break;
        }
        if (spillInProgress.contains(end))
        {
            /// Already reserved by another in-flight spill — a LATER cold slice may still qualify, so continue.
            continue;
        }
        const auto slice = weak.lock();
        if (slice == nullptr)
        {
            /// Base GC dropped this slice — a later cold slice may still qualify, so continue.
            continue;
        }
        const auto spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(slice);
        /// L1: BOTH residency checks are intentional. spillable->isResident() reads the slice's own internal flag;
        /// isSliceResident(end) reads spilledKeys. They must agree for a correctly-tracked slice, but checking both
        /// defends the spill path against any transient divergence between the two residency-tracking mechanisms
        /// (e.g. a spill that flipped the flag but had not yet inserted into spilledKeys). A non-spillable / already
        /// non-resident slice is not a candidate, but a later cold slice may still qualify — so continue.
        if (spillable == nullptr || !spillable->isResident() || !isSliceResident(end))
        {
            continue;
        }
        if (emittedKeys.rlock()->contains(end))
        {
            /// Emitted to the async probe — must never be spilled, but a later cold slice may still qualify, so continue.
            continue;
        }
        if (pinnedKeys.contains(end))
        {
            /// Increment C: a late write re-activated this band slice (its raw HashMap* is cached on a worker), so it must
            /// stay resident until it emits — not a spill candidate. A later cold slice may still qualify, so continue.
            continue;
        }
        spillInProgress.insert(end);
        NES_DEBUG("Reserved cold slice {} for spill", end);
        return end;
    }
    return std::nullopt;
}

void SpillableTimeBasedSliceStore::finalizeSpill(const SliceEnd end)
{
    /// M2: keep the evictionMutex scope to the in-memory mutation ONLY (spilledKeys.insert + spillInProgress.erase).
    /// residentBytes() does a full createdSlices scan (rlock + per-slice weak_ptr::lock); calling it under
    /// evictionMutex inflates the hold time the PR-3 spiller/workers contend on, so keep it out of the lock here.
    {
        std::unique_lock lock(evictionMutex);
        spilledKeys.wlock()->insert(end);
        spillInProgress.erase(end);
    }
    /// PR-3: notify any trigger WAITing on this slice's in-flight spill (unspillAndMarkEmitted's spillerCv.wait). The
    /// reservation is now cleared, so its predicate (!spillInProgress.contains(end)) becomes true. notify_all because
    /// multiple triggers could be waiting on distinct sliceEnds sharing this single condvar.
    spillerCv.notify_all();
    NES_DEBUG("Spilled cold slice {}", end);
}

bool SpillableTimeBasedSliceStore::spillOneColdSlice()
{
    const auto picked = pickAndReserveColdSlice();
    if (!picked.has_value())
    {
        return false;
    }
    /// The reservation is now held; the guard erases it on EVERY path that does not finalize (expired weak_ptr,
    /// spill() throw) so spillInProgress can never leak a SliceEnd (H1).
    ReservationGuard guard{*this, *picked};
    /// Lock the spillable OUTSIDE evictionMutex. If it expired between reserve and lookup, the guard clears the
    /// reservation and we report progress (the slice is gone, so the budget already dropped).
    const auto opt = DefaultTimeBasedSliceStore::getSliceBySliceEnd(*picked);
    std::shared_ptr<SpillableAggregationSlice> spillable;
    if (opt.has_value())
    {
        spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(opt.value());
    }
    if (spillable == nullptr)
    {
        return true; /// guard releases the reservation
    }
    /// I/O OUTSIDE evictionMutex, protected by the reservation. O-B5: the eviction path must NEVER propagate — if the
    /// spill() I/O throws, swallow it, but the reservation MUST be released (the guard does that) so the slice is not
    /// stuck in-flight forever. Proceed over-budget rather than crash a worker.
    try
    {
        spillable->spill(*backend);
    }
    catch (const std::exception& e)
    {
        NES_WARNING("spill of cold slice {} failed: {}; reservation released, proceeding over-budget", *picked, e.what());
        return false; /// guard releases the reservation
    }
    /// L3: ++spillCount is bumped here, just BEFORE finalizeSpill inserts into spilledKeys. So spillCountForTest()
    /// momentarily reads "one ahead" of the finalized-spill set. The PR-3 stress test must treat spillCountForTest()
    /// as ">= finalized spills" (assert > 0), NOT an exact equality tied to spilledKeys size.
    ++spillCount;
    finalizeSpill(*picked); /// finalizeSpill already erased the reservation
    guard.committed = true; /// do not double-erase
    return true;
}

void SpillableTimeBasedSliceStore::evictUntilUnderHard()
{
    /// O-B5 (hard): proceed over-budget, never throw, never block. Stop once back under hard OR no cold candidate
    /// remains (then the only resident state is genuinely-needed filling/emitted memory we are forbidden to spill).
    while (residentBytes() > hardThresholdBytes)
    {
        if (!spillOneColdSlice())
        {
            NES_WARNING(
                "Over hard budget ({} > {}) but no COLD slice to evict; proceeding over-budget", residentBytes(), hardThresholdBytes);
            break;
        }
    }
}

bool SpillableTimeBasedSliceStore::isSliceResident(const SliceEnd sliceEnd) const
{
    return !spilledKeys.rlock()->contains(sliceEnd);
}

uint64_t SpillableTimeBasedSliceStore::residentBytesOf(const Slice& slice) const
{
    /// A spilled slice has freed its maps → zero resident footprint (the on-disk copy is not counted).
    if (const auto* spillable = dynamic_cast<const SpillableAggregationSlice*>(&slice); spillable != nullptr && !spillable->isResident())
    {
        return 0;
    }
    const auto* hashMapSlice = dynamic_cast<const HashMapSlice*>(&slice);
    INVARIANT(hashMapSlice != nullptr, "residentBytesOf expects a HashMapSlice-derived slice");
    uint64_t bytes = 0;
    for (uint64_t worker = 0; worker < hashMapSlice->getNumberOfHashMaps(); ++worker)
    {
        if (const auto* map = hashMapSlice->getHashMapPtr(WorkerThreadId(static_cast<WorkerThreadId::Underlying>(worker))); map != nullptr)
        {
            const uint64_t tuples = map->getNumberOfTuples();
            bytes += ((tuples + entriesPerPage - 1) / entriesPerPage) * pageSize;
        }
    }
    return bytes;
}

uint64_t SpillableTimeBasedSliceStore::residentBytes() const
{
    /// O-B-residentBytes, made concurrency-safe for PR-3. The per-slice footprint read (residentBytesOf →
    /// HashMapSlice::getHashMapPtr / getNumberOfTuples) dereferences the slice's `hashMaps`, which spill()/unspill()
    /// mutate (and free) OUTSIDE evictionMutex while a slice is reserved in spillInProgress (spill) or being
    /// unspilled-on-read (also reserved in spillInProgress, see unspillAndMarkEmitted / getSliceBySliceEnd). To avoid a
    /// data race / the resident-INVARIANT trip, we hold evictionMutex while snapshotting BOTH the tracked slices and
    /// the in-flight reservation set, then SKIP any slice that is in-flight (its footprint is transitioning to/from 0
    /// anyway) and read only the stable, non-reserved slices. Lock order evictionMutex → createdSlices (consistent).
    /// CALLER CONTRACT: must NOT already hold evictionMutex (the spiller loop computes resident bytes WITHOUT the lock —
    /// see spillerLoop, which releases evictionMutex before each residentBytes() call). residentBytesOf is read WHILE
    /// holding evictionMutex: an unreserved slice cannot transition INTO spillInProgress (and thus cannot begin a
    /// concurrent spill()/unspill() that frees its maps) while we hold the lock, so the footprint read is race-free.
    std::unique_lock lock(evictionMutex);
    uint64_t sum = 0;
    createdSlices.withRLock(
        [&](const auto& tracked)
        {
            for (const auto& [end, weak] : tracked)
            {
                if (spillInProgress.contains(end))
                {
                    continue; /// in-flight spill/unspill mutating this slice's maps — skip (footprint transitioning to/from 0)
                }
                if (const auto slice = weak.lock())
                {
                    sum += residentBytesOf(*slice);
                }
            }
        });
    return sum;
}

void SpillableTimeBasedSliceStore::setLastSeenWatermarkForTest(const Timestamp watermark)
{
    /// Test hook: seed the learned watermark directly (not monotonic-guarded — the test owns ordering).
    lastSeenWatermark.store(watermark.getRawValue(), std::memory_order_relaxed);
}

std::size_t SpillableTimeBasedSliceStore::createdSlicesSizeForTest() const
{
    return createdSlices.rlock()->size();
}

void SpillableTimeBasedSliceStore::spillColdSlicesUnderHardForTest()
{
    evictUntilUnderHard();
}

std::size_t SpillableTimeBasedSliceStore::spillCountForTest() const
{
    return spillCount.load();
}

void SpillableTimeBasedSliceStore::spillerLoop(std::stop_token stopToken)
{
    /// O-B2 soft path: block on spillerCv (woken by the getSlicesOrCreate soft signal / trigger feeder / stopSpiller),
    /// bounded by SPILLER_POLL_INTERVAL as a safety net so a store that stopped creating slices but is still over soft
    /// is still drained and the stop token is re-checked promptly.
    ///
    /// IMPORTANT (deadlock avoidance): residentBytes() itself acquires evictionMutex, so it must NEVER be called while
    /// this loop holds evictionMutex. The wait therefore uses a plain timed wait_for with NO predicate touching
    /// residentBytes(); the budget is evaluated AFTER the lock is released. spillOneColdSlice likewise manages its own
    /// locks and does its I/O outside the mutex, so it is always called with the loop NOT holding evictionMutex.
    while (!stopToken.stop_requested())
    {
        {
            std::unique_lock lock(evictionMutex);
            spillerCv.wait_for(lock, SPILLER_POLL_INTERVAL, [&] { return stopToken.stop_requested(); });
        }
        if (stopToken.stop_requested())
        {
            break;
        }
        /// Spill cold slices down to soft. O-B5: if nothing cold is available, log once and go back to waiting.
        /// L2: capture the budget check once per iteration and reuse it in the no-candidate log — residentBytes() takes
        /// evictionMutex and scans createdSlices, so calling it a second time only to format the trace line doubles the
        /// lock contention on the spiller path when trace logging is enabled. No behavior change (same authoritative
        /// value drives the condition and the message).
        uint64_t resident = residentBytes();
        while (resident > softThresholdBytes)
        {
            if (!spillOneColdSlice())
            {
                NES_TRACE("Over soft ({} > {}) but no cold candidate; spiller sleeping", resident, softThresholdBytes);
                break;
            }
            if (stopToken.stop_requested())
            {
                break;
            }
            /// A slice was spilled this iteration — re-read the (now-lower) budget for the next loop condition. This is
            /// the single authoritative read per iteration that L2 deduplicates; it replaces the original condition-only
            /// residentBytes() call so the loop still drains down to soft.
            resident = residentBytes();
        }
    }
}

void SpillableTimeBasedSliceStore::startSpiller()
{
    bool expected = false;
    if (spillerStarted.compare_exchange_strong(expected, true))
    {
        spiller = std::jthread([this](std::stop_token stopToken) { spillerLoop(std::move(stopToken)); });
        NES_DEBUG("Started background spiller thread (soft={}B, hard={}B)", softThresholdBytes, hardThresholdBytes);
    }
}

void SpillableTimeBasedSliceStore::stopSpiller()
{
    /// Idempotent: callable from BOTH the dtor and deleteState. Setting `quiesced` first makes pickAndReserveColdSlice
    /// bail so the loop drains; then request_stop + notify wakes the wait_for, and join blocks until the loop exits.
    {
        std::unique_lock lock(evictionMutex);
        quiesced = true;
    }
    if (spiller.joinable())
    {
        NES_DEBUG("Stopping and joining background spiller thread");
        spiller.request_stop();
        spillerCv.notify_all();
        spiller.join();
    }
}

}
