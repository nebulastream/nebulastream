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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/RocksDBSpillBackend.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Aggregation/SpillableAggregationSlice.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

SpillableTimeBasedSliceStore::SpillableTimeBasedSliceStore(
    const uint64_t windowSize,
    const uint64_t windowSlide,
    SliceCacheConfiguration sliceCacheConfiguration,
    std::unique_ptr<SpillBackend> backend,
    const uint64_t entriesPerPage,
    const uint64_t pageSize,
    const uint64_t softThresholdBytes,
    const uint64_t hardThresholdBytes)
    : DefaultTimeBasedSliceStore(windowSize, windowSlide, std::move(sliceCacheConfiguration))
    , backend(std::move(backend))
    , entriesPerPage(entriesPerPage)
    , pageSize(pageSize)
    , softThresholdBytes(softThresholdBytes)
    , hardThresholdBytes(hardThresholdBytes)
{
    PRECONDITION(this->backend != nullptr, "SpillableTimeBasedSliceStore requires a non-null SpillBackend");
    PRECONDITION(entriesPerPage > 0, "entriesPerPage must be > 0");
    PRECONDITION(pageSize > 0, "pageSize must be > 0");
    NES_DEBUG(
        "Constructed SpillableTimeBasedSliceStore (entriesPerPage={}, pageSize={}, softThresholdBytes={}, hardThresholdBytes={})",
        entriesPerPage,
        pageSize,
        softThresholdBytes,
        hardThresholdBytes);
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
    /// M1: gate the hard-threshold check on a GENUINELY NEW slice. recordCreatedSlice returns false on a cache-hit /
    /// insert-race re-return of an already-tracked slice; re-running residentBytes()/eviction there would spuriously
    /// re-arm the budget against a slice already accounted for.
    if (recordCreatedSlice(result[0]))
    {
        if (const auto resident = residentBytes(); resident > hardThresholdBytes)
        {
            NES_DEBUG(
                "Hard threshold crossed (resident={} > hard={}); synchronous spill on worker path", resident, hardThresholdBytes);
            evictUntilUnderHard();
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
            if (!spillable->isResident())
            {
                INVARIANT(provider != nullptr, "buffer provider must be stashed before an unspill-on-read");
                NES_DEBUG("Unspilling slice {} on trigger read", sliceEnd);
                spillable->unspill(*backend, *provider);
                spilledKeys.wlock()->erase(sliceEnd);
            }
            /// Returned to the trigger path → its raw HashMap* will be snapshotted for the async probe; never spill now.
            emittedKeys.wlock()->insert(sliceEnd);
        }
    }
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
SpillableTimeBasedSliceStore::getTriggerableWindowSlices(const Timestamp globalWatermark)
{
    auto result = DefaultTimeBasedSliceStore::getTriggerableWindowSlices(globalWatermark);
    unspillAndMarkEmitted(result);
    /// O-B-watermark: learn the build watermark monotonically (CAS-max). Slices ending before it are cold. A lagging
    /// value only under-selects cold slices (safe — never spills something the engine still considers hot).
    auto current = lastSeenWatermark.load(std::memory_order_relaxed);
    const auto incoming = globalWatermark.getRawValue();
    while (incoming > current && !lastSeenWatermark.compare_exchange_weak(current, incoming, std::memory_order_relaxed))
    {
    }
    return result;
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
SpillableTimeBasedSliceStore::getAllNonTriggeredSlices()
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
        if (auto spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(opt.value());
            spillable != nullptr && !spillable->isResident())
        {
            auto provider = bufferProvider.copy();
            INVARIANT(provider != nullptr, "buffer provider must be stashed before an unspill-on-read");
            NES_DEBUG("Unspilling slice {} on getSliceBySliceEnd", sliceEnd);
            spillable->unspill(*backend, *provider);
            spilledKeys.wlock()->erase(sliceEnd);
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
    /// Drop all on-disk records and tracked state before the base clears its in-memory maps. (Locks taken one at a
    /// time — see the H1 note in garbageCollectSlicesAndWindows.)
    std::vector<SliceEnd> keysToErase;
    {
        auto keys = spilledKeys.wlock();
        keysToErase.assign(keys->begin(), keys->end());
        keys->clear();
    }
    emittedKeys.wlock()->clear();
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
    /// M1 (Increment A TOCTOU, closed in PR-2): the emitted-check and the reservation now happen atomically under
    /// evictionMutex, sharing the serializer with the eviction path so a concurrent trigger cannot mark the slice
    /// emitted between the check and the reserve. The spill() I/O is performed OUTSIDE the mutex (uniform
    /// no-I/O-under-lock), protected by the spillInProgress reservation.
    std::shared_ptr<SpillableAggregationSlice> spillable;
    {
        std::unique_lock lock(evictionMutex);
        if (emittedKeys.rlock()->contains(sliceEnd))
        {
            throw SpillStoreFailure(
                "refusing to spill slice {} that has been emitted to the probe (async-probe-drain invariant)", sliceEnd);
        }
        if (spillInProgress.contains(sliceEnd))
        {
            /// Another path already reserved this slice for an in-flight spill — do not double-write.
            NES_DEBUG("forceSpill: slice {} already has an in-flight spill, no-op", sliceEnd);
            return;
        }
        auto opt = DefaultTimeBasedSliceStore::getSliceBySliceEnd(sliceEnd);
        if (!opt.has_value())
        {
            NES_DEBUG("forceSpill: no slice for sliceEnd {}", sliceEnd);
            return;
        }
        spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(opt.value());
        INVARIANT(spillable != nullptr, "forceSpill on a non-spillable slice {}", sliceEnd);
        if (!spillable->isResident())
        {
            NES_DEBUG("forceSpill: slice {} is already spilled, no-op", sliceEnd);
            return;
        }
        spillInProgress.insert(sliceEnd);
    }
    /// I/O OUTSIDE evictionMutex, protected by the reservation.
    NES_DEBUG("Spilling slice {} ({} bytes resident)", sliceEnd, residentBytesOf(*spillable));
    spillable->spill(*backend);
    ++spillCount;
    finalizeSpill(sliceEnd);
}

std::optional<SliceEnd> SpillableTimeBasedSliceStore::pickAndReserveColdSlice()
{
    std::unique_lock lock(evictionMutex);
    const auto wm = lastSeenWatermark.load(std::memory_order_relaxed);

    /// Snapshot (end, weak) pairs under ONE createdSlices rlock, then RELEASE it before evaluating the predicates —
    /// never nest createdSlices with the folly set locks (spilledKeys/emittedKeys) and never take a base map under
    /// evictionMutex (NH1). The map is sorted ascending by SliceEnd, so this snapshot is already oldest-first.
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
            /// Not cold yet (still within the build watermark). Ascending order ⇒ no later entry is cold either, but
            /// keep scanning for clarity — the predicate short-circuits cheaply.
            continue;
        }
        if (spillInProgress.contains(end))
        {
            continue;
        }
        const auto slice = weak.lock();
        if (slice == nullptr)
        {
            continue;
        }
        const auto spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(slice);
        if (spillable == nullptr || !spillable->isResident() || !isSliceResident(end))
        {
            continue;
        }
        if (emittedKeys.rlock()->contains(end))
        {
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
    std::unique_lock lock(evictionMutex);
    spilledKeys.wlock()->insert(end);
    spillInProgress.erase(end);
    NES_DEBUG("Spilled cold slice {}; resident now {}", end, residentBytes());
}

bool SpillableTimeBasedSliceStore::spillOneColdSlice()
{
    const auto picked = pickAndReserveColdSlice();
    if (!picked.has_value())
    {
        return false;
    }
    /// Lock the spillable OUTSIDE evictionMutex. If it expired between reserve and lookup, clear the reservation and
    /// report progress (the slice is gone, so the budget already dropped).
    const auto opt = DefaultTimeBasedSliceStore::getSliceBySliceEnd(*picked);
    std::shared_ptr<SpillableAggregationSlice> spillable;
    if (opt.has_value())
    {
        spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(opt.value());
    }
    if (spillable == nullptr)
    {
        std::unique_lock lock(evictionMutex);
        spillInProgress.erase(*picked);
        return true;
    }
    /// I/O OUTSIDE evictionMutex, protected by the reservation.
    spillable->spill(*backend);
    ++spillCount;
    finalizeSpill(*picked);
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
                "Over hard budget ({} > {}) but no COLD slice to evict; proceeding over-budget",
                residentBytes(),
                hardThresholdBytes);
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
    if (const auto* spillable = dynamic_cast<const SpillableAggregationSlice*>(&slice);
        spillable != nullptr && !spillable->isResident())
    {
        return 0;
    }
    const auto* hashMapSlice = dynamic_cast<const HashMapSlice*>(&slice);
    INVARIANT(hashMapSlice != nullptr, "residentBytesOf expects a HashMapSlice-derived slice");
    uint64_t bytes = 0;
    for (uint64_t worker = 0; worker < hashMapSlice->getNumberOfHashMaps(); ++worker)
    {
        if (const auto* map = hashMapSlice->getHashMapPtr(WorkerThreadId(static_cast<WorkerThreadId::Underlying>(worker)));
            map != nullptr)
        {
            const uint64_t tuples = map->getNumberOfTuples();
            bytes += ((tuples + entriesPerPage - 1) / entriesPerPage) * pageSize;
        }
    }
    return bytes;
}

uint64_t SpillableTimeBasedSliceStore::residentBytes() const
{
    /// Read-only snapshot (O-B-residentBytes): lock `createdSlices`, lock() each weak_ptr, skip expired entries, and
    /// sum the existing per-slice footprint (0 for spilled). Touches no private base map; tolerates slight staleness.
    uint64_t sum = 0;
    createdSlices.withRLock(
        [&](const auto& tracked)
        {
            for (const auto& [end, weak] : tracked)
            {
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

}
