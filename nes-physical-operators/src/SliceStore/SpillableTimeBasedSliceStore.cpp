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

#include <cstdint>
#include <map>
#include <memory>
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
    return result;
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
SpillableTimeBasedSliceStore::getAllNonTriggeredSlices()
{
    auto result = DefaultTimeBasedSliceStore::getAllNonTriggeredSlices();
    unspillAndMarkEmitted(result);
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
    /// Erase backend records for spilled slices the base GC has now dropped. Use the base (non-unspilling)
    /// lookup so checking presence does not rebuild the very slice we are trying to drop.
    auto keys = spilledKeys.wlock();
    for (auto it = keys->begin(); it != keys->end();)
    {
        if (!DefaultTimeBasedSliceStore::getSliceBySliceEnd(*it).has_value())
        {
            NES_TRACE("Erasing backend record for garbage-collected slice {}", *it);
            backend->erase(*it);
            emittedKeys.wlock()->erase(*it);
            it = keys->erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void SpillableTimeBasedSliceStore::forceSpill(const SliceEnd sliceEnd)
{
    if (emittedKeys.rlock()->contains(sliceEnd))
    {
        throw SpillStoreFailure(
            "refusing to spill slice {} that has been emitted to the probe (async-probe-drain invariant)", sliceEnd);
    }
    auto opt = DefaultTimeBasedSliceStore::getSliceBySliceEnd(sliceEnd);
    if (!opt.has_value())
    {
        NES_DEBUG("forceSpill: no slice for sliceEnd {}", sliceEnd);
        return;
    }
    auto spillable = std::dynamic_pointer_cast<SpillableAggregationSlice>(opt.value());
    INVARIANT(spillable != nullptr, "forceSpill on a non-spillable slice {}", sliceEnd);
    NES_DEBUG("Spilling slice {} ({} bytes resident)", sliceEnd, residentBytesOf(*spillable));
    spillable->spill(*backend);
    spilledKeys.wlock()->insert(sliceEnd);
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

}
