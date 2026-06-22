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
#include <HashMapSlice.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Spill/ArenaMemoryResource.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Process-wide counter to give each slice's arena backing file a unique name.
std::atomic<uint64_t> spillSliceCounter{0};
/// Small pooled count for the per-slice BufferManager: the hash maps allocate via getUnpooledBuffer (the unpooled path,
/// which routes through the arena), so the pooled area is essentially unused and is kept tiny.
constexpr uint32_t SLICE_POOL_BUFFERS = 2;
}

HashMapSlice::HashMapSlice(
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
    const uint64_t numberOfHashMaps,
    const uint64_t numberOfInputStreams,
    std::shared_ptr<SpillManager> spillManager)
    : Slice(sliceStart, sliceEnd)
    , createNewHashMapSliceArgs(createNewHashMapSliceArgs)
    , numberOfHashMapsPerInputStream(numberOfHashMaps)
    , numberOfInputStreams(numberOfInputStreams)
    , spillManager(std::move(spillManager))
    , spillEnabled(this->spillManager != nullptr && this->spillManager->configuration().enabled)
{
    for (uint64_t i = 0; i < numberOfHashMaps * numberOfInputStreams; i++)
    {
        hashMaps.emplace_back(nullptr);
    }
}

HashMapSlice::~HashMapSlice()
{
    INVARIANT(createNewHashMapSliceArgs.nautilusCleanup.size() == numberOfInputStreams, "We expect one cleanup function per input ");

    /// Stop the governor from tracking this slice before we touch its state.
    if (spillManager != nullptr)
    {
        spillManager->unregisterState(this);
    }
    /// The nautilus cleanup below dereferences the hash maps' entries, which live in the arena. If the slice is
    /// currently evicted, reload it first so cleanup reads valid memory rather than discarded (zero-fill) pages.
    if (spillEvicted.load() && spillArena != nullptr)
    {
        spillArena->reload();
        spillEvicted.store(false);
    }

    /// As we assume that each hashmap of an input stream lie one after the other.
    /// Thus, we need to call #numbnumberOfHashMaps times the same nautilusCleanup function and then move to the next one.
    for (size_t i = 0; i < hashMaps.size(); i++)
    {
        if (hashMaps[i] and hashMaps[i]->getNumberOfTuples() > 0)
        {
            /// Calling the compiled nautilus function
            createNewHashMapSliceArgs.nautilusCleanup[i / numberOfHashMapsPerInputStream]->operator()(hashMaps[i].get());
        }
    }

    hashMaps.clear();
}

AbstractBufferProvider* HashMapSlice::spillProviderOrNull()
{
    if (!spillEnabled)
    {
        return nullptr;
    }
    const std::lock_guard guard(spillMutex);
    if (spillArena == nullptr)
    {
        const auto& config = spillManager->configuration();
        const auto backingFile = config.spillDirectory + "/nes-slice-" + std::to_string(spillSliceCounter.fetch_add(1)) + ".spill";
        spillArena = std::make_shared<ArenaMemoryResource>(config.arenaMode, backingFile);
        spillBufferManager
            = BufferManager::create(static_cast<uint32_t>(createNewHashMapSliceArgs.pageSize), SLICE_POOL_BUFFERS, spillArena);
    }
    return spillBufferManager.get();
}

size_t HashMapSlice::residentBytes() const
{
    if (spillEvicted.load() || spillArena == nullptr)
    {
        return 0;
    }
    return spillArena->liveBytes();
}

bool HashMapSlice::isEvicted() const
{
    return spillEvicted.load();
}

void HashMapSlice::evictState()
{
    if (spillArena != nullptr && !spillEvicted.load())
    {
        spillArena->evict();
        spillEvicted.store(true);
    }
}

void HashMapSlice::reloadState()
{
    if (spillArena != nullptr && spillEvicted.load())
    {
        spillArena->reload();
        spillEvicted.store(false);
    }
}

uint64_t HashMapSlice::coldnessKey() const
{
    return sliceEnd.getRawValue();
}

uint64_t HashMapSlice::getNumberOfHashMaps() const
{
    return hashMaps.size();
}

uint64_t HashMapSlice::getNumberOfTuples() const
{
    return std::accumulate(
        hashMaps.begin(),
        hashMaps.end(),
        0,
        [](uint64_t runningSum, const auto& hashMap) { return runningSum + hashMap->getNumberOfTuples(); });
}
}
