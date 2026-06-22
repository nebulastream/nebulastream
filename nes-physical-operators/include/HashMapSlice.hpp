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
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Spill/ArenaMemoryResource.hpp>
#include <Runtime/Spill/SpillManager.hpp>
#include <SliceStore/Slice.hpp>
#include <CompilationContext.hpp>

namespace NES
{

struct CreateNewHashMapSliceArgs : CreateNewSlicesArguments
{
    /// Handle into the pipeline's compiled module; keeps the compiled cleanup function alive
    /// even if the slice outlives the pipeline stage that compiled it.
    using NautilusCleanupExec = PipelineFunction<void(HashMap*)>;

    CreateNewHashMapSliceArgs(
        std::vector<std::shared_ptr<NautilusCleanupExec>> nautilusCleanup,
        const uint64_t keySize,
        const uint64_t valueSize,
        const uint64_t pageSize,
        const uint64_t numberOfBuckets)
        : nautilusCleanup(std::move(nautilusCleanup))
        , keySize(keySize)
        , valueSize(valueSize)
        , pageSize(pageSize)
        , numberOfBuckets(numberOfBuckets)
    {
    }

    ~CreateNewHashMapSliceArgs() override = default;
    std::vector<std::shared_ptr<NautilusCleanupExec>> nautilusCleanup;
    uint64_t keySize;
    uint64_t valueSize;
    uint64_t pageSize;
    uint64_t numberOfBuckets;
};

/// A HashMapSlice stores a number of hashmaps per input stream. We assume that each input stream has the same number of hashmaps
/// We store first all hashmaps of each stream followed by the hashmaps of the next stream, c.f.,
/// +---------------------+---------------------+---------------------+---------------------+---------------------+
/// | Stream 1: [HashMap1][HashMap2][HashMap3]... | Stream 2: [HashMap1][HashMap2][HashMap3]... | ... | Stream N: [HashMap1][HashMap2][HashMap3]... |
/// +---------------------+---------------------+---------------------+---------------------+---------------------+
///
/// As the hashmap might need to clean up its state, we expect multiple clean up functions as part of the @struct CreateNewHashMapSliceArgs
/// For each stream, we expect one cleanup function and once this HashMapSlice gets destroyed they are being called.
/// HashMapSlice is also a SpillableState: when a SpillManager with spilling enabled is supplied, the slice allocates
/// all of its hash maps' memory from its own arena-backed BufferManager (see spillProviderOrNull), so the whole slice
/// can be evicted to / reloaded from disk as a unit by the governor.
class HashMapSlice : public Slice, public SpillableState
{
public:
    explicit HashMapSlice(
        SliceStart sliceStart,
        SliceEnd sliceEnd,
        const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
        uint64_t numberOfHashMaps,
        uint64_t numberOfInputStreams,
        std::shared_ptr<SpillManager> spillManager = nullptr);

    ~HashMapSlice() override;

    /// In our current implementation, we expect one hashmap per worker thread. Thus, we return the number of hashmaps == number of worker threads.
    [[nodiscard]] uint64_t getNumberOfHashMaps() const;

    [[nodiscard]] uint64_t getNumberOfTuples() const;

    /// SpillableState: the slice's resident footprint, eviction/reload, and victim-ordering key (the slice end, so the
    /// oldest slice is evicted first).
    [[nodiscard]] size_t residentBytes() const override;
    [[nodiscard]] bool isEvicted() const override;
    void evictState() override;
    void reloadState() override;
    [[nodiscard]] uint64_t coldnessKey() const override;

protected:
    /// Returns the slice's own arena-backed buffer provider when spilling is enabled (creating the arena lazily on
    /// first use), or nullptr when spilling is disabled (in which case callers use the global pipeline provider, i.e.
    /// behaviour is unchanged). Pinning the returned provider into a hash map makes that hash map spillable.
    AbstractBufferProvider* spillProviderOrNull();

    std::vector<std::unique_ptr<HashMap>> hashMaps;
    CreateNewHashMapSliceArgs createNewHashMapSliceArgs;
    uint64_t numberOfHashMapsPerInputStream;
    uint64_t numberOfInputStreams;

private:
    /// Spilling state (only used when spillManager is set and spilling is enabled). One arena + BufferManager per slice;
    /// all of the slice's hash maps allocate from it, so the slice is a single spill unit.
    std::shared_ptr<SpillManager> spillManager;
    std::shared_ptr<ArenaMemoryResource> spillArena;
    std::shared_ptr<BufferManager> spillBufferManager;
    bool spillEnabled{false};
    std::atomic<bool> spillEvicted{false};
    std::mutex spillMutex; /// guards lazy arena creation and the evicted flag
};

}
