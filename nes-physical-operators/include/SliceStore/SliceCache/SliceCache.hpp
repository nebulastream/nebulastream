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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <type_traits>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <SliceCacheConfiguration.hpp>
#include <val_concepts.hpp>

namespace NES
{


/// Represents the C++ struct that is stored in the SliceCache
struct SliceCacheEntry
{
    using DataStructure = const std::byte*;

    /// As we are doing everything in Nautilus, we do not care about the initialization of these values
    SliceCacheEntry() : sliceStart(0), sliceEnd(0), dataStructure(nullptr) { }

    SliceCacheEntry(const Timestamp::Underlying sliceStart, const Timestamp::Underlying sliceEnd, DataStructure dataStructure)
        : sliceStart(sliceStart), sliceEnd(sliceEnd), dataStructure(dataStructure)
    {
    }

    Timestamp::Underlying sliceStart;
    Timestamp::Underlying sliceEnd;
    DataStructure dataStructure;
};

/// We do not wish to have a virtual destructor, as this leads to a 8B vtable pointer for every slice cache entry.
/// Due to use using the structs purely in nautilus, this would simply waste space, as the struct is never polymorphically destroyed.
static_assert(std::is_trivially_destructible_v<SliceCacheEntry>);

/// Slice cache that sits between our slice store and an operator. Each entry stores a pointer to a data structure, e.g., Hashmap or
/// PagedVector, for a specific slice. This allows to not needing access the slice store, skipping at least one lock and one nautilus invoke call
class SliceCache
{
public:
    explicit SliceCache(uint64_t numberOfEntries, uint64_t sizeOfEntry);
    SliceCache(const SliceCache& cache);
    virtual ~SliceCache() = default;
    [[nodiscard]] virtual std::unique_ptr<SliceCache> clone() const = 0;
    static std::unique_ptr<SliceCache> createSliceCache(const SliceCacheConfiguration& sliceCacheConfiguration);
    using SliceCacheReplaceEntry = std::function<void(const nautilus::val<SliceCacheEntry*>&)>;
    virtual nautilus::val<SliceCacheEntry::DataStructure> getDataStructureRef(
        const nautilus::val<Timestamp>& timestamp,
        const nautilus::val<WorkerThreadId>& workerThreadId,
        const SliceCacheReplaceEntry& replaceEntry)
        = 0;
    /// Memory layout: [Thread0 entries][Thread1 entries]...[ThreadN entries]
    [[nodiscard]] virtual uint64_t getCacheMemorySize() const;

    /// Sets the number of worker threads so that per-thread cache memory can be allocated.
    void setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads);

    void setStartOfEntries(const std::span<std::byte>& startOfSliceCache);

protected:
    SliceCacheEntry* startOfSliceCache;
    uint64_t numberOfEntries;
    uint64_t sizeOfEntry;
    uint64_t numberOfWorkerThreads = 1;
};
}
