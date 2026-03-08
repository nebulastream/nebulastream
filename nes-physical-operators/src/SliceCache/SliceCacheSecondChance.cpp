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

#include <SliceCache/SliceCacheSecondChance.hpp>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <SliceCache/SliceCache.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_bool.hpp>
#include <nautilus/val_ptr.hpp>
#include <nautilus/val_std.hpp>

namespace NES
{

/// Reads a struct field using traced pointer arithmetic instead of val<T*>::get().
/// val<T*>::get() uses this->value (raw pointer) for address computation, which fails in COMPILER
/// mode when the val was produced by operator+ (value = nullptr). This helper uses traced arithmetic
/// so the compiled code computes the address at runtime from the properly traced base pointer.
template <typename T, typename F>
nautilus::val<F> tracedGet(const nautilus::val<T*>& ptr, F T::* pm)
{
    const auto offset = nautilus::val<uint64_t>{nautilus::field_offset(pm)};
    nautilus::val<int8_t*> fieldPtr = nautilus::val<int8_t*>{ptr} + offset;
    return *nautilus::val<F*>{fieldPtr};
}

/// Writes a struct field using traced pointer arithmetic instead of val<T*>::set().
template <typename T, typename F>
void tracedSet(const nautilus::val<T*>& ptr, F T::* pm, nautilus::val<F> value)
{
    const auto offset = nautilus::val<uint64_t>{nautilus::field_offset(pm)};
    nautilus::val<int8_t*> fieldPtr = nautilus::val<int8_t*>{ptr} + offset;
    *nautilus::val<F*>{fieldPtr} = value;
}

SliceCacheSecondChance::SliceCacheSecondChance(const uint64_t numberOfEntries, const uint64_t sizeOfEntry)
    : SliceCache(numberOfEntries, sizeOfEntry), replacementIndexRaw(nullptr)
{
}

uint64_t SliceCacheSecondChance::getCacheMemorySize() const
{
    return SliceCache::getCacheMemorySize() + sizeof(uint64_t);
}

void SliceCacheSecondChance::setStartOfEntries(SliceCacheEntry* startOfEntries)
{
    SliceCache::setStartOfEntries(startOfEntries);
    /// The replacement index is stored right after the cache entries in the same memory buffer.
    replacementIndexRaw = reinterpret_cast<uint64_t*>(reinterpret_cast<SliceCacheEntrySecondChance*>(startOfEntries) + numberOfEntries);
}

SliceCacheSecondChance::EntryFound
SliceCacheSecondChance::searchInCache(const nautilus::val<SliceCacheEntry*>& startOfEntries, const nautilus::val<Timestamp>& timestamp)
{
    /// We assume that a timestamp is in the cache, if the timestamp is in the range of the slice, e.g., sliceStart <= timestamp < sliceEnd.
    for (nautilus::val<uint64_t> i = 0; i < numberOfEntries; ++i)
    {
        nautilus::val<SliceCacheEntry*> sliceCacheEntry = startOfEntries + i;
        const auto sliceStart = nautilus::val<Timestamp>{tracedGet(sliceCacheEntry, &SliceCacheEntry::sliceStart)};
        const auto sliceEnd = nautilus::val<Timestamp>{tracedGet(sliceCacheEntry, &SliceCacheEntry::sliceEnd)};
        if (sliceStart <= timestamp && timestamp < sliceEnd)
        {
            return {i, true};
        }
    }
    return {0, false};
}

nautilus::val<int8_t*>
SliceCacheSecondChance::getDataStructureRef(const nautilus::val<Timestamp>& timestamp, const SliceCacheReplaceEntry& replaceEntry)
{
    /// Create val wrappers from raw pointers inside the traced function. traceConstant(real_ptr)
    /// produces proper traced constants with real addresses for COMPILER mode.
    const nautilus::val<SliceCacheEntry*> startOfEntries{startOfEntriesRaw};
    nautilus::val<uint64_t*> replacementIndexPtr{replacementIndexRaw};

    /// First, we check if the timestamp is already in the cache.
    if (const auto [pos, foundInCache] = searchInCache(startOfEntries, timestamp); foundInCache)
    {
        nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntryToReplace = startOfEntries + pos;
        tracedSet(sliceCacheEntryToReplace, &SliceCacheEntrySecondChance::secondChanceBit, nautilus::val<bool>{true});
        return tracedGet(nautilus::val<SliceCacheEntry*>{sliceCacheEntryToReplace}, &SliceCacheEntry::dataStructure);
    }

    /// If this is not the case, we iterate through the cache until we have find a slice that has the second chance bit set to false.
    /// If we find such a slice, we set the second chance bit to true, replace the slice and return the data structure.
    /// We must start at the current replacement index, as we have to replace the oldest entry.
    nautilus::val<uint64_t> replacementIndex = *replacementIndexPtr;
    for (nautilus::val<uint64_t> i = 0; i < 2 * numberOfEntries; ++i)
    {
        nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntryToReplace = startOfEntries + replacementIndex;
        const nautilus::val<bool> secondChanceBit = tracedGet(sliceCacheEntryToReplace, &SliceCacheEntrySecondChance::secondChanceBit);
        if (secondChanceBit == nautilus::val<bool>{false})
        {
            break;
        }
        tracedSet(sliceCacheEntryToReplace, &SliceCacheEntrySecondChance::secondChanceBit, nautilus::val<bool>{false});
        replacementIndex = (replacementIndex + 1) % numberOfEntries;
    }
    *replacementIndexPtr = replacementIndex;

    /// Replacing the slice and returning the data structure.
    nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntryToReplace = startOfEntries + replacementIndex;
    tracedSet(sliceCacheEntryToReplace, &SliceCacheEntrySecondChance::secondChanceBit, nautilus::val<bool>{true});
    replaceEntry(nautilus::val<SliceCacheEntry*>{startOfEntries + replacementIndex});
    return tracedGet(nautilus::val<SliceCacheEntry*>{startOfEntries + replacementIndex}, &SliceCacheEntry::dataStructure);
}

}
