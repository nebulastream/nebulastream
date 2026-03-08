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

#include <SliceCache/SliceCacheNone.hpp>

#include <cstdint>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <SliceCache/SliceCache.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_std.hpp>

namespace NES
{
/// Using 1 entry so that startOfEntries points to real memory, which is needed for COMPILER mode.
SliceCacheNone::SliceCacheNone()
    : SliceCache{1, sizeof(SliceCacheEntry)}
{
}

nautilus::val<int8_t*>
SliceCacheNone::getDataStructureRef(const nautilus::val<Timestamp>&, const SliceCacheReplaceEntry& replaceEntry)
{
    /// Create a val from the raw pointer inside the traced function. traceConstant(real_ptr)
    /// produces a proper traced constant with the real address for COMPILER mode.
    nautilus::val<SliceCacheEntry*> startOfEntries{startOfEntriesRaw};
    /// As this slice cache does nothing, we simply replace the single entry and return the data structure
    replaceEntry(startOfEntries);
    return startOfEntries.get(&SliceCacheEntry::dataStructure);
}
}
