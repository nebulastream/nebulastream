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
SliceCacheNone::SliceCacheNone() : SliceCache{1, sizeof(SliceCacheEntry)}
{
}

nautilus::val<int8_t*> SliceCacheNone::getDataStructureRef(
    const nautilus::val<Timestamp>&, const nautilus::val<WorkerThreadId>& workerThreadId, const SliceCacheReplaceEntry& replaceEntry)
{
    /// Each worker thread uses its own entry to avoid data races.
    nautilus::val<SliceCacheEntry*> startOfEntries{startOfAllEntries};
    nautilus::val<SliceCacheEntry*> threadEntry = startOfEntries + workerThreadId.convertToValue();
    /// As this slice cache does nothing, we simply replace the single entry and return the data structure
    replaceEntry(threadEntry);
    return threadEntry.get(&SliceCacheEntry::dataStructure);
}
}
