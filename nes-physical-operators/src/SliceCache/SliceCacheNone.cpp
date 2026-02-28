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

namespace NES
{
/// Using here 0 is fine, as this slice cache does not store anything
SliceCacheNone::SliceCacheNone()
    : SliceCache{0, 0}
{
}

nautilus::val<int8_t*>
SliceCacheNone::getDataStructureRef(const nautilus::val<Timestamp>&, const SliceCacheReplacement& newCacheItem)
{
    /// As this slice cache does nothing, we simply return the created new cache item
    return newCacheItem();
}
}
