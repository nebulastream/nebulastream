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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

HashMapSlice::HashMapSlice(
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    CreateNewHashMapSliceArgs createNewHashMapSliceArgs,
    const uint64_t numberOfHashMaps,
    const uint64_t numberOfInputStreams)
    : Slice(sliceStart, sliceEnd)
    , createNewHashMapSliceArgs(std::move(createNewHashMapSliceArgs))
    , numberOfHashMapsPerInputStream(numberOfHashMaps)
    , numberOfInputStreams(numberOfInputStreams)
{
    for (uint64_t i = 0; i < numberOfHashMaps * numberOfInputStreams; i++)
    {
        hashMaps.emplace_back(nullptr);
    }
}

HashMapSlice::~HashMapSlice()
{
    INVARIANT(createNewHashMapSliceArgs.nautilusCleanup.size() == numberOfInputStreams, "We expect one cleanup function per input ");

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
