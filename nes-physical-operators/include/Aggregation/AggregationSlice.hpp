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

#include <cstdint>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

/// This class represents a single slice for the (keyed) aggregation. It stores the aggregation state in a hashmap.
/// If it is a global aggregation, each hashmap contains a single entry for the keyValue = 0.
/// In our current implementation, we have one hashmap per worker thread.
class AggregationSlice final : public HashMapSlice
{
public:
    AggregationSlice(
        SliceStart sliceStart, SliceEnd sliceEnd, CreateNewHashMapSliceArgs createNewHashMapSliceArgs, uint64_t numberOfHashMaps);

    /// Returns the pointer to the underlying hashmap.
    /// IMPORTANT: This method should only be used for passing the hashmap to the nautilus executable.
    [[nodiscard]] Nautilus::Interface::HashMap* getHashMapPtr(WorkerThreadId workerThreadId) const;
    [[nodiscard]] Nautilus::Interface::HashMap* getHashMapPtrOrCreate(WorkerThreadId workerThreadId);
};

}
