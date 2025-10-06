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
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/DataStructures/OffsetBasedHashMap.hpp>
#include <Nautilus/State/Reflection/StateDefinitions.hpp>
#include <SliceStore/Slice.hpp>
#include <HashMapSlice.hpp>
#include <Nautilus/DataStructures/OffsetHashMapWrapper.hpp>

namespace NES
{

/// This class represents a single slice for serializable aggregation using OffsetHashMapWrapper.
/// It stores the aggregation state in serializable offset-based hashmaps that implement the HashMap interface.
/// Each slice contains one hashmap per worker thread.
class SerializableAggregationSlice final : public HashMapSlice
{
public:
    SerializableAggregationSlice(
        SliceStart sliceStart, 
        SliceEnd sliceEnd, 
        const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs, 
        uint64_t numberOfHashMaps,
        const State::AggregationState::Config& config);

    ~SerializableAggregationSlice() override;

    /// Returns the pointer to the underlying hashmap as the HashMap interface.
    /// IMPORTANT: This method should only be used for passing the hashmap to the nautilus executable.
    [[nodiscard]] Nautilus::Interface::HashMap* getHashMapPtr(WorkerThreadId workerThreadId) const;
    [[nodiscard]] Nautilus::Interface::HashMap* getHashMapPtrOrCreate(WorkerThreadId workerThreadId);
    
    /// Returns the underlying OffsetHashMapWrapper for direct access
    [[nodiscard]] DataStructures::OffsetHashMapWrapper* getOffsetHashMapWrapper(WorkerThreadId workerThreadId) const;

    /// Get the serializable state for a specific hashmap
    [[nodiscard]] const DataStructures::OffsetHashMapState& getHashMapState(WorkerThreadId workerThreadId) const;
    [[nodiscard]] DataStructures::OffsetHashMapState& getHashMapState(WorkerThreadId workerThreadId);

private:
    void initializeHashMap(WorkerThreadId workerThreadId);

    State::AggregationState::Config config_;
    mutable std::vector<DataStructures::OffsetHashMapState> hashMapStates_;
};

} // namespace NES