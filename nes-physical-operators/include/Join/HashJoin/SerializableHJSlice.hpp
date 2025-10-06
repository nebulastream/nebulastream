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
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/DataStructures/OffsetBasedHashMap.hpp>
#include <Nautilus/DataStructures/OffsetHashMapWrapper.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <HashMapSlice.hpp>

namespace NES {

// Serializable hash join slice using offset-based hash maps for left and right sides
class SerializableHJSlice final : public HashMapSlice {
public:
    SerializableHJSlice(
        SliceStart sliceStart,
        SliceEnd sliceEnd,
        const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
        uint64_t numberOfHashMaps,
        uint64_t keySize,
        uint64_t valueSize,
        uint64_t numberOfBuckets);

    ~SerializableHJSlice() override;

    // Accessors returning interface pointers (for Nautilus)
    [[nodiscard]] Nautilus::Interface::HashMap* getHashMapPtr(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const;
    [[nodiscard]] Nautilus::Interface::HashMap* getHashMapPtrOrCreate(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide);

    // Direct access to offset wrapper
    [[nodiscard]] DataStructures::OffsetHashMapWrapper* getOffsetHashMapWrapper(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const;

    // State access for capture/rehydrate
    [[nodiscard]] DataStructures::OffsetHashMapState& getHashMapState(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide);
    [[nodiscard]] const DataStructures::OffsetHashMapState& getHashMapState(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const;

    [[nodiscard]] uint64_t getNumberOfHashMapsForSide() const;

private:
    void initializeHashMap(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide);

    // Separate states for left and right sides
    mutable std::vector<DataStructures::OffsetHashMapState> leftStates_;
    mutable std::vector<DataStructures::OffsetHashMapState> rightStates_;

    // Lazily created wrappers relying on the above states
    mutable std::vector<std::unique_ptr<DataStructures::OffsetHashMapWrapper>> leftWrappers_;
    mutable std::vector<std::unique_ptr<DataStructures::OffsetHashMapWrapper>> rightWrappers_;

    uint64_t keySize_;
    uint64_t valueSize_;
    uint64_t bucketCount_;
};

}
