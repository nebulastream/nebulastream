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
#include <Join/HashJoin/SerializableHJSlice.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <ErrorHandling.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

SerializableHJSlice::SerializableHJSlice(
    SliceStart sliceStart,
    SliceEnd sliceEnd,
    const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
    uint64_t numberOfHashMaps,
    uint64_t keySize,
    uint64_t valueSize,
    uint64_t numberOfBuckets)
    : HashMapSlice(sliceStart, sliceEnd, createNewHashMapSliceArgs, numberOfHashMaps, /*numberOfInputStreams*/ 2)
    , keySize_(keySize)
    , valueSize_(valueSize)
    , bucketCount_(numberOfBuckets)
{
    // Prepare per-thread state for both build sides
    leftStates_.resize(numberOfHashMaps);
    rightStates_.resize(numberOfHashMaps);

    auto initState = [&](DataStructures::OffsetHashMapState& st) {
        st.keySize = keySize_;
        st.valueSize = valueSize_;
        st.bucketCount = bucketCount_;
        st.entrySize = sizeof(DataStructures::OffsetEntry) + st.keySize + st.valueSize;
        st.tupleCount = 0;
        st.chains.assign(st.bucketCount, 0);
        // Reserve according to configured page size to reduce reallocations
        st.memory.reserve(createNewHashMapSliceArgs.pageSize);
        st.varSizedOffset = 0;
        st.varSizedMemory.clear();
    };

    for (uint64_t i = 0; i < numberOfHashMaps; ++i) {
        initState(leftStates_[i]);
        initState(rightStates_[i]);
    }

    NES_TRACE("Created SerializableHJSlice for slice {}-{} with {} hashmaps per side (keySize={}, valueSize={}, buckets={})",
              sliceStart, sliceEnd, numberOfHashMaps, keySize_, valueSize_, bucketCount_);
}

SerializableHJSlice::~SerializableHJSlice()
{
    // Avoid calling generic cleanup functors (meant for chained hash maps).
    // OffsetHashMapWrapper manages its own memory through RAII.
    hashMaps.clear();
}

static inline uint64_t sidePos(uint64_t threadIdx, uint64_t perSideCount, const JoinBuildSideType& side) {
    return (threadIdx % perSideCount) + ((static_cast<uint64_t>(side == JoinBuildSideType::Right)) * perSideCount);
}

Nautilus::Interface::HashMap* SerializableHJSlice::getHashMapPtr(
    WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const
{
    const auto idx = workerThreadId.getRawValue();
    const auto pos = sidePos(idx, numberOfHashMapsPerInputStream, buildSide);

    PRECONDITION(!hashMaps.empty() && pos < hashMaps.size(),
                 "No hashmap slot for workerThreadId {} at pos {} (size={})",
                 idx, pos, hashMaps.size());

    if (!hashMaps[pos]) {
        const_cast<SerializableHJSlice*>(this)->initializeHashMap(workerThreadId, buildSide);
    }
    return hashMaps[pos].get();
}

Nautilus::Interface::HashMap* SerializableHJSlice::getHashMapPtrOrCreate(
    WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide)
{
    const auto idx = workerThreadId.getRawValue();
    const auto pos = sidePos(idx, numberOfHashMapsPerInputStream, buildSide);

    PRECONDITION(!hashMaps.empty() && pos < hashMaps.size(),
                 "No hashmap slot for workerThreadId {} at pos {} (size={})",
                 idx, pos, hashMaps.size());

    if (!hashMaps[pos]) {
        initializeHashMap(workerThreadId, buildSide);
    }
    return hashMaps[pos].get();
}

DataStructures::OffsetHashMapWrapper* SerializableHJSlice::getOffsetHashMapWrapper(
    WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const
{
    const auto idx = workerThreadId.getRawValue();
    const auto pos = sidePos(idx, numberOfHashMapsPerInputStream, buildSide);
    PRECONDITION(!hashMaps.empty() && pos < hashMaps.size(),
                 "No hashmap slot for workerThreadId {} at pos {} (size={})",
                 idx, pos, hashMaps.size());

    if (!hashMaps[pos]) {
        const_cast<SerializableHJSlice*>(this)->initializeHashMap(workerThreadId, buildSide);
    }
    return static_cast<DataStructures::OffsetHashMapWrapper*>(hashMaps[pos].get());
}

DataStructures::OffsetHashMapState& SerializableHJSlice::getHashMapState(
    WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide)
{
    const auto idx = workerThreadId.getRawValue();
    PRECONDITION(idx < numberOfHashMapsPerInputStream,
                 "Worker thread {} exceeds per-side hashmap count {}", idx, numberOfHashMapsPerInputStream);
    return (buildSide == JoinBuildSideType::Left) ? leftStates_[idx] : rightStates_[idx];
}

const DataStructures::OffsetHashMapState& SerializableHJSlice::getHashMapState(
    WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const
{
    const auto idx = workerThreadId.getRawValue();
    PRECONDITION(idx < numberOfHashMapsPerInputStream,
                 "Worker thread {} exceeds per-side hashmap count {}", idx, numberOfHashMapsPerInputStream);
    return (buildSide == JoinBuildSideType::Left) ? leftStates_[idx] : rightStates_[idx];
}

uint64_t SerializableHJSlice::getNumberOfHashMapsForSide() const
{
    return numberOfHashMapsPerInputStream;
}

void SerializableHJSlice::initializeHashMap(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide)
{
    const auto idx = workerThreadId.getRawValue();
    const auto pos = sidePos(idx, numberOfHashMapsPerInputStream, buildSide);

    PRECONDITION(!hashMaps.empty() && pos < hashMaps.size(),
                 "No hashmap slot for workerThreadId {} at pos {} (size={})",
                 idx, pos, hashMaps.size());

    if (!hashMaps[pos]) {
        auto& state = (buildSide == JoinBuildSideType::Left) ? leftStates_[idx] : rightStates_[idx];
        // Create wrapper bound to the provided state (RAII managed by unique_ptr in hashMaps)
        hashMaps[pos] = std::make_unique<DataStructures::OffsetHashMapWrapper>(state);
        NES_DEBUG("SerializableHJSlice: initialized wrapper for {} side, threadIdx={}, pos={}, ptr={}",
                  (buildSide == JoinBuildSideType::Left ? "Left" : "Right"), idx, pos,
                  static_cast<void*>(hashMaps[pos].get()));
    }
}

} // namespace NES
