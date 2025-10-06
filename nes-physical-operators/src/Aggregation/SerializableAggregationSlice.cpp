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

#include <Aggregation/SerializableAggregationSlice.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

SerializableAggregationSlice::SerializableAggregationSlice(
    SliceStart sliceStart, 
    SliceEnd sliceEnd, 
    const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs, 
    uint64_t numberOfHashMaps,
    const State::AggregationState::Config& config)
    : HashMapSlice(sliceStart, sliceEnd, createNewHashMapSliceArgs, numberOfHashMaps, 1)
    , config_(config)
{
    hashMapStates_.resize(numberOfHashMaps);
    
    // Initialize all hashmap states
    for (uint64_t i = 0; i < numberOfHashMaps; ++i) {
        auto& state = hashMapStates_[i];
        state.keySize = config_.keySize;
        state.valueSize = config_.valueSize;
        state.bucketCount = config_.numberOfBuckets;
        state.entrySize = sizeof(DataStructures::OffsetEntry) + state.keySize + state.valueSize;
        state.chains.resize(state.bucketCount, 0);
        state.memory.reserve(config_.pageSize);
        state.tupleCount = 0;
    }
    
    NES_TRACE("Created SerializableAggregationSlice for slice {}-{} with {} hashmaps", 
              sliceStart, sliceEnd, numberOfHashMaps);
}

SerializableAggregationSlice::~SerializableAggregationSlice()
{
    // Clear the hashmaps vector without calling the cleanup function
    // The cleanup function is designed for ChainedHashMap and incompatible with OffsetHashMapWrapper
    // OffsetHashMapWrapper manages its own memory through its destructor
    hashMaps.clear();
    
    NES_TRACE("SerializableAggregationSlice destructor called for slice {}-{}", 
              getSliceStart(), getSliceEnd());
}

Nautilus::Interface::HashMap* SerializableAggregationSlice::getHashMapPtr(WorkerThreadId workerThreadId) const
{
    auto threadIdx = workerThreadId.getRawValue();
    NES_TRACE("getHashMapPtr called for workerThreadId={}, threadIdx={}", workerThreadId.getRawValue(), threadIdx);
    PRECONDITION(threadIdx < hashMaps.size(), 
                "Worker thread ID {} exceeds number of hashmaps {}", threadIdx, hashMaps.size());
    
    if (!hashMaps[threadIdx]) {
        NES_TRACE("HashMap not initialized for threadIdx={}, initializing now", threadIdx);
        const_cast<SerializableAggregationSlice*>(this)->initializeHashMap(workerThreadId);
    } else {
        NES_TRACE("HashMap already exists for threadIdx={}", threadIdx);
    }
    
    // Debug: Check state consistency
    auto& sliceState = hashMapStates_[threadIdx];
    auto* hashMap = hashMaps[threadIdx].get();
    auto* wrapper = static_cast<DataStructures::OffsetHashMapWrapper*>(hashMap);
    auto& wrapperState = wrapper->getImpl().extractState();
    
    printf("STATE_DEBUG: getHashMapPtr Thread %lu - slice state addr: %p, tupleCount: %lu\n", 
           threadIdx, &sliceState, sliceState.tupleCount);
    printf("STATE_DEBUG: getHashMapPtr Thread %lu - wrapper state addr: %p, tupleCount: %lu\n", 
           threadIdx, &wrapperState, wrapperState.tupleCount);
    fflush(stdout);
    
    NES_TRACE("Returning hashMap pointer: {}", static_cast<void*>(hashMap));
    return hashMap;
}

Nautilus::Interface::HashMap* SerializableAggregationSlice::getHashMapPtrOrCreate(WorkerThreadId workerThreadId)
{
    auto threadIdx = workerThreadId.getRawValue();
    NES_DEBUG("getHashMapPtrOrCreate called for workerThreadId={}, threadIdx={}", workerThreadId.getRawValue(), threadIdx);
    PRECONDITION(threadIdx < hashMaps.size(), 
                "Worker thread ID {} exceeds number of hashmaps {}", threadIdx, hashMaps.size());
    
    if (!hashMaps[threadIdx]) {
        NES_DEBUG("HashMap not initialized for threadIdx={}, calling initializeHashMap", threadIdx);
        initializeHashMap(workerThreadId);
    } else {
        NES_DEBUG("HashMap already exists for threadIdx={}", threadIdx);
    }
    
    auto* hashMap = hashMaps[threadIdx].get();
    NES_DEBUG("Returning hashMap pointer from getHashMapPtrOrCreate: {}", static_cast<void*>(hashMap));
    return hashMap;
}

DataStructures::OffsetHashMapWrapper* SerializableAggregationSlice::getOffsetHashMapWrapper(WorkerThreadId workerThreadId) const
{
    auto threadIdx = workerThreadId.getRawValue();
    PRECONDITION(threadIdx < hashMaps.size(), 
                "Worker thread ID {} exceeds number of hashmaps {}", threadIdx, hashMaps.size());
    
    if (!hashMaps[threadIdx]) {
        const_cast<SerializableAggregationSlice*>(this)->initializeHashMap(workerThreadId);
    }
    
    return static_cast<DataStructures::OffsetHashMapWrapper*>(hashMaps[threadIdx].get());
}

const DataStructures::OffsetHashMapState& SerializableAggregationSlice::getHashMapState(WorkerThreadId workerThreadId) const
{
    auto threadIdx = workerThreadId.getRawValue();
    PRECONDITION(threadIdx < hashMapStates_.size(), 
                "Worker thread ID {} exceeds number of hashmaps {}", threadIdx, hashMapStates_.size());
    
    return hashMapStates_[threadIdx];
}

DataStructures::OffsetHashMapState& SerializableAggregationSlice::getHashMapState(WorkerThreadId workerThreadId)
{
    auto threadIdx = workerThreadId.getRawValue();
    PRECONDITION(threadIdx < hashMapStates_.size(), 
                "Worker thread ID {} exceeds number of hashmaps {}", threadIdx, hashMapStates_.size());
    
    return hashMapStates_[threadIdx];
}

void SerializableAggregationSlice::initializeHashMap(WorkerThreadId workerThreadId)
{
    auto threadIdx = workerThreadId.getRawValue();
    
    NES_DEBUG("initializeHashMap called for threadIdx={}", threadIdx);
    
    if (!hashMaps[threadIdx]) {
        NES_DEBUG("Creating OffsetHashMapWrapper for threadIdx={}", threadIdx);
        
        // Log the state configuration and address
        auto& state = hashMapStates_[threadIdx];
        printf("STATE_DEBUG: Thread %lu - state address: %p, tupleCount: %lu, memory.size(): %lu\n", 
               threadIdx, &state, state.tupleCount, state.memory.size());
        fflush(stdout);
        
        NES_DEBUG("HashMap state: keySize={}, valueSize={}, bucketCount={}, memory.size()={}", 
                  state.keySize, state.valueSize, state.bucketCount, state.memory.size());
        
        // Create OffsetHashMapWrapper from the pre-initialized state
        hashMaps[threadIdx] = std::make_unique<DataStructures::OffsetHashMapWrapper>(hashMapStates_[threadIdx]);
        
        // Verify the wrapper is using the same state
        auto* wrapper = static_cast<DataStructures::OffsetHashMapWrapper*>(hashMaps[threadIdx].get());
        auto& wrapperState = wrapper->getImpl().extractState();
        printf("STATE_DEBUG: Thread %lu - wrapper state address: %p, tupleCount: %lu\n", 
               threadIdx, &wrapperState, wrapperState.tupleCount);
        fflush(stdout);
        
        NES_DEBUG("Successfully created OffsetHashMapWrapper for thread {} in slice {}-{}, pointer: {}", 
                  threadIdx, getSliceStart(), getSliceEnd(), static_cast<void*>(hashMaps[threadIdx].get()));
    } else {
        NES_DEBUG("HashMap already exists for threadIdx={}, not reinitializing", threadIdx);
    }
}

}