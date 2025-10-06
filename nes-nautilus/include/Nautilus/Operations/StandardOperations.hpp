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
#include "Operation.hpp"
#include "OperationRegistry.hpp"
#include "../State/Reflection/StateDefinitions.hpp"
#include "../DataStructures/OffsetBasedHashMap.hpp"
#include <Runtime/TupleBuffer.hpp>

namespace NES::Operations {

using TupleBuffer = Memory::TupleBuffer;

template<typename T>
class SumOperation : public Operation<State::AggregationState> {
public:
    using Params = SumParams;
    
    explicit SumOperation(const SumParams& params) : params_(params) {}
    
    void execute(State::AggregationState& state, const TupleBuffer& input) override {
        if (state.hashMaps.empty()) {
            state.hashMaps.resize(1);
        }
        
        // Initialize hashMap state if needed
        auto& hashMapData = state.hashMaps[0];
        if (hashMapData.chains.empty()) {
            hashMapData.keySize = sizeof(int64_t);
            hashMapData.valueSize = sizeof(int64_t);
            hashMapData.bucketCount = 1024;
            hashMapData.entrySize = sizeof(DataStructures::OffsetEntry) + hashMapData.keySize + hashMapData.valueSize;
            hashMapData.chains.resize(hashMapData.bucketCount, 0);
            hashMapData.memory.reserve(hashMapData.bucketCount * 64);
        }
        
        // Create OffsetBasedHashMap from state
        auto hashMap = DataStructures::OffsetBasedHashMap::fromState(
            reinterpret_cast<DataStructures::OffsetHashMapState&>(hashMapData)
        );
        
        // For POC: assume TupleBuffer contains key-value pairs
        // In real implementation, this would extract fields based on params_.fieldName
        if (input.getBufferSize() >= sizeof(int64_t) * 2) {
            auto* data = input.getBuffer<int64_t>();
            int64_t key = data[0];
            int64_t value = data[1];
            
            uint64_t hash = std::hash<int64_t>{}(key);
            
            // Try to find existing entry
            auto* entry = hashMap.find(hash, &key);
            if (entry != nullptr) {
                // Update existing sum
                int64_t* existingSum = reinterpret_cast<int64_t*>(hashMap.getValue(entry));
                *existingSum += value;
            } else {
                // Insert new entry
                hashMap.insert(hash, &key, &value);
            }
        }
    }
    
    State::Operation describe() const override {
        return State::Operation{
            .id = static_cast<uint32_t>(OperationID::SUM_INT64),
            .name = "sum",
            .parameters = OperationParams{rfl::Field<"sum", SumParams>{params_}}
        };
    }
    
private:
    SumParams params_;
};

class CountOperation : public Operation<State::AggregationState> {
public:
    using Params = CountParams;
    
    explicit CountOperation(const CountParams& params) : params_(params) {}
    
    void execute(State::AggregationState& state, const TupleBuffer& input) override {
        if (state.hashMaps.empty()) {
            state.hashMaps.resize(1);
        }
        
        // Initialize hashMap state if needed
        auto& hashMapData = state.hashMaps[0];
        if (hashMapData.chains.empty()) {
            hashMapData.keySize = sizeof(int64_t);
            hashMapData.valueSize = sizeof(int64_t);
            hashMapData.bucketCount = 1024;
            hashMapData.entrySize = sizeof(DataStructures::OffsetEntry) + hashMapData.keySize + hashMapData.valueSize;
            hashMapData.chains.resize(hashMapData.bucketCount, 0);
            hashMapData.memory.reserve(hashMapData.bucketCount * 64);
        }
        
        // Create OffsetBasedHashMap from state
        auto hashMap = DataStructures::OffsetBasedHashMap::fromState(
            reinterpret_cast<DataStructures::OffsetHashMapState&>(hashMapData)
        );
        
        // For COUNT: assume TupleBuffer contains a key to count
        // In real implementation, this would extract fields based on GROUP BY keys
        if (input.getBufferSize() >= sizeof(int64_t)) {
            auto* data = input.getBuffer<int64_t>();
            int64_t key = data[0];
            
            uint64_t hash = std::hash<int64_t>{}(key);
            
            // Try to find existing entry
            auto* entry = hashMap.find(hash, &key);
            if (entry != nullptr) {
                // Increment existing count
                int64_t* count = reinterpret_cast<int64_t*>(hashMap.getValue(entry));
                (*count)++;
            } else {
                // Insert new entry with count = 1
                int64_t initialCount = 1;
                hashMap.insert(hash, &key, &initialCount);
            }
        }
    }
    
    State::Operation describe() const override {
        return State::Operation{
            .id = static_cast<uint32_t>(OperationID::COUNT),
            .name = "count",
            .parameters = OperationParams{rfl::Field<"count", CountParams>{params_}}
        };
    }
    
private:
    CountParams params_;
};

template<typename T>
class AvgOperation : public Operation<State::AggregationState> {
public:
    using Params = AvgParams;
    
    explicit AvgOperation(const AvgParams& params) : params_(params) {}
    
    void execute(State::AggregationState& state, const TupleBuffer& input) override {
        if (state.hashMaps.empty()) {
            state.hashMaps.resize(1);
        }
        
        // Initialize hashMap state if needed - AVG needs sum+count, so bigger value
        auto& hashMapData = state.hashMaps[0];
        if (hashMapData.chains.empty()) {
            hashMapData.keySize = sizeof(int64_t);
            hashMapData.valueSize = sizeof(double) + sizeof(int64_t); // sum + count
            hashMapData.bucketCount = 1024;
            hashMapData.entrySize = sizeof(DataStructures::OffsetEntry) + hashMapData.keySize + hashMapData.valueSize;
            hashMapData.chains.resize(hashMapData.bucketCount, 0);
            hashMapData.memory.reserve(hashMapData.bucketCount * 64);
        }
        
        // Create OffsetBasedHashMap from state
        auto hashMap = DataStructures::OffsetBasedHashMap::fromState(
            reinterpret_cast<DataStructures::OffsetHashMapState&>(hashMapData)
        );
        
        // For AVG: assume TupleBuffer contains key-value pairs
        if (input.getBufferSize() >= sizeof(int64_t) * 2) {
            auto* data = input.getBuffer<int64_t>();
            int64_t key = data[0];
            double value = static_cast<double>(data[1]);
            
            uint64_t hash = std::hash<int64_t>{}(key);
            
            // Try to find existing entry
            auto* entry = hashMap.find(hash, &key);
            if (entry != nullptr) {
                // Update existing sum and count
                double* sum = reinterpret_cast<double*>(hashMap.getValue(entry));
                int64_t* count = reinterpret_cast<int64_t*>(reinterpret_cast<uint8_t*>(sum) + sizeof(double));
                *sum += value;
                (*count)++;
            } else {
                // Insert new entry with sum=value, count=1
                struct AvgValue {
                    double sum;
                    int64_t count;
                } avgValue = {value, 1};
                hashMap.insert(hash, &key, &avgValue);
            }
        }
    }
    
    State::Operation describe() const override {
        return State::Operation{
            .id = static_cast<uint32_t>(OperationID::AVG_FLOAT64),
            .name = "avg",
            .parameters = OperationParams{rfl::Field<"avg", AvgParams>{params_}}
        };
    }
    
private:
    AvgParams params_;
};

inline void registerStandardOperations() {
    auto& registry = OperationRegistry::getInstance();
    
    registry.registerOperation<SumOperation<int64_t>>(
        static_cast<uint32_t>(OperationID::SUM_INT64), 
        "sum_int64"
    );
    
    registry.registerOperation<CountOperation>(
        static_cast<uint32_t>(OperationID::COUNT), 
        "count"
    );
    
    registry.registerOperation<AvgOperation<double>>(
        static_cast<uint32_t>(OperationID::AVG_FLOAT64), 
        "avg_float64"
    );
}

}
