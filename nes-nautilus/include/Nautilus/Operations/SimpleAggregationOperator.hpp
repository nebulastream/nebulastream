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
#include <memory>
#include <vector>
#include <Nautilus/State/Reflection/StateDefinitions.hpp>
#include <Nautilus/State/Serialization/StateSerializer.hpp>
#include <Nautilus/DataStructures/OffsetBasedHashMap.hpp>
#include "OperationRegistry.hpp"
#include "StandardOperations.hpp"
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES::Operations {

using TupleBuffer = Memory::TupleBuffer;
using AbstractBufferProvider = Memory::AbstractBufferProvider;

class SimpleAggregationOperator {
public:
    explicit SimpleAggregationOperator(State::AggregationState initialState = {}) 
        : state_(std::move(initialState)) {
        
        registerStandardOperations();
        
        if (state_.hashMaps.empty()) {
            initializeDefaultState();
        }
    }
    
    void processTuple(const TupleBuffer& input) {
        auto& registry = OperationRegistry::getInstance();
        
        for (const auto& op : state_.operations) {
            auto operation = registry.create<State::AggregationState>(op);
            operation->execute(state_, input);
        }
        
        state_.metadata.processedRecords++;
    }
    
    TupleBuffer serialize(AbstractBufferProvider* provider) {
        return Serialization::StateSerializer<State::AggregationState>::serialize(state_, provider);
    }
    
    static std::unique_ptr<SimpleAggregationOperator> deserialize(const TupleBuffer& buffer) {
        auto stateResult = Serialization::StateSerializer<State::AggregationState>::deserialize(buffer);
        if (!stateResult) {
            throw std::runtime_error("Failed to deserialize aggregation state");
        }
        
        return std::make_unique<SimpleAggregationOperator>(stateResult.value());
    }
    
    const State::AggregationState& getState() const { return state_; }
    
    void addOperation(OperationID opId, const OperationParams& params = OperationParams{rfl::Field<"count", CountParams>{CountParams{false}}}) {
        auto& registry = OperationRegistry::getInstance();
        auto descriptor = registry.getDescriptor(static_cast<uint32_t>(opId));
        descriptor.parameters = params;
        state_.operations.push_back(descriptor);
    }
    
    uint64_t getTupleCount() const {
        uint64_t total = 0;
        for (const auto& hashMap : state_.hashMaps) {
            total += hashMap.tupleCount;
        }
        return total;
    }
    
private:
    void initializeDefaultState() {
        state_.metadata = {
            .operatorId = 1,
            .operatorType = 1, // AggregationType
            .processedRecords = 0,
            .lastWatermark = 0,
            .version = 1
        };
        
        state_.config = {
            .keySize = 8,
            .valueSize = 8,
            .numberOfBuckets = 1024,
            .pageSize = 4096
        };
        
        state_.hashMaps.resize(1);
        
        // Add default COUNT operation
        addOperation(OperationID::COUNT);
    }
    
    State::AggregationState state_;
};

}
