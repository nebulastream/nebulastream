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
#include <rfl.hpp>
#include <functional>
#include <memory>
#include <map>
#include <any>
#include <stdexcept>
#include "Operation.hpp"
#include "../State/Reflection/StateDefinitions.hpp"

namespace NES::Operations {

enum class OperationID : uint32_t {
    SUM_INT64 = 1001,
    SUM_FLOAT64 = 1002,
    COUNT = 1003,
    AVG_FLOAT64 = 1004,
    MIN_INT64 = 1005,
    MAX_INT64 = 1006,
};

// Use the parameter types from StateDefinitions
using SumParams = State::SumParams;
using CountParams = State::CountParams;
using AvgParams = State::AvgParams;
using OperationParams = State::OperationParams;

class OperationRegistry {
public:
    using AggregationOperationFactory = std::function<std::unique_ptr<Operation<State::AggregationState>>(const OperationParams&)>;
    
    static OperationRegistry& getInstance() {
        static OperationRegistry instance;
        return instance;
    }
    
    void registerAggregationOperation(uint32_t id, std::string name, AggregationOperationFactory factory) {
        aggregationFactories_[id] = std::move(factory);
        // Create default parameters based on operation type
        OperationParams defaultParams = (name == "sum" || name == "sum_int64") 
            ? OperationParams{rfl::Field<"sum", SumParams>{SumParams{"value", true}}}
            : OperationParams{rfl::Field<"count", CountParams>{CountParams{false}}};
        descriptors_.emplace(id, State::Operation{id, name, defaultParams});
    }
    
    template<typename OpType>
    void registerOperation(uint32_t id, std::string name) {
        registerAggregationOperation(id, name, [](const OperationParams& params) {
            // Extract the specific parameters from the TaggedUnion
            return params.visit([](const auto& specificParams) -> std::unique_ptr<Operation<State::AggregationState>> {
                using ParamType = std::decay_t<decltype(specificParams)>;
                if constexpr (std::is_same_v<ParamType, typename OpType::Params>) {
                    return std::make_unique<OpType>(specificParams);
                } else {
                    // Use default parameters if types don't match
                    typename OpType::Params defaultParams{};
                    return std::make_unique<OpType>(defaultParams);
                }
            });
        });
    }
    
    template<typename StateType>
    std::unique_ptr<Operation<StateType>> create(const State::Operation& op) {
        static_assert(std::is_same_v<StateType, State::AggregationState>, 
                     "Only AggregationState is supported in POC");
        
        auto it = aggregationFactories_.find(op.id);
        if (it != aggregationFactories_.end()) {
            return it->second(op.parameters);
        }
        throw std::runtime_error("Unknown operation: " + op.name);
    }
    
    State::Operation getDescriptor(uint32_t id) {
        auto it = descriptors_.find(id);
        if (it != descriptors_.end()) {
            return it->second;
        }
        throw std::runtime_error("Unknown operation ID: " + std::to_string(id));
    }
    
private:
    OperationRegistry() = default;
    std::map<uint32_t, AggregationOperationFactory> aggregationFactories_;
    std::map<uint32_t, State::Operation> descriptors_;
};

}