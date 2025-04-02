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

#include <Exceptions/NotImplementedException.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/VariationAggregation.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Aggregation {

//---------------------------------------------------------------------
// Helper functions (implemented from scratch) to compute the minimum
// and maximum of two values in a type‐aware manner.
// These functions assume that the two input values are of the same type.
//---------------------------------------------------------------------
namespace {

Nautilus::Value<> computeMin(const Nautilus::Value<>& leftValue, const Nautilus::Value<>& rightValue) {
    if (leftValue->isType<Nautilus::Int8>()) {
        auto l = leftValue.as<Nautilus::Int8>();
        auto r = rightValue.as<Nautilus::Int8>();
        return (l < r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::Int16>()) {
        auto l = leftValue.as<Nautilus::Int16>();
        auto r = rightValue.as<Nautilus::Int16>();
        return (l < r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::Int32>()) {
        auto l = leftValue.as<Nautilus::Int32>();
        auto r = rightValue.as<Nautilus::Int32>();
        return (l < r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::Int64>()) {
        auto l = leftValue.as<Nautilus::Int64>();
        auto r = rightValue.as<Nautilus::Int64>();
        return (l < r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::UInt8>()) {
        auto l = leftValue.as<Nautilus::UInt8>();
        auto r = rightValue.as<Nautilus::UInt8>();
        return (l < r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::UInt16>()) {
        auto l = leftValue.as<Nautilus::UInt16>();
        auto r = rightValue.as<Nautilus::UInt16>();
        return (l < r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::UInt32>()) {
        auto l = leftValue.as<Nautilus::UInt32>();
        auto r = rightValue.as<Nautilus::UInt32>();
        return (l < r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::UInt64>()) {
        auto l = leftValue.as<Nautilus::UInt64>();
        auto r = rightValue.as<Nautilus::UInt64>();
        return (l < r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::Float>()) {
        auto l = leftValue.as<Nautilus::Float>();
        auto r = rightValue.as<Nautilus::Float>();
        return (l < r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::Double>()) {
        auto l = leftValue.as<Nautilus::Double>();
        auto r = rightValue.as<Nautilus::Double>();
        return (l < r) ? leftValue : rightValue;
    }
    throw Exceptions::NotImplementedException("Type not implemented in computeMin");
}

Nautilus::Value<> computeMax(const Nautilus::Value<>& leftValue, const Nautilus::Value<>& rightValue) {
    if (leftValue->isType<Nautilus::Int8>()) {
        auto l = leftValue.as<Nautilus::Int8>();
        auto r = rightValue.as<Nautilus::Int8>();
        return (l > r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::Int16>()) {
        auto l = leftValue.as<Nautilus::Int16>();
        auto r = rightValue.as<Nautilus::Int16>();
        return (l > r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::Int32>()) {
        auto l = leftValue.as<Nautilus::Int32>();
        auto r = rightValue.as<Nautilus::Int32>();
        return (l > r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::Int64>()) {
        auto l = leftValue.as<Nautilus::Int64>();
        auto r = rightValue.as<Nautilus::Int64>();
        return (l > r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::UInt8>()) {
        auto l = leftValue.as<Nautilus::UInt8>();
        auto r = rightValue.as<Nautilus::UInt8>();
        return (l > r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::UInt16>()) {
        auto l = leftValue.as<Nautilus::UInt16>();
        auto r = rightValue.as<Nautilus::UInt16>();
        return (l > r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::UInt32>()) {
        auto l = leftValue.as<Nautilus::UInt32>();
        auto r = rightValue.as<Nautilus::UInt32>();
        return (l > r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::UInt64>()) {
        auto l = leftValue.as<Nautilus::UInt64>();
        auto r = rightValue.as<Nautilus::UInt64>();
        return (l > r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::Float>()) {
        auto l = leftValue.as<Nautilus::Float>();
        auto r = rightValue.as<Nautilus::Float>();
        return (l > r) ? leftValue : rightValue;
    } else if (leftValue->isType<Nautilus::Double>()) {
        auto l = leftValue.as<Nautilus::Double>();
        auto r = rightValue.as<Nautilus::Double>();
        return (l > r) ? leftValue : rightValue;
    }
    throw Exceptions::NotImplementedException("Type not implemented in computeMax");
}

} // end anonymous namespace

//---------------------------------------------------------------------
// VariationAggregationFunction implementation (min‑max range variant)
//
// The state is organized as two consecutive values:
//   - Offset 0: current minimum value.
//   - Offset inputType->size(): current maximum value.
//---------------------------------------------------------------------

Nautilus::Value<Nautilus::MemRef> VariationAggregationFunction::loadSumMemRef(const Nautilus::Value<Nautilus::MemRef>& memref) {
    // Return pointer to the second slot (i.e. the maximum value)
    return (memref + inputType->size()).as<Nautilus::MemRef>();
}

void VariationAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& record) {
    // Load current min (first slot) and max (second slot) from the state.
    auto currentMin = AggregationFunction::loadFromMemref(state, inputType);
    auto currentMax = AggregationFunction::loadFromMemref(loadSumMemRef(state), inputType);
    
    // Execute the input expression to obtain the new value.
    auto value = inputExpression->execute(record);
    
    // Compute new min and max using our own functions.
    auto newMin = computeMin(value, currentMin);
    auto newMax = computeMax(value, currentMax);
    
    // Store the updated values back to the state.
    state.store(newMin);
    loadSumMemRef(state).store(newMax);
}

void VariationAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> state1,
                                             Nautilus::Value<Nautilus::MemRef> state2) {
    // Load min and max from both states.
    auto leftMin  = AggregationFunction::loadFromMemref(state1, inputType);
    auto leftMax  = AggregationFunction::loadFromMemref(loadSumMemRef(state1), inputType);
    auto rightMin = AggregationFunction::loadFromMemref(state2, inputType);
    auto rightMax = AggregationFunction::loadFromMemref(loadSumMemRef(state2), inputType);
    
    // Combine the states by taking the minimum of the mins and the maximum of the maxes.
    auto newMin = computeMin(leftMin, rightMin);
    auto newMax = computeMax(leftMax, rightMax);
    
    // Write the combined state back into state1.
    state1.store(newMin);
    loadSumMemRef(state1).store(newMax);
}

void VariationAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Record& resultRecord) {
    // Load the final min and max from the state.
    auto minValue = AggregationFunction::loadFromMemref(memref, inputType);
    auto maxValue = AggregationFunction::loadFromMemref(loadSumMemRef(memref), inputType);
    
    // Compute the range as (max - min).
    auto finalVal = maxValue - minValue;
    
    // Write the final range to the result record.
    resultRecord.write(resultFieldIdentifier, finalVal);
}

void VariationAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    // Reset the state:
    //   - For the minimum, use the maximum possible value.
    //   - For the maximum, use the minimum possible value.
    auto minInitial = createMaxValue(inputType);
    auto maxInitial = createMinValue(inputType);
    memref.store(minInitial);
    loadSumMemRef(memref).store(maxInitial);
}

uint64_t VariationAggregationFunction::getSize() {
    // The state now contains two values, each of size inputType->size().
    return inputType->size() * 2;
}

} // namespace NES::Runtime::Execution::Aggregation