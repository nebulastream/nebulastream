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

#include <Execution/Aggregation/MinAggregation.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Runtime::Execution::Aggregation {

MinAggregationFunction::MinAggregationFunction(const PhysicalTypePtr& inputType, const PhysicalTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

template<class T>
T min(T first, T second) {
    return first < second ? first : second;
}

void MinAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load
    auto oldValue = AggregationFunction::loadFromMemref(memref, inputType);
    // compare
    // TODO implement the function in nautilus if #3500 is fixed
    auto result = FunctionCall<>("min", min<int64_t>, value.as<Nautilus::Int64>(), oldValue);
    memref.store(result);
    auto isLessThan = Nautilus::LessThanOp(value, oldValue);

    if (isLessThan) {
        // store
        memref.store(value);
    }
}

void MinAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    auto left = memref1.load<Nautilus::Int64>();
    auto right = memref2.load<Nautilus::Int64>();
    auto result = FunctionCall<>("min", min<int64_t>, left, right);
    memref1.store(result);
    auto left = AggregationFunction::loadFromMemref(memref1, inputType);
    auto right = AggregationFunction::loadFromMemref(memref2, inputType);

    auto isLeftLessThanRight = Nautilus::LessThanOp(left, right);

    if (isLeftLessThanRight) {
        memref1.store(left);
    } else {
        memref1.store(right);
    }
}

Nautilus::Value<> MinAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    auto finalVal = AggregationFunction::loadFromMemref(memref, finalType);
    return finalVal;
}

void MinAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto minVal = createConstValue(std::numeric_limits<int64_t>::max(), inputType);

    memref.store(minVal);
}
uint64_t MinAggregationFunction::getSize() { return inputType->size(); }

}// namespace NES::Runtime::Execution::Aggregation