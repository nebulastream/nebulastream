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

namespace NES::Runtime::Execution::Aggregation {

MinAggregationFunction::MinAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

void MinAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load
    auto oldValue = memref.load<Nautilus::Int64>();
    // compare
    auto newValue = (value < oldValue) ? value : oldValue;
    // store
    memref.store(newValue);
}

void MinAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    auto left = memref1.load<Nautilus::Int64>();
    auto right = memref2.load<Nautilus::Int64>();
    auto tmp = std::min(left, right);
    memref1.store(tmp);
}

Nautilus::Value<> MinAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    auto finalVal = memref.load<Nautilus::Int64>();// TODO 3280 check the type
    return memref.load<Nautilus::Int64>();
}

void MinAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto minVal = Nautilus::Value<Nautilus::Int64>((int64_t) std::numeric_limits<int64_t>::max());
    memref.store(minVal);// TODO 3280 check the type
}

}// namespace NES::Runtime::Execution::Aggregation