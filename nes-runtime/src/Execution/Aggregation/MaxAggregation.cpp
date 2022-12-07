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

#include <Execution/Aggregation/MaxAggregation.hpp>

namespace NES::Runtime::Execution::Aggregation {
MaxAggregationFunction::MaxAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

void MaxAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load
    auto oldValue = memref.load<Nautilus::Int64>();
    // compare
    auto newValue = (value > oldValue) ? value : oldValue;
    // store
    memref.store(newValue);
}

void MaxAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    auto left = memref1.load<Nautilus::Int64>();
    auto right = memref2.load<Nautilus::Int64>();
    auto tmp = std::max(left, right);
    memref1.store(tmp);
}

Nautilus::Value<> MaxAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    auto finalVal = memref.load<Nautilus::Int64>();// TODO 3280 check the type
    return memref.load<Nautilus::Int64>();
}

void MaxAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto maxVal = Nautilus::Value<Nautilus::Int64>((int64_t) std::numeric_limits<int64_t>::min());
    memref.store(maxVal);// TODO 3280 check the type
}
}// namespace NES::Runtime::Execution::Aggregation