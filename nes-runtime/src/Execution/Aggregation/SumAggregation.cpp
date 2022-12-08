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
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
namespace NES::Runtime::Execution::Aggregation {

SumAggregationFunction::SumAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

void SumAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load memref
    auto oldValue = memref.load<Nautilus::Int64>();// TODO 3250 check the type
    // add the value
    auto newValue = oldValue + value;
    // put back to the memref
    memref.store(newValue);
}

void SumAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memre2) {
    auto left = memref1.load<Nautilus::Int64>();
    auto right = memre2.load<Nautilus::Int64>();

    auto tmp = left + right;
    memref1.store(tmp);
}

Nautilus::Value<> SumAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    auto finalVal = memref.load<Nautilus::Int64>();// TODO 3250 check the type

    return finalVal;
}

void SumAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = Nautilus::Value<Nautilus::Int64>((int64_t) 0);
    memref.store(zero);
}

}// namespace NES::Runtime::Execution::Aggregation
