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

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>

namespace NES::Runtime::Execution::Aggregation {

AvgAggregationFunction::AvgAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

void AvgAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load memref
    auto oldSum = memref.load<Nautilus::Int64>();// TODO 3280 check the type
    // calc the offset to get Memref of the count value
    auto countMemref = (memref + (uint64_t) offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto oldCount = countMemref.load<Nautilus::Int64>();

    // add the values
    auto newSum = oldSum + value;
    auto newCount = oldCount + 1;
    // put updated values back to the memref
    memref.store(newSum);
    countMemref.store(newCount);
}

void AvgAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    // load memref1
    auto sumLeft = memref1.load<Nautilus::Int64>();
    // calc the offset to get Memref of the count value
    auto countLeftMemref = (memref1 + (uint64_t) offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto countLeft = countLeftMemref.load<Nautilus::Int64>();
    // load memref2
    auto sumRight = memref2.load<Nautilus::Int64>();
    // calc the offset to get Memref of the count value
    auto countRightMemref = (memref2 + (uint64_t) offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto countRight = countRightMemref.load<Nautilus::Int64>();

    // add the values
    auto tmpSum = sumLeft + sumRight;
    auto tmpCount = countLeft + countRight;
    // put updated values back to the memref
    memref1.store(tmpSum);
    countLeftMemref.store(tmpCount);
}

Nautilus::Value<> AvgAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    // load memrefs
    auto sum = memref.load<Nautilus::Int64>();// TODO 3280 check the type
    auto countMemref = (memref + (uint64_t) offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto count = countMemref.load<Nautilus::Int64>();
    // calc the average
    auto finalVal = sum / count;
    // return the average
    return finalVal;
}

void AvgAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = Nautilus::Value<Nautilus::Int64>((int64_t) 0);
    auto countMemref = (memref + (uint64_t) offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();

    memref.store(zero);
    countMemref.store(zero);
}

}// namespace NES::Runtime::Execution::Aggregation
