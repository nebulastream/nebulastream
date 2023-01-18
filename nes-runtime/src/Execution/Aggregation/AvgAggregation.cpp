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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>

namespace NES::Runtime::Execution::Aggregation {

AvgAggregationFunction::AvgAggregationFunction(const PhysicalTypePtr& inputType, const PhysicalTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();

    // assuming that the count is always of Int64
    countType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
}

void AvgAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load memref
    auto oldSum = AggregationFunction::loadFromMemref(memref, inputType);
    // calc the offset to get Memref of the count value
    auto countMemref = (memref + (uint64_t) offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto oldCount = AggregationFunction::loadFromMemref(countMemref, countType);

    // add the values
    auto newSum = oldSum + value;
    auto newCount = oldCount + 1;
    // put updated values back to the memref
    memref.store(newSum);
    countMemref.store(newCount);
}

void AvgAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    // load memref1
    auto sumLeft = AggregationFunction::loadFromMemref(memref1, inputType);
    // calc the offset to get Memref of the count value
    auto countLeftMemref = (memref1 + (uint64_t) offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto countLeft = AggregationFunction::loadFromMemref(countLeftMemref, countType);
    // load memref2
    auto sumRight = AggregationFunction::loadFromMemref(memref2, inputType);
    // calc the offset to get Memref of the count value
    auto countRightMemref = (memref2 + (uint64_t) offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto countRight = AggregationFunction::loadFromMemref(countRightMemref, countType);

    // add the values
    auto tmpSum = sumLeft + sumRight;
    auto tmpCount = countLeft + countRight;
    // put updated values back to the memref
    memref1.store(tmpSum);
    countLeftMemref.store(tmpCount);
}

Nautilus::Value<> AvgAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    // load memrefs
    auto sum = AggregationFunction::loadFromMemref(memref, inputType);
    auto countMemref = (memref + (uint64_t) offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto count = AggregationFunction::loadFromMemref(countMemref, countType);

    // calc the average
    // TODO 3280: where do we set the final type?
    auto finalVal = DivOp(sum, count);

    // return the average
    return finalVal;
}

void AvgAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = Nautilus::Value<Nautilus::Int64>((int64_t) 0);
    auto countMemref = (memref + (uint64_t) offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();

    memref.store(zero);
    countMemref.store(zero);
}
uint64_t AvgAggregationFunction::getSize() {
    // physically an aggregation value for an avg is 16 byte.
    return sizeof(AvgAggregationValue);
}

}// namespace NES::Runtime::Execution::Aggregation
