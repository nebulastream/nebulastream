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
    auto oldCount = AggregationFunction::loadFromMemref(memref, countType);
    // calc the offset to get Memref of the count value
    auto oldSumMemref = (memref + (uint64_t) sizeOfCountInBytes).as<Nautilus::MemRef>();
    auto oldSum = AggregationFunction::loadFromMemref(oldSumMemref, inputType);

    // add the values
    auto newSum = oldSum + value;
    auto newCount = oldCount + 1;
    // put updated values back to the memref
    memref.store(newCount);
    oldSumMemref.store(newSum);
}

void AvgAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    // load memref1
    auto countLeft = AggregationFunction::loadFromMemref(memref1, countType);
    // calc the offset to get Memref of the count value
    auto sumLeftMemref = (memref1 + (uint64_t) sizeOfCountInBytes).as<Nautilus::MemRef>();
    auto sumLeft = AggregationFunction::loadFromMemref(sumLeftMemref, inputType);
    // load memref2
    auto countRight = AggregationFunction::loadFromMemref(memref2, countType);
    // calc the offset to get Memref of the count value
    auto sumRightMemref = (memref2 + (uint64_t) sizeOfCountInBytes).as<Nautilus::MemRef>();
    auto sumRight = AggregationFunction::loadFromMemref(sumRightMemref, inputType);

    // add the values
    auto tmpSum = sumLeft + sumRight;
    auto tmpCount = countLeft + countRight;
    // put updated values back to the memref
    memref1.store(tmpCount);
    sumLeftMemref.store(tmpSum);
}

Nautilus::Value<> AvgAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    // load memrefs
    auto count = AggregationFunction::loadFromMemref(memref, inputType); // load the count as an inputType to allow Division
    auto sumMemref = (memref + (uint64_t) sizeOfCountInBytes).as<Nautilus::MemRef>();
    auto sum = AggregationFunction::loadFromMemref(sumMemref, inputType);

    // calc the average
    // TODO 3280: Can we avoid this extra step of storing the result of DivOp?
    auto finalVal = DivOp(sum, count);
    sumMemref.store(finalVal);

    // return the average
    return loadFromMemref(sumMemref, finalType);
}

// TODO 3280 check the type when resetting
void AvgAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = Nautilus::Value<Nautilus::Int64>((int64_t) 0);
    auto sumMemref = (memref + (uint64_t) sizeOfCountInBytes).as<Nautilus::MemRef>();

    memref.store(zero);
    sumMemref.store(zero);
}
uint64_t AvgAggregationFunction::getSize() {
    // physically an aggregation value for an avg is 16 byte.
    return inputType->size() + sizeOfCountInBytes; // the count is always int64, hence always 8bytes
}

}// namespace NES::Runtime::Execution::Aggregation
