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
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Aggregation {

AvgAggregationFunction::AvgAggregationFunction(const PhysicalTypePtr& inputType,
                                               const PhysicalTypePtr& resultType,
                                               const Expressions::ExpressionPtr& inputExpression,
                                               const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, resultType, inputExpression, resultFieldIdentifier) {
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();

    // assuming that the count is always of Int64
    countType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
}

Nautilus::MemRefVal AvgAggregationFunction::loadSumMemRef(const Nautilus::MemRefVal& memRef) {
    const nautilus::val<int64_t> sizeOfCountInBytes = 8L;// the sum is stored after the count, and the count is of type uint64
    return (memRef + sizeOfCountInBytes);
}

void AvgAggregationFunction::lift(Nautilus::MemRefVal state, Nautilus::Record& record) {
    // load memRef
    auto oldCount = AggregationFunction::loadFromMemRef(state, countType);
    // calc the offset to get MemRefVal of the count value
    auto oldSumMemRef = loadSumMemRef(state);
    auto oldSum = AggregationFunction::loadFromMemRef(oldSumMemRef, inputType);

    // add the values
    auto value = inputExpression->execute(record);
    auto newSum = oldSum + value;
    auto newCount = oldCount + 1;
    // put updated values back to the memRef
    AggregationFunction::storeToMemRef(state, newCount, inputType);
    AggregationFunction::storeToMemRef(oldSumMemRef, newSum, inputType);
}

void AvgAggregationFunction::combine(Nautilus::MemRefVal state1, Nautilus::MemRefVal state2) {
    // load memRef1
    auto countLeft = AggregationFunction::loadFromMemRef(state1, countType);
    // calc the offset to get MemRefVal of the count value
    auto sumLeftMemRef = loadSumMemRef(state1);
    auto sumLeft = AggregationFunction::loadFromMemRef(sumLeftMemRef, inputType);
    // load memRef2
    auto countRight = AggregationFunction::loadFromMemRef(state2, countType);
    // calc the offset to get MemRefVal of the count value
    auto sumRightMemRef = loadSumMemRef(state2);
    auto sumRight = AggregationFunction::loadFromMemRef(sumRightMemRef, inputType);

    // add the values
    auto tmpSum = sumLeft + sumRight;
    auto tmpCount = countLeft + countRight;
    // put updated values back to the memRef
    AggregationFunction::storeToMemRef(state1, tmpCount, inputType);
    AggregationFunction::storeToMemRef(sumLeftMemRef, tmpSum, inputType);
}

void AvgAggregationFunction::lower(Nautilus::MemRefVal memRef, Nautilus::Record& resultRecord) {
    // load memRefs
    auto count = AggregationFunction::loadFromMemRef(memRef, countType);
    auto sumMemRef = loadSumMemRef(memRef);
    auto sum = AggregationFunction::loadFromMemRef(sumMemRef, inputType);
    // calc the average
    // TODO #3602: If inputType is an integer then the result is also an integer
    // (specifically UINT64 because that is the count type).
    // However, it should be a float.
    auto finalVal = sum / count;
    AggregationFunction::storeToMemRef(sumMemRef, finalVal, inputType);

    // write the average
    resultRecord.write(resultFieldIdentifier, finalVal);
}

void AvgAggregationFunction::reset(Nautilus::MemRefVal memRef) {
    auto zero = createConstValue(0L, inputType);
    auto sumMemRef = loadSumMemRef(memRef);

    AggregationFunction::storeToMemRef(memRef, zero, inputType);
    AggregationFunction::storeToMemRef(sumMemRef, zero, inputType);
}
uint64_t AvgAggregationFunction::getSize() {
    return inputType->size() + 8L;// the count is always uint64, hence always 8bytes
}

}// namespace NES::Runtime::Execution::Aggregation
