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
#include <Nautilus/Interface/FunctionCall.hpp>
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

AvgAggregationFunction::AvgAggregationFunction(const PhysicalTypePtr& inputType,
                                               const PhysicalTypePtr& resultType,
                                               const Expressions::ExpressionPtr& inputExpression,
                                               const Expressions::ExpressionPtr& inputTsExpression,
                                               const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
        : AggregationFunction(inputType, resultType, inputExpression, inputTsExpression, resultFieldIdentifier) {
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();

    // assuming that the count is always of Int64
    countType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
}

template<class T>
T max(T first, T second) {
    return first > second ? first : second;
}

template<class T>
Nautilus::Value<> callMaxTyped(Nautilus::Value<> leftValue, Nautilus::Value<> rightValue) {
    return FunctionCall<>("max", max<typename T::RawType>, leftValue.as<T>(), rightValue.as<T>());
}

Nautilus::Value<Nautilus::MemRef> AvgAggregationFunction::loadSumMemRef(const Nautilus::Value<Nautilus::MemRef>& memref) {
    const static int64_t sizeOfCountInBytes = 8L;// the sum is stored after the count, and the count is of type uint64
    const static int64_t sizeOfTsInBytes = 8L;// the sum is stored after the count, and the count is of type uint64
    return (memref + sizeOfCountInBytes + sizeOfTsInBytes).as<Nautilus::MemRef>();
}

Nautilus::Value<Nautilus::MemRef> AvgAggregationFunction::loadTsMemRef(const Nautilus::Value<Nautilus::MemRef>& memref) {
    const static int64_t sizeOfCountInBytes = 8L;// the sum is stored after the count, and the count is of type uint64
    return (memref + sizeOfCountInBytes).as<Nautilus::MemRef>();
}

void AvgAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& record) {
    // load memref
    auto oldCount = AggregationFunction::loadFromMemref(state, countType);
    // calc the offset to get Memref of the count value
    auto oldSumMemref = loadSumMemRef(state);
    auto oldSum = AggregationFunction::loadFromMemref(oldSumMemref, inputType);

    auto oldTsMemRef = loadTsMemRef(state);
    auto oldTs = AggregationFunction::loadFromMemref(oldTsMemRef, inputType);
    auto inputTsValue = inputTsExpression->execute(record);
    auto newTs = callMaxTyped<Nautilus::UInt64>(oldTs, inputTsValue);

    // add the values
    auto value = inputExpression->execute(record);
    auto newSum = oldSum + value;
    auto newCount = oldCount + 1;
    // put updated values back to the memref
    state.store(newCount);
    oldSumMemref.store(newSum);
    oldTsMemRef.store(newTs);
}

void AvgAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> state1, Nautilus::Value<Nautilus::MemRef> state2) {
    // load memref1
    auto countLeft = AggregationFunction::loadFromMemref(state1, countType);
    // calc the offset to get Memref of the count value
    auto sumLeftMemref = loadSumMemRef(state1);
    auto sumLeft = AggregationFunction::loadFromMemref(sumLeftMemref, inputType);
    // load memref2
    auto countRight = AggregationFunction::loadFromMemref(state2, countType);
    // calc the offset to get Memref of the count value
    auto sumRightMemref = loadSumMemRef(state2);
    auto sumRight = AggregationFunction::loadFromMemref(sumRightMemref, inputType);

    // add the values
    auto tmpSum = sumLeft + sumRight;
    auto tmpCount = countLeft + countRight;
    // put updated values back to the memref
    state1.store(tmpCount);
    sumLeftMemref.store(tmpSum);
}

void AvgAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Record& resultRecord) {
    // load memrefs
    auto count = AggregationFunction::loadFromMemref(memref, countType);
    auto sumMemref = loadSumMemRef(memref);
    auto sum = AggregationFunction::loadFromMemref(sumMemref, inputType);
    // calc the average
    // TODO #3602: If inputType is an integer then the result is also an integer
    // (specifically UINT64 because that is the count type).
    // However, it should be a float.
    auto finalVal = sum / count;
    sumMemref.store(finalVal);

    // write the average
    resultRecord.write(resultFieldIdentifier, finalVal);
}

void AvgAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = createConstValue(0L, inputType);
    auto sumMemref = loadSumMemRef(memref);

    memref.store(zero);
    sumMemref.store(zero);
}
uint64_t AvgAggregationFunction::getSize() {
    return inputType->size() + 8L;// the count is always uint64, hence always 8bytes
}

}// namespace NES::Runtime::Execution::Aggregation
