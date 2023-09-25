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
#include <Nautilus/Interface/Record.hpp>
namespace NES::Runtime::Execution::Aggregation {

SumAggregationFunction::SumAggregationFunction(const PhysicalTypePtr& inputType,
                                               const PhysicalTypePtr& resultType,
                                               const Expressions::ExpressionPtr& inputExpression,
                                               const Nautilus::Record::RecordFieldIdentifier& inputFieldIdentifier,
                                               const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, resultType, inputExpression, inputFieldIdentifier, resultFieldIdentifier) {}

void SumAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& inputRecord) {
    // load memref
    auto oldValue = AggregationFunction::loadFromMemref(state, inputType);
    // add the value
    auto inputValue = inputExpression->execute(inputRecord);
    auto newValue = oldValue + inputValue;
    // put back to the memref
    state.store(newValue);
}

void SumAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> state1, Nautilus::Value<Nautilus::MemRef> state2) {
    auto left = AggregationFunction::loadFromMemref(state1, inputType);
    auto right = AggregationFunction::loadFromMemref(state2, inputType);
    auto combinedSum = left + right;
    state1.store(combinedSum);
}

void SumAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& resultRecord) {

    auto finalVal = AggregationFunction::loadFromMemref(state, resultType);
    resultRecord.write(resultFieldIdentifier, finalVal);
}

void SumAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> state) {
    auto zero = createConstValue(0L, inputType);
    state.store(zero);
}
uint64_t SumAggregationFunction::getSize() { return inputType->size(); }

}// namespace NES::Runtime::Execution::Aggregation
