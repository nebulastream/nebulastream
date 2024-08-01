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
#include <Nautilus/Interface/Record.hpp>
namespace NES::Runtime::Execution::Aggregation {

SumAggregationFunction::SumAggregationFunction(const PhysicalTypePtr& inputType,
                                               const PhysicalTypePtr& resultType,
                                               const Expressions::ExpressionPtr& inputExpression,
                                               const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, resultType, inputExpression, resultFieldIdentifier) {}

void SumAggregationFunction::lift(Nautilus::MemRef state, Nautilus::Record& record) {
    // load memRef
    auto oldValue = AggregationFunction::loadFromMemRef(state, inputType);
    // add the value
    auto inputValue = inputExpression->execute(record);
    auto newValue = oldValue + inputValue;
    // put back to the memRef
    AggregationFunction::storeToMemRef(state, newValue, inputType);
}

void SumAggregationFunction::combine(Nautilus::MemRef state1, Nautilus::MemRef state2) {
    auto left = AggregationFunction::loadFromMemRef(state1, inputType);
    auto right = AggregationFunction::loadFromMemRef(state2, inputType);
    auto combinedSum = left + right;
    AggregationFunction::storeToMemRef(state1, combinedSum, inputType);
}

void SumAggregationFunction::lower(Nautilus::MemRef state, Nautilus::Record& resultRecord) {

    auto finalVal = AggregationFunction::loadFromMemRef(state, resultType);
    resultRecord.write(resultFieldIdentifier, finalVal);
}

void SumAggregationFunction::reset(Nautilus::MemRef state) {
    auto zero = createConstValue(0L, inputType);
    AggregationFunction::storeToMemRef(state, zero, inputType);
}
uint64_t SumAggregationFunction::getSize() { return inputType->size(); }

}// namespace NES::Runtime::Execution::Aggregation
