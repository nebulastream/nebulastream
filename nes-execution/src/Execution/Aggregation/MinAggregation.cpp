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

#include <Exceptions/NotImplementedException.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Nautilus/DataTypes/ExecutableDataType.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <limits>

namespace NES::Runtime::Execution::Aggregation {

MinAggregationFunction::MinAggregationFunction(const PhysicalTypePtr& inputType,
                                               const PhysicalTypePtr& resultType,
                                               const Expressions::ExpressionPtr& inputExpression,
                                               const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, resultType, inputExpression, resultFieldIdentifier) {}

Nautilus::ExecDataType callMin(const Nautilus::ExecDataType& leftValue, const Nautilus::ExecDataType& rightValue) {
    if (leftValue < rightValue) {
        return leftValue;
    } else {
        return rightValue;
    }
}

void MinAggregationFunction::lift(Nautilus::MemRef state, Nautilus::Record& inputRecord) {
    // load
    auto oldValue = AggregationFunction::loadFromMemRef(state, inputType);
    // compare
    // TODO implement the function in nautilus if #3500 is fixed
    auto inputValue = inputExpression->execute(inputRecord);
    auto result = callMin(inputValue, oldValue);
    AggregationFunction::storeToMemRef(state, result, inputType);
}

void MinAggregationFunction::combine(Nautilus::MemRef state1, Nautilus::MemRef state2) {
    auto left = AggregationFunction::loadFromMemRef(state1, inputType);
    auto right = AggregationFunction::loadFromMemRef(state2, inputType);
    // TODO implement the function in nautilus if #3500 is fixed
    auto result = callMin(left, right);
    AggregationFunction::storeToMemRef(state1, result, inputType);
}

void MinAggregationFunction::lower(Nautilus::MemRef state, Nautilus::Record& resultRecord) {
    auto finalVal = AggregationFunction::loadFromMemRef(state, resultType);
    resultRecord.write(resultFieldIdentifier, finalVal);
}

void MinAggregationFunction::reset(Nautilus::MemRef memRef) {
    auto minVal = createMaxValue(inputType);
    AggregationFunction::storeToMemRef(memRef, minVal, inputType);
}
uint64_t MinAggregationFunction::getSize() { return inputType->size(); }

}// namespace NES::Runtime::Execution::Aggregation
