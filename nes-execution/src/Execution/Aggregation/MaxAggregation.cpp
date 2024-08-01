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
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <limits>

namespace NES::Runtime::Execution::Aggregation {
MaxAggregationFunction::MaxAggregationFunction(const PhysicalTypePtr& inputType,
                                               const PhysicalTypePtr& resultType,
                                               const Expressions::ExpressionPtr& inputExpression,
                                               const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, resultType, inputExpression, resultFieldIdentifier) {}


Nautilus::ExecDataType callMax(const Nautilus::ExecDataType& leftValue, const Nautilus::ExecDataType& rightValue) {
    if (leftValue < rightValue) {
        return rightValue;
    } else {
        return leftValue;
    }
}

void MaxAggregationFunction::lift(Nautilus::MemRef state, Nautilus::Record& inputRecord) {
    // load
    auto oldValue = AggregationFunction::loadFromMemRef(state, inputType);
    // compare
    // TODO implement the function in nautilus if #3500 is fixed
    auto inputValue = inputExpression->execute(inputRecord);
    auto result = callMax(inputValue, oldValue);
    // store
    AggregationFunction::storeToMemRef(state, result, inputType);
}

void MaxAggregationFunction::combine(Nautilus::MemRef state1, Nautilus::MemRef state2) {
    auto left = AggregationFunction::loadFromMemRef(state1, inputType);
    auto right = AggregationFunction::loadFromMemRef(state2, inputType);
    // TODO implement the function in nautilus if #3500 is fixed
    auto result = callMax(left, right);
    // store
    AggregationFunction::storeToMemRef(state1, result, inputType);
}

void MaxAggregationFunction::lower(Nautilus::MemRef state, Nautilus::Record& resultRecord) {
    auto finalVal = AggregationFunction::loadFromMemRef(state, resultType);
    resultRecord.write(resultFieldIdentifier, finalVal);
}
void MaxAggregationFunction::reset(Nautilus::MemRef memRef) {
    auto maxVal = createMinValue(inputType);
    AggregationFunction::storeToMemRef(memRef, maxVal, inputType);
}
uint64_t MaxAggregationFunction::getSize() { return inputType->size(); }
}// namespace NES::Runtime::Execution::Aggregation
