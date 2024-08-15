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

void MinAggregationFunction::storeMin(const Nautilus::ExecDataType& leftValue,
              const Nautilus::ExecDataType& rightValue,
              const Nautilus::MemRefVal& state,
              const PhysicalTypePtr& inputType) {
    // we have to do this, as otherwise we do not check if the val<> is true bti if there exists a pointer
    if ((leftValue > rightValue)->as<Nautilus::ExecDataBoolean>()->getRawValue()) {
        // store the rightValue
        AggregationFunction::storeToMemRef(state, rightValue, inputType);
    } else if ((leftValue < rightValue)->as<Nautilus::ExecDataBoolean>()->getRawValue())  {
        // store the leftValue
        AggregationFunction::storeToMemRef(state, leftValue, inputType);
    }
}

void MinAggregationFunction::lift(Nautilus::MemRefVal state, Nautilus::Record& inputRecord) {
    const auto oldValue = AggregationFunction::loadFromMemRef(state, inputType);
    const auto inputValue = inputExpression->execute(inputRecord);
    storeMin(inputValue, oldValue, state, inputType);
}

void MinAggregationFunction::combine(Nautilus::MemRefVal state1, Nautilus::MemRefVal state2) {
    const auto left = AggregationFunction::loadFromMemRef(state1, inputType);
    const auto right = AggregationFunction::loadFromMemRef(state2, inputType);
    storeMin(left, right, state1, inputType);
}

void MinAggregationFunction::lower(Nautilus::MemRefVal state, Nautilus::Record& resultRecord) {
    const auto finalVal = AggregationFunction::loadFromMemRef(state, resultType);
    resultRecord.write(resultFieldIdentifier, finalVal);
}

void MinAggregationFunction::reset(Nautilus::MemRefVal memRef) {
    const auto minVal = createMaxValue(inputType);
    AggregationFunction::storeToMemRef(memRef, minVal, inputType);
}
uint64_t MinAggregationFunction::getSize() { return inputType->size(); }

}// namespace NES::Runtime::Execution::Aggregation
