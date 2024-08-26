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

#include <Execution/Aggregation/CountAggregation.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution::Aggregation {

CountAggregationFunction::CountAggregationFunction(const PhysicalTypePtr& inputType,
                                                   const PhysicalTypePtr& resultType,
                                                   const Expressions::ExpressionPtr& inputExpression,
                                                   const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, resultType, inputExpression, resultFieldIdentifier) {}

void CountAggregationFunction::lift(Nautilus::MemRefVal& state, Nautilus::Record&) {
    // load memRef
    const auto oldValue = AggregationFunction::loadFromMemRef(state, resultType);
    // add the value
    const auto newValue = oldValue + createConstValue(1, resultType);
    // put back to the memRef
    AggregationFunction::storeToMemRef(state, newValue, resultType);
}

void CountAggregationFunction::combine(Nautilus::MemRefVal& state1, Nautilus::MemRefVal& state2) {
    const auto left = AggregationFunction::loadFromMemRef(state1, resultType);
    const auto right = AggregationFunction::loadFromMemRef(state2, resultType);

    const auto tmp = left + right;
    AggregationFunction::storeToMemRef(state1, tmp, inputType);
}

void CountAggregationFunction::lower(Nautilus::MemRefVal& state, Nautilus::Record& record) {
    const auto finalVal = AggregationFunction::loadFromMemRef(state, resultType);
    record.write(resultFieldIdentifier, finalVal);
}

void CountAggregationFunction::reset(Nautilus::MemRefVal& memRef) {
    const auto zero = createConstValue(0L, resultType);
    AggregationFunction::storeToMemRef(memRef, zero, resultType);
}
uint64_t CountAggregationFunction::getSize() { return sizeof(int64_t); }

}// namespace NES::Runtime::Execution::Aggregation
