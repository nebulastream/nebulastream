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
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution::Aggregation {

CountAggregationFunction::CountAggregationFunction(const PhysicalTypePtr& inputType,
                                                   const PhysicalTypePtr& resultType,
                                                   const Expressions::ExpressionPtr& inputExpression,
                                                   const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, resultType, inputExpression, resultFieldIdentifier) {}

void CountAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record&) {
    // load memref
    auto oldValue = AggregationFunction::loadFromMemref(state, resultType);
    // add the value
    auto newValue = oldValue + 1_u64;
    // put back to the memref
    state.store(newValue);
}

void CountAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> state1, Nautilus::Value<Nautilus::MemRef> state2) {
    auto left = AggregationFunction::loadFromMemref(state1, resultType);
    auto right = AggregationFunction::loadFromMemref(state2, resultType);

    auto tmp = left + right;
    state1.store(tmp);
}

void CountAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& record) {
    auto finalVal = AggregationFunction::loadFromMemref(state, resultType);
    record.write(resultFieldIdentifier, finalVal);
}

void CountAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = Nautilus::Value<Nautilus::UInt64>(0_u64);// count always use UInt64
    memref.store(zero);
}
uint64_t CountAggregationFunction::getSize() { return sizeof(int64_t); }

}// namespace NES::Runtime::Execution::Aggregation
