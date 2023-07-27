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
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <limits>

namespace NES::Runtime::Execution::Aggregation {

MinAggregationFunction::MinAggregationFunction(const PhysicalTypePtr& inputType,
                                               const PhysicalTypePtr& resultType,
                                               const Expressions::ExpressionPtr& inputExpression,
                                               const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, resultType, inputExpression, resultFieldIdentifier) {}

template<class T>
T min(T first, T second) {
    return first < second ? first : second;
}

template<class T>
Nautilus::Value<> callMinTyped(Nautilus::Value<> leftValue, Nautilus::Value<> rightValue) {
    return FunctionCall<>("min", min<typename T::RawType>, leftValue.as<T>(), rightValue.as<T>());
}

Nautilus::Value<> callMin(const Nautilus::Value<>& leftValue, const Nautilus::Value<>& rightValue) {
    if (leftValue->isType<Nautilus::Int8>()) {
        return callMinTyped<Nautilus::Int8>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::Int16>()) {
        return callMinTyped<Nautilus::Int16>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::Int32>()) {
        return callMinTyped<Nautilus::Int32>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::Int64>()) {
        return callMinTyped<Nautilus::Int64>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::UInt8>()) {
        return callMinTyped<Nautilus::UInt8>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::UInt16>()) {
        return callMinTyped<Nautilus::UInt16>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::UInt32>()) {
        return callMinTyped<Nautilus::UInt32>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::UInt64>()) {
        return callMinTyped<Nautilus::UInt64>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::Float>()) {
        return callMinTyped<Nautilus::Float>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::Double>()) {
        return callMinTyped<Nautilus::Double>(leftValue, rightValue);
    }
    throw Exceptions::NotImplementedException("Type not implemented");
}

void MinAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& inputRecord) {
    // load
    auto oldValue = AggregationFunction::loadFromMemref(state, inputType);
    // compare
    // TODO implement the function in nautilus if #3500 is fixed
    auto inputValue = inputExpression->execute(inputRecord);
    auto result = callMin(inputValue, oldValue);
    state.store(result);
}

void MinAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> state1, Nautilus::Value<Nautilus::MemRef> state2) {
    auto left = AggregationFunction::loadFromMemref(state1, inputType);
    auto right = AggregationFunction::loadFromMemref(state2, inputType);
    // TODO implement the function in nautilus if #3500 is fixed
    auto result = callMin(left, right);
    state1.store(result);
}

void MinAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& resultRecord) {
    auto finalVal = AggregationFunction::loadFromMemref(state, resultType);
    resultRecord.write(resultFieldIdentifier, finalVal);
}

void MinAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto minVal = createMaxValue(inputType);
    memref.store(minVal);
}
uint64_t MinAggregationFunction::getSize() { return inputType->size(); }

}// namespace NES::Runtime::Execution::Aggregation