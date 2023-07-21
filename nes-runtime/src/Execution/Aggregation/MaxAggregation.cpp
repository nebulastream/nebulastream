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
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <limits>

namespace NES::Runtime::Execution::Aggregation {
MaxAggregationFunction::MaxAggregationFunction(const PhysicalTypePtr& inputType,
                                               const PhysicalTypePtr& resultType,
                                               const Expressions::ExpressionPtr& inputExpression,
                                               const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, resultType, inputExpression, resultFieldIdentifier) {}

template<class T>
T max(T first, T second) {
    return first > second ? first : second;
}

template<class T>
Nautilus::Value<> callMaxTyped(Nautilus::Value<> leftValue, Nautilus::Value<> rightValue) {
    return FunctionCall<>("max", max<typename T::RawType>, leftValue.as<T>(), rightValue.as<T>());
}

Nautilus::Value<> callMax(const Nautilus::Value<>& leftValue, const Nautilus::Value<>& rightValue) {
    if (leftValue->isType<Nautilus::Int8>()) {
        return callMaxTyped<Nautilus::Int8>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::Int16>()) {
        return callMaxTyped<Nautilus::Int16>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::Int32>()) {
        return callMaxTyped<Nautilus::Int32>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::Int64>()) {
        return callMaxTyped<Nautilus::Int64>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::UInt8>()) {
        return callMaxTyped<Nautilus::UInt8>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::UInt16>()) {
        return callMaxTyped<Nautilus::UInt16>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::UInt32>()) {
        return callMaxTyped<Nautilus::UInt32>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::UInt64>()) {
        return callMaxTyped<Nautilus::UInt64>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::Float>()) {
        return callMaxTyped<Nautilus::Float>(leftValue, rightValue);
    } else if (leftValue->isType<Nautilus::Double>()) {
        return callMaxTyped<Nautilus::Double>(leftValue, rightValue);
    }
    throw Exceptions::NotImplementedException("Type not implemented");
}

void MaxAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& inputRecord) {
    // load
    auto oldValue = AggregationFunction::loadFromMemref(state, inputType);
    // compare
    // TODO implement the function in nautilus if #3500 is fixed
    auto inputValue = inputExpression->execute(inputRecord);
    auto result = callMax(inputValue, oldValue);
    // store
    state.store(result);
}

void MaxAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> state1, Nautilus::Value<Nautilus::MemRef> state2) {
    auto left = AggregationFunction::loadFromMemref(state1, inputType);
    auto right = AggregationFunction::loadFromMemref(state2, inputType);
    // TODO implement the function in nautilus if #3500 is fixed
    auto result = callMax(left, right);
    // store
    state1.store(result);
}

void MaxAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& resultRecord) {
    auto finalVal = AggregationFunction::loadFromMemref(state, resultType);
    resultRecord.write(resultFieldIdentifier, finalVal);
}
void MaxAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto maxVal = createMinValue(inputType);
    memref.store(maxVal);
}
uint64_t MaxAggregationFunction::getSize() { return inputType->size(); }
}// namespace NES::Runtime::Execution::Aggregation
