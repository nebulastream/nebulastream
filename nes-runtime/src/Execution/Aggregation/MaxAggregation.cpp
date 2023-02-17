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

#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Runtime::Execution::Aggregation {
MaxAggregationFunction::MaxAggregationFunction(const PhysicalTypePtr& inputType, const PhysicalTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

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
    NES_NOT_IMPLEMENTED();
}

void MaxAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load
    auto oldValue = AggregationFunction::loadFromMemref(memref, inputType);
    // compare
    // TODO implement the function in nautilus if #3500 is fixed
    auto result = callMax(oldValue, value);
    // store
    memref.store(result);
}

void MaxAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    auto left = AggregationFunction::loadFromMemref(memref1, inputType);
    auto right = AggregationFunction::loadFromMemref(memref2, inputType);
    // TODO implement the function in nautilus if #3500 is fixed
    auto result = callMax(left, right);
    // store
    memref1.store(result);
}

Nautilus::Value<> MaxAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    auto finalVal = AggregationFunction::loadFromMemref(memref, finalType);
    return finalVal;
}
void MaxAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto maxVal = createConstValue(std::numeric_limits<int64_t>::min(), inputType);

    memref.store(maxVal);
}
uint64_t MaxAggregationFunction::getSize() { return inputType->size(); }
}// namespace NES::Runtime::Execution::Aggregation