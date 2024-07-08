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

#include <Common/DataTypes/Float.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/QuantileEstimationAggregation.hpp>
#include <Execution/Aggregation/Util/digestible.h>

namespace NES::Runtime::Execution::Aggregation {
QuantileEstimationAggregation::QuantileEstimationAggregation(const PhysicalTypePtr& inputType,
                                                             const PhysicalTypePtr& finalType,
                                                             const Expressions::ExpressionPtr& inputExpression,
                                                             const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, finalType, inputExpression, resultFieldIdentifier) {}

void insert(void* memrefPtr, float inputValue) {
    //get the Tdigest instance
    QuantileEstimationValue* obj = static_cast<QuantileEstimationValue*>(memrefPtr);
    // insert the new input
    obj->digest.insert(inputValue);
}

void QuantileEstimationAggregation::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Record& record) {
    auto inputValue = inputExpression->execute(record);
    if (inputValue->isType<Nautilus::Int8>()) {
        FunctionCall<>("insert", insert, memref, inputValue.as<Nautilus::Int8>());
    } else if (inputValue->isType<Nautilus::Int16>()) {
        FunctionCall<>("insert", insert, memref, inputValue.as<Nautilus::Int16>());
    } else if (inputValue->isType<Nautilus::Int32>()) {
        FunctionCall<>("insert", insert, memref, inputValue.as<Nautilus::Int32>());
    } else if (inputValue->isType<Nautilus::Int64>()) {
        FunctionCall<>("insert", insert, memref, inputValue.as<Nautilus::Int64>());
    } else if (inputValue->isType<Nautilus::UInt8>()) {
        FunctionCall<>("insert", insert, memref, inputValue.as<Nautilus::UInt8>());
    } else if (inputValue->isType<Nautilus::UInt16>()) {
        FunctionCall<>("insert", insert, memref, inputValue.as<Nautilus::UInt16>());
    } else if (inputValue->isType<Nautilus::UInt32>()) {
        FunctionCall<>("insert", insert, memref, inputValue.as<Nautilus::UInt32>());
    } else if (inputValue->isType<Nautilus::UInt64>()) {
        FunctionCall<>("insert", insert, memref, inputValue.as<Nautilus::UInt64>());
    } else if (inputValue->isType<Nautilus::Float>()) {
        FunctionCall<>("insert", insert, memref, inputValue.as<Nautilus::Float>());
    } else if (inputValue->isType<Nautilus::Double>()) {
        FunctionCall<>("insert", insert, memref, inputValue.as<Nautilus::Double>());
    } else {
        throw Exceptions::NotImplementedException("Type not implemented " + inputValue->getType()->toString());
    }
}

void mergeTDigest(void* memref1Ptr, void* memref2Ptr) {
    QuantileEstimationValue* obj1 = static_cast<QuantileEstimationValue*>(memref1Ptr);
    QuantileEstimationValue* obj2 = static_cast<QuantileEstimationValue*>(memref2Ptr);
    obj1->digest.insert(obj2->digest);
}

void QuantileEstimationAggregation::combine(Nautilus::Value<Nautilus::MemRef> memref1,
                                            Nautilus::Value<Nautilus::MemRef> memref2) {
    FunctionCall<>("mergeTDigest", mergeTDigest, memref1, memref2);
}

double estimateTDigest(void* memrefPtr) {
    QuantileEstimationValue* obj = static_cast<QuantileEstimationValue*>(memrefPtr);
    obj->digest.merge();// before querying the object a merge is required
    return obj->digest.quantile(50.0);
    //TODO: here we can add any % once fixed issue #3889, right now we derive the median
}

void QuantileEstimationAggregation::lower(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Record& resultRecord) {
    auto result = Nautilus::FunctionCall<>("estimateTDigest", estimateTDigest, memref);
    resultRecord.write(resultFieldIdentifier, result);
}

uint64_t QuantileEstimationAggregation::getSize() { return inputType->size(); }

void clearTDigest(void* memrefPtr) {
    QuantileEstimationValue* obj = static_cast<QuantileEstimationValue*>(memrefPtr);
    obj->digest.reset();
}

void QuantileEstimationAggregation::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    Nautilus::FunctionCall<>("clearTDigest", clearTDigest, memref);
}

}// namespace NES::Runtime::Execution::Aggregation
