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

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/HyperLogLogDistinctCountApproximation.hpp>
#include <Execution/Aggregation/Util/HyperLogLog.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Runtime::Execution::Aggregation {

HyperLogLogDistinctCountApproximation::HyperLogLogDistinctCountApproximation(
    const PhysicalTypePtr& inputType,
    const PhysicalTypePtr& finalType,
    const Expressions::ExpressionPtr& inputExpression,
    const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier)
    : AggregationFunction(inputType, finalType, inputExpression, resultFieldIdentifier) {}

void hlladd(void* memrefPtr, uint64_t hash) {
    //get the HyperLogLog instance
    HyperLogLogDistinctCountApproximationValue* obj = static_cast<HyperLogLogDistinctCountApproximationValue*>(memrefPtr);
    // pass the hash value
    obj->hyperLogLog.add(hash);
}

void HyperLogLogDistinctCountApproximation::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Record& record) {
    // create instance of Murmur3HashFunction and hash the income value
    NES::Nautilus::Interface::MurMur3HashFunction murMur3HashFunction;
    auto inputValue = inputExpression->execute(record);
    NES::Nautilus::Interface::HashFunction::HashValue hashValueNautilus = murMur3HashFunction.calculate(inputValue);
    FunctionCall<>("hlladd", hlladd, memref, hashValueNautilus);
}

void mergeHLL(void* memref1Ptr, void* memref2Ptr) {
    HyperLogLogDistinctCountApproximationValue* obj1 = static_cast<HyperLogLogDistinctCountApproximationValue*>(memref1Ptr);
    HyperLogLogDistinctCountApproximationValue* obj2 = static_cast<HyperLogLogDistinctCountApproximationValue*>(memref2Ptr);
    obj1->hyperLogLog.merge(obj2->hyperLogLog);
}

void HyperLogLogDistinctCountApproximation::combine(Nautilus::Value<Nautilus::MemRef> memref1,
                                                    Nautilus::Value<Nautilus::MemRef> memref2) {
    FunctionCall<>("mergeHLL", mergeHLL, memref1, memref2);
}

double estimateHLL(void* memrefPtr) {
    HyperLogLogDistinctCountApproximationValue* obj = static_cast<HyperLogLogDistinctCountApproximationValue*>(memrefPtr);
    return obj->hyperLogLog.estimate();
}

void HyperLogLogDistinctCountApproximation::lower(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Record& resultRecord) {
    auto result = Nautilus::FunctionCall<>("estimateHLL", estimateHLL, memref);
    resultRecord.write(resultFieldIdentifier, result);
}

uint64_t HyperLogLogDistinctCountApproximation::getSize() { return inputType->size(); }

void clearHLL(void* memrefPtr) {
    HyperLogLogDistinctCountApproximationValue* obj = static_cast<HyperLogLogDistinctCountApproximationValue*>(memrefPtr);
    obj->hyperLogLog.clear();
}

void HyperLogLogDistinctCountApproximation::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    Nautilus::FunctionCall<>("clearHLL", clearHLL, memref);
}

}// namespace NES::Runtime::Execution::Aggregation
