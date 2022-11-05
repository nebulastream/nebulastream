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
#include <Execution/Aggregation/DistinctAggregation.hpp>
#include <Execution/Aggregation/HyperLogLog.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <vector>

namespace NES::Runtime::Execution::Aggregation {

DistinctAggregationFunction::DistinctAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

hll::HyperLogLog  hlladd(Nautilus::Value<Nautilus::Int64> oldValue, Nautilus::Value<> newValue){
    hll::HyperLogLog oldHLL = static_cast<uint8_t>(oldValue);
    oldHLL.add(newValue->toString().c_str(),newValue->toString().length());
    return oldHLL;
}

void DistinctAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load memref
    auto oldValue = memref.load<Nautilus::Int64>();
    // add the value
    auto newValue = FunctionCall<>("hlladd",hlladd,oldValue, value);
    // put back to the memref
    memref.store(newValue);
}

void merge(const hll::HyperLogLog& other) { return hll::HyperLogLog::merge(other); }

void DistinctAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    hll::HyperLogLog left = static_cast<uint8_t>(memref1.load<Nautilus::Int64>();
    hll::HyperLogLog right = static_cast<uint8_t>(memref2.load<Nautilus::Int64>());

    auto tmp = Nautilus::FunctionCall<>("merge",merge, left.merge(right));
    memref1.store(tmp);
}

double estimate(const hll::HyperLogLog& other) { return hll::HyperLogLog::estimate(other); }

Nautilus::Value<> DistinctAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref){
    hll::HyperLogLog oldValue = static_cast<uint8_t>(memref.load<Nautilus::Int64>());
    auto erg = Nautilus::FunctionCall<>("estimate",estimate, oldValue.estimate());
    return erg;
}

}
