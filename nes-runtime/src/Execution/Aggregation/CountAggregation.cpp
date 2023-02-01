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
namespace NES::Runtime::Execution::Aggregation {

CountAggregationFunction::CountAggregationFunction(const PhysicalTypePtr& inputType, const PhysicalTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

void CountAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<>) {
    // load memref
    auto oldValue = AggregationFunction::loadFromMemref(memref, finalType);
    // add the value
    auto newValue = oldValue + 1UL;
    // put back to the memref
    memref.store(newValue);
}

void CountAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    auto left =AggregationFunction::loadFromMemref(memref1, finalType);
    auto right = AggregationFunction::loadFromMemref(memref2, finalType);

    auto tmp = left + right;
    memref1.store(tmp);
}

Nautilus::Value<> CountAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    auto finalVal = AggregationFunction::loadFromMemref(memref, finalType);
    return finalVal;
}

void CountAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = Nautilus::Value<Nautilus::UInt64>(0UL); // count always use UInt64
    memref.store(zero);
}
uint64_t CountAggregationFunction::getSize() { return sizeof(int64_t); }

}// namespace NES::Runtime::Execution::Aggregation
