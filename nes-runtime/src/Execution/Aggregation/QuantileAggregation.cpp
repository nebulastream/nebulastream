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
#include <Execution/Aggregation/QuantileAggregation.hpp>
#include <Execution/Aggregation/tdigest.h>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>

namespace NES::Runtime::Execution::Aggregation {
QuantileAggregationFunction::QuantileAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

void insert(digestible::tdigest input, Nautilus::Value<> value) {
    return digestible::tdigest::tdinsert(input,value) ;
}

void QuantileAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value){
    // load memref
    auto oldValue = memref.load<Nautilus::Int64>();
    // add the value
    auto newValue = FunctionCall<>("insert",insert, oldValue, value);
    // put back to the memref
    memref.store(newValue);
}

void merge(digestible::tdigest input) {
    return digestible::tdigest::tdmerge(input);
}

void QuantileAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    auto left = memref1.load<Nautilus::Int64>();
    auto right = memref2.load<Nautilus::Int64>();

    auto tmp = FunctionCall<>("merge",merge, left,right);
    memref1.store(tmp);
}

void calculate(digestible::tdigest input, int64_t quantile){
    digestible::tdigest::tdquantile(input,quantile);
}

Nautilus::Value<> QuantileAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    digestible::tdigest oldValue = memref.load<Nautilus::Int64>();
    double erg = FunctionCall<>("calculate",calculate, oldValue);
    return erg;

}
}