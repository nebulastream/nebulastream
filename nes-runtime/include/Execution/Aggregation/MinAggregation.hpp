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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_MINAGGREGATION_HPP
#define NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_MINAGGREGATION_HPP

#include <Execution/Aggregation/AggregationFunction.hpp>

namespace NES::Runtime::Execution::Aggregation {
class MinAggregationFunction : public AggregationFunction {

  public:
    MinAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType);

    void lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) override;
    void combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) override;
    Nautilus::Value<> lower(Nautilus::Value<Nautilus::MemRef> memref) override;
    void reset(Nautilus::Value<Nautilus::MemRef> memref) override;
};
}// namespace NES::Runtime::Execution::Aggregation

#endif//NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_MINAGGREGATION_HPP
