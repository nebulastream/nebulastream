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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_AGGREGATION_VariAGGREGATION_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_AGGREGATION_VariAGGREGATION_HPP_
#include <Execution/Aggregation/AggregationFunction.hpp>

namespace NES::Runtime::Execution::Aggregation {
class VariationAggregationFunction : public AggregationFunction {

  public:
    VariationAggregationFunction(const PhysicalTypePtr& inputType,
                           const PhysicalTypePtr& resultType,
                           const Expressions::ExpressionPtr& inputExpression,
                           const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier);

    void lift(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& inputRecord) override;
    void combine(Nautilus::Value<Nautilus::MemRef> state1, Nautilus::Value<Nautilus::MemRef> state2) override;
    void lower(Nautilus::Value<Nautilus::MemRef> state, Nautilus::Record& resultRecord) override;
    void reset(Nautilus::Value<Nautilus::MemRef> state) override;
    uint64_t getSize() override;

  private:
    PhysicalTypePtr countType;
    Nautilus::Value<Nautilus::MemRef> loadSumMemRef(const Nautilus::Value<Nautilus::MemRef>& memref);
};
}// namespace NES::Runtime::Execution::Aggregation

#endif// NES_EXECUTION_INCLUDE_EXECUTION_AGGREGATION_VariAGGREGATION_HPP_
