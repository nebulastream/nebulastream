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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZEDNONKEYEDPREAGGREGATION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZEDNONKEYEDPREAGGREGATION_HPP_

#include <Execution/Operators/Experimental/Vectorization/VectorizableOperator.hpp>

#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregation.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief
 */
class VectorizedNonKeyedPreAggregation : public VectorizableOperator {
public:
    /**
     * @brief Constructor.
     */
    VectorizedNonKeyedPreAggregation(
        std::shared_ptr<NonKeyedSlicePreAggregation> nonKeyedSlicePreAggregationOperator,
        std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider,
        std::vector<Nautilus::Record::RecordFieldIdentifier> projections = {}
    );

    void setup(ExecutionContext& ctx) const override;
    void open(ExecutionContext& ctx, RecordBuffer& rb) const override;
    void execute(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

private:
    std::shared_ptr<NonKeyedSlicePreAggregation> nonKeyedSlicePreAggregationOperator;
    std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider;
    std::vector<Nautilus::Record::RecordFieldIdentifier> projections;
};

} // namespace NES::Runtime::Execution::Operators

#endif // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZEDNONKEYEDPREAGGREGATION_HPP_
