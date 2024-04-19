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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZEDBATCHAGGREGATION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZEDBATCHAGGREGATION_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Experimental/Vectorization/VectorizableOperator.hpp>
#include <Execution/Aggregation/AggregationFunction.hpp>

namespace NES::Runtime::Execution::Operators {
/**
* @brief The vectorized map operator that applies the kernel programming model to a single-record map operator.
*/
class VectorizedBatchAggregation : public VectorizableOperator {
public:
    explicit VectorizedBatchAggregation(std::shared_ptr<BatchAggregation> batchAggregation,
                           std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider);

    void execute(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

private:
    std::shared_ptr<BatchAggregation> batchAggregation;
    std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider;
};


} // namespace NES::Runtime::Execution::Operators

#endif//NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZEDBATCHAGGREGATION_HPP_
